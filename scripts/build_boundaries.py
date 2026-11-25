"""Download Census cartographic boundary files and extract target counties.

Source: US Census Bureau, Cartographic Boundary Files, Counties (500k), 2022.
URL:    https://www2.census.gov/geo/tiger/GENZ2022/shp/cb_2022_us_county_500k.zip
License: Public domain (U.S. Government work); no attribution required by Census.

Extracts (California STATEFP=06):
  METR-LA:   Los Angeles County   COUNTYFP 037 (FIPS 06037)
  PEMS-BAY:  Alameda 001, Contra Costa 013, Marin 041, Napa 055,
             San Francisco 075, San Mateo 081, Santa Clara 085,
             Solano 095, Sonoma 097
Writes per-county GeoJSON + a merged per-region MultiPolygon GeoJSON to
datasets/boundaries/. Each region file carries its source/version metadata.
"""

from __future__ import annotations

import json
import zipfile
from pathlib import Path

import httpx
import shapefile  # pyshp

PROJECT_ROOT = Path(__file__).resolve().parent.parent
BOUNDARY_DIR = PROJECT_ROOT / "datasets" / "boundaries"
SOURCE_ZIP = BOUNDARY_DIR / "cb_2022_us_county_500k.zip"
CENSUS_URL = (
    "https://www2.census.gov/geo/tiger/GENZ2022/shp/cb_2022_us_county_500k.zip"
)

# COUNTYFP -> (region, display_name)
TARGET_COUNTIES = {
    "037": ("metr-la", "Los Angeles County"),
    "001": ("pems-bay", "Alameda County"),
    "013": ("pems-bay", "Contra Costa County"),
    "041": ("pems-bay", "Marin County"),
    "055": ("pems-bay", "Napa County"),
    "075": ("pems-bay", "San Francisco County"),
    "081": ("pems-bay", "San Mateo County"),
    "085": ("pems-bay", "Santa Clara County"),
    "095": ("pems-bay", "Solano County"),
    "097": ("pems-bay", "Sonoma County"),
}


def ring_area(ring) -> float:
    """Signed area of a ring [[lon,lat], ...] via shoelace formula."""
    area = 0.0
    for i in range(len(ring)):
        x1, y1 = ring[i]
        x2, y2 = ring[(i + 1) % len(ring)]
        area += x1 * y2 - x2 * y1
    return area / 2.0


def point_in_ring(x: float, y: float, ring) -> bool:
    """Ray-casting test. x=lon, y=lat."""
    inside = False
    n = len(ring)
    j = n - 1
    for i in range(n):
        xi, yi = ring[i]
        xj, yj = ring[j]
        if (yi > y) != (yj > y):
            xinters = (xj - xi) * (y - yi) / (yj - yi) + xi
            if x < xinters:
                inside = not inside
        j = i
    return inside


def rings_to_polygons(rings) -> list:
    """Convert a flat list of rings into [polygon, ...] where each polygon is
    [exterior_ring, hole1, hole2, ...]. Rings are classified as exterior or
    hole by area-ordered containment nesting."""
    infos = [(ring, abs(ring_area(ring))) for ring in rings]
    infos.sort(key=lambda r: -r[1])  # largest first
    exteriors = []  # list of [exterior_ring, [holes], abs_area]
    for ring, area in infos:
        # Find the smallest already-accepted exterior that contains this ring
        # (and where the ring is not inside one of that exterior's holes).
        container = None
        for ext, holes, ext_area in exteriors:
            px, py = ring[0]
            if point_in_ring(px, py, ext) and not any(
                point_in_ring(px, py, h) for h in holes
            ):
                if container is None or ext_area < container[2]:
                    container = (ext, holes, ext_area)
        if container is None:
            exteriors.append([ring, [], area])
        else:
            container[1].append(ring)
    return [[ext, holes] for ext, holes, _ in exteriors]


def shape_to_geojson_geometry(shape) -> dict:
    """Convert a pyshp Polygon shape (possibly multi-part) to a valid GeoJSON
    Polygon or MultiPolygon geometry with correct exterior/hole nesting."""
    parts = list(shape.parts)
    points = list(shape.points)
    if len(parts) == 1:
        rings = [points]
    else:
        rings = [
            points[parts[i] : (parts[i + 1] if i + 1 < len(parts) else len(points))]
            for i in range(len(parts))
        ]
    polygons = rings_to_polygons(rings)
    if len(polygons) == 1:
        ext, holes = polygons[0]
        return {"type": "Polygon", "coordinates": [ext] + holes}
    return {
        "type": "MultiPolygon",
        "coordinates": [[ext] + holes for ext, holes in polygons],
    }


def download() -> Path:
    BOUNDARY_DIR.mkdir(parents=True, exist_ok=True)
    if SOURCE_ZIP.exists():
        print(f"Using existing {SOURCE_ZIP}")
        return SOURCE_ZIP
    print(f"Downloading {CENSUS_URL}")
    with httpx.stream("GET", CENSUS_URL, timeout=120) as resp:
        resp.raise_for_status()
        with SOURCE_ZIP.open("wb") as f:
            for chunk in resp.iter_bytes():
                f.write(chunk)
    print(f"Saved {SOURCE_ZIP} ({SOURCE_ZIP.stat().st_size / 1024 / 1024:.1f} MB)")
    return SOURCE_ZIP


def main() -> None:
    zip_path = download()
    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(BOUNDARY_DIR)

    shp_path = BOUNDARY_DIR / "cb_2022_us_county_500k.shp"
    reader = shapefile.Reader(str(shp_path))
    fields = reader.fields[1:]
    field_names = [f[0] for f in fields]

    counties: dict[str, dict] = {}
    for sr in reader.shapeRecords():
        rec = dict(zip(field_names, sr.record))
        statefp = str(rec.get("STATEFP", "")).strip()
        countyfp = str(rec.get("COUNTYFP", "")).strip()
        if statefp != "06" or countyfp not in TARGET_COUNTIES:
            continue
        region, name = TARGET_COUNTIES[countyfp]
        geom = shape_to_geojson_geometry(sr.shape)
        counties[countyfp] = {
            "fips": f"{statefp}{countyfp}",
            "name": name,
            "region": region,
            "geometry": geom,
        }

    print(f"Extracted {len(counties)} counties:")
    for fips, c in sorted(counties.items()):
        print(f"  {fips} {c['name']} -> {c['region']}")

    # Per-county GeoJSON files.
    for fips, c in counties.items():
        feature = {
            "type": "Feature",
            "properties": {"fips": c["fips"], "name": c["name"], "region": c["region"]},
            "geometry": c["geometry"],
        }
        out = BOUNDARY_DIR / f"county_{c['fips']}.geojson"
        out.write_text(json.dumps(feature, indent=2), encoding="utf-8")
        print(f"Wrote {out}")

    # Merged per-region MultiPolygon GeoJSON.
    for region in ("metr-la", "pems-bay"):
        region_counties = [c for c in counties.values() if c["region"] == region]
        all_polygons: list = []
        for c in region_counties:
            geom = c["geometry"]
            if geom["type"] == "Polygon":
                all_polygons.append(geom["coordinates"])
            else:
                all_polygons.extend(geom["coordinates"])
        meta = {
            "region": region,
            "display_name": {
                "metr-la": "Los Angeles County",
                "pems-bay": "San Francisco Bay Area (9 counties)",
            }[region],
            "county_count": len(region_counties),
            "source": "US Census Bureau Cartographic Boundary Files, Counties, 2022 (500k)",
            "source_url": CENSUS_URL,
            "source_year": 2022,
            "scale": "1:500,000 (cartographic/generalized)",
        }
        out = BOUNDARY_DIR / f"region_{region}.geojson"
        payload = {
            "type": "FeatureCollection",
            "properties": meta,
            "features": [
                {
                    "type": "Feature",
                    "properties": {"fips": c["fips"], "name": c["name"]},
                    "geometry": c["geometry"],
                }
                for c in region_counties
            ],
            "_geometry": {"type": "MultiPolygon", "coordinates": all_polygons},
        }
        out.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        print(f"Wrote {out} ({len(region_counties)} counties)")

    # Remove intermediate shapefile components; keep geojson + source zip.
    for ext in (
        ".shp", ".shx", ".dbf", ".prj", ".cpg", ".sbn", ".sbx",
        ".shp.ea.iso.xml", ".shp.iso.xml",
    ):
        p = BOUNDARY_DIR / (shp_path.stem + ext)
        if p.exists():
            p.unlink()
    print("Done.")


if __name__ == "__main__":
    main()
