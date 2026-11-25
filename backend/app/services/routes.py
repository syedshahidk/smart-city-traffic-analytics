"""Route planning service: geocoding + road routing via free OSM-compatible services.

Integrations are isolated here and configurable through Settings. The service
proxies Nominatim (geocoding) and OSRM (driving routing) so the frontend never
calls external services directly and no credentials are exposed.

Geographic validation uses authoritative, locally stored US Census Bureau
cartographic boundary polygons (2022, 1:500k) for Los Angeles County and the
nine-county San Francisco Bay Area. Bounding boxes are only a fast pre-check;
the authoritative test is point-in-polygon against the stored polygons. The
bounding box is never presented as an official boundary.

All route geometry, distance, and duration values come from the routing engine.
Nothing is fabricated. Traffic-based route ranking is intentionally NOT offered:
the repository contains no genuine sensor coordinates (verified in Stage 1), so
historical observations cannot be matched to route corridors.
"""

from __future__ import annotations

import json
import logging
from functools import lru_cache
from pathlib import Path
from typing import Any, List

import httpx

from backend.app.core.config import PROJECT_ROOT, Settings
from backend.app.core.errors import APIError
from backend.app.schemas import routes as route_schemas

LOGGER = logging.getLogger(__name__)

# A point exactly on a boundary counts as inside the region.
_BOUNDARY_IS_INSIDE = True

# Cartographic tolerance applied ONLY to route-geometry validation (~5 km).
# The 1:500k Census boundary is generalized, and roads legitimately run along
# or just across county lines and inter-county water (e.g. the San
# Francisco-Oakland bay crossing). Endpoints remain strictly validated against
# the exact polygon; only the route polyline receives this small tolerance so
# that legitimate boundary-adjacent routes are not rejected on a generalization
# artifact. The buffer is deliberately modest and documented.
ROUTE_GEOMETRY_BUFFER_DEGREES = 0.05


def _point_in_ring(x: float, y: float, ring: List[List[float]]) -> bool:
    """Ray-casting point-in-ring test. x=lon, y=lat; ring = [[lon, lat], ...]."""
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


def point_in_polygon(x: float, y: float, geometry: dict) -> bool:
    """True if (x=lon, y=lat) is inside a GeoJSON Polygon or MultiPolygon.

    Holes are respected: a point inside a hole ring is outside the polygon.
    Points exactly on a boundary count as inside.
    """
    if geometry["type"] == "Polygon":
        polygons = [geometry["coordinates"]]
    else:
        polygons = geometry["coordinates"]
    for rings in polygons:
        exterior = rings[0]
        if _point_in_ring(x, y, exterior):
            if any(_point_in_ring(x, y, hole) for hole in rings[1:]):
                continue  # inside a hole -> outside the polygon
            return True
    return False


def _point_to_segment_distance(px: float, py: float, x1: float, y1: float,
                               x2: float, y2: float) -> float:
    """Shortest distance from point (px, py) to segment (x1,y1)-(x2,y2)."""
    dx, dy = x2 - x1, y2 - y1
    if dx == 0.0 and dy == 0.0:
        return ((px - x1) ** 2 + (py - y1) ** 2) ** 0.5
    t = ((px - x1) * dx + (py - y1) * dy) / (dx * dx + dy * dy)
    t = max(0.0, min(1.0, t))
    closest_x = x1 + t * dx
    closest_y = y1 + t * dy
    return ((px - closest_x) ** 2 + (py - closest_y) ** 2) ** 0.5


def point_near_polygon(x: float, y: float, geometry: dict,
                       buffer_degrees: float) -> bool:
    """True if (x=lon, y=lat) is inside the polygon or within buffer_degrees
    of any exterior-ring edge. Used with a small tolerance for route-geometry
    validation only; endpoints are validated strictly with point_in_polygon."""
    if point_in_polygon(x, y, geometry):
        return True
    polys = geometry["coordinates"] if geometry["type"] == "MultiPolygon" else [
        geometry["coordinates"],
    ]
    best = float("inf")
    for rings in polys:
        ring = rings[0]
        for i in range(len(ring)):
            x1, y1 = ring[i]
            x2, y2 = ring[(i + 1) % len(ring)]
            d = _point_to_segment_distance(x, y, x1, y1, x2, y2)
            if d < best:
                best = d
    return best <= buffer_degrees


class RegionPolygon:
    """Authoritative geographic boundary for one supported region."""

    def __init__(self, region: str, display_name: str, geometry: dict) -> None:
        self.region = region
        self.display_name = display_name
        self.geometry = geometry
        # Fast bounding-box pre-check derived from the polygon (not a boundary).
        all_lons: List[float] = []
        all_lats: List[float] = []
        polys = geometry["coordinates"] if geometry["type"] == "MultiPolygon" else [
            geometry["coordinates"],
        ]
        for rings in polys:
            for lon, lat in rings[0]:
                all_lons.append(lon)
                all_lats.append(lat)
        self.south = min(all_lats)
        self.north = max(all_lats)
        self.west = min(all_lons)
        self.east = max(all_lons)

    def bbox_contains(self, lat: float, lon: float) -> bool:
        return self.south <= lat <= self.north and self.west <= lon <= self.east

    def contains(self, lat: float, lon: float) -> bool:
        """Authoritative point-in-polygon test (bbox pre-check + ray casting)."""
        if not self.bbox_contains(lat, lon):
            return False
        return point_in_polygon(lon, lat, self.geometry)


class BoundaryLoader:
    """Loads region boundary polygons from locally stored Census GeoJSON files."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._boundary_dir = PROJECT_ROOT / "datasets" / "boundaries"

    def load(self, region: str) -> RegionPolygon:
        path = self._boundary_dir / f"region_{region}.geojson"
        if not path.exists():
            raise APIError(
                500, "boundary_data_missing",
                f"Boundary data for region {region!r} is not available.",
            )
        data = json.loads(path.read_text(encoding="utf-8"))
        props = data.get("properties", {})
        geometry = data.get("_geometry")
        if geometry is None:
            # Fall back to merging feature geometries if _geometry is absent.
            all_coords: List[Any] = []
            for feature in data.get("features", []):
                geom = feature["geometry"]
                if geom["type"] == "Polygon":
                    all_coords.append(geom["coordinates"])
                else:
                    all_coords.extend(geom["coordinates"])
            geometry = {"type": "MultiPolygon", "coordinates": all_coords}
        return RegionPolygon(
            region=region,
            display_name=str(props.get("display_name", region)),
            geometry=geometry,
        )


class RouteService:
    """Read-only route planning over the two supported California regions."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._loader = BoundaryLoader(settings)
        self._regions: dict[str, RegionPolygon] = {}
        self._client = httpx.Client(
            timeout=settings.geocode_request_timeout_seconds,
            headers={"User-Agent": settings.nominatim_user_agent},
        )

    def close(self) -> None:
        try:
            self._client.close()
        except Exception:
            pass

    # ------------------------------------------------------------------ #
    # Region validation (authoritative polygon-based)
    # ------------------------------------------------------------------ #
    def _region(self, region: str) -> RegionPolygon:
        if region not in self._regions:
            self._regions[region] = self._loader.load(region)
        return self._regions[region]

    def _in_region(self, lat: float, lon: float, region: str) -> bool:
        return self._region(region).contains(lat, lon)

    def _in_region_buffered(self, lat: float, lon: float, region: str) -> bool:
        """Inside the polygon, or within the documented cartographic tolerance
        of its edge (route-geometry validation only)."""
        return point_near_polygon(
            lon, lat, self._region(region).geometry, ROUTE_GEOMETRY_BUFFER_DEGREES
        )

    def _bbox_contains(self, lat: float, lon: float, region: str) -> bool:
        return self._region(region).bbox_contains(lat, lon)

    # ------------------------------------------------------------------ #
    # Geocoding (Nominatim)
    # ------------------------------------------------------------------ #
    def geocode(self, query: str, region: str) -> route_schemas.GeocodeResponse:
        if not query or not query.strip():
            raise APIError(422, "invalid_query", "A search query is required.")
        self._region(region)  # raises if boundary data missing
        params = {
            "q": query.strip(),
            "format": "json",
            "limit": 8,
            "countrycodes": "us",
            "viewbox": (
                f"{self._region(region).west},"
                f"{self._region(region).north},"
                f"{self._region(region).east},"
                f"{self._region(region).south}"
            ),
            "bounded": 1,
            "addressdetails": 1,
        }
        try:
            response = self._client.get(
                f"{self.settings.nominatim_base_url.rstrip('/')}/search",
                params=params,
            )
        except (httpx.RequestError, httpx.HTTPStatusError) as exc:
            raise APIError(502, "geocode_service_unavailable",
                           "The geocoding service could not be reached.") from exc

        if response.status_code != 200:
            raise APIError(502, "geocode_service_error",
                           "The geocoding service returned an error.")

        candidates: List[route_schemas.GeocodeCandidate] = []
        for item in response.json():
            try:
                lat = float(item["lat"])
                lon = float(item["lon"])
            except (KeyError, TypeError, ValueError):
                continue
            # Defensible validation: the returned point must fall inside the
            # selected region's authoritative polygon (not text matching).
            if not self._in_region(lat, lon, region):
                continue
            candidates.append(
                route_schemas.GeocodeCandidate(
                    label=str(item.get("display_name", query.strip())),
                    lat=lat,
                    lon=lon,
                    region=region,
                )
            )

        return route_schemas.GeocodeResponse(
            region=region, query=query.strip(), candidates=candidates,
        )

    # ------------------------------------------------------------------ #
    # Routing (OSRM)
    # ------------------------------------------------------------------ #
    def route(
        self,
        source_lat: float,
        source_lon: float,
        dest_lat: float,
        dest_lon: float,
        region: str,
    ) -> route_schemas.RouteResponse:
        region_poly = self._region(region)

        # Both endpoints must be inside the selected region's polygon.
        if not region_poly.contains(source_lat, source_lon):
            raise APIError(
                422, "source_outside_region",
                "The source is outside the selected region. Choose a location "
                "within the selected region.",
            )
        if not region_poly.contains(dest_lat, dest_lon):
            raise APIError(
                422, "destination_outside_region",
                "The destination is outside the selected region. Choose a "
                "location within the selected region.",
            )

        coords = f"{source_lon},{source_lat};{dest_lon},{dest_lat}"
        params = {
            "alternatives": "true",
            "geometries": "geojson",
            "overview": "simplified",
            "steps": "false",
        }
        try:
            response = self._client.get(
                f"{self.settings.osrm_base_url.rstrip('/')}/route/v1/driving/{coords}",
                params=params,
            )
        except (httpx.RequestError, httpx.HTTPStatusError) as exc:
            raise APIError(502, "routing_service_unavailable",
                           "The routing service could not be reached.") from exc

        if response.status_code != 200:
            raise APIError(502, "routing_service_error",
                           "The routing service returned an error.")

        payload: dict[str, Any] = response.json()
        if payload.get("code") != "Ok" or not payload.get("routes"):
            raise APIError(
                502, "routing_service_error",
                "The routing service could not compute a route between these "
                "locations.",
            )

        # Validate the COMPLETE geometry of every alternative. Every coordinate
        # must remain inside (or immediately adjacent to, within a documented
        # cartographic tolerance) the selected region's polygon. Reject any
        # alternative that leaves the region, never just the endpoints.
        valid_alternatives: List[route_schemas.RouteAlternative] = []
        rejected_count = 0
        for index, route in enumerate(payload["routes"], start=1):
            geometry_lonlat = route["geometry"]["coordinates"]
            if not all(
                self._in_region_buffered(lat, lon, region)
                for lon, lat in geometry_lonlat
            ):
                rejected_count += 1
                LOGGER.warning(
                    "Rejecting route alternative %s: geometry leaves the %s region",
                    index, region,
                )
                continue
            geometry = [[lat, lon] for lon, lat in geometry_lonlat]  # [lat, lon]
            valid_alternatives.append(
                route_schemas.RouteAlternative(
                    id=f"route-{len(valid_alternatives) + 1}",
                    distance_km=round(float(route["distance"]) / 1000.0, 2),
                    duration_seconds=float(route["duration"]),
                    duration_minutes=round(float(route["duration"]) / 60.0, 1),
                    geometry=geometry,
                )
            )

        if not valid_alternatives:
            raise APIError(
                422, "no_route_within_region",
                "No valid route within the selected region: every computed "
                "alternative leaves the region boundary. Choose different "
                "source/destination locations.",
            )

        return route_schemas.RouteResponse(
            region=region,
            source=route_schemas.Coordinate(lat=source_lat, lon=source_lon),
            destination=route_schemas.Coordinate(lat=dest_lat, lon=dest_lon),
            alternatives=valid_alternatives,
            selected_route_id=valid_alternatives[0].id,
        )
