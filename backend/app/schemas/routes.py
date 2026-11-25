"""Pydantic schemas for the LA / Bay Area route planner.

The route planner is intentionally read-only and region-scoped. It proxies free
OpenStreetMap-compatible services (OSRM for routing, Nominatim for geocoding)
and never fabricates route geometries, distances, durations, or traffic values.
"""

from typing import List, Literal

from pydantic import BaseModel, Field

from backend.app.schemas.common import DatasetId

Region = Literal["metr-la", "pems-bay"]

# Approximate bounding boxes used *only* for geographic validation and for
# biasing Nominatim results to the selected region. They are not precise
# political boundaries and are never presented as authoritative region borders.
REGION_BOUNDS: dict[Region, dict[str, object]] = {
    "metr-la": {
        "display_name": "Los Angeles County",
        "south": 33.70,
        "west": -118.80,
        "north": 34.85,
        "east": -117.60,
    },
    "pems-bay": {
        "display_name": "San Francisco Bay Area",
        "south": 37.05,
        "west": -122.80,
        "north": 38.30,
        "east": -121.80,
    },
}


def region_bounds(region: Region) -> dict[str, float]:
    """Return the validation bounding box for a region."""
    if region not in REGION_BOUNDS:
        from backend.app.core.errors import APIError
        raise APIError(404, "region_not_found", f"Unsupported region: {region}")
    return REGION_BOUNDS[region]  # type: ignore[return-value]


class RegionInfo(BaseModel):
    region: Region
    display_name: str
    south: float
    west: float
    north: float
    east: float


class Coordinate(BaseModel):
    lat: float = Field(..., ge=-90, le=90)
    lon: float = Field(..., ge=-180, le=180)


class GeocodeCandidate(BaseModel):
    label: str
    lat: float
    lon: float
    region: Region


class GeocodeResponse(BaseModel):
    region: Region
    query: str
    candidates: List[GeocodeCandidate]


class RouteAlternative(BaseModel):
    id: str
    distance_km: float
    duration_seconds: float
    duration_minutes: float
    geometry: List[List[float]]  # [[lat, lon], ...] for map display


class RouteResponse(BaseModel):
    region: Region
    source: Coordinate
    destination: Coordinate
    alternatives: List[RouteAlternative]
    selected_route_id: str


class TrafficEvidenceResponse(BaseModel):
    available: bool
    message: str
    reason: str
