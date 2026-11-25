"""Route planning endpoints (read-only, region-scoped).

Proxies Nominatim (geocoding) and OSRM (driving routing). No credentials are
required by these free services. All route geometry, distance, and duration
values come from the routing engine; nothing is fabricated.
"""

from fastapi import APIRouter, Depends, Query

from backend.app.api.dependencies import get_settings
from backend.app.core.config import Settings
from backend.app.core.errors import APIError
from backend.app.services.routes import RouteService
from backend.app.schemas import routes as route_schemas
from backend.app.schemas.common import DatasetId

router = APIRouter(prefix="/route", tags=["route"])


def get_route_service(settings: Settings = Depends(get_settings)) -> RouteService:
    return RouteService(settings)


@router.get("/regions", response_model=list[route_schemas.RegionInfo])
def list_regions(settings: Settings = Depends(get_settings)) -> list[route_schemas.RegionInfo]:
    """Supported regions: Los Angeles County and San Francisco Bay Area."""
    regions: list[route_schemas.RegionInfo] = []
    for region in ("metr-la", "pems-bay"):
        bounds = route_schemas.region_bounds(region)
        regions.append(
            route_schemas.RegionInfo(
                region=region,
                display_name=str(bounds["display_name"]),
                south=float(bounds["south"]),
                west=float(bounds["west"]),
                north=float(bounds["north"]),
                east=float(bounds["east"]),
            )
        )
    return regions


@router.get("/geocode", response_model=route_schemas.GeocodeResponse)
def geocode(
    q: str = Query("", min_length=1, description="Location search query"),
    region: DatasetId = Query("metr-la", description="Selected region"),
    service: RouteService = Depends(get_route_service),
) -> route_schemas.GeocodeResponse:
    if not q or not q.strip():
        raise APIError(422, "invalid_query", "A search query is required.")
    return service.geocode(q.strip(), region)


@router.get("/route", response_model=route_schemas.RouteResponse)
def get_route(
    source_lat: float = Query(..., ge=-90, le=90, description="Source latitude"),
    source_lon: float = Query(..., ge=-180, le=180, description="Source longitude"),
    dest_lat: float = Query(..., ge=-90, le=90, description="Destination latitude"),
    dest_lon: float = Query(..., ge=-180, le=180, description="Destination longitude"),
    region: DatasetId = Query("metr-la", description="Selected region"),
    service: RouteService = Depends(get_route_service),
) -> route_schemas.RouteResponse:
    return service.route(source_lat, source_lon, dest_lat, dest_lon, region)


@router.get("/traffic-evidence", response_model=route_schemas.TrafficEvidenceResponse)
def traffic_evidence(
    dataset: DatasetId = Query("metr-la", description="Dataset/region"),
) -> route_schemas.TrafficEvidenceResponse:
    """Traffic-evidence availability for route-specific scoring.

    The repository contains no genuine sensor coordinates (verified in Stage 1),
    so historical observations cannot be matched to route corridors and no
    congestion-based route ranking is offered. This endpoint truthfully reports
    unavailability rather than fabricating traffic values.
    """
    return route_schemas.TrafficEvidenceResponse(
        available=False,
        message=(
            "Traffic recommendation unavailable — insufficient geographically "
            "matched traffic observations."
        ),
        reason="no_sensor_coordinates_in_dataset",
    )
