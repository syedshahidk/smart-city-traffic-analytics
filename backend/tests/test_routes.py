"""Tests for the LA / Bay Area route planner endpoints.

The route service calls Nominatim (geocoding) and OSRM (routing), so tests use
a fake service to keep them offline and deterministic. Region validation is
exercised directly against the real RouteService bounds.
"""

from typing import Any

import pytest
from fastapi.testclient import TestClient

from backend.app.api.v1.endpoints.routes import get_route_service
from backend.app.core.config import Settings
from backend.app.core.errors import APIError
from backend.app.main import create_app
from backend.app.services.routes import RouteService
from backend.app.schemas import routes as route_schemas


class FakeRouteService:
    def __init__(self) -> None:
        self.calls: list[tuple[str, ...]] = []

    def geocode(self, query: str, region: str) -> route_schemas.GeocodeResponse:
        self.calls.append(("geocode", query, region))
        return route_schemas.GeocodeResponse(
            region=region,
            query=query,
            candidates=[
                route_schemas.GeocodeCandidate(
                    label="Los Angeles, CA, USA", lat=34.05, lon=-118.25, region=region
                ),
                route_schemas.GeocodeCandidate(
                    label="Long Beach, CA, USA", lat=33.77, lon=-118.19, region=region
                ),
            ],
        )

    def route(
        self,
        source_lat: float,
        source_lon: float,
        dest_lat: float,
        dest_lon: float,
        region: str,
    ) -> route_schemas.RouteResponse:
        self.calls.append(("route", source_lat, source_lon, dest_lat, dest_lon, region))
        return route_schemas.RouteResponse(
            region=region,
            source=route_schemas.Coordinate(lat=source_lat, lon=source_lon),
            destination=route_schemas.Coordinate(lat=dest_lat, lon=dest_lon),
            alternatives=[
                route_schemas.RouteAlternative(
                    id="route-1",
                    distance_km=10.5,
                    duration_seconds=1200.0,
                    duration_minutes=20.0,
                    geometry=[[source_lat, source_lon], [dest_lat, dest_lon]],
                ),
                route_schemas.RouteAlternative(
                    id="route-2",
                    distance_km=12.3,
                    duration_seconds=1500.0,
                    duration_minutes=25.0,
                    geometry=[[source_lat, source_lon], [dest_lat, dest_lon]],
                ),
            ],
            selected_route_id="route-1",
        )

    def close(self) -> None:
        return None


@pytest.fixture
def client(tmp_path, fake_database):
    app = create_app(Settings(models_dir=tmp_path), database=fake_database)
    fake_service = FakeRouteService()

    def fake_route_service() -> FakeRouteService:
        return fake_service

    app.dependency_overrides[get_route_service] = fake_route_service
    with TestClient(app) as test_client:
        yield test_client


def test_regions_endpoint_returns_both_supported_regions(client: TestClient) -> None:
    response = client.get("/api/v1/route/regions")
    assert response.status_code == 200
    payload = response.json()
    assert {item["region"] for item in payload} == {"metr-la", "pems-bay"}
    by_region = {item["region"]: item for item in payload}
    assert by_region["metr-la"]["display_name"] == "Los Angeles County"
    assert by_region["pems-bay"]["display_name"] == "San Francisco Bay Area"
    # Bounding boxes are sane lat/lon ranges.
    assert by_region["metr-la"]["south"] < by_region["metr-la"]["north"]
    assert by_region["metr-la"]["west"] < by_region["metr-la"]["east"]
    assert by_region["pems-bay"]["south"] < by_region["pems-bay"]["north"]
    assert by_region["pems-bay"]["west"] < by_region["pems-bay"]["east"]


def test_geocode_endpoint(client: TestClient) -> None:
    response = client.get("/api/v1/route/geocode", params={"q": "Los Angeles", "region": "metr-la"})
    assert response.status_code == 200
    payload = response.json()
    assert payload["region"] == "metr-la"
    assert payload["query"] == "Los Angeles"
    assert len(payload["candidates"]) == 2
    assert payload["candidates"][0]["label"] == "Los Angeles, CA, USA"


def test_route_endpoint_returns_alternatives(client: TestClient) -> None:
    response = client.get(
        "/api/v1/route/route",
        params={
            "source_lat": 34.05,
            "source_lon": -118.25,
            "dest_lat": 33.77,
            "dest_lon": -118.19,
            "region": "metr-la",
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["region"] == "metr-la"
    assert len(payload["alternatives"]) == 2
    assert payload["selected_route_id"] == "route-1"
    r1 = payload["alternatives"][0]
    assert r1["id"] == "route-1"
    assert r1["distance_km"] == 10.5
    assert r1["duration_minutes"] == 20.0
    assert r1["duration_seconds"] == 1200.0
    assert r1["geometry"][0] == [34.05, -118.25]


def test_route_service_rejects_source_outside_region() -> None:
    # San Francisco coordinates are outside the LA County validation box.
    service = RouteService(Settings())
    try:
        with pytest.raises(APIError) as exc:
            service.route(37.77, -122.42, 34.05, -118.25, "metr-la")
        assert exc.value.status_code == 422
        assert exc.value.code == "source_outside_region"
    finally:
        service.close()


def test_route_service_rejects_destination_outside_region() -> None:
    service = RouteService(Settings())
    try:
        with pytest.raises(APIError) as exc:
            service.route(34.05, -118.25, 37.77, -122.42, "metr-la")
        assert exc.value.status_code == 422
        assert exc.value.code == "destination_outside_region"
    finally:
        service.close()


def test_traffic_evidence_is_unavailable(client: TestClient) -> None:
    response = client.get("/api/v1/route/traffic-evidence", params={"dataset": "metr-la"})
    assert response.status_code == 200
    payload = response.json()
    assert payload["available"] is False
    assert payload["message"] == (
        "Traffic recommendation unavailable — insufficient geographically "
        "matched traffic observations."
    )
    assert payload["reason"] == "no_sensor_coordinates_in_dataset"


class FakeResponse:
    status_code = 200

    def __init__(self, payload: Any) -> None:
        self._payload = payload

    def json(self) -> Any:
        return self._payload


def test_route_service_geocode_filters_out_of_region_results() -> None:
    """Nominatim candidates outside the selected region's bounding box are dropped."""
    service = RouteService(Settings())
    service._client.get = lambda url, params: FakeResponse(  # type: ignore[method-assign]
        [
            {"display_name": "Los Angeles, CA, USA", "lat": "34.05", "lon": "-118.25"},
            {"display_name": "San Francisco, CA, USA", "lat": "37.77", "lon": "-122.42"},
        ]
    )
    result = service.geocode("Los Angeles", "metr-la")
    assert len(result.candidates) == 1
    assert result.candidates[0].label == "Los Angeles, CA, USA"
    assert result.candidates[0].region == "metr-la"
    service.close()


def test_route_service_bounds_are_region_specific() -> None:
    service = RouteService(Settings())
    try:
        # LA polygon contains downtown LA and Long Beach but not San Francisco.
        assert service._in_region(34.05, -118.25, "metr-la")
        assert service._in_region(33.77, -118.19, "metr-la")
        assert not service._in_region(37.77, -122.42, "metr-la")
        # Bay polygon contains San Francisco but not downtown LA.
        assert service._in_region(37.77, -122.42, "pems-bay")
        assert not service._in_region(34.05, -118.25, "pems-bay")
        # Neither polygon contains a location outside both regions (Seattle).
        assert not service._in_region(47.60, -122.33, "metr-la")
        assert not service._in_region(47.60, -122.33, "pems-bay")
    finally:
        service.close()


# --------------------------------------------------------------------------- #
# Authoritative polygon validation (real Census boundary files, offline)
# --------------------------------------------------------------------------- #
def test_point_in_polygon_inside_each_region() -> None:
    service = RouteService(Settings())
    try:
        assert service._in_region(34.0522, -118.2437, "metr-la")  # downtown LA
        assert service._in_region(33.7701, -118.1936, "metr-la")  # Long Beach
        assert service._in_region(37.7749, -122.4194, "pems-bay")  # San Francisco
        assert service._in_region(37.8044, -122.2712, "pems-bay")  # Oakland
        assert service._in_region(37.3382, -121.8863, "pems-bay")  # San Jose
    finally:
        service.close()


def test_point_in_polygon_outside_each_region() -> None:
    service = RouteService(Settings())
    try:
        assert not service._in_region(37.7749, -122.4194, "metr-la")  # SF not in LA
        assert not service._in_region(34.0522, -118.2437, "pems-bay")  # LA not in Bay
        assert not service._in_region(47.6062, -122.3321, "metr-la")  # Seattle
        assert not service._in_region(47.6062, -122.3321, "pems-bay")  # Seattle
        assert not service._in_region(36.1699, -115.1398, "metr-la")  # Las Vegas
        assert not service._in_region(38.5816, -121.4944, "pems-bay")  # Sacramento
    finally:
        service.close()


def test_boundary_points_count_as_inside() -> None:
    service = RouteService(Settings())
    try:
        # A vertex of the LA County mainland polygon is on the boundary.
        region = service._region("metr-la")
        polys = region.geometry["coordinates"]
        # Pick the first coordinate of the largest (mainland) exterior ring.
        mainland = max(polys, key=lambda p: len(p[0]))
        lon, lat = mainland[0][0]
        assert region.contains(lat, lon)
    finally:
        service.close()


# --------------------------------------------------------------------------- #
# Complete route-geometry validation
# --------------------------------------------------------------------------- #
def _fake_osrm(routes_payload: dict[str, Any]):
    return lambda url, params: FakeResponse(routes_payload)


def test_route_accepts_alternatives_entirely_inside_region() -> None:
    service = RouteService(Settings())
    try:
        service._client.get = _fake_osrm(  # type: ignore[method-assign]
            {
                "code": "Ok",
                "routes": [
                    {
                        "distance": 10500,
                        "duration": 1200,
                        "geometry": {
                            "coordinates": [
                                [-118.25, 34.05],
                                [-118.24, 34.05],
                                [-118.23, 34.05],
                            ]
                        },
                    }
                ],
            }
        )
        resp = service.route(34.05, -118.25, 34.05, -118.23, "metr-la")
        assert len(resp.alternatives) == 1
        assert resp.alternatives[0].distance_km == 10.5
    finally:
        service.close()


def test_route_rejects_alternative_that_leaves_region() -> None:
    service = RouteService(Settings())
    try:
        service._client.get = _fake_osrm(  # type: ignore[method-assign]
            {
                "code": "Ok",
                "routes": [
                    {
                        "distance": 10500,
                        "duration": 1200,
                        "geometry": {
                            "coordinates": [
                                [-118.25, 34.05],
                                [-118.24, 34.05],
                                [-118.23, 34.05],
                            ]
                        },
                    },
                    {
                        "distance": 20000,
                        "duration": 2400,
                        "geometry": {
                            "coordinates": [
                                [-118.25, 34.05],
                                [-118.24, 34.05],
                                [-120.00, 35.00],  # leaves LA County
                                [-118.23, 34.05],
                            ]
                        },
                    },
                ],
            }
        )
        resp = service.route(34.05, -118.25, 34.05, -118.23, "metr-la")
        # Only the in-region alternative survives.
        assert len(resp.alternatives) == 1
        assert resp.alternatives[0].distance_km == 10.5
        assert resp.selected_route_id == "route-1"
    finally:
        service.close()


def test_route_no_valid_alternatives_returns_clear_error() -> None:
    service = RouteService(Settings())
    try:
        service._client.get = _fake_osrm(  # type: ignore[method-assign]
            {
                "code": "Ok",
                "routes": [
                    {
                        "distance": 20000,
                        "duration": 2400,
                        "geometry": {
                            "coordinates": [
                                [-118.25, 34.05],
                                [-120.00, 35.00],  # leaves LA County
                                [-118.23, 34.05],
                            ]
                        },
                    },
                    {
                        "distance": 25000,
                        "duration": 3000,
                        "geometry": {
                            "coordinates": [
                                [-118.25, 34.05],
                                [-122.00, 37.00],  # leaves LA County
                                [-118.23, 34.05],
                            ]
                        },
                    },
                ],
            }
        )
        with pytest.raises(APIError) as exc:
            service.route(34.05, -118.25, 34.05, -118.23, "metr-la")
        assert exc.value.status_code == 422
        assert exc.value.code == "no_route_within_region"
        assert "leaves the region" in exc.value.message
    finally:
        service.close()


def test_geocode_drops_out_of_polygon_candidates() -> None:
    service = RouteService(Settings())
    try:
        service._client.get = lambda url, params: FakeResponse(  # type: ignore[method-assign]
            [
                {"display_name": "Los Angeles, CA, USA", "lat": "34.05", "lon": "-118.25"},
                {"display_name": "San Francisco, CA, USA", "lat": "37.77", "lon": "-122.42"},
                {"display_name": "Seattle, WA, USA", "lat": "47.60", "lon": "-122.33"},
            ]
        )
        result = service.geocode("Los Angeles", "metr-la")
        assert len(result.candidates) == 1
        assert result.candidates[0].label == "Los Angeles, CA, USA"
        assert result.candidates[0].region == "metr-la"
    finally:
        service.close()

