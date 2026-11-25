from datetime import timezone

from fastapi.testclient import TestClient


START = "2024-01-01T00:00:00Z"
END = "2024-01-01T01:00:00Z"


def test_api_startup_and_health_endpoints(client: TestClient) -> None:
    assert client.get("/health/live").status_code == 200
    assert client.get("/health/live").json() == {"status": "live"}
    ready = client.get("/health/ready")
    assert ready.status_code == 200
    assert ready.json() == {"status": "ready"}


def test_dashboard_cors_allows_only_configured_local_origins(client: TestClient) -> None:
    allowed = client.options(
        "/api/v1/datasets",
        headers={
            "Origin": "http://localhost:5173",
            "Access-Control-Request-Method": "GET",
        },
    )
    assert allowed.status_code == 200
    assert allowed.headers["access-control-allow-origin"] == "http://localhost:5173"
    assert "GET" in allowed.headers["access-control-allow-methods"]

    denied = client.get("/health/live", headers={"Origin": "https://untrusted.example"})
    assert denied.status_code == 200
    assert "access-control-allow-origin" not in denied.headers


def test_database_connection_failure_is_a_503(client: TestClient) -> None:
    client.app.state.database.fail = True
    assert client.get("/health/live").status_code == 200
    response = client.get("/health/ready")
    assert response.status_code == 503
    assert response.json()["error"]["code"] == "database_unavailable"
    assert client.get("/api/v1/datasets").status_code == 503


def test_datasets_are_allowlisted_and_include_freshness(client: TestClient) -> None:
    response = client.get("/api/v1/datasets")
    assert response.status_code == 200
    payload = response.json()
    assert [item["dataset"] for item in payload] == ["metr-la", "pems-bay"]
    assert payload[0]["sensor_count"] == 207
    assert payload[0]["metric_row_count"] == 30
    assert payload[0]["freshness_seconds"] is not None
    assert client.get("/api/v1/datasets/unknown").status_code == 404
    assert client.get("/api/v1/dashboard/kpis", params={"dataset": "unknown"}).status_code == 422


def test_metrics_filtering_and_keyset_pagination(client: TestClient) -> None:
    response = client.get("/api/v1/metrics", params={
        "dataset": "metr-la", "start": START, "end": END, "limit": 2,
    })
    assert response.status_code == 200
    payload = response.json()
    assert len(payload["items"]) == 2
    assert payload["has_more"] is True
    assert payload["next_cursor"] == "2024-01-01T00:05:00Z"
    assert payload["freshness_seconds"] is not None
    sql, params = next(call for call in client.app.state.database.calls
                       if "ORDER BY window_start ASC" in call[0])
    assert "dataset = %s" in sql and params[0] == "metr-la"
    assert params[1].tzinfo == timezone.utc and params[2].tzinfo == timezone.utc
    assert params[-1] == 3


def test_dashboard_kpis_are_sensor_reading_weighted(client: TestClient) -> None:
    response = client.get("/api/v1/dashboard/kpis", params={
        "dataset": "metr-la", "start": START, "end": END,
    })
    assert response.status_code == 200
    payload = response.json()
    assert payload["weighted_average_speed"] == 40.0
    assert payload["weighted_congestion_percentage"] == 25.0
    assert payload["latest_metric"]["average_speed"] == 39.0
    assert payload["freshness_seconds"] is not None


def test_dashboard_kpi_congestion_percentage_matches_sensor_count_ratio(client: TestClient) -> None:
    """Congestion share must equal congested sensors * 100 / total valid readings."""
    response = client.get("/api/v1/dashboard/kpis", params={
        "dataset": "metr-la", "start": START, "end": END,
    })
    assert response.status_code == 200
    payload = response.json()
    readings = payload["total_sensor_readings"]
    congested = payload["congested_sensor_count"]
    assert payload["weighted_congestion_percentage"] is not None
    if readings:
        expected = round(congested * 100.0 / readings, 6)
        assert abs(payload["weighted_congestion_percentage"] - expected) < 1e-6


def test_congestion_trends_are_weighted_and_bucket_is_whitelisted(client: TestClient) -> None:
    response = client.get("/api/v1/congestion/trends", params={
        "dataset": "metr-la", "start": START, "end": END, "bucket": "1h",
    })
    assert response.status_code == 200
    payload = response.json()
    assert payload["items"][0]["weighted_average_speed"] == 40.0
    assert payload["items"][0]["weighted_congestion_percentage"] == 25.0
    assert payload["freshness_seconds"] is not None
    sql, params = next(call for call in client.app.state.database.calls
                       if "date_bin" in call[0])
    assert "date_bin(%s::interval" in sql
    assert params[0] == "1 hour"
    assert client.get("/api/v1/congestion/trends", params={
        "dataset": "metr-la", "bucket": "2m",
    }).status_code == 422


def test_invalid_ranges_and_cursors_are_rejected(client: TestClient) -> None:
    reversed_range = client.get("/api/v1/metrics", params={
        "dataset": "metr-la", "start": END, "end": START,
    })
    assert reversed_range.status_code == 422
    assert reversed_range.json()["error"]["code"] == "invalid_time_range"
    naive = client.get("/api/v1/metrics", params={
        "dataset": "metr-la", "start": "2024-01-01T00:00:00",
        "end": "2024-01-01T01:00:00Z",
    })
    assert naive.status_code == 422
    too_wide = client.get("/api/v1/dashboard/kpis", params={
        "dataset": "metr-la", "start": "2022-01-01T00:00:00Z",
        "end": "2024-01-01T01:00:00Z",
    })
    assert too_wide.status_code == 422
    bad_cursor = client.get("/api/v1/metrics", params={
        "dataset": "metr-la", "start": START, "end": END,
        "after": "2025-01-01T00:00:00Z",
    })
    assert bad_cursor.status_code == 422


def test_empty_kpis_return_null_weighted_values(client: TestClient) -> None:
    client.app.state.database.kpi = {
        "window_count": 0, "total_sensor_readings": 0,
        "congested_sensor_count": 0, "weighted_speed_sum": 0.0,
        "first_window_start": None, "latest_window_end": None,
        "last_processed_at": None, "latest_dataset": None,
    }
    response = client.get("/api/v1/dashboard/kpis", params={"dataset": "metr-la"})
    assert response.status_code == 200
    assert response.json()["weighted_average_speed"] is None
    assert response.json()["weighted_congestion_percentage"] is None
    assert response.json()["latest_metric"] is None


def test_validation_errors_use_the_common_error_envelope(client: TestClient) -> None:
    response = client.get("/api/v1/metrics", params={"dataset": "metr-la", "limit": 0})
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "validation_error"
    assert response.json()["details"]
