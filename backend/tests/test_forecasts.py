from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

from backend.app.core.config import Settings
from backend.app.core.errors import APIError
from analytics.forecasting import ForecastingError
from backend.app.main import create_app
from backend.app.services import forecasts as forecast_service
from backend.tests.conftest import NOW, forecast_metric_rows


def _prediction(dataset: str, metrics, **kwargs):
    origin = metrics.index[-1]
    return {
        "dataset": dataset,
        "forecast_origin": origin.isoformat(),
        "target_timestamp": (origin + timedelta(minutes=15)).isoformat(),
        "prediction_horizon_minutes": 15,
        "predicted_average_speed": 42.5,
        "predicted_congestion_percentage": 25.0,
        "congestion_status": "not_congested",
        "congestion_threshold_speed_below": 20.0,
        "congestion_percentage_threshold": kwargs["congestion_percentage_threshold"],
        "selected_models": {
            "speed": f"{dataset}-speed",
            "congestion_percentage": f"{dataset}-congestion",
        },
        "model_version": f"version-{dataset}",
    }


@pytest.mark.parametrize("dataset", ["metr-la", "pems-bay"])
def test_latest_forecast_returns_typed_15_minute_prediction(
    client: TestClient, monkeypatch: pytest.MonkeyPatch, dataset: str
) -> None:
    captured = {}

    def fake_forecast(selected_dataset, metrics, **kwargs):
        captured.update(dataset=selected_dataset, metrics=metrics, kwargs=kwargs)
        return _prediction(selected_dataset, metrics, **kwargs)

    monkeypatch.setattr(forecast_service.forecasting, "forecast_from_metrics", fake_forecast)
    monkeypatch.setattr(forecast_service, "utc_now", lambda: NOW)

    response = client.get("/api/v1/forecasts/latest", params={"dataset": dataset})

    assert response.status_code == 200
    payload = response.json()
    assert payload["dataset"] == dataset
    assert payload["prediction_horizon_minutes"] == 15
    assert datetime.fromisoformat(payload["target_timestamp"]) == (
        datetime.fromisoformat(payload["forecast_origin"]) + timedelta(minutes=15)
    )
    assert payload["predicted_average_speed"] == 42.5
    assert payload["predicted_congestion_percentage"] == 25.0
    assert payload["speed_model"] == f"{dataset}-speed"
    assert payload["congestion_model"] == f"{dataset}-congestion"
    assert payload["model_version"] == f"version-{dataset}"
    assert payload["input_window_count"] == 13
    assert payload["input_window_end"] == payload["latest_event_timestamp"]
    assert payload["freshness_seconds"] == 0.0
    assert payload["data_provenance"] == "historical_dataset_replay_simulated"
    assert payload["is_live_traffic"] is False
    assert captured["dataset"] == dataset
    assert len(captured["metrics"]) == 13
    assert captured["metrics"].index.tz is not None
    query, params = next(
        call for call in client.app.state.database.calls
        if "window_end <= %s" in call[0]
    )
    assert "dataset = %s" in query
    assert "LIMIT %s" in query
    assert params == (dataset, NOW, 13)


def test_forecast_rejects_invalid_dataset(client: TestClient) -> None:
    response = client.get("/api/v1/forecasts/latest", params={"dataset": "unknown"})
    assert response.status_code == 422


def test_forecast_database_failure_keeps_503_behavior(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(forecast_service, "utc_now", lambda: NOW)
    client.app.state.database.fail = True
    response = client.get("/api/v1/forecasts/latest", params={"dataset": "metr-la"})
    assert response.status_code == 503
    assert response.json()["error"]["code"] == "database_unavailable"


def test_forecast_model_failures_return_safe_503(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(forecast_service, "utc_now", lambda: NOW)

    def missing_model(*args, **kwargs):
        raise FileNotFoundError("sensitive model path")

    monkeypatch.setattr(forecast_service.forecasting, "forecast_from_metrics", missing_model)
    response = client.get("/api/v1/forecasts/latest", params={"dataset": "metr-la"})
    assert response.status_code == 503
    assert response.json()["error"]["code"] == "forecast_model_unavailable"
    assert "sensitive" not in response.text

    def prediction_error(*args, **kwargs):
        raise RuntimeError("internal estimator details")

    monkeypatch.setattr(forecast_service.forecasting, "forecast_from_metrics", prediction_error)
    response = client.get("/api/v1/forecasts/latest", params={"dataset": "metr-la"})
    assert response.status_code == 503
    assert response.json()["error"]["code"] == "forecast_model_error"
    assert "internal estimator" not in response.text

    def corrupt_model(*args, **kwargs):
        raise ForecastingError("internal artifact details")

    monkeypatch.setattr(forecast_service.forecasting, "forecast_from_metrics", corrupt_model)
    response = client.get("/api/v1/forecasts/latest", params={"dataset": "metr-la"})
    assert response.status_code == 503
    assert response.json()["error"]["code"] == "forecast_model_error"
    assert "internal artifact" not in response.text


def test_stale_event_time_is_rejected_even_when_recently_processed(
    fake_database, monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    old_rows = forecast_metric_rows("metr-la", latest_end=NOW - timedelta(minutes=30))
    for row in old_rows:
        row["processed_at"] = NOW
    fake_database.forecast_rows["metr-la"] = old_rows
    monkeypatch.setattr(forecast_service, "utc_now", lambda: NOW)
    with TestClient(create_app(Settings(models_dir=tmp_path), database=fake_database)) as app_client:
        response = app_client.get("/api/v1/forecasts/latest", params={"dataset": "metr-la"})
    assert response.status_code == 503
    assert response.json()["error"]["code"] == "forecast_data_unavailable"
    assert "stale" in response.json()["error"]["message"]


def test_forecast_rejects_an_origin_whose_target_is_already_past() -> None:
    rows = forecast_metric_rows("metr-la", latest_end=NOW - timedelta(minutes=15))
    with pytest.raises(APIError, match="no longer has a future"):
        _validate(rows)


def test_no_metric_rows_returns_service_unavailable(client: TestClient,
                                                    monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(forecast_service, "utc_now", lambda: NOW)
    client.app.state.database.forecast_rows["metr-la"] = []
    response = client.get("/api/v1/forecasts/latest", params={"dataset": "metr-la"})
    assert response.status_code == 503
    assert response.json()["error"]["code"] == "forecast_data_unavailable"


def test_freshness_limit_is_environment_configurable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("API_FORECAST_MAX_INPUT_AGE_SECONDS", "900")
    assert Settings.from_environment().forecast_max_input_age_seconds == 900
    monkeypatch.setenv("API_FORECAST_MAX_INPUT_AGE_SECONDS", "0")
    with pytest.raises(ValueError, match="freshness limit"):
        Settings.from_environment()


def _validate(rows, *, now=NOW, max_age=1200):
    return forecast_service._validate_and_frame(rows, "metr-la", now, max_age)


def test_validation_requires_thirteen_windows() -> None:
    with pytest.raises(APIError, match="13 consecutive"):
        _validate(forecast_metric_rows("metr-la")[:-1])


def test_validation_rejects_missing_and_duplicate_timestamps() -> None:
    missing = forecast_metric_rows("metr-la", latest_end=NOW - timedelta(minutes=10))
    for row in missing[7:]:
        row["window_start"] += timedelta(minutes=5)
        row["window_end"] += timedelta(minutes=5)
    with pytest.raises(APIError, match="duplicate|missing or nonconsecutive"):
        _validate(missing)

    duplicate = forecast_metric_rows("metr-la")
    duplicate[6]["window_start"] = duplicate[5]["window_start"]
    duplicate[6]["window_end"] = duplicate[5]["window_end"]
    with pytest.raises(APIError, match="duplicate"):
        _validate(duplicate)


def test_validation_rejects_off_grid_incomplete_and_future_windows() -> None:
    off_grid = forecast_metric_rows("metr-la")
    off_grid[4]["window_start"] += timedelta(minutes=1)
    off_grid[4]["window_end"] += timedelta(minutes=1)
    with pytest.raises(APIError, match="five-minute boundaries"):
        _validate(off_grid)

    incomplete = forecast_metric_rows("metr-la")
    incomplete[3]["window_end"] -= timedelta(seconds=1)
    with pytest.raises(APIError, match="invalid five-minute window"):
        _validate(incomplete)

    future = forecast_metric_rows("metr-la", latest_end=NOW + timedelta(minutes=5))
    with pytest.raises(APIError, match="incomplete or future"):
        _validate(future)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("average_speed", float("nan"), "average speed"),
        ("average_speed", -1.0, "out-of-range"),
        ("congestion_percentage", 101.0, "out-of-range"),
        ("congestion_percentage", 30.0, "inconsistent"),
        ("total_sensor_readings", 0, "sensor counts"),
        ("congested_sensor_count", 11, "sensor counts"),
    ],
)
def test_validation_rejects_invalid_metric_values_and_counts(
    field: str, value, message: str
) -> None:
    rows = forecast_metric_rows("metr-la")
    rows[5][field] = value
    with pytest.raises(APIError, match=message):
        _validate(rows)


def test_validation_rejects_rows_from_another_dataset() -> None:
    rows = forecast_metric_rows("pems-bay")
    with pytest.raises(APIError, match="different dataset"):
        _validate(rows)
