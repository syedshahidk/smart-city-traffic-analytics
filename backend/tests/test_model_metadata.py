import json
from pathlib import Path
from typing import Any

import joblib
import pytest
from fastapi.testclient import TestClient

from backend.app.core.config import Settings
from backend.app.main import create_app
from backend.app.services.model_metadata import STATUS_LOGIC
from backend.tests.conftest import FakeDatabase

VERSION = "20260101T000000000000Z"
DATASET = "metr-la"
ENDPOINT = "/api/v1/forecasts/model-metadata"


def _report(
    *,
    speed_model: str,
    congestion_model: str,
    artifact_file: str,
) -> dict[str, Any]:
    target_metrics = {
        "speed": {"mae": 1.428, "r2": 0.872},
        "congestion_percentage": {"mae": 0.232, "r2": 0.867},
    }
    return {
        "dataset": DATASET,
        "model_version": VERSION,
        "model_artifact": f"models/traffic_forecasting/{DATASET}/{artifact_file}",
        "prediction_horizon_minutes": 15,
        "threshold_speed_below": 20.0,
        "feature_definitions": {"speed_lag_0": "current value", "time_of_day_sin": "cyclic"},
        "model_configurations": {
            "persistence": {"rule": "use the metric observed at the forecast origin"},
            "hist_gradient_boosting": {"max_iter": 160},
        },
        "sample_counts": {"train": 10, "validation": 2, "test": 3},
        "models": {
            "speed": {
                "selected_model": speed_model,
                "test_metrics": target_metrics["speed"],
                "test_sample_count": 3,
            },
            "congestion_percentage": {
                "selected_model": congestion_model,
                "test_metrics": target_metrics["congestion_percentage"],
                "test_sample_count": 3,
            },
        },
    }


def _write_fixture(
    root: Path,
    *,
    write_artifact: bool = True,
    write_report: bool = True,
    report_congestion_model: str | None = None,
) -> Path:
    models_dir = root / "models"
    reports_dir = root / "reports"
    artifact_dir = models_dir / DATASET
    artifact_dir.mkdir(parents=True, exist_ok=True)
    reports_dir.mkdir(parents=True, exist_ok=True)
    artifact_file = f"model_{VERSION}.joblib"
    artifact_path = artifact_dir / artifact_file

    if write_artifact:
        bundle = {
            "schema_version": 1,
            "dataset": DATASET,
            "model_version": VERSION,
            "prediction_horizon_minutes": 15,
            "interval_minutes": 5,
            "congestion_threshold_speed_below": 20.0,
            "feature_columns": ["speed_lag_0", "time_of_day_sin"],
            "model_configurations": {
                "persistence": {"rule": "use the metric observed at the forecast origin"},
                "hist_gradient_boosting": {"max_iter": 160},
            },
            "models": {
                "speed": {"name": "persistence", "estimator": None},
                "congestion_percentage": {"name": "hist_gradient_boosting", "estimator": None},
            },
            "training_rows": 10,
            "validation_rows": 2,
            "test_rows": 3,
        }
        joblib.dump(bundle, artifact_path)

    if write_report:
        report = _report(
            speed_model="persistence",
            congestion_model=report_congestion_model or "hist_gradient_boosting",
            artifact_file=artifact_file,
        )
        (reports_dir / f"traffic_forecast_{DATASET}_{VERSION}.json").write_text(
            json.dumps(report), encoding="utf-8"
        )

    return artifact_path


def _client(root: Path) -> TestClient:
    settings = Settings(models_dir=root / "models", reports_dir=root / "reports")
    return TestClient(create_app(settings=settings, database=FakeDatabase()))


def test_model_metadata_is_derived_from_report_and_artifact(tmp_path: Path) -> None:
    artifact_path = _write_fixture(tmp_path)
    artifact_size = artifact_path.stat().st_size

    with _client(tmp_path) as client:
        response = client.get(ENDPOINT, params={"dataset": DATASET})

    assert response.status_code == 200
    body = response.json()
    assert body["dataset"] == DATASET
    assert body["model_version"] == VERSION
    assert body["speed"]["selected_model"] == "persistence"
    assert body["congestion"]["selected_model"] == "hist_gradient_boosting"
    assert body["speed"]["mae"] == pytest.approx(1.428)
    assert body["speed"]["r2"] == pytest.approx(0.872)
    assert body["congestion"]["mae"] == pytest.approx(0.232)
    assert body["congestion"]["r2"] == pytest.approx(0.867)
    assert body["artifact_size_bytes"] == artifact_size
    assert body["training_samples"] == 10
    assert body["validation_samples"] == 2
    assert body["feature_count"] == 2
    assert body["speed_congestion_threshold"] == 20.0
    assert body["default_congestion_percentage_threshold"] == 50.0
    assert body["status_logic"] == STATUS_LOGIC
    assert body["source"] == "offline_evaluation_report_and_model_artifact"


def test_model_metadata_fails_closed_without_artifact(tmp_path: Path) -> None:
    _write_fixture(tmp_path, write_artifact=False)

    with _client(tmp_path) as client:
        response = client.get(ENDPOINT, params={"dataset": DATASET})

    assert response.status_code == 503
    assert response.json()["error"]["code"] == "model_metadata_unavailable"


def test_model_metadata_fails_closed_without_report(tmp_path: Path) -> None:
    _write_fixture(tmp_path, write_report=False)

    with _client(tmp_path) as client:
        response = client.get(ENDPOINT, params={"dataset": DATASET})

    assert response.status_code == 503
    assert response.json()["error"]["code"] == "model_metadata_unavailable"


def test_model_metadata_rejects_report_artifact_model_mismatch(tmp_path: Path) -> None:
    _write_fixture(tmp_path, report_congestion_model="random_forest")

    with _client(tmp_path) as client:
        response = client.get(ENDPOINT, params={"dataset": DATASET})

    assert response.status_code == 503
    assert response.json()["error"]["code"] == "model_metadata_mismatch"
