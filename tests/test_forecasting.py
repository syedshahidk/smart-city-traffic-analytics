from __future__ import annotations

import csv
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from analytics.forecasting import (
    FEATURE_COLUMNS,
    HORIZON_MINUTES,
    TARGET_COLUMNS,
    _congestion_status,
    build_features,
    build_supervised_frame,
    chronological_split,
    forecast_from_metrics,
    forecast_latest,
    load_latest_model,
    load_network_metrics,
    parse_args,
    persist_predictions,
    train_dataset,
)


def write_csv(path: Path, rows: list[list[object]]) -> Path:
    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(["timestamp", "sensor_a", "sensor_b"])
        writer.writerows(rows)
    return path


def make_rows(count: int = 240) -> list[list[object]]:
    start = pd.Timestamp("2017-01-01T00:00:00Z")
    rows = []
    for step in range(count):
        speed = 35.0 + 8.0 * np.sin(step / 11.0) + 2.0 * np.cos(step / 3.0)
        congestion = 10.0 + 8.0 * (1.0 + np.sin(step / 13.0))
        rows.append(
            [
                (start + pd.Timedelta(minutes=5 * step)).isoformat(),
                round(float(speed), 4),
                round(float(congestion), 4),
            ]
        )
    return rows


def test_network_metrics_match_threshold_and_zero_policy(tmp_path: Path) -> None:
    path = write_csv(
        tmp_path / "metr_la_cleaned.csv",
        [
            ["2012-03-01T00:00:00Z", 0, 20],
            ["2012-03-01T00:05:00Z", "bad", -1],
            ["2012-03-01T00:10:00Z", 10, float("inf")],
            ["2012-03-01T00:15:00Z", 21, ""],
            ["invalid-time", 5, 7],
        ],
    )
    prepared = load_network_metrics("metr-la", path)
    metrics = prepared.metrics
    first = metrics.loc[pd.Timestamp("2012-03-01T00:00:00Z")]
    assert first["average_speed"] == 10.0
    assert first["total_sensor_readings"] == 2
    assert first["congested_sensor_count"] == 1
    assert first["congestion_percentage"] == 50.0
    assert prepared.quality["zero_readings_preserved"] == 1
    assert prepared.quality["negative_readings_excluded"] == 1
    assert prepared.quality["infinite_readings_excluded"] == 1
    assert prepared.quality["invalid_timestamps_excluded"] == 1
    assert pd.Timestamp("2012-03-01T00:05:00Z") not in metrics.index


def test_exact_horizon_and_no_future_feature_leakage() -> None:
    metrics = pd.DataFrame(
        {
            "average_speed": np.arange(100, dtype=float),
            "total_sensor_readings": 5,
            "congested_sensor_count": np.arange(100, dtype=int) % 5,
            "congestion_percentage": np.arange(100, dtype=float),
        },
        index=pd.date_range("2017-01-01", periods=100, freq="5min", tz="UTC", name="timestamp"),
    )
    frame = build_supervised_frame(metrics)
    origin = metrics.index[30]
    sample = frame.loc[origin]
    assert sample["target_timestamp"] == origin + pd.Timedelta(minutes=HORIZON_MINUTES)
    assert sample[TARGET_COLUMNS["speed"]] == metrics.iloc[33]["average_speed"]
    assert sample["speed_lag_1"] == metrics.iloc[29]["average_speed"]

    altered = metrics.copy()
    altered.loc[origin + pd.Timedelta(minutes=5), "average_speed"] = -999.0
    assert build_features(altered).loc[origin, FEATURE_COLUMNS].equals(
        build_features(metrics).loc[origin, FEATURE_COLUMNS]
    )


def test_gap_does_not_shift_target_by_row_position() -> None:
    idx = pd.date_range("2017-01-01", periods=100, freq="5min", tz="UTC", name="timestamp")
    metrics = pd.DataFrame(
        {
            "average_speed": np.arange(len(idx) - 1, dtype=float),
            "total_sensor_readings": 3,
            "congested_sensor_count": 1,
            "congestion_percentage": 33.3,
        },
        index=idx.delete(33),
    )
    samples = build_supervised_frame(metrics)
    assert idx[30] not in samples.index


def test_chronological_split_has_no_target_overlap() -> None:
    metrics = pd.DataFrame(
        {
            "average_speed": np.arange(240, dtype=float) + 10,
            "total_sensor_readings": 3,
            "congested_sensor_count": 1,
            "congestion_percentage": (np.arange(240) % 100).astype(float),
        },
        index=pd.date_range("2017-01-01", periods=240, freq="5min", tz="UTC", name="timestamp"),
    )
    samples = build_supervised_frame(metrics)
    train, validation, test, _ = chronological_split(samples, metrics)
    assert train["target_timestamp"].max() < validation["target_timestamp"].min()
    assert validation["target_timestamp"].max() < test["target_timestamp"].min()


def test_train_artifact_and_latest_prediction_round_trip(tmp_path: Path) -> None:
    csv_path = write_csv(tmp_path / "pems_bay_cleaned.csv", make_rows())
    model_root = tmp_path / "models"
    report_root = tmp_path / "reports"
    report = train_dataset(
        "pems-bay",
        csv_path=csv_path,
        model_dir=model_root,
        report_dir=report_root,
    )
    assert report["prediction_horizon_minutes"] == 15
    assert report["models"]["speed"]["test_metrics"]["mae"] >= 0
    assert list((model_root / "pems-bay").glob("model_*.joblib"))
    saved_report = next(report_root.glob("traffic_forecast_pems-bay_*.json"))
    assert json.loads(saved_report.read_text(encoding="utf-8"))["dataset"] == "pems-bay"
    prediction = forecast_latest("pems-bay", csv_path=csv_path, model_dir=model_root)
    assert prediction["target_timestamp"] == (
        pd.Timestamp(prediction["forecast_origin"]) + pd.Timedelta(minutes=15)
    ).isoformat()
    assert prediction["prediction_horizon_minutes"] == 15
    assert 0.0 <= prediction["predicted_congestion_percentage"] <= 100.0
    assert prediction["congestion_status"] in {"congested", "not_congested"}


def test_prediction_refuses_insufficient_history(tmp_path: Path) -> None:
    csv_path = write_csv(tmp_path / "metr_la_cleaned.csv", make_rows(40))
    model_root = tmp_path / "models"
    train_dataset(
        "metr-la", csv_path=csv_path, model_dir=model_root, report_dir=tmp_path / "reports"
    )
    short_path = write_csv(tmp_path / "short.csv", make_rows(10))
    with pytest.raises(RuntimeError, match="Insufficient contiguous history"):
        forecast_latest("metr-la", csv_path=short_path, model_dir=model_root)


@pytest.mark.parametrize(
    ("predicted_percentage", "expected_status"),
    [
        (49.99, "not_congested"),
        (50.0, "congested"),
        (50.01, "congested"),
    ],
)
def test_congestion_status_percentage_threshold_boundaries(
    predicted_percentage: float, expected_status: str
) -> None:
    assert _congestion_status(predicted_percentage) == expected_status


def test_forecast_status_uses_percentage_and_keeps_numeric_predictions(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from analytics import forecasting

    class ConstantEstimator:
        def __init__(self, value: float):
            self.value = value

        def predict(self, features: pd.DataFrame) -> np.ndarray:
            return np.full(len(features), self.value)

    bundle = {
        "prediction_horizon_minutes": HORIZON_MINUTES,
        "congestion_threshold_speed_below": 20.0,
        "model_version": "test-version",
        "feature_columns": FEATURE_COLUMNS,
        "models": {
            "speed": {"name": "test_speed", "estimator": ConstantEstimator(75.0)},
            "congestion_percentage": {
                "name": "test_percentage",
                "estimator": ConstantEstimator(50.0),
            },
        },
    }
    artifact = tmp_path / "model_test.joblib"
    monkeypatch.setattr(
        forecasting, "load_latest_model", lambda dataset, model_dir: (bundle, artifact)
    )
    csv_path = write_csv(tmp_path / "metr_la_cleaned.csv", make_rows())

    prediction = forecast_latest("metr-la", csv_path=csv_path)

    assert prediction["predicted_average_speed"] == 75.0
    assert prediction["predicted_congestion_percentage"] == 50.0
    assert prediction["congestion_percentage_threshold"] == 50.0
    assert prediction["congestion_status"] == "congested"


def test_metrics_inference_preserves_training_feature_order_and_exact_horizon(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from analytics import forecasting

    captured: list[pd.DataFrame] = []

    class CaptureEstimator:
        def predict(self, features: pd.DataFrame) -> np.ndarray:
            captured.append(features.copy())
            return np.array([35.0])

    bundle = {
        "schema_version": 1,
        "dataset": "metr-la",
        "model_version": "test-2026",
        "prediction_horizon_minutes": HORIZON_MINUTES,
        "congestion_threshold_speed_below": 20.0,
        "feature_columns": FEATURE_COLUMNS,
        "models": {
            "speed": {"name": "test-speed", "estimator": CaptureEstimator()},
            "congestion_percentage": {
                "name": "test-congestion", "estimator": CaptureEstimator()
            },
        },
    }
    artifact = Path("model_20261123T112833179040Z.joblib")
    monkeypatch.setattr(forecasting, "load_latest_model", lambda dataset, model_dir: (bundle, artifact))
    metrics = pd.DataFrame(
        {
            "average_speed": np.arange(13, dtype=float) + 30.0,
            "congestion_percentage": np.full(13, 20.0),
        },
        index=pd.date_range("2026-01-01T00:00:00Z", periods=13, freq="5min", name="timestamp"),
    )

    result = forecast_from_metrics("metr-la", metrics)

    assert len(captured) == 2
    assert all(frame.shape == (1, 28) for frame in captured)
    assert all(frame.columns.tolist() == FEATURE_COLUMNS for frame in captured)
    expected = build_features(metrics).loc[[metrics.index[-1]], FEATURE_COLUMNS]
    pd.testing.assert_frame_equal(captured[0], expected)
    assert result["forecast_origin"] == metrics.index[-1].isoformat()
    assert result["target_timestamp"] == (metrics.index[-1] + pd.Timedelta(minutes=15)).isoformat()
    assert result["model_version"] == "test-2026"


def test_predict_cli_accepts_congestion_percentage_threshold() -> None:
    args = parse_args(
        ["predict", "--dataset", "metr-la", "--congestion-percentage-threshold", "37.5"]
    )
    assert args.congestion_percentage_threshold == 37.5


def test_load_latest_model_reuses_cached_artifact(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import joblib

    dataset = f"cache-{tmp_path.name}"
    artifact = tmp_path / dataset / "model_20261123T100000000000Z.joblib"
    artifact.parent.mkdir()
    artifact.write_bytes(b"test artifact")
    loads: list[Path] = []

    def fake_load(path: Path) -> dict[str, object]:
        loads.append(Path(path))
        return {"dataset": dataset, "schema_version": 1}

    monkeypatch.setattr(joblib, "load", fake_load)
    first, first_path = load_latest_model(dataset, tmp_path)
    second, second_path = load_latest_model(dataset, tmp_path)

    assert first is second
    assert first_path == second_path == artifact
    assert loads == [artifact]


def test_load_latest_model_loads_new_artifact_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import joblib

    dataset = f"cache-{tmp_path.name}"
    dataset_dir = tmp_path / dataset
    dataset_dir.mkdir()
    old_artifact = dataset_dir / "model_20261123T100000000000Z.joblib"
    old_artifact.write_bytes(b"old artifact")
    loads: list[Path] = []

    def fake_load(path: Path) -> dict[str, object]:
        artifact_path = Path(path)
        loads.append(artifact_path)
        return {"dataset": dataset, "schema_version": 1, "loaded_from": artifact_path.name}

    monkeypatch.setattr(joblib, "load", fake_load)
    old_bundle, _ = load_latest_model(dataset, tmp_path)
    new_artifact = dataset_dir / "model_20261123T110000000000Z.joblib"
    new_artifact.write_bytes(b"new artifact")

    new_bundle, new_path = load_latest_model(dataset, tmp_path)

    assert new_path == new_artifact
    assert new_bundle is not old_bundle
    assert new_bundle["loaded_from"] == new_artifact.name
    assert loads == [old_artifact, new_artifact]


def test_load_latest_model_cache_is_separate_by_dataset(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import joblib

    datasets = (f"cache-a-{tmp_path.name}", f"cache-b-{tmp_path.name}")
    paths = []
    for dataset in datasets:
        artifact = tmp_path / dataset / "model_20261123T100000000000Z.joblib"
        artifact.parent.mkdir()
        artifact.write_bytes(b"test artifact")
        paths.append(artifact)
    loads: list[Path] = []

    def fake_load(path: Path) -> dict[str, object]:
        artifact_path = Path(path)
        loads.append(artifact_path)
        return {"dataset": artifact_path.parent.name, "schema_version": 1}

    monkeypatch.setattr(joblib, "load", fake_load)
    first, _ = load_latest_model(datasets[0], tmp_path)
    second, _ = load_latest_model(datasets[1], tmp_path)

    assert first is not second
    assert first["dataset"] == datasets[0]
    assert second["dataset"] == datasets[1]
    assert loads == paths


def test_postgres_persistence_is_parameterized_and_idempotent(monkeypatch: pytest.MonkeyPatch) -> None:
    from types import ModuleType

    class Cursor:
        def __init__(self):
            self.executed = []
            self.many = []
        def __enter__(self): return self
        def __exit__(self, *_): return False
        def execute(self, sql): self.executed.append(sql)
        def executemany(self, sql, rows): self.many.append((sql, rows))

    class Connection:
        def __init__(self): self.cursor_obj = Cursor(); self.closed = False
        def __enter__(self): return self
        def __exit__(self, *_): return False
        def cursor(self): return self.cursor_obj
        def close(self): self.closed = True

    connection = Connection()
    fake_driver = ModuleType("psycopg2")
    fake_driver.connect = lambda **kwargs: connection
    monkeypatch.setitem(__import__("sys").modules, "psycopg2", fake_driver)
    for key, value in {
        "POSTGRES_HOST": "postgres", "POSTGRES_PORT": "5432",
        "POSTGRES_DB": "traffic_analytics", "POSTGRES_USER": "traffic_user",
        "POSTGRES_PASSWORD": "test-only",
    }.items():
        monkeypatch.setenv(key, value)
    persist_predictions(
        [
            {
                "dataset": "metr-la", "forecast_origin": "2012-03-01T00:00:00+00:00",
                "target_timestamp": "2012-03-01T00:15:00+00:00",
                "prediction_horizon_minutes": 15, "predicted_average_speed": 25.0,
                "predicted_congestion_percentage": 12.5, "congestion_status": "not_congested",
                "selected_models": {"speed": "random_forest", "congestion_percentage": "persistence"},
                "congestion_threshold_speed_below": 20.0, "model_artifact": "models/model.joblib",
            }
        ]
    )
    assert "CREATE TABLE IF NOT EXISTS traffic_predictions" in connection.cursor_obj.executed[0]
    sql, rows = connection.cursor_obj.many[0]
    assert "%s" in sql and "ON CONFLICT" in sql
    assert rows[0][0] == "metr-la"
    assert connection.closed
