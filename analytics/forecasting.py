"""Train and run network-level, 15-minute traffic forecasts.

Each processed CSV row represents a five-minute sensor snapshot. Network
metrics match ``stream_processing/traffic_stream.py``: finite, non-negative
speeds are valid (including zero), congestion is speed < 20.0, and congestion
percentage divides congested valid readings by all valid readings. Features
use the current and past 60 minutes only. Labels are looked up at the exact
timestamp 15 minutes after each feature timestamp, so gaps never shift the
forecast horizon by row position.

Run with ``python -m analytics.forecasting train --dataset both`` or
``python -m analytics.forecasting predict --dataset both``.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import logging
import math
import os
from pathlib import Path
import sys
import threading
from typing import Any, Iterable

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingRegressor, RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "datasets" / "processed"
DEFAULT_MODEL_DIR = PROJECT_ROOT / "models" / "traffic_forecasting"
DEFAULT_REPORT_DIR = PROJECT_ROOT / "analytics" / "reports"
DEFAULT_THRESHOLD = 20.0
# A network is categorized as congested when at least half of its valid sensors
# are predicted to be below DEFAULT_THRESHOLD. Callers can override this at prediction time.
DEFAULT_CONGESTION_PERCENTAGE_THRESHOLD = 50.0
HORIZON_MINUTES = 15
INTERVAL_MINUTES = 5
LAG_STEPS = (0, 1, 2, 3, 6, 12)
ROLLING_STEPS = (3, 6, 12)
DATASET_FILES = {
    "metr-la": "metr_la_cleaned.csv",
    "pems-bay": "pems_bay_cleaned.csv",
}
FEATURE_DEFINITIONS = {
    **{
        f"{metric}_lag_{step}": (
            "value at the prediction time" if step == 0
            else f"value exactly {step * INTERVAL_MINUTES} minutes before prediction time"
        )
        for metric in ("speed", "congestion_percentage")
        for step in LAG_STEPS
    },
    **{
        f"{metric}_past_{steps * INTERVAL_MINUTES}m_{stat}":
        f"{stat} over the preceding {steps} valid five-minute grid slots, ending before prediction time"
        for metric in ("speed", "congestion_percentage")
        for steps in ROLLING_STEPS
        for stat in ("mean", "std")
    },
    "time_of_day_sin": "sine of UTC minute-of-day angle at prediction time",
    "time_of_day_cos": "cosine of UTC minute-of-day angle at prediction time",
    "day_of_week_sin": "sine of UTC weekday angle at prediction time",
    "day_of_week_cos": "cosine of UTC weekday angle at prediction time",
}
FEATURE_COLUMNS = list(FEATURE_DEFINITIONS)
MODEL_CONFIGURATIONS = {
    "persistence": {"rule": "use the metric observed at the forecast origin"},
    "random_forest": {
        "n_estimators": 160,
        "max_features": 0.8,
        "min_samples_leaf": 2,
        "n_jobs": -1,
        "random_state": 42,
    },
    "hist_gradient_boosting": {
        "max_iter": 160,
        "learning_rate": 0.08,
        "max_leaf_nodes": 15,
        "l2_regularization": 1.0,
        "random_state": 42,
    },
}
TARGET_COLUMNS = {
    "speed": "target_average_speed",
    "congestion_percentage": "target_congestion_percentage",
}
BASELINE_COLUMNS = {
    "speed": "speed_lag_0",
    "congestion_percentage": "congestion_percentage_lag_0",
}
LOGGER = logging.getLogger("traffic_forecasting")
_MODEL_CACHE: dict[str, tuple[tuple[str, int, int], dict[str, Any]]] = {}
_MODEL_CACHE_LOCK = threading.RLock()


class ForecastingError(RuntimeError):
    """Raised when forecast data or model artifacts cannot be used safely."""


@dataclass
class DatasetPreparation:
    metrics: pd.DataFrame
    quality: dict[str, Any]


def _dataset_path(dataset: str, data_dir: Path = DATA_DIR) -> Path:
    try:
        filename = DATASET_FILES[dataset]
    except KeyError as exc:
        raise ValueError(f"Unsupported dataset {dataset!r}") from exc
    return data_dir / filename


def load_network_metrics(
    dataset: str,
    csv_path: Path | None = None,
    threshold: float = DEFAULT_THRESHOLD,
) -> DatasetPreparation:
    """Load a processed sensor CSV and reproduce the streaming network metrics.

    Rows with invalid/off-grid timestamps are excluded. Non-numeric, null,
    infinite, and negative readings are excluded from calculations. Zero
    readings remain valid and count as congested under the existing threshold.
    Duplicate timestamps are combined as multiple snapshot events would be by
    the streaming aggregation.
    """
    if not math.isfinite(threshold):
        raise ValueError("congestion threshold must be finite")
    source = csv_path or _dataset_path(dataset)
    if not source.is_file():
        raise FileNotFoundError(f"Processed dataset not found: {source}")

    frame = pd.read_csv(source, low_memory=False)
    if "timestamp" not in frame.columns:
        raise ForecastingError(f"Missing required timestamp column in {source}")
    sensor_columns = [column for column in frame.columns if column != "timestamp"]
    if not sensor_columns:
        raise ForecastingError(f"No sensor columns found in {source}")

    parsed_timestamps = pd.to_datetime(frame.pop("timestamp"), errors="coerce", utc=True)
    invalid_timestamp_count = int(parsed_timestamps.isna().sum())
    on_grid = parsed_timestamps.notna() & parsed_timestamps.eq(
        parsed_timestamps.dt.floor(f"{INTERVAL_MINUTES}min")
    )
    off_grid_count = int((parsed_timestamps.notna() & ~on_grid).sum())

    numeric = frame[sensor_columns].apply(pd.to_numeric, errors="coerce")
    original_missing = frame[sensor_columns].isna().to_numpy()
    values = numeric.to_numpy(dtype=np.float64, copy=False)
    missing_mask = np.isnan(values)
    valid_mask = np.isfinite(values) & (values >= 0.0)
    valid_counts = valid_mask.sum(axis=1, dtype=np.int64)
    speed_sums = np.where(valid_mask, values, 0.0).sum(axis=1, dtype=np.float64)
    congested_counts = (valid_mask & (values < threshold)).sum(axis=1, dtype=np.int64)

    summary: dict[str, Any] = {
        "dataset": dataset,
        "source_file": str(source),
        "source_rows": int(len(frame)),
        "sensor_count": int(len(sensor_columns)),
        "invalid_timestamps_excluded": invalid_timestamp_count,
        "off_grid_timestamps_excluded": off_grid_count,
        "missing_readings": int((missing_mask & original_missing).sum()),
        "non_numeric_readings_excluded": int((missing_mask & ~original_missing).sum()),
        "infinite_readings_excluded": int(np.isinf(values).sum()),
        "negative_readings_excluded": int((np.isfinite(values) & (values < 0.0)).sum()),
        "zero_readings_preserved": int((valid_mask & (values == 0.0)).sum()),
        "rows_without_valid_readings": int((valid_counts == 0).sum()),
        "threshold_speed_below": float(threshold),
        "interval_minutes": INTERVAL_MINUTES,
    }

    eligible = on_grid.to_numpy() & (valid_counts > 0)
    if not eligible.any():
        raise ForecastingError(f"No timestamps with valid sensor readings in {source}")
    event_rows = pd.DataFrame(
        {
            "timestamp": parsed_timestamps[eligible].to_numpy(),
            "speed_sum": speed_sums[eligible],
            "total_sensor_readings": valid_counts[eligible],
            "congested_sensor_count": congested_counts[eligible],
        }
    )
    grouped = event_rows.groupby("timestamp", sort=True, as_index=False).agg(
        speed_sum=("speed_sum", "sum"),
        total_sensor_readings=("total_sensor_readings", "sum"),
        congested_sensor_count=("congested_sensor_count", "sum"),
    )
    grouped["average_speed"] = (
        grouped["speed_sum"] / grouped["total_sensor_readings"]
    )
    grouped["congestion_percentage"] = (
        grouped["congested_sensor_count"]
        * 100.0
        / grouped["total_sensor_readings"]
    )
    metrics = grouped.set_index("timestamp").drop(columns="speed_sum")
    metrics.index = pd.DatetimeIndex(metrics.index, name="timestamp")
    metrics["total_sensor_readings"] = metrics["total_sensor_readings"].astype("int64")
    metrics["congested_sensor_count"] = metrics["congested_sensor_count"].astype("int64")
    summary.update(
        {
            "network_metric_timestamps": int(len(metrics)),
            "start_timestamp": metrics.index.min().isoformat(),
            "end_timestamp": metrics.index.max().isoformat(),
            "threshold_definition": "a valid sensor speed strictly below threshold is congested",
            "zero_policy": "zero is valid, included in averages/counts, and congested below threshold",
            "null_policy": "null, non-numeric, infinite, and negative speeds are excluded",
        }
    )
    return DatasetPreparation(metrics=metrics, quality=summary)


def build_features(metrics: pd.DataFrame) -> pd.DataFrame:
    """Create time-safe lag, rolling, and cyclical time features.

    Lags are aligned on a regular UTC five-minute grid. Missing timestamps are
    left missing, never forward-filled. Rolling statistics use only rows before
    the prediction timestamp; lag zero explicitly provides the current metric.
    """
    required = {"average_speed", "congestion_percentage"}
    if not required.issubset(metrics.columns):
        raise ForecastingError(f"Metrics missing columns: {sorted(required - set(metrics.columns))}")
    if metrics.empty:
        return pd.DataFrame(columns=FEATURE_COLUMNS, index=metrics.index)
    ordered = metrics.sort_index()
    if ordered.index.has_duplicates:
        raise ForecastingError("Network metric timestamps must be unique before feature generation")
    if not isinstance(ordered.index, pd.DatetimeIndex) or ordered.index.tz is None:
        raise ForecastingError("Metrics require a timezone-aware DatetimeIndex")
    if not ordered.index.equals(ordered.index.floor(f"{INTERVAL_MINUTES}min")):
        raise ForecastingError("Metric timestamps must align to five-minute boundaries")

    grid = pd.date_range(
        ordered.index.min(), ordered.index.max(), freq=f"{INTERVAL_MINUTES}min", tz="UTC", name="timestamp"
    )
    regular = ordered.reindex(grid)
    features = pd.DataFrame(index=grid)
    metric_sources = {
        "speed": regular["average_speed"].astype(float),
        "congestion_percentage": regular["congestion_percentage"].astype(float),
    }
    for name, series in metric_sources.items():
        for step in LAG_STEPS:
            features[f"{name}_lag_{step}"] = series.shift(step)
        past = series.shift(1)
        for steps in ROLLING_STEPS:
            rolling = past.rolling(window=steps, min_periods=steps)
            features[f"{name}_past_{steps * INTERVAL_MINUTES}m_mean"] = rolling.mean()
            features[f"{name}_past_{steps * INTERVAL_MINUTES}m_std"] = rolling.std(ddof=0)

    minute_of_day = grid.hour * 60 + grid.minute
    day_angle = 2.0 * np.pi * minute_of_day / 1440.0
    weekday_angle = 2.0 * np.pi * grid.day_of_week / 7.0
    features["time_of_day_sin"] = np.sin(day_angle)
    features["time_of_day_cos"] = np.cos(day_angle)
    features["day_of_week_sin"] = np.sin(weekday_angle)
    features["day_of_week_cos"] = np.cos(weekday_angle)
    return features[FEATURE_COLUMNS]


def build_supervised_frame(metrics: pd.DataFrame) -> pd.DataFrame:
    """Join features at time t to labels at the exact t + 15 minute timestamp."""
    features = build_features(metrics)
    future_times = features.index + pd.Timedelta(minutes=HORIZON_MINUTES)
    future = metrics.reindex(future_times)
    frame = features.copy()
    frame["target_timestamp"] = future_times
    frame[TARGET_COLUMNS["speed"]] = future["average_speed"].to_numpy()
    frame[TARGET_COLUMNS["congestion_percentage"]] = future[
        "congestion_percentage"
    ].to_numpy()
    frame["baseline_average_speed"] = frame[BASELINE_COLUMNS["speed"]]
    frame["baseline_congestion_percentage"] = frame[
        BASELINE_COLUMNS["congestion_percentage"]
    ]
    needed = FEATURE_COLUMNS + list(TARGET_COLUMNS.values()) + list(
        BASELINE_COLUMNS.values()
    )
    return frame.dropna(subset=needed)


def chronological_split(
    samples: pd.DataFrame, metrics: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, str]]:
    """Split by target time at 70/15/15 chronological boundaries."""
    if metrics.empty:
        raise ForecastingError("Cannot split an empty time series")
    timeline = pd.date_range(
        metrics.index.min(), metrics.index.max(), freq=f"{INTERVAL_MINUTES}min", tz="UTC"
    )
    if len(timeline) < 20:
        raise ForecastingError("At least 20 five-minute timestamps are required for chronological splits")
    validation_boundary = timeline[min(int(len(timeline) * 0.70), len(timeline) - 1)]
    test_boundary = timeline[min(int(len(timeline) * 0.85), len(timeline) - 1)]
    train = samples[samples["target_timestamp"] < validation_boundary]
    validation = samples[
        (samples["target_timestamp"] >= validation_boundary)
        & (samples["target_timestamp"] < test_boundary)
    ]
    test = samples[samples["target_timestamp"] >= test_boundary]
    if min(len(train), len(validation), len(test)) < 2:
        raise ForecastingError(
            "Chronological split produced fewer than two samples in a partition; "
            "provide more usable history or inspect missing observations"
        )
    ranges = {
        "train_target_start": train["target_timestamp"].min().isoformat(),
        "train_target_end": train["target_timestamp"].max().isoformat(),
        "validation_target_start": validation["target_timestamp"].min().isoformat(),
        "validation_target_end": validation["target_timestamp"].max().isoformat(),
        "test_target_start": test["target_timestamp"].min().isoformat(),
        "test_target_end": test["target_timestamp"].max().isoformat(),
        "validation_boundary": validation_boundary.isoformat(),
        "test_boundary": test_boundary.isoformat(),
    }
    return train, validation, test, ranges


def _new_models() -> dict[str, Any]:
    return {
        "random_forest": RandomForestRegressor(
            n_estimators=160,
            max_features=0.8,
            min_samples_leaf=2,
            n_jobs=-1,
            random_state=42,
        ),
        "hist_gradient_boosting": HistGradientBoostingRegressor(
            max_iter=160,
            learning_rate=0.08,
            max_leaf_nodes=15,
            l2_regularization=1.0,
            random_state=42,
        ),
    }


def _bounded_predictions(target: str, values: Iterable[float]) -> np.ndarray:
    array = np.asarray(list(values), dtype=float)
    if target == "speed":
        return np.maximum(array, 0.0)
    return np.clip(array, 0.0, 100.0)


def regression_metrics(actual: Iterable[float], predicted: Iterable[float]) -> dict[str, float | None]:
    actual_array = np.asarray(list(actual), dtype=float)
    predicted_array = np.asarray(list(predicted), dtype=float)
    result: dict[str, float | None] = {
        "mae": float(mean_absolute_error(actual_array, predicted_array)),
        "rmse": float(np.sqrt(mean_squared_error(actual_array, predicted_array))),
        "r2": None,
    }
    if len(actual_array) >= 2 and float(np.var(actual_array)) > 0.0:
        result["r2"] = float(r2_score(actual_array, predicted_array))
    return result


def _stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")


def _display_path(path: Path) -> str:
    try:
        return str(path.relative_to(PROJECT_ROOT))
    except ValueError:
        return str(path)


def train_dataset(
    dataset: str,
    *,
    csv_path: Path | None = None,
    model_dir: Path = DEFAULT_MODEL_DIR,
    report_dir: Path = DEFAULT_REPORT_DIR,
    threshold: float = DEFAULT_THRESHOLD,
) -> dict[str, Any]:
    """Train per-target candidates, select on validation, and evaluate on test."""
    prepared = load_network_metrics(dataset, csv_path, threshold)
    metrics = prepared.metrics
    samples = build_supervised_frame(metrics)
    train, validation, test, ranges = chronological_split(samples, metrics)
    x_train = train[FEATURE_COLUMNS]
    x_validation = validation[FEATURE_COLUMNS]
    x_test = test[FEATURE_COLUMNS]
    fit_features = pd.concat([x_train, x_validation])
    stamp = _stamp()
    model_dir = model_dir / dataset
    report_dir.mkdir(parents=True, exist_ok=True)
    model_dir.mkdir(parents=True, exist_ok=True)
    artifact_path = model_dir / f"model_{stamp}.joblib"
    report_path = report_dir / f"traffic_forecast_{dataset}_{stamp}.json"

    selected_models: dict[str, dict[str, Any]] = {}
    evaluation: dict[str, Any] = {}
    for target_name, target_column in TARGET_COLUMNS.items():
        actual_validation = validation[target_column].to_numpy(dtype=float)
        baseline_validation = validation[BASELINE_COLUMNS[target_name]].to_numpy(dtype=float)
        candidates: dict[str, tuple[Any | None, np.ndarray]] = {
            "persistence": (None, baseline_validation)
        }
        for model_name, model in _new_models().items():
            model.fit(x_train, train[target_column].to_numpy(dtype=float))
            candidates[model_name] = (
                model,
                _bounded_predictions(target_name, model.predict(x_validation)),
            )

        validation_results = {
            model_name: regression_metrics(actual_validation, prediction)
            for model_name, (_, prediction) in candidates.items()
        }
        selected_name = min(
            validation_results,
            key=lambda name: validation_results[name]["mae"],
        )
        chosen, _ = candidates[selected_name]
        if chosen is not None:
            chosen.fit(
                fit_features,
                pd.concat([train[target_column], validation[target_column]]).to_numpy(dtype=float),
            )
            test_predictions = _bounded_predictions(target_name, chosen.predict(x_test))
        else:
            test_predictions = _bounded_predictions(
                target_name, test[BASELINE_COLUMNS[target_name]].to_numpy(dtype=float)
            )
        actual_test = test[target_column].to_numpy(dtype=float)
        selected_models[target_name] = {
            "name": selected_name,
            "estimator": chosen,
            "validation_metrics": validation_results[selected_name],
        }
        evaluation[target_name] = {
            "validation_comparison": validation_results,
            "selected_model": selected_name,
            "test_metrics": regression_metrics(actual_test, test_predictions),
            "test_sample_count": int(len(test)),
        }

    bundle = {
        "schema_version": 1,
        "dataset": dataset,
        "model_version": stamp,
        "trained_at_utc": datetime.now(timezone.utc).isoformat(),
        "prediction_horizon_minutes": HORIZON_MINUTES,
        "interval_minutes": INTERVAL_MINUTES,
        "congestion_threshold_speed_below": float(threshold),
        "congestion_status_logic": (
            "congested when predicted_congestion_percentage is greater than or equal "
            "to the configured congestion percentage threshold"
        ),
        "default_congestion_percentage_threshold": DEFAULT_CONGESTION_PERCENTAGE_THRESHOLD,
        "feature_columns": FEATURE_COLUMNS,
        "feature_definitions": FEATURE_DEFINITIONS,
        "model_configurations": MODEL_CONFIGURATIONS,
        "prediction_bounds": {
            "average_speed_minimum": 0.0,
            "congestion_percentage_minimum": 0.0,
            "congestion_percentage_maximum": 100.0,
        },
        "models": selected_models,
        "training_rows": int(len(train)),
        "validation_rows": int(len(validation)),
        "test_rows": int(len(test)),
        "split_ranges": ranges,
        "quality_summary": prepared.quality,
    }
    if artifact_path.exists() or report_path.exists():
        raise FileExistsError("Refusing to overwrite an existing model artifact or report")
    import joblib

    joblib.dump(bundle, artifact_path)
    report = {
        "dataset": dataset,
        "model_version": stamp,
        "model_artifact": _display_path(artifact_path),
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "prediction_horizon_minutes": HORIZON_MINUTES,
        "threshold_speed_below": float(threshold),
        "congestion_status_logic": bundle["congestion_status_logic"],
        "default_congestion_percentage_threshold": DEFAULT_CONGESTION_PERCENTAGE_THRESHOLD,
        "feature_definitions": FEATURE_DEFINITIONS,
        "model_configurations": MODEL_CONFIGURATIONS,
        "chronological_split": ranges,
        "sample_counts": {
            "train": int(len(train)),
            "validation": int(len(validation)),
            "test": int(len(test)),
            "usable_total": int(len(samples)),
            "grid_rows_without_complete_features_and_target": int(
                len(pd.date_range(
                    metrics.index.min(), metrics.index.max(),
                    freq=f"{INTERVAL_MINUTES}min", tz="UTC"
                )) - len(samples)
            ),
        },
        "data_quality": prepared.quality,
        "models": evaluation,
    }
    with report_path.open("x", encoding="utf-8") as report_file:
        json.dump(report, report_file, indent=2, allow_nan=False)
        report_file.write("\n")
    LOGGER.info(
        "Trained %s: speed=%s (test MAE %.4f), congestion=%s (test MAE %.4f); report=%s",
        dataset,
        evaluation["speed"]["selected_model"],
        evaluation["speed"]["test_metrics"]["mae"],
        evaluation["congestion_percentage"]["selected_model"],
        evaluation["congestion_percentage"]["test_metrics"]["mae"],
        report_path,
    )
    return report


def load_latest_model(dataset: str, model_dir: Path = DEFAULT_MODEL_DIR) -> tuple[dict[str, Any], Path]:
    """Load the newest timestamped model, reusing its in-process cached bundle."""
    directory = model_dir / dataset
    artifacts = sorted(directory.glob("model_*.joblib")) if directory.is_dir() else []
    if not artifacts:
        raise FileNotFoundError(
            f"No trained artifact for {dataset}; train it with the forecasting train command first"
        )
    path = artifacts[-1]
    stat = path.stat()
    artifact_identity = (str(path.resolve()), stat.st_mtime_ns, stat.st_size)
    with _MODEL_CACHE_LOCK:
        cached = _MODEL_CACHE.get(dataset)
        if cached is not None and cached[0] == artifact_identity:
            bundle = cached[1]
        else:
            import joblib

            bundle = joblib.load(path)
            if bundle.get("dataset") != dataset or bundle.get("schema_version") != 1:
                raise ForecastingError(f"Model artifact metadata is invalid: {path}")
            # Keep only the current artifact for this dataset to bound retained model memory.
            _MODEL_CACHE[dataset] = (artifact_identity, bundle)
    return bundle, path


def _congestion_status(
    predicted_congestion_percentage: float,
    threshold: float = DEFAULT_CONGESTION_PERCENTAGE_THRESHOLD,
) -> str:
    """Classify the network as congested at or above the configured sensor-share threshold."""
    if not math.isfinite(predicted_congestion_percentage):
        raise ValueError("predicted congestion percentage must be finite")
    if not math.isfinite(threshold) or not 0.0 <= threshold <= 100.0:
        raise ValueError("congestion percentage threshold must be finite and between 0 and 100")
    return (
        "congested"
        if predicted_congestion_percentage >= threshold
        else "not_congested"
    )


def forecast_latest(
    dataset: str,
    *,
    csv_path: Path | None = None,
    model_dir: Path = DEFAULT_MODEL_DIR,
    congestion_percentage_threshold: float = DEFAULT_CONGESTION_PERCENTAGE_THRESHOLD,
) -> dict[str, Any]:
    """Forecast from the latest timestamp with a complete 60-minute history.

    ``congestion_percentage_threshold`` classifies the forecast as congested when
    at least that percentage of individual sensors is predicted below the speed threshold.
    """
    _congestion_status(0.0, congestion_percentage_threshold)
    bundle, artifact_path = load_latest_model(dataset, model_dir)
    prepared = load_network_metrics(
        dataset, csv_path, float(bundle["congestion_threshold_speed_below"])
    )
    return _forecast_metrics_with_bundle(
        dataset, prepared.metrics, bundle, artifact_path, congestion_percentage_threshold
    )


def forecast_from_metrics(
    dataset: str,
    metrics: pd.DataFrame,
    *,
    model_dir: Path = DEFAULT_MODEL_DIR,
    congestion_percentage_threshold: float = DEFAULT_CONGESTION_PERCENTAGE_THRESHOLD,
) -> dict[str, Any]:
    """Forecast from network metrics indexed by UTC feature timestamps.

    This shares feature generation, artifact loading/caching, model selection,
    output bounds, and horizon semantics with CSV-based forecasting.
    """
    _congestion_status(0.0, congestion_percentage_threshold)
    bundle, artifact_path = load_latest_model(dataset, model_dir)
    return _forecast_metrics_with_bundle(
        dataset, metrics, bundle, artifact_path, congestion_percentage_threshold
    )


def _forecast_metrics_with_bundle(
    dataset: str,
    metrics: pd.DataFrame,
    bundle: dict[str, Any],
    artifact_path: Path,
    congestion_percentage_threshold: float,
) -> dict[str, Any]:
    if bundle.get("feature_columns") != FEATURE_COLUMNS:
        raise ForecastingError("Model artifact feature schema is incompatible")
    if bundle.get("prediction_horizon_minutes") != HORIZON_MINUTES:
        raise ForecastingError("Model artifact forecast horizon is incompatible")
    features = build_features(metrics)
    if metrics.empty:
        raise ForecastingError(f"No network metrics available for {dataset}")
    origin = metrics.index.max()
    if origin not in features.index or features.loc[origin].isna().any():
        raise ForecastingError(
            f"Insufficient contiguous history through latest timestamp {origin.isoformat()} "
            f"for the required lags and rolling windows; no forecast generated"
        )
    x = features.loc[[origin], bundle["feature_columns"]]
    outputs: dict[str, float] = {}
    model_names: dict[str, str] = {}
    for target_name, spec in bundle["models"].items():
        estimator = spec["estimator"]
        if estimator is None:
            feature_name = BASELINE_COLUMNS[target_name]
            value = float(x.iloc[0][feature_name])
        else:
            value = float(estimator.predict(x)[0])
        outputs[target_name] = float(_bounded_predictions(target_name, [value])[0])
        model_names[target_name] = spec["name"]
    threshold = float(bundle["congestion_threshold_speed_below"])
    target_time = origin + pd.Timedelta(minutes=HORIZON_MINUTES)
    return {
        "dataset": dataset,
        "forecast_origin": origin.isoformat(),
        "target_timestamp": target_time.isoformat(),
        "prediction_horizon_minutes": HORIZON_MINUTES,
        "predicted_average_speed": outputs["speed"],
        "predicted_congestion_percentage": outputs["congestion_percentage"],
        "congestion_status": _congestion_status(
            outputs["congestion_percentage"], congestion_percentage_threshold
        ),
        "congestion_threshold_speed_below": threshold,
        "congestion_percentage_threshold": float(congestion_percentage_threshold),
        "selected_models": model_names,
        "model_version": bundle.get("model_version", artifact_path.stem.removeprefix("model_")),
        "model_artifact": _display_path(artifact_path),
    }


CREATE_PREDICTIONS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS traffic_predictions (
    id BIGSERIAL PRIMARY KEY,
    dataset TEXT NOT NULL,
    forecast_origin TIMESTAMPTZ NOT NULL,
    target_timestamp TIMESTAMPTZ NOT NULL,
    prediction_horizon_minutes INTEGER NOT NULL,
    predicted_average_speed DOUBLE PRECISION NOT NULL,
    predicted_congestion_percentage DOUBLE PRECISION NOT NULL,
    congestion_status TEXT NOT NULL,
    speed_model TEXT NOT NULL,
    congestion_model TEXT NOT NULL,
    congestion_threshold DOUBLE PRECISION NOT NULL,
    model_artifact TEXT NOT NULL,
    generated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_traffic_prediction_origin_target
        UNIQUE (dataset, forecast_origin, target_timestamp)
)
"""
UPSERT_PREDICTION_SQL = """
INSERT INTO traffic_predictions (
    dataset, forecast_origin, target_timestamp, prediction_horizon_minutes,
    predicted_average_speed, predicted_congestion_percentage, congestion_status,
    speed_model, congestion_model, congestion_threshold, model_artifact
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (dataset, forecast_origin, target_timestamp) DO UPDATE SET
    prediction_horizon_minutes = EXCLUDED.prediction_horizon_minutes,
    predicted_average_speed = EXCLUDED.predicted_average_speed,
    predicted_congestion_percentage = EXCLUDED.predicted_congestion_percentage,
    congestion_status = EXCLUDED.congestion_status,
    speed_model = EXCLUDED.speed_model,
    congestion_model = EXCLUDED.congestion_model,
    congestion_threshold = EXCLUDED.congestion_threshold,
    model_artifact = EXCLUDED.model_artifact,
    generated_at = CURRENT_TIMESTAMP
"""


def persist_predictions(records: list[dict[str, Any]]) -> None:
    """Optionally upsert forecasts into a separate table, leaving metrics intact."""
    if not records:
        LOGGER.info("No predictions to persist")
        return
    required = ("POSTGRES_HOST", "POSTGRES_PORT", "POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD")
    missing = [name for name in required if not os.environ.get(name)]
    if missing:
        raise ForecastingError("Missing PostgreSQL environment variable(s): " + ", ".join(missing))
    try:
        import psycopg2
    except ImportError as exc:
        raise ForecastingError(
            "psycopg2-binary is required only when --persist-postgres is selected"
        ) from exc
    try:
        connection = psycopg2.connect(
            host=os.environ["POSTGRES_HOST"],
            port=int(os.environ["POSTGRES_PORT"]),
            dbname=os.environ["POSTGRES_DB"],
            user=os.environ["POSTGRES_USER"],
            password=os.environ["POSTGRES_PASSWORD"],
            connect_timeout=10,
            options="-c timezone=UTC",
        )
    except Exception as exc:
        raise ForecastingError(f"Could not connect to PostgreSQL for forecast persistence: {exc}") from exc
    rows = [
        (
            item["dataset"], item["forecast_origin"], item["target_timestamp"],
            item["prediction_horizon_minutes"], item["predicted_average_speed"],
            item["predicted_congestion_percentage"], item["congestion_status"],
            item["selected_models"]["speed"],
            item["selected_models"]["congestion_percentage"],
            item["congestion_threshold_speed_below"], item["model_artifact"],
        )
        for item in records
    ]
    try:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(CREATE_PREDICTIONS_TABLE_SQL)
                cursor.executemany(UPSERT_PREDICTION_SQL, rows)
    finally:
        connection.close()
    LOGGER.info("Upserted %d rows into traffic_predictions", len(rows))


def _timestamped_output(records: list[dict[str, Any]], output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = _stamp()
    datasets = "_".join(record["dataset"] for record in records)
    output = output_dir / f"traffic_forecasts_{datasets}_{stamp}.json"
    with output.open("x", encoding="utf-8") as output_file:
        json.dump(records, output_file, indent=2, allow_nan=False)
        output_file.write("\n")
    return output


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train and run 15-minute traffic forecasts.")
    subparsers = parser.add_subparsers(dest="command", required=True)
    train_parser = subparsers.add_parser("train", help="train separate dataset models and evaluate them")
    predict_parser = subparsers.add_parser("predict", help="predict from the latest historical observations")
    for subparser in (train_parser, predict_parser):
        subparser.add_argument("--dataset", choices=("metr-la", "pems-bay", "both"), default="both")
        subparser.add_argument("--data-dir", type=Path, default=DATA_DIR)
        subparser.add_argument("--model-dir", type=Path, default=DEFAULT_MODEL_DIR)
    train_parser.add_argument("--report-dir", type=Path, default=DEFAULT_REPORT_DIR)
    train_parser.add_argument("--threshold", type=float, default=DEFAULT_THRESHOLD)
    predict_parser.add_argument("--output-dir", type=Path, default=DEFAULT_REPORT_DIR)
    predict_parser.add_argument(
        "--congestion-percentage-threshold",
        type=float,
        default=DEFAULT_CONGESTION_PERCENTAGE_THRESHOLD,
        help=(
            "classify the forecast as congested when at least this percentage of sensors "
            "is predicted below the speed threshold (default: 50)"
        ),
    )
    predict_parser.add_argument(
        "--persist-postgres",
        action="store_true",
        help="opt in to idempotent writes to the separate traffic_predictions table",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    datasets = list(DATASET_FILES) if args.dataset == "both" else [args.dataset]
    try:
        if args.command == "train":
            for dataset in datasets:
                train_dataset(
                    dataset,
                    csv_path=_dataset_path(dataset, args.data_dir),
                    model_dir=args.model_dir,
                    report_dir=args.report_dir,
                    threshold=args.threshold,
                )
        else:
            predictions = [
                forecast_latest(
                    dataset,
                    csv_path=_dataset_path(dataset, args.data_dir),
                    model_dir=args.model_dir,
                    congestion_percentage_threshold=args.congestion_percentage_threshold,
                )
                for dataset in datasets
            ]
            output = _timestamped_output(predictions, args.output_dir)
            if args.persist_postgres:
                persist_predictions(predictions)
            print(json.dumps(predictions, indent=2))
            LOGGER.info("Forecast output saved to %s", output)
    except (ForecastingError, FileNotFoundError, ValueError, OSError) as exc:
        LOGGER.error("Forecasting failed: %s", exc)
        return 2
    except Exception:
        LOGGER.exception("Forecasting failed unexpectedly")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
