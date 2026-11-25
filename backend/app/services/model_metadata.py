from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any

from analytics import forecasting
from backend.app.core.config import Settings
from backend.app.core.errors import APIError
from backend.app.schemas.common import DatasetId
from backend.app.schemas.model_metadata import ModelEvaluation, ModelMetadata

STATUS_LOGIC = (
    "Network status uses predicted congestion percentage >= the configured percentage "
    "threshold. The 20 mph value classifies an individual sensor reading."
)


def _unavailable(message: str) -> APIError:
    return APIError(503, "model_metadata_unavailable", message)


def _mismatch(message: str) -> APIError:
    return APIError(503, "model_metadata_mismatch", message)


def _finite_number(value: Any, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise _unavailable(f"Model metadata contains an invalid {field}")
    number = float(value)
    if not math.isfinite(number):
        raise _unavailable(f"Model metadata contains an invalid {field}")
    return number


def _integer(value: Any, field: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise _unavailable(f"Model metadata contains an invalid {field}")
    return value


def _mapping(value: Any, field: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise _unavailable(f"Model metadata contains an invalid {field}")
    return value


def _target_evaluation(
    report: dict[str, Any],
    bundle: dict[str, Any],
    *,
    report_key: str,
    label: str,
) -> ModelEvaluation:
    report_target = _mapping(
        _mapping(report.get("models"), "models").get(report_key),
        f"{label} report entry",
    )
    artifact_target = _mapping(
        _mapping(bundle.get("models"), "artifact models").get(report_key),
        f"{label} artifact entry",
    )

    selected_model = report_target.get("selected_model")
    artifact_model = artifact_target.get("name")
    if not isinstance(selected_model, str) or not selected_model:
        raise _unavailable(f"Model metadata contains an invalid {label} selected model")
    if selected_model != artifact_model:
        raise _mismatch(
            f"{label} model in report ({selected_model}) does not match the served artifact "
            f"({artifact_model})"
        )

    metrics = _mapping(report_target.get("test_metrics"), f"{label} test metrics")
    mae = _finite_number(metrics.get("mae"), f"{label} test MAE")
    r2 = _finite_number(metrics.get("r2"), f"{label} test R²")
    test_samples = _integer(
        report_target.get("test_sample_count"), f"{label} test sample count", minimum=1
    )

    configurations = _mapping(report.get("model_configurations"), "model configurations")
    parameters = configurations.get(selected_model, {})
    if not isinstance(parameters, dict):
        parameters = {}

    return ModelEvaluation(
        selected_model=selected_model,
        mae=mae,
        r2=r2,
        test_samples=test_samples,
        parameters=parameters,
    )


def get_model_metadata(settings: Settings, dataset: str) -> ModelMetadata:
    """Return report-derived metadata cross-checked against the served model artifact."""
    if dataset not in ("metr-la", "pems-bay"):
        raise _unavailable(f"Unsupported model dataset: {dataset}")

    try:
        bundle, artifact_path = forecasting.load_latest_model(dataset, settings.models_dir)
    except (FileNotFoundError, OSError, forecasting.ForecastingError) as exc:
        raise _unavailable(
            f"Model artifact metadata is unavailable for {dataset}: {exc}"
        ) from exc

    if not isinstance(bundle, dict):
        raise _unavailable("Model artifact metadata is invalid")

    version = bundle.get("model_version")
    if not isinstance(version, str) or not version:
        version = artifact_path.stem.removeprefix("model_")
    if not version:
        raise _unavailable("Model artifact version is unavailable")

    report_path = settings.reports_dir / f"traffic_forecast_{dataset}_{version}.json"
    if not report_path.is_file():
        raise _unavailable(
            f"No evaluation report found for {dataset} artifact version {version}"
        )
    try:
        report = json.loads(report_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise _unavailable(f"Evaluation report could not be read: {exc}") from exc
    if not isinstance(report, dict):
        raise _unavailable("Evaluation report is invalid")

    if report.get("dataset") != dataset or report.get("model_version") != version:
        raise _mismatch("Evaluation report dataset or version does not match the served artifact")
    reported_artifact = report.get("model_artifact")
    if not isinstance(reported_artifact, str) or Path(reported_artifact).name != artifact_path.name:
        raise _mismatch("Evaluation report does not reference the served artifact")

    horizon = _finite_number(report.get("prediction_horizon_minutes"), "prediction horizon")
    artifact_horizon = _finite_number(
        bundle.get("prediction_horizon_minutes"), "artifact prediction horizon"
    )
    if not math.isclose(horizon, artifact_horizon):
        raise _mismatch("Evaluation report horizon does not match the served artifact")

    speed_threshold = _finite_number(report.get("threshold_speed_below"), "speed threshold")
    artifact_speed_threshold = _finite_number(
        bundle.get("congestion_threshold_speed_below"), "artifact speed threshold"
    )
    if not math.isclose(speed_threshold, artifact_speed_threshold):
        raise _mismatch("Evaluation report speed threshold does not match the served artifact")

    window_minutes = _finite_number(bundle.get("interval_minutes"), "artifact interval")
    if not math.isclose(window_minutes, 5.0):
        raise _unavailable("Served artifact does not use five-minute metric windows")

    report_features = _mapping(report.get("feature_definitions"), "feature definitions")
    artifact_features = bundle.get("feature_columns")
    if not isinstance(artifact_features, list):
        raise _unavailable("Model artifact feature schema is invalid")
    if not report_features or len(report_features) != len(artifact_features):
        raise _mismatch("Evaluation report feature count does not match the served artifact")

    sample_counts = _mapping(report.get("sample_counts"), "sample counts")
    training_samples = _integer(sample_counts.get("train"), "training sample count", minimum=1)
    validation_samples = _integer(
        sample_counts.get("validation"), "validation sample count", minimum=1
    )
    if training_samples != _integer(
        bundle.get("training_rows"), "artifact training row count", minimum=1
    ) or validation_samples != _integer(
        bundle.get("validation_rows"), "artifact validation row count", minimum=1
    ):
        raise _mismatch("Evaluation report split counts do not match the served artifact")

    percentage_threshold = forecasting.DEFAULT_CONGESTION_PERCENTAGE_THRESHOLD
    if not 0.0 <= percentage_threshold <= 100.0:
        raise _unavailable("Runtime congestion percentage threshold is invalid")

    speed = _target_evaluation(
        report, bundle, report_key="speed", label="Speed model"
    )
    congestion = _target_evaluation(
        report,
        bundle,
        report_key="congestion_percentage",
        label="Congestion model",
    )

    artifact_stat = artifact_path.stat()
    return ModelMetadata(
        dataset=dataset,
        model_version=version,
        artifact_path=str(artifact_path),
        artifact_size_bytes=artifact_stat.st_size,
        report_path=str(report_path),
        prediction_horizon_minutes=int(horizon),
        window_minutes=int(window_minutes),
        speed=speed,
        congestion=congestion,
        training_samples=training_samples,
        validation_samples=validation_samples,
        feature_count=len(artifact_features),
        speed_congestion_threshold=speed_threshold,
        default_congestion_percentage_threshold=float(percentage_threshold),
        status_logic=STATUS_LOGIC,
        source="offline_evaluation_report_and_model_artifact",
    )
