from typing import Any, Literal

from pydantic import BaseModel, ConfigDict

from backend.app.schemas.common import DatasetId


class ModelEvaluation(BaseModel):
    model_config = ConfigDict(extra="forbid")

    selected_model: str
    mae: float
    r2: float
    test_samples: int
    parameters: dict[str, Any]


class ModelMetadata(BaseModel):
    model_config = ConfigDict(extra="forbid")

    dataset: DatasetId
    model_version: str
    artifact_path: str
    artifact_size_bytes: int
    report_path: str
    prediction_horizon_minutes: int
    window_minutes: int
    speed: ModelEvaluation
    congestion: ModelEvaluation
    training_samples: int
    validation_samples: int
    feature_count: int
    speed_congestion_threshold: float
    default_congestion_percentage_threshold: float
    status_logic: str
    source: Literal["offline_evaluation_report_and_model_artifact"]
