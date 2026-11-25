from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict

from backend.app.schemas.common import DatasetId


class LatestForecast(BaseModel):
    model_config = ConfigDict(extra="forbid")

    dataset: DatasetId
    forecast_origin: datetime
    target_timestamp: datetime
    prediction_horizon_minutes: Literal[15]
    predicted_average_speed: float
    predicted_congestion_percentage: float
    congestion_status: Literal["congested", "not_congested"]
    congestion_threshold_speed_below: float
    congestion_percentage_threshold: float
    speed_model: str
    congestion_model: str
    model_version: str
    input_window_count: int
    input_window_start: datetime
    input_window_end: datetime
    latest_event_timestamp: datetime
    freshness_seconds: float
    freshness_limit_seconds: int
    data_provenance: Literal["historical_dataset_replay_simulated"]
    is_live_traffic: Literal[False]
