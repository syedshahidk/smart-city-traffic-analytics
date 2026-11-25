from datetime import datetime

from pydantic import BaseModel

from backend.app.schemas.common import DatasetId


class DatasetInfo(BaseModel):
    dataset: DatasetId
    display_name: str
    sensor_count: int
    training_start: datetime
    training_end: datetime
    metric_row_count: int
    first_window_start: datetime | None
    latest_window_end: datetime | None
    last_processed_at: datetime | None
    freshness_seconds: float | None
    model_available: bool
    model_version: str | None
