from datetime import datetime

from pydantic import BaseModel, ConfigDict

from backend.app.schemas.common import DatasetId


class MetricRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    dataset: DatasetId
    window_start: datetime
    window_end: datetime
    average_speed: float
    total_sensor_readings: int
    congested_sensor_count: int
    congestion_percentage: float
    processed_at: datetime


class MetricsPage(BaseModel):
    dataset: DatasetId
    range_start: datetime
    range_end: datetime
    limit: int
    has_more: bool
    next_cursor: datetime | None
    latest_metric_at: datetime | None
    freshness_seconds: float | None
    items: list[MetricRecord]


class DashboardKpis(BaseModel):
    dataset: DatasetId
    range_start: datetime
    range_end: datetime
    window_count: int
    total_sensor_readings: int
    congested_sensor_count: int
    weighted_average_speed: float | None
    weighted_congestion_percentage: float | None
    latest_metric: MetricRecord | None
    freshness_seconds: float | None


class CongestionTrendPoint(BaseModel):
    bucket_start: datetime
    window_count: int
    total_sensor_readings: int
    congested_sensor_count: int
    weighted_average_speed: float | None
    weighted_congestion_percentage: float | None


class CongestionTrends(BaseModel):
    dataset: DatasetId
    bucket: str
    range_start: datetime
    range_end: datetime
    latest_metric_at: datetime | None
    freshness_seconds: float | None
    items: list[CongestionTrendPoint]
