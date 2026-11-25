from datetime import datetime, timezone
from typing import Any

from backend.app.core.config import Settings
from backend.app.db import queries
from backend.app.db.pool import DatabasePool
from backend.app.schemas.metrics import DashboardKpis, MetricRecord
from backend.app.services.common import validate_dataset


def get_dashboard_kpis(database: DatabasePool, dataset: str,
                       start: datetime, end: datetime) -> DashboardKpis:
    dataset = validate_dataset(dataset)
    row = queries.get_dashboard_aggregate(database, dataset, start, end)
    if row is None:
        row = {}
    readings = int(row.get("total_sensor_readings") or 0)
    congested = int(row.get("congested_sensor_count") or 0)
    windows = int(row.get("window_count") or 0)
    weighted_speed = (float(row.get("weighted_speed_sum") or 0.0) / readings
                      if readings else None)
    weighted_congestion = (congested * 100.0 / readings if readings else None)
    latest = None
    if row.get("latest_dataset") is not None:
        latest = MetricRecord(
            dataset=row["latest_dataset"],
            window_start=row["latest_window_start"],
            window_end=row["latest_window_end_value"],
            average_speed=row["latest_average_speed"],
            total_sensor_readings=row["latest_total_sensor_readings"],
            congested_sensor_count=row["latest_congested_sensor_count"],
            congestion_percentage=row["latest_congestion_percentage"],
            processed_at=row["latest_processed_at"],
        )
    latest_end = row.get("latest_window_end")
    age = None if latest_end is None else max(
        0.0, (datetime.now(timezone.utc) - latest_end.astimezone(timezone.utc)).total_seconds()
    )
    return DashboardKpis(
        dataset=dataset, range_start=start, range_end=end, window_count=windows,
        total_sensor_readings=readings, congested_sensor_count=congested,
        weighted_average_speed=weighted_speed,
        weighted_congestion_percentage=weighted_congestion,
        latest_metric=latest, freshness_seconds=round(age, 3) if age is not None else None,
    )



