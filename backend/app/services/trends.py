from datetime import datetime

from backend.app.core.errors import APIError
from backend.app.db import queries
from backend.app.db.pool import DatabasePool
from backend.app.schemas.metrics import CongestionTrendPoint, CongestionTrends
from backend.app.services.common import freshness_seconds, validate_dataset

BUCKET_INTERVALS = {"5m": "5 minutes", "15m": "15 minutes", "1h": "1 hour", "1d": "1 day"}


def get_congestion_trends(database: DatabasePool, dataset: str, start: datetime,
                          end: datetime, bucket: str) -> CongestionTrends:
    dataset = validate_dataset(dataset)
    interval = BUCKET_INTERVALS.get(bucket)
    if interval is None:
        raise APIError(422, "invalid_bucket", "bucket must be one of: 5m, 15m, 1h, 1d")
    rows = queries.get_congestion_buckets(database, dataset, start, end, interval)
    latest_at = queries.get_latest_window_end(database, dataset)
    points = []
    for row in rows:
        readings = int(row["total_sensor_readings"] or 0)
        congested = int(row["congested_sensor_count"] or 0)
        points.append(CongestionTrendPoint(
            bucket_start=row["bucket_start"],
            window_count=int(row["window_count"]),
            total_sensor_readings=readings,
            congested_sensor_count=congested,
            weighted_average_speed=(float(row["weighted_speed_sum"]) / readings if readings else None),
            weighted_congestion_percentage=(congested * 100.0 / readings if readings else None),
        ))
    return CongestionTrends(dataset=dataset, bucket=bucket, range_start=start,
                            range_end=end, latest_metric_at=latest_at,
                            freshness_seconds=freshness_seconds(latest_at), items=points)
