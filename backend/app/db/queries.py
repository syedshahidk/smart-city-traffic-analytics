from datetime import datetime
from typing import Any

from backend.app.db.pool import DatabasePool

METRICS_COLUMNS = """dataset, window_start, window_end, average_speed,
    total_sensor_readings, congested_sensor_count, congestion_percentage, processed_at"""


def list_dataset_rollups(database: DatabasePool, datasets: list[str]) -> list[dict[str, Any]]:
    sql = """
        SELECT dataset, COUNT(*)::bigint AS metric_row_count,
               MIN(window_start) AS first_window_start,
               MAX(window_end) AS latest_window_end,
               MAX(processed_at) AS last_processed_at
        FROM public.traffic_metrics
        WHERE dataset = ANY(%s)
        GROUP BY dataset
    """
    return database.fetch_all(sql, (datasets,))


def get_latest_window_end(database: DatabasePool, dataset: str) -> datetime | None:
    sql = """
        SELECT window_end
        FROM public.traffic_metrics
        WHERE dataset = %s
        ORDER BY window_start DESC, window_end DESC
        LIMIT 1
    """
    row = database.fetch_one(sql, (dataset,))
    return row["window_end"] if row else None


def get_metric_page(database: DatabasePool, dataset: str, start: datetime, end: datetime,
                    after: datetime | None, fetch_limit: int) -> list[dict[str, Any]]:
    sql = f"""
        SELECT {METRICS_COLUMNS}
        FROM public.traffic_metrics
        WHERE dataset = %s AND window_start >= %s AND window_start < %s
          AND (%s::timestamptz IS NULL OR window_start > %s)
        ORDER BY window_start ASC, window_end ASC
        LIMIT %s
    """
    return database.fetch_all(sql, (dataset, start, end, after, after, fetch_limit))


def get_latest_forecast_metrics(database: DatabasePool, dataset: str,
                                completed_by: datetime,
                                limit: int = 13) -> list[dict[str, Any]]:
    """Fetch a bounded, dataset-scoped set of completed windows for inference."""
    sql = """
        SELECT dataset, window_start, window_end, average_speed,
               total_sensor_readings, congested_sensor_count,
               congestion_percentage, processed_at
        FROM public.traffic_metrics
        WHERE dataset = %s AND window_end <= %s
        ORDER BY window_start DESC, window_end DESC
        LIMIT %s
    """
    return database.fetch_all(sql, (dataset, completed_by, limit))


def get_dashboard_aggregate(database: DatabasePool, dataset: str, start: datetime,
                           end: datetime) -> dict[str, Any] | None:
    sql = """
        WITH filtered AS (
            SELECT dataset, window_start, window_end, average_speed,
                   total_sensor_readings, congested_sensor_count,
                   congestion_percentage, processed_at
            FROM public.traffic_metrics
            WHERE dataset = %s AND window_start >= %s AND window_start < %s
        ), summary AS (
            SELECT COUNT(*)::bigint AS window_count,
                   COALESCE(SUM(total_sensor_readings), 0)::bigint AS total_sensor_readings,
                   COALESCE(SUM(congested_sensor_count), 0)::bigint AS congested_sensor_count,
                   COALESCE(SUM(average_speed * total_sensor_readings), 0.0)
                       AS weighted_speed_sum,
                   MIN(window_start) AS first_window_start,
                   MAX(window_end) AS latest_window_end,
                   MAX(processed_at) AS last_processed_at
            FROM filtered
        ), latest AS (
            SELECT dataset, window_start, window_end, average_speed,
                   total_sensor_readings, congested_sensor_count,
                   congestion_percentage, processed_at
            FROM filtered ORDER BY window_end DESC, window_start DESC LIMIT 1
        )
        SELECT summary.*, latest.dataset AS latest_dataset,
               latest.window_start AS latest_window_start,
               latest.window_end AS latest_window_end_value,
               latest.average_speed AS latest_average_speed,
               latest.total_sensor_readings AS latest_total_sensor_readings,
               latest.congested_sensor_count AS latest_congested_sensor_count,
               latest.congestion_percentage AS latest_congestion_percentage,
               latest.processed_at AS latest_processed_at
        FROM summary LEFT JOIN latest ON TRUE
    """
    return database.fetch_one(sql, (dataset, start, end))


def get_congestion_buckets(database: DatabasePool, dataset: str, start: datetime,
                           end: datetime, interval: str) -> list[dict[str, Any]]:
    sql = """
        SELECT date_bin(%s::interval, window_start,
                        TIMESTAMPTZ '2000-01-01 00:00:00+00') AS bucket_start,
               COUNT(*)::bigint AS window_count,
               SUM(total_sensor_readings)::bigint AS total_sensor_readings,
               SUM(congested_sensor_count)::bigint AS congested_sensor_count,
               SUM(average_speed * total_sensor_readings) AS weighted_speed_sum
        FROM public.traffic_metrics
        WHERE dataset = %s AND window_start >= %s AND window_start < %s
        GROUP BY bucket_start ORDER BY bucket_start ASC
    """
    return database.fetch_all(sql, (interval, dataset, start, end))
