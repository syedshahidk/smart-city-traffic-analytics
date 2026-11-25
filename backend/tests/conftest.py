from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from fastapi.testclient import TestClient

from backend.app.core.config import Settings
from backend.app.core.errors import DatabaseUnavailable
from backend.app.main import create_app

NOW = datetime(2024, 1, 1, 1, 0, tzinfo=timezone.utc)


def metric_row(step: int) -> dict[str, Any]:
    start = datetime(2024, 1, 1, 0, step * 5, tzinfo=timezone.utc)
    return {
        "dataset": "metr-la", "window_start": start,
        "window_end": start.replace(minute=start.minute + 5),
        "average_speed": 40.0 + step, "total_sensor_readings": 10,
        "congested_sensor_count": 2, "congestion_percentage": 20.0,
        "processed_at": start.replace(minute=start.minute + 5),
    }


def forecast_metric_rows(dataset: str, *, latest_end: datetime = NOW) -> list[dict[str, Any]]:
    latest_end = latest_end.astimezone(timezone.utc)
    latest_start = latest_end - timedelta(minutes=5)
    first_start = latest_start - timedelta(minutes=60)
    return [
        {
            "dataset": dataset,
            "window_start": first_start + timedelta(minutes=5 * step),
            "window_end": first_start + timedelta(minutes=5 * (step + 1)),
            "average_speed": 40.0 + step,
            "total_sensor_readings": 10,
            "congested_sensor_count": 2,
            "congestion_percentage": 20.0,
            "processed_at": latest_end,
        }
        for step in range(13)
    ]


class FakeDatabase:
    def __init__(self, fail: bool = False) -> None:
        self.fail = fail
        self.calls: list[tuple[str, tuple[Any, ...]]] = []
        self.rollups = [
            {"dataset": name, "metric_row_count": 30,
             "first_window_start": datetime(2024, 1, 1, tzinfo=timezone.utc),
             "latest_window_end": NOW, "last_processed_at": NOW}
            for name in ("metr-la", "pems-bay")
        ]
        self.metric_rows = [metric_row(i) for i in range(3)]
        self.forecast_rows = {
            dataset: forecast_metric_rows(dataset)
            for dataset in ("metr-la", "pems-bay")
        }
        self.kpi = {
            "window_count": 2, "total_sensor_readings": 100,
            "congested_sensor_count": 25, "weighted_speed_sum": 4000.0,
            "first_window_start": datetime(2024, 1, 1, tzinfo=timezone.utc),
            "latest_window_end": NOW, "last_processed_at": NOW,
            "latest_dataset": "metr-la",
            "latest_window_start": datetime(2024, 1, 1, 0, 55, tzinfo=timezone.utc),
            "latest_window_end_value": NOW, "latest_average_speed": 39.0,
            "latest_total_sensor_readings": 10, "latest_congested_sensor_count": 3,
            "latest_congestion_percentage": 30.0, "latest_processed_at": NOW,
        }
        self.trend_rows = [{
            "bucket_start": datetime(2024, 1, 1, tzinfo=timezone.utc),
            "window_count": 2, "total_sensor_readings": 100,
            "congested_sensor_count": 25, "weighted_speed_sum": 4000.0,
        }]

    def _check(self) -> None:
        if self.fail:
            raise DatabaseUnavailable("PostgreSQL is unavailable")

    def check_ready(self) -> None:
        """Mirror DatabasePool readiness, including simulated DB failures."""
        result = self.fetch_one("SELECT to_regclass('public.traffic_metrics') AS table_name")
        if not result or result["table_name"] is None:
            raise DatabaseUnavailable("Required table public.traffic_metrics is unavailable")

    def fetch_one(self, sql: str, params: tuple[Any, ...] = ()) -> dict[str, Any] | None:
        self._check()
        self.calls.append((sql, params))
        if "to_regclass" in sql:
            return {"table_name": "traffic_metrics"}
        if "ORDER BY window_start DESC" in sql:
            return {"window_end": NOW}
        return self.kpi

    def fetch_all(self, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        self._check()
        self.calls.append((sql, params))
        if "GROUP BY dataset" in sql:
            return self.rollups
        if "date_bin" in sql:
            return self.trend_rows
        if "window_end <= %s" in sql and "LIMIT %s" in sql:
            dataset, completed_by, limit = params
            rows = [row for row in self.forecast_rows[dataset]
                    if row["window_end"] <= completed_by]
            return sorted(rows, key=lambda row: (row["window_start"], row["window_end"]),
                          reverse=True)[:limit]
        return self.metric_rows

    def close(self) -> None:
        return None


@pytest.fixture
def fake_database() -> FakeDatabase:
    return FakeDatabase()


@pytest.fixture
def client(tmp_path, fake_database):
    app = create_app(Settings(models_dir=tmp_path), database=fake_database)
    with TestClient(app) as test_client:
        yield test_client
