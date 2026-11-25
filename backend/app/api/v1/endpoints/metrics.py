from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, Depends, Query

from backend.app.api.dependencies import get_database, get_settings
from backend.app.core.config import Settings
from backend.app.db import queries
from backend.app.db.pool import DatabasePool
from backend.app.schemas.common import DatasetId
from backend.app.schemas.metrics import MetricsPage, MetricRecord
from backend.app.services.common import freshness_seconds, validate_cursor, validate_time_range
from backend.app.services.datasets import validate_dataset

router = APIRouter(prefix="/metrics", tags=["metrics"])


@router.get("", response_model=MetricsPage)
def metrics(dataset: DatasetId, start: datetime | None = None,
            end: datetime | None = None, after: datetime | None = None,
            limit: Annotated[int, Query(ge=1, le=500)] = 100,
            database: DatabasePool = Depends(get_database),
            settings: Settings = Depends(get_settings)) -> MetricsPage:
    dataset = validate_dataset(dataset)
    start, end = validate_time_range(start, end, settings.max_query_range_days)
    after = validate_cursor(after, start, end)
    rows = queries.get_metric_page(database, dataset, start, end, after, limit + 1)
    latest_at = queries.get_latest_window_end(database, dataset)
    has_more = len(rows) > limit
    page = rows[:limit]
    next_cursor = page[-1]["window_start"] if has_more and page else None
    return MetricsPage(dataset=dataset, range_start=start, range_end=end, limit=limit,
                       has_more=has_more, next_cursor=next_cursor,
                       latest_metric_at=latest_at, freshness_seconds=freshness_seconds(latest_at),
                       items=[MetricRecord(**row) for row in page])
