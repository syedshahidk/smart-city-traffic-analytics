from datetime import datetime
from typing import Literal

from fastapi import APIRouter, Depends

from backend.app.api.dependencies import get_database, get_settings
from backend.app.core.config import Settings
from backend.app.db.pool import DatabasePool
from backend.app.schemas.common import DatasetId
from backend.app.schemas.metrics import CongestionTrends
from backend.app.services.common import validate_time_range
from backend.app.services.trends import get_congestion_trends

router = APIRouter(prefix="/congestion", tags=["congestion"])


@router.get("/trends", response_model=CongestionTrends)
def congestion_trends(dataset: DatasetId, start: datetime | None = None,
                      end: datetime | None = None,
                      bucket: Literal["5m", "15m", "1h", "1d"] = "15m",
                      database: DatabasePool = Depends(get_database),
                      settings: Settings = Depends(get_settings)) -> CongestionTrends:
    start, end = validate_time_range(start, end, settings.max_query_range_days)
    return get_congestion_trends(database, dataset, start, end, bucket)
