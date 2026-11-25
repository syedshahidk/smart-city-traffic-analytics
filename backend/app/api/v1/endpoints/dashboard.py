from datetime import datetime

from fastapi import APIRouter, Depends

from backend.app.api.dependencies import get_database, get_settings
from backend.app.core.config import Settings
from backend.app.db.pool import DatabasePool
from backend.app.schemas.common import DatasetId
from backend.app.schemas.metrics import DashboardKpis
from backend.app.services.common import validate_time_range
from backend.app.services.dashboard import get_dashboard_kpis

router = APIRouter(prefix="/dashboard", tags=["dashboard"])


@router.get("/kpis", response_model=DashboardKpis)
def dashboard_kpis(dataset: DatasetId, start: datetime | None = None,
                   end: datetime | None = None,
                   database: DatabasePool = Depends(get_database),
                   settings: Settings = Depends(get_settings)) -> DashboardKpis:
    start, end = validate_time_range(start, end, settings.max_query_range_days)
    return get_dashboard_kpis(database, dataset, start, end)

