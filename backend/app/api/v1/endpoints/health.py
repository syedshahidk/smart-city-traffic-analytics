from fastapi import APIRouter, Depends

from backend.app.api.dependencies import get_database
from backend.app.db.pool import DatabasePool
from backend.app.schemas.common import HealthResponse

router = APIRouter(tags=["health"])


@router.get("/live", response_model=HealthResponse)
def live() -> HealthResponse:
    return HealthResponse(status="live")


@router.get("/ready", response_model=HealthResponse)
def ready(database: DatabasePool = Depends(get_database)) -> HealthResponse:
    database.check_ready()
    return HealthResponse(status="ready")
