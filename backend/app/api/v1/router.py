from fastapi import APIRouter

from backend.app.api.v1.endpoints import (
    dashboard,
    datasets,
    forecasts,
    health,
    metrics,
    routes,
    trends,
)

router = APIRouter()
router.include_router(health.router, prefix="/health")
router.include_router(datasets.router, prefix="/api/v1")
router.include_router(dashboard.router, prefix="/api/v1")
router.include_router(metrics.router, prefix="/api/v1")
router.include_router(trends.router, prefix="/api/v1")
router.include_router(forecasts.router, prefix="/api/v1")
router.include_router(routes.router, prefix="/api/v1")

