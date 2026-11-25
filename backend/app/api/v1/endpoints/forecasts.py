from typing import Annotated

from fastapi import APIRouter, Depends, Query

from analytics.forecasting import DEFAULT_CONGESTION_PERCENTAGE_THRESHOLD
from backend.app.api.dependencies import get_database, get_settings
from backend.app.core.config import Settings
from backend.app.db.pool import DatabasePool
from backend.app.schemas.common import DatasetId
from backend.app.schemas.forecast import LatestForecast
from backend.app.schemas.model_metadata import ModelMetadata
from backend.app.services.forecasts import get_latest_forecast
from backend.app.services.model_metadata import get_model_metadata

router = APIRouter(prefix="/forecasts", tags=["forecasts"])


@router.get("/latest", response_model=LatestForecast)
def latest_forecast(
    dataset: DatasetId,
    congestion_percentage_threshold: Annotated[float, Query(ge=0.0, le=100.0)] =
        DEFAULT_CONGESTION_PERCENTAGE_THRESHOLD,
    database: DatabasePool = Depends(get_database),
    settings: Settings = Depends(get_settings),
) -> LatestForecast:
    return get_latest_forecast(
        database, settings, dataset, congestion_percentage_threshold
    )


@router.get("/model-metadata", response_model=ModelMetadata)
def model_metadata(
    dataset: DatasetId,
    settings: Settings = Depends(get_settings),
) -> ModelMetadata:
    """Report-derived metadata cross-checked against the served model artifact."""
    return get_model_metadata(settings, dataset)
