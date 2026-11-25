from fastapi import APIRouter, Depends

from backend.app.api.dependencies import get_database, get_settings
from backend.app.core.config import Settings
from backend.app.db.pool import DatabasePool
from backend.app.schemas.datasets import DatasetInfo
from backend.app.services.datasets import get_dataset, list_datasets

router = APIRouter(prefix="/datasets", tags=["datasets"])


@router.get("", response_model=list[DatasetInfo])
def datasets(database: DatabasePool = Depends(get_database),
             settings: Settings = Depends(get_settings)) -> list[DatasetInfo]:
    return list_datasets(database, settings)


@router.get("/{dataset}", response_model=DatasetInfo)
def dataset_info(dataset: str, database: DatabasePool = Depends(get_database),
                 settings: Settings = Depends(get_settings)) -> DatasetInfo:
    return get_dataset(database, settings, dataset)
