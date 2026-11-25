from datetime import datetime
from pathlib import Path
from typing import Any

from backend.app.core.config import Settings
from backend.app.db import queries
from backend.app.db.pool import DatabasePool
from backend.app.schemas.datasets import DatasetInfo
from backend.app.services.common import SUPPORTED_DATASETS, freshness_seconds, validate_dataset

CATALOG = {
    "metr-la": {
        "display_name": "METR-LA",
        "sensor_count": 207,
        "training_start": datetime.fromisoformat("2012-03-01T00:00:00+00:00"),
        "training_end": datetime.fromisoformat("2012-06-27T23:55:00+00:00"),
    },
    "pems-bay": {
        "display_name": "PEMS-BAY",
        "sensor_count": 325,
        "training_start": datetime.fromisoformat("2017-01-01T00:00:00+00:00"),
        "training_end": datetime.fromisoformat("2017-06-30T23:55:00+00:00"),
    },
}


def _artifact_metadata(model_dir: Path, dataset: str) -> tuple[bool, str | None]:
    directory = model_dir / dataset
    artifacts = sorted(directory.glob("model_*.joblib")) if directory.is_dir() else []
    if not artifacts:
        return False, None
    name = artifacts[-1].stem
    return True, name.removeprefix("model_")


def _to_info(dataset: str, rollup: dict[str, Any] | None,
             model_dir: Path) -> DatasetInfo:
    metadata = CATALOG[dataset]
    available, version = _artifact_metadata(model_dir, dataset)
    rollup = rollup or {}
    latest = rollup.get("latest_window_end")
    return DatasetInfo(
        dataset=dataset,
        **metadata,
        metric_row_count=int(rollup.get("metric_row_count") or 0),
        first_window_start=rollup.get("first_window_start"),
        latest_window_end=latest,
        last_processed_at=rollup.get("last_processed_at"),
        freshness_seconds=freshness_seconds(latest),
        model_available=available,
        model_version=version,
    )


def list_datasets(database: DatabasePool, settings: Settings) -> list[DatasetInfo]:
    rollups = {row["dataset"]: row for row in
               queries.list_dataset_rollups(database, list(SUPPORTED_DATASETS))}
    return [_to_info(dataset, rollups.get(dataset), settings.models_dir)
            for dataset in SUPPORTED_DATASETS]


def get_dataset(database: DatabasePool, settings: Settings, dataset: str) -> DatasetInfo:
    dataset = validate_dataset(dataset)
    rollups = {row["dataset"]: row for row in
               queries.list_dataset_rollups(database, [dataset])}
    return _to_info(dataset, rollups.get(dataset), settings.models_dir)
