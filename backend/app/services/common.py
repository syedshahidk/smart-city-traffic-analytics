from datetime import datetime, timedelta, timezone

from backend.app.core.errors import APIError

SUPPORTED_DATASETS = ("metr-la", "pems-bay")


def validate_dataset(dataset: str) -> str:
    if dataset not in SUPPORTED_DATASETS:
        raise APIError(404, "dataset_not_found", f"Unsupported dataset: {dataset}")
    return dataset


def validate_time_range(start: datetime | None, end: datetime | None,
                        max_days: int) -> tuple[datetime, datetime]:
    if start is None and end is None:
        end = datetime.now(timezone.utc)
        start = end - timedelta(hours=24)
    elif start is None or end is None:
        raise APIError(422, "invalid_time_range", "start and end must be supplied together")
    if start.tzinfo is None or start.utcoffset() is None:
        raise APIError(422, "invalid_time_range", "start must include a UTC offset")
    if end.tzinfo is None or end.utcoffset() is None:
        raise APIError(422, "invalid_time_range", "end must include a UTC offset")
    start = start.astimezone(timezone.utc)
    end = end.astimezone(timezone.utc)
    if end <= start:
        raise APIError(422, "invalid_time_range", "end must be later than start")
    if end - start > timedelta(days=max_days):
        raise APIError(422, "invalid_time_range", f"time range cannot exceed {max_days} days")
    return start, end


def validate_cursor(cursor: datetime | None, start: datetime, end: datetime) -> datetime | None:
    if cursor is None:
        return None
    if cursor.tzinfo is None or cursor.utcoffset() is None:
        raise APIError(422, "invalid_cursor", "after must include a UTC offset")
    cursor = cursor.astimezone(timezone.utc)
    if cursor < start or cursor >= end:
        raise APIError(422, "invalid_cursor", "after must fall within the selected time range")
    return cursor


def freshness_seconds(timestamp: datetime | None) -> float | None:
    if timestamp is None:
        return None
    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
        raise ValueError("database timestamps must include a UTC offset")
    age = (datetime.now(timezone.utc) - timestamp.astimezone(timezone.utc)).total_seconds()
    return round(max(age, 0.0), 3)
