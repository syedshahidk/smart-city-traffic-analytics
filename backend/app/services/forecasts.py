from __future__ import annotations

from datetime import datetime, timedelta, timezone
import logging
import math
from typing import Any

import pandas as pd

from analytics import forecasting
from backend.app.core.config import Settings
from backend.app.core.errors import APIError
from backend.app.db import queries
from backend.app.db.pool import DatabasePool
from backend.app.schemas.forecast import LatestForecast
from backend.app.services.common import validate_dataset

LOGGER = logging.getLogger(__name__)
INPUT_WINDOW_COUNT = 13
WINDOW_DURATION = timedelta(minutes=5)


def utc_now() -> datetime:
    """Clock seam for deterministic freshness validation in tests."""
    return datetime.now(timezone.utc)


def _input_error(message: str) -> APIError:
    return APIError(503, "forecast_data_unavailable", message)


def _utc_timestamp(value: Any, field: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise _input_error(f"Forecast input contains an invalid {field} timestamp")
    return value.astimezone(timezone.utc)


def _is_five_minute_boundary(value: datetime) -> bool:
    return value.minute % 5 == 0 and value.second == 0 and value.microsecond == 0


def _finite_number(value: Any, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise _input_error(f"Forecast input contains an invalid {field}")
    number = float(value)
    if not math.isfinite(number):
        raise _input_error(f"Forecast input contains an invalid {field}")
    return number


def _validate_and_frame(
    rows: list[dict[str, Any]], dataset: str, now: datetime, max_age_seconds: int
) -> tuple[pd.DataFrame, datetime, datetime, datetime, float]:
    if not rows:
        raise _input_error("No completed traffic metric windows are available")
    if len(rows) != INPUT_WINDOW_COUNT:
        raise _input_error(
            f"Forecast requires {INPUT_WINDOW_COUNT} consecutive completed windows; "
            f"received {len(rows)}"
        )
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("Forecast validation clock must be timezone-aware")
    now = now.astimezone(timezone.utc)

    normalized: list[dict[str, Any]] = []
    for row in rows:
        if row.get("dataset") != dataset:
            raise _input_error("Forecast input contains metrics for a different dataset")
        start = _utc_timestamp(row.get("window_start"), "window_start")
        end = _utc_timestamp(row.get("window_end"), "window_end")
        processed_at = _utc_timestamp(row.get("processed_at"), "processed_at")
        if not _is_five_minute_boundary(start):
            raise _input_error("Forecast input timestamps must align to five-minute boundaries")
        if end != start + WINDOW_DURATION:
            raise _input_error("Forecast input contains an invalid five-minute window")
        if end > now:
            raise _input_error("Forecast input contains an incomplete or future window")

        speed = _finite_number(row.get("average_speed"), "average speed")
        percentage = _finite_number(row.get("congestion_percentage"), "congestion percentage")
        total = row.get("total_sensor_readings")
        congested = row.get("congested_sensor_count")
        if (isinstance(total, bool) or not isinstance(total, int) or total <= 0
                or isinstance(congested, bool) or not isinstance(congested, int)
                or congested < 0 or congested > total):
            raise _input_error("Forecast input contains invalid sensor counts")
        if speed < 0.0 or not 0.0 <= percentage <= 100.0:
            raise _input_error("Forecast input contains out-of-range metric values")
        expected_percentage = congested * 100.0 / total
        if not math.isclose(percentage, expected_percentage, rel_tol=1e-9, abs_tol=1e-8):
            raise _input_error("Forecast congestion percentage is inconsistent with sensor counts")
        normalized.append({
            "window_start": start,
            "window_end": end,
            "processed_at": processed_at,
            "average_speed": speed,
            "congestion_percentage": percentage,
        })

    normalized.sort(key=lambda item: item["window_start"])
    starts = [item["window_start"] for item in normalized]
    if len(set(starts)) != INPUT_WINDOW_COUNT:
        raise _input_error("Forecast input contains duplicate metric timestamps")
    if any(right - left != WINDOW_DURATION for left, right in zip(starts, starts[1:])):
        raise _input_error("Forecast input contains missing or nonconsecutive windows")

    origin = starts[-1]
    latest_event = normalized[-1]["window_end"]
    age_seconds = (now - latest_event).total_seconds()
    if age_seconds < 0:
        raise _input_error("Forecast input contains a future event timestamp")
    if age_seconds > max_age_seconds:
        raise _input_error("Latest traffic metrics are stale for forecasting")
    if origin + timedelta(minutes=15) <= now:
        raise _input_error("Latest completed window no longer has a future 15-minute target")

    frame = pd.DataFrame(
        {
            "average_speed": [item["average_speed"] for item in normalized],
            "congestion_percentage": [item["congestion_percentage"] for item in normalized],
        },
        index=pd.DatetimeIndex(starts, name="timestamp"),
    )
    return frame, starts[0], normalized[-1]["window_end"], latest_event, age_seconds


def get_latest_forecast(
    database: DatabasePool,
    settings: Settings,
    dataset: str,
    congestion_percentage_threshold: float = forecasting.DEFAULT_CONGESTION_PERCENTAGE_THRESHOLD,
    *,
    now: datetime | None = None,
) -> LatestForecast:
    dataset = validate_dataset(dataset)
    current_time = now or utc_now()
    if current_time.tzinfo is None or current_time.utcoffset() is None:
        raise ValueError("Forecast validation clock must be timezone-aware")
    current_time = current_time.astimezone(timezone.utc)
    rows = queries.get_latest_forecast_metrics(
        database, dataset, current_time, INPUT_WINDOW_COUNT
    )
    frame, window_start, window_end, latest_event, age_seconds = _validate_and_frame(
        rows, dataset, current_time, settings.forecast_max_input_age_seconds
    )
    try:
        result = forecasting.forecast_from_metrics(
            dataset,
            frame,
            model_dir=settings.models_dir,
            congestion_percentage_threshold=congestion_percentage_threshold,
        )
    except FileNotFoundError as exc:
        LOGGER.warning("Forecast artifact unavailable for dataset %s", dataset)
        raise APIError(503, "forecast_model_unavailable", "No trained forecast model is available") from exc
    except Exception as exc:
        LOGGER.exception("Forecast inference failed for dataset %s", dataset)
        raise APIError(503, "forecast_model_error", "Forecast model could not produce a prediction") from exc

    origin = datetime.fromisoformat(result["forecast_origin"]).astimezone(timezone.utc)
    target = datetime.fromisoformat(result["target_timestamp"]).astimezone(timezone.utc)
    if target - origin != timedelta(minutes=15):
        raise APIError(503, "forecast_model_error", "Forecast model returned an invalid target timestamp")
    selected = result["selected_models"]
    return LatestForecast(
        dataset=dataset,
        forecast_origin=origin,
        target_timestamp=target,
        prediction_horizon_minutes=15,
        predicted_average_speed=result["predicted_average_speed"],
        predicted_congestion_percentage=result["predicted_congestion_percentage"],
        congestion_status=result["congestion_status"],
        congestion_threshold_speed_below=result["congestion_threshold_speed_below"],
        congestion_percentage_threshold=result["congestion_percentage_threshold"],
        speed_model=selected["speed"],
        congestion_model=selected["congestion_percentage"],
        model_version=result["model_version"],
        input_window_count=INPUT_WINDOW_COUNT,
        input_window_start=window_start,
        input_window_end=window_end,
        latest_event_timestamp=latest_event,
        freshness_seconds=round(age_seconds, 3),
        freshness_limit_seconds=settings.forecast_max_input_age_seconds,
        data_provenance="historical_dataset_replay_simulated",
        is_live_traffic=False,
    )
