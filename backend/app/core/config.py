from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]


@dataclass(frozen=True)
class Settings:
    """Runtime settings; credentials are supplied by the environment."""

    database_host: str = "postgres"
    database_port: int = 5432
    database_name: str = "traffic_analytics"
    database_user: str = "traffic_api_reader"
    database_password: str | None = None
    database_pool_min: int = 1
    database_pool_max: int = 5
    database_statement_timeout_ms: int = 8000
    models_dir: Path = PROJECT_ROOT / "models" / "traffic_forecasting"
    reports_dir: Path = PROJECT_ROOT / "analytics" / "reports"
    max_query_range_days: int = 366
    forecast_max_input_age_seconds: int = 1200
    cors_origins: tuple[str, ...] = (
        "http://localhost:5173", "http://127.0.0.1:5173",
        "http://localhost:8080", "http://127.0.0.1:8080",
    )
    # External routing / geocoding integration (free OpenStreetMap-compatible services).
    # Kept configurable and isolated; no credentials are required by OSRM/Nominatim.
    osrm_base_url: str = "https://router.project-osrm.org"
    nominatim_base_url: str = "https://nominatim.openstreetmap.org"
    nominatim_user_agent: str = "smart-city-traffic-analytics/1.0 (academic demonstration)"
    route_request_timeout_seconds: float = 25.0
    geocode_request_timeout_seconds: float = 15.0

    @classmethod
    def from_environment(cls) -> Settings:
        port = int(os.getenv("API_DB_PORT", "5432"))
        pool_min = int(os.getenv("API_DB_POOL_MIN", "1"))
        pool_max = int(os.getenv("API_DB_POOL_MAX", "5"))
        statement_timeout = int(os.getenv("API_DB_STATEMENT_TIMEOUT_MS", "8000"))
        max_range = int(os.getenv("API_MAX_QUERY_RANGE_DAYS", "366"))
        forecast_max_age = int(os.getenv("API_FORECAST_MAX_INPUT_AGE_SECONDS", "1200"))
        route_timeout = float(os.getenv("API_ROUTE_REQUEST_TIMEOUT_SECONDS", "25"))
        geocode_timeout = float(os.getenv("API_GEOCODE_REQUEST_TIMEOUT_SECONDS", "15"))
        cors_origins = tuple(
            origin.strip()
            for origin in os.getenv(
                "API_CORS_ORIGINS",
                "http://localhost:5173,http://127.0.0.1:5173,"
                "http://localhost:8080,http://127.0.0.1:8080",
            ).split(",")
            if origin.strip()
        )
        if not 1 <= port <= 65535:
            raise ValueError("API_DB_PORT must be between 1 and 65535")
        if pool_min < 1 or pool_max < pool_min:
            raise ValueError("API_DB_POOL_MAX must be at least API_DB_POOL_MIN >= 1")
        if statement_timeout < 1 or max_range < 1 or forecast_max_age < 1:
            raise ValueError("Timeouts, query range, and forecast freshness limit must be positive")
        if route_timeout < 1 or geocode_timeout < 1:
            raise ValueError("Route and geocoding timeouts must be positive")
        return cls(
            database_host=os.getenv("API_DB_HOST", "postgres"),
            database_port=port,
            database_name=os.getenv("API_DB_NAME", "traffic_analytics"),
            database_user=os.getenv("API_DB_USER", "traffic_api_reader"),
            database_password=os.getenv("API_DB_PASSWORD") or None,
            database_pool_min=pool_min,
            database_pool_max=pool_max,
            database_statement_timeout_ms=statement_timeout,
            models_dir=Path(os.getenv("API_MODEL_DIR", str(PROJECT_ROOT / "models" / "traffic_forecasting"))),
            reports_dir=Path(os.getenv("API_MODEL_REPORTS_DIR", str(PROJECT_ROOT / "analytics" / "reports"))),
            max_query_range_days=max_range,
            forecast_max_input_age_seconds=forecast_max_age,
            cors_origins=cors_origins,
            osrm_base_url=os.getenv("API_OSRM_BASE_URL", "https://router.project-osrm.org"),
            nominatim_base_url=os.getenv("API_NOMINATIM_BASE_URL", "https://nominatim.openstreetmap.org"),
            nominatim_user_agent=os.getenv("API_NOMINATIM_USER_AGENT",
                                           "smart-city-traffic-analytics/1.0 (academic demonstration)"),
            route_request_timeout_seconds=route_timeout,
            geocode_request_timeout_seconds=geocode_timeout,
        )
