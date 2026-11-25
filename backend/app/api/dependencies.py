from fastapi import Request

from backend.app.core.config import Settings
from backend.app.db.pool import DatabasePool


def get_database(request: Request) -> DatabasePool:
    return request.app.state.database


def get_settings(request: Request) -> Settings:
    return request.app.state.settings
