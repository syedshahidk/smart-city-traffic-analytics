from contextlib import asynccontextmanager
import logging
from typing import AsyncIterator

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

from backend.app.api.v1.router import router
from backend.app.core.config import Settings
from backend.app.core.errors import APIError, DatabaseUnavailable
from backend.app.db.pool import DatabasePool

LOGGER = logging.getLogger(__name__)


def create_app(settings: Settings | None = None,
               database: DatabasePool | None = None) -> FastAPI:
    app_settings = settings or Settings.from_environment()
    database_pool = database or DatabasePool(app_settings)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        yield
        close = getattr(app.state.database, "close", None)
        if close is not None:
            close()

    app = FastAPI(
        title="Smart City Traffic Analytics API",
        version="1.0.0",
        description="Read-only analytics over Spark-aggregated traffic metrics.",
        lifespan=lifespan,
    )
    app.state.settings = app_settings
    app.state.database = database_pool
    app.add_middleware(
        CORSMiddleware,
        allow_origins=list(app_settings.cors_origins),
        allow_credentials=False,
        allow_methods=["GET"],
        allow_headers=["Accept", "Content-Type"],
    )
    app.include_router(router)

    @app.exception_handler(APIError)
    async def api_error_handler(request: Request, exc: APIError) -> JSONResponse:
        return JSONResponse(
            status_code=exc.status_code,
            content={"error": {"code": exc.code, "message": exc.message}},
        )

    @app.exception_handler(DatabaseUnavailable)
    async def database_error_handler(request: Request, exc: DatabaseUnavailable) -> JSONResponse:
        return JSONResponse(
            status_code=503,
            content={"error": {"code": "database_unavailable", "message": str(exc)}},
        )

    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(request: Request,
                                       exc: RequestValidationError) -> JSONResponse:
        details = [
            {"location": ".".join(str(part) for part in error["loc"]),
             "message": error["msg"], "type": error["type"]}
            for error in exc.errors()
        ]
        return JSONResponse(
            status_code=422,
            content={"error": {"code": "validation_error",
                                "message": "Request validation failed"},
                     "details": details},
        )

    @app.exception_handler(Exception)
    async def unexpected_error_handler(request: Request, exc: Exception) -> JSONResponse:
        LOGGER.exception("Unhandled API error", exc_info=exc)
        return JSONResponse(
            status_code=500,
            content={"error": {"code": "internal_error",
                                "message": "An unexpected server error occurred"}},
        )

    return app


app = create_app()
