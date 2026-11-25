from __future__ import annotations

from contextlib import contextmanager
import logging
import threading
from typing import Any, Iterator

import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import PoolError, ThreadedConnectionPool

from backend.app.core.config import Settings
from backend.app.core.errors import DatabaseUnavailable

LOGGER = logging.getLogger(__name__)


class DatabasePool:
    """Lazy, thread-safe PostgreSQL pool restricted to read-only transactions."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._pool: ThreadedConnectionPool | None = None
        self._lock = threading.RLock()

    def _get_pool(self) -> ThreadedConnectionPool:
        with self._lock:
            if self._pool is not None:
                return self._pool
            if not all((self._settings.database_host, self._settings.database_name,
                        self._settings.database_user, self._settings.database_password)):
                raise DatabaseUnavailable(
                    "Database credentials are not configured; set API_DB_USER and API_DB_PASSWORD"
                )
            options = (
                "-c default_transaction_read_only=on -c timezone=UTC "
                f"-c statement_timeout={self._settings.database_statement_timeout_ms}"
            )
            try:
                self._pool = ThreadedConnectionPool(
                    minconn=self._settings.database_pool_min,
                    maxconn=self._settings.database_pool_max,
                    host=self._settings.database_host,
                    port=self._settings.database_port,
                    dbname=self._settings.database_name,
                    user=self._settings.database_user,
                    password=self._settings.database_password,
                    connect_timeout=5,
                    options=options,
                    application_name="smart-city-traffic-api",
                )
            except (psycopg2.Error, OSError, PoolError) as exc:
                LOGGER.warning("PostgreSQL pool initialization failed: %s", type(exc).__name__)
                raise DatabaseUnavailable("PostgreSQL is unavailable") from exc
            return self._pool

    @contextmanager
    def connection(self) -> Iterator[Any]:
        pool = self._get_pool()
        connection = None
        discard = False
        try:
            connection = pool.getconn()
            yield connection
        except (psycopg2.Error, PoolError, OSError) as exc:
            if connection is not None:
                try:
                    connection.rollback()
                except psycopg2.Error:
                    discard = True
            LOGGER.warning("PostgreSQL operation failed: %s", type(exc).__name__)
            raise DatabaseUnavailable("PostgreSQL is unavailable") from exc
        except Exception:
            if connection is not None:
                try:
                    connection.rollback()
                except psycopg2.Error:
                    discard = True
            raise
        else:
            try:
                connection.rollback()
            except psycopg2.Error as exc:
                discard = True
                raise DatabaseUnavailable("Could not reset the PostgreSQL connection") from exc
        finally:
            if connection is not None:
                discard = discard or bool(connection.closed)
                try:
                    pool.putconn(connection, close=discard)
                except PoolError:
                    LOGGER.exception("Could not return a PostgreSQL connection to its pool")

    def fetch_all(self, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        with self.connection() as connection:
            with connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(sql, params)
                return [dict(row) for row in cursor.fetchall()]

    def fetch_one(self, sql: str, params: tuple[Any, ...] = ()) -> dict[str, Any] | None:
        rows = self.fetch_all(sql, params)
        return rows[0] if rows else None

    def check_ready(self) -> None:
        result = self.fetch_one("SELECT to_regclass('public.traffic_metrics') AS table_name")
        if not result or result["table_name"] is None:
            raise DatabaseUnavailable("Required table public.traffic_metrics is unavailable")

    def close(self) -> None:
        with self._lock:
            if self._pool is not None:
                self._pool.closeall()
                self._pool = None
