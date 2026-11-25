import psycopg2
import pytest

from backend.app.core.config import Settings
from backend.app.core.errors import DatabaseUnavailable
from backend.app.db import pool as pool_module
from backend.app.db.pool import DatabasePool


def test_missing_database_credentials_fail_gracefully() -> None:
    database = DatabasePool(Settings(database_password=None))
    with pytest.raises(DatabaseUnavailable, match="credentials"):
        database.check_ready()


def test_database_connection_failure_is_wrapped(monkeypatch: pytest.MonkeyPatch) -> None:
    def fail_to_connect(**kwargs):
        raise psycopg2.OperationalError("connection refused")

    monkeypatch.setattr(pool_module, "ThreadedConnectionPool", fail_to_connect)
    database = DatabasePool(Settings(database_password="test-only"))
    with pytest.raises(DatabaseUnavailable, match="PostgreSQL is unavailable"):
        database.check_ready()


def test_connections_are_rolled_back_and_returned_to_pool(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def execute(self, sql, params):
            self.sql = sql
            self.params = params

        def fetchall(self):
            return [{"value": 1}]

    class Connection:
        closed = 0

        def __init__(self):
            self.rollbacks = 0

        def cursor(self, **kwargs):
            return Cursor()

        def rollback(self):
            self.rollbacks += 1

    class Pool:
        def __init__(self):
            self.connection = Connection()
            self.returned = []

        def getconn(self):
            return self.connection

        def putconn(self, connection, close=False):
            self.returned.append((connection, close))

        def closeall(self):
            return None

    pool = Pool()
    options = {}

    def pool_factory(**kwargs):
        options.update(kwargs)
        return pool

    monkeypatch.setattr(pool_module, "ThreadedConnectionPool", pool_factory)
    database = DatabasePool(Settings(database_password="test-only"))

    assert database.fetch_one("SELECT %s AS value", (1,)) == {"value": 1}
    assert pool.connection.rollbacks == 1
    assert pool.returned == [(pool.connection, False)]
    assert "default_transaction_read_only=on" in options["options"]
    assert options["user"] == "traffic_api_reader"
