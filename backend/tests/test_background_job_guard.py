from __future__ import annotations

from types import SimpleNamespace

import app.background_job_guard as guard_module
from app.background_job_guard import check_background_job_guard


class _FakeResult:
    def __init__(self, row):
        self.row = row

    def mappings(self):
        return self

    def one(self):
        return self.row


class _FakeSession:
    def __init__(self, row):
        self.row = row
        self.closed = False

    def execute(self, _stmt):
        return _FakeResult(self.row)

    def close(self):
        self.closed = True


def test_background_job_guard_allows_sqlite_without_db_probe(monkeypatch):
    monkeypatch.setattr(guard_module, "DATABASE_URL", "sqlite:////tmp/app.db")

    result = check_background_job_guard("feed-pnl-repair")

    assert result.proceed is True
    assert result.reason == "sqlite_noop"


def test_background_job_guard_skips_when_active_connections_hit_limit(monkeypatch):
    monkeypatch.setattr(guard_module, "DATABASE_URL", "postgresql+psycopg://example/db")
    monkeypatch.setenv("BACKGROUND_DB_ACTIVE_CONNECTION_LIMIT", "2")
    session = _FakeSession({"active_connections": 2, "total_connections": 5})

    result = check_background_job_guard("enrichment-queue", db=session)

    assert result.proceed is False
    assert result.reason == "db_active_connection_pressure"
    assert result.active_connections == 2


def test_background_job_guard_paused_env_skips_before_probe(monkeypatch):
    monkeypatch.setattr(guard_module, "DATABASE_URL", "postgresql+psycopg://example/db")
    monkeypatch.setenv("BACKGROUND_JOBS_PAUSED", "true")
    session = SimpleNamespace(execute=lambda _stmt: (_ for _ in ()).throw(AssertionError("should not probe")))

    result = check_background_job_guard("priority-ticker-prewarm", db=session)

    assert result.proceed is False
    assert result.reason == "background_jobs_paused"
