from __future__ import annotations

from app.request_priority import reset_request_context, set_request_context
from app.services.provider_usage import provider_usage_summary, record_cache_hit, reset_provider_usage


def test_provider_usage_skips_db_persistence_on_feed_hot_route(monkeypatch):
    reset_provider_usage()
    monkeypatch.setenv("FMP_PERSIST_USAGE_EVENTS", "1")
    monkeypatch.delenv("FMP_PERSIST_HOT_ROUTE_USAGE_EVENTS", raising=False)
    calls: list[str] = []

    def fail_session():
        calls.append("SessionLocal")
        raise AssertionError("hot feed provider telemetry should stay in-process")

    monkeypatch.setattr("app.db.SessionLocal", fail_session)
    token = set_request_context({"path": "/api/events", "priority": "heavy"})
    try:
        record_cache_hit(category="quote", symbol="AAPL")
    finally:
        reset_request_context(token)

    summary = provider_usage_summary()
    assert summary["totals"]["cache_hits"] == 1
    assert calls == []
    reset_provider_usage()


def test_provider_usage_can_persist_non_hot_route(monkeypatch):
    reset_provider_usage()
    monkeypatch.setenv("FMP_PERSIST_USAGE_EVENTS", "1")
    added: list[object] = []
    commits: list[bool] = []

    class FakeSession:
        def add(self, row):
            added.append(row)

        def commit(self):
            commits.append(True)

        def close(self):
            pass

    monkeypatch.setattr("app.db.SessionLocal", lambda: FakeSession())
    token = set_request_context({"path": "/api/tickers/AAPL/context-bundle", "priority": "normal"})
    try:
        record_cache_hit(category="quote", symbol="AAPL")
    finally:
        reset_request_context(token)

    assert len(added) == 1
    assert commits == [True]
    reset_provider_usage()
