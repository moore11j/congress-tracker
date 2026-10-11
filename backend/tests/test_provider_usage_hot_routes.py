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


def test_provider_usage_skips_db_persistence_on_context_bundle_hot_route(monkeypatch):
    reset_provider_usage()
    monkeypatch.setenv("FMP_PERSIST_USAGE_EVENTS", "1")
    monkeypatch.delenv("FMP_PERSIST_HOT_ROUTE_USAGE_EVENTS", raising=False)
    calls: list[str] = []

    def fail_session():
        calls.append("SessionLocal")
        raise AssertionError("hot context-bundle provider telemetry should stay in-process")

    monkeypatch.setattr("app.db.SessionLocal", fail_session)
    token = set_request_context({"path": "/api/tickers/AAPL/context-bundle", "priority": "normal"})
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
    token = set_request_context({"path": "/api/tickers/AAPL/profile", "priority": "normal"})
    try:
        record_cache_hit(category="quote", symbol="AAPL")
    finally:
        reset_request_context(token)

    assert len(added) == 1
    assert commits == [True]
    reset_provider_usage()


def test_member_cache_bookkeeping_stays_in_memory_but_real_activity_persists(monkeypatch):
    from app.services import provider_usage as usage
    reset_provider_usage()
    monkeypatch.setenv("FMP_PERSIST_USAGE_EVENTS", "1")
    monkeypatch.delenv("FMP_PERSIST_HOT_ROUTE_USAGE_EVENTS", raising=False)
    added = []
    class Session:
        def add(self, row): added.append(row)
        def commit(self): pass
        def close(self): pass
    monkeypatch.setattr("app.db.SessionLocal", Session)
    for path in ["/api/members/by-slug/CLEO_FIELDS", "/api/events", "/api/market/quotes", "/api/tickers/AAPL/context-bundle"]:
        token = set_request_context({"path": path})
        try:
            usage.record_cache_hit(category="quote", symbol="AAPL")
            usage.record_cache_miss(category="quote", symbol="AAPL")
            assert not added
            for kind in ["provider_call", "provider_error", "throttle", "fallback", "content_write"]:
                usage._record(kind, category="quote", symbol="AAPL")
            assert len(added) == 5
            assert all(row.route == path for row in added)
            assert [row.cache_status for row in added] == ["provider_call", "provider_error", "throttle", "fallback", "content_write"]
            added.clear()
        finally:
            reset_request_context(token)
    assert provider_usage_summary()["totals"]["cache_hits"] == 4
    reset_provider_usage()


def test_explicit_hot_route_cache_persistence_override_remains(monkeypatch):
    from app.services.provider_usage import _skip_hot_route_usage_persistence
    monkeypatch.setenv("FMP_PERSIST_HOT_ROUTE_USAGE_EVENTS", "1")
    assert not _skip_hot_route_usage_persistence({"route":"/api/members/F000110", "kind":"cache_hit"})


def test_persisted_summary_counts_only_attempts_and_exposes_legacy_unknowns(monkeypatch):
    from sqlalchemy import create_engine, select
    from sqlalchemy.orm import sessionmaker
    from app.db import Base
    from app.models import ProviderUsageEvent
    from app.services import provider_usage as usage
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    sessions = sessionmaker(bind=engine)
    monkeypatch.setattr("app.db.SessionLocal", sessions)
    monkeypatch.setenv("FMP_PERSIST_USAGE_EVENTS", "1")
    monkeypatch.setenv("FMP_PROVIDER_DISABLED", "1")
    monkeypatch.delenv("FMP_PERSIST_HOT_ROUTE_USAGE_EVENTS", raising=False)
    usage.reset_provider_usage()
    token = set_request_context({"path": "/api/members/test"})
    try:
        try:
            usage.ensure_fmp_live_allowed(category="quote", symbol="AAPL")
        except usage.ProviderDisabled:
            pass
        else:
            raise AssertionError("disabled provider must be denied")
        with sessions() as db:
            summary = usage.provider_usage_summary(db=db)
            assert summary["database_usage_status"] == "ok"
            assert summary["calls_today"] == 0
            assert summary["legacy_unclassified_events_24h"] == 0
        usage._record("provider_call", category="quote")
        usage._record("provider_error", category="quote", status_code=500)
        usage._record("fallback", category="quote", reason="provider_unavailable")
        usage._record("content_write", category="news", item_count=5)
        with sessions() as db:
            db.add(ProviderUsageEvent(provider="fmp", cache_status=None))
            db.commit()
            summary = usage.provider_usage_summary(db=db)
            assert summary["database_usage_status"] == "ok"
            assert summary["calls_today"] == 1
            assert summary["call_windows"]["last_24_hours"] == 1
            assert summary["legacy_unclassified_events_24h"] == 1
            kinds = list(db.scalars(select(ProviderUsageEvent.cache_status)))
            assert kinds.count("provider_call") == 1
            assert "provider_error" in kinds and "content_write" in kinds
    finally:
        reset_request_context(token); usage.reset_provider_usage(); engine.dispose()
