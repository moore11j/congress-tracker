from concurrent.futures import ThreadPoolExecutor
from threading import Event, Lock

import pytest

from app.request_priority import reset_request_context, set_request_context
from app.services import provider_usage as usage


@pytest.fixture(autouse=True)
def clean_usage(monkeypatch):
    usage.reset_provider_usage()
    monkeypatch.setenv("FMP_PROVIDER_DISABLED", "false")
    monkeypatch.setenv("FMP_PERSIST_USAGE_EVENTS", "true")
    monkeypatch.setenv("FMP_PERSIST_HOT_ROUTE_USAGE_EVENTS", "false")
    monkeypatch.setenv("FMP_HARD_LIMIT_PER_MINUTE", "7")
    monkeypatch.setattr(usage.time, "time", lambda: 1000.0)
    yield
    usage.reset_provider_usage()


@pytest.mark.parametrize("exhausted", [False, True])
def test_slow_telemetry_commit_does_not_block_cached_feed(monkeypatch, exhausted):
    if exhausted:
        usage._STATE.provider_call_timestamps.extend([1000.0] * 7)
    committing, release, reader_started = Event(), Event(), Event()
    persisted = []

    class SlowSession:
        def add(self, row):
            persisted.append(row)

        def commit(self):
            committing.set()
            assert release.wait(5)

        def close(self):
            pass

    monkeypatch.setattr("app.db.SessionLocal", SlowSession)

    def reserve():
        try:
            usage.ensure_fmp_live_allowed(category="profile", symbol="LIVE")
            return "allowed"
        except usage.ProviderBudgetExceeded:
            return "throttled"

    def cached_feed():
        token = set_request_context({"path": "/api/events"})
        try:
            reader_started.set()
            usage.record_cache_hit(category="quote", symbol="CACHED")
            return usage.provider_usage_summary()
        finally:
            reset_request_context(token)

    with ThreadPoolExecutor(max_workers=2) as pool:
        writer = pool.submit(reserve)
        assert committing.wait(3)
        reader = pool.submit(cached_feed)
        assert reader_started.wait(3)
        try:
            summary = reader.result(timeout=1)
            assert summary["totals"]["cache_hits"] == 1
            assert not writer.done(), "The database write must still be waiting"
        finally:
            release.set()
        assert writer.result(timeout=3) == ("throttled" if exhausted else "allowed")

    assert len(persisted) == 1
    assert persisted[0].symbol == "LIVE"
    assert persisted[0].cache_status == ("throttle" if exhausted else "provider_call")
    assert persisted[0].route == "background"


def test_concurrent_reservations_keep_exact_budget_and_each_callers_event(monkeypatch):
    persisted = []
    persisted_lock = Lock()

    def persist(event):
        # Another event is inserted between reserving this event and recording
        # it, so looking up the shared deque after unlocking would be incorrect.
        if event["kind"] in {"provider_call", "throttle"}:
            usage.record_cache_hit(category="quote", symbol="INTERLEAVED")
        with persisted_lock:
            persisted.append(dict(event))

    monkeypatch.setattr(usage, "_persist_event", persist)

    def reserve(index):
        try:
            usage.ensure_fmp_live_allowed(category="profile", symbol=f"S{index}")
            return True
        except usage.ProviderBudgetExceeded:
            return False

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(reserve, range(40)))
    assert sum(results) == 7
    assert len(usage._STATE.provider_call_timestamps) == 7
    assert usage._STATE.counters["provider_call"] == 7
    assert usage._STATE.counters["throttle"] == 33
    attempts = [event for event in persisted if event["kind"] in {"provider_call", "throttle"}]
    assert len(attempts) == 40
    assert {event["symbol"] for event in attempts} == {f"S{i}" for i in range(40)}
    assert sum(event["kind"] == "provider_call" for event in attempts) == 7
    assert all(event["budget_used"] == 7 and event["budget_limit"] == 7
               for event in attempts if event["kind"] == "throttle")
