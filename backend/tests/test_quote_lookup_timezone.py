from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db import Base, ensure_provider_control_schema
import app.services.quote_lookup as quote_lookup


def _reset_quote_lookup_state() -> None:
    quote_lookup._QUOTE_CACHE.clear()
    quote_lookup._QUOTE_META_CACHE.clear()
    quote_lookup._MISS_CACHE.clear()
    quote_lookup._QUOTE_CALL_TIMESTAMPS.clear()
    quote_lookup._quotes_disabled_until = None
    quote_lookup._quotes_disable_reason = None


@pytest.fixture(autouse=True)
def reset_quote_lookup_state_between_tests():
    _reset_quote_lookup_state()
    yield
    _reset_quote_lookup_state()


def _quote_meta_for_cached_asof(monkeypatch, asof_ts):
    _reset_quote_lookup_state()
    monkeypatch.setenv("QUOTE_CACHE_TTL_SECONDS", "300")
    monkeypatch.delenv("FMP_API_KEY", raising=False)
    monkeypatch.setattr(
        quote_lookup,
        "quote_cache_get_many_with_age",
        lambda _db, _symbols: {"AAPL": (190.0, asof_ts)},
    )
    return quote_lookup.get_current_prices_meta_db(
        SimpleNamespace(),
        ["AAPL"],
        allow_cache_write=False,
    )


def _db():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False}, future=True)
    Base.metadata.create_all(engine)
    ensure_provider_control_schema(engine)
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    return Session()


def test_quote_cache_freshness_accepts_naive_cached_timestamp(monkeypatch):
    asof_ts = (datetime.now(timezone.utc) - timedelta(seconds=60)).replace(tzinfo=None)

    meta = _quote_meta_for_cached_asof(monkeypatch, asof_ts)

    assert meta["AAPL"]["price"] == 190.0
    assert meta["AAPL"]["is_stale"] is False


def test_quote_cache_freshness_accepts_aware_utc_cached_timestamp(monkeypatch):
    asof_ts = datetime.now(timezone.utc) - timedelta(seconds=60)

    meta = _quote_meta_for_cached_asof(monkeypatch, asof_ts)

    assert meta["AAPL"]["price"] == 190.0
    assert meta["AAPL"]["is_stale"] is False


def test_quote_cache_freshness_accepts_aware_non_utc_cached_timestamp(monkeypatch):
    eastern = timezone(timedelta(hours=-5))
    asof_ts = (datetime.now(timezone.utc) - timedelta(seconds=60)).astimezone(eastern)

    meta = _quote_meta_for_cached_asof(monkeypatch, asof_ts)

    assert meta["AAPL"]["price"] == 190.0
    assert meta["AAPL"]["is_stale"] is False


def test_quote_cache_freshness_preserves_stale_decision_for_aware_timestamp(monkeypatch):
    asof_ts = datetime.now(timezone.utc) - timedelta(seconds=600)

    meta = _quote_meta_for_cached_asof(monkeypatch, asof_ts)

    assert meta["AAPL"]["price"] == 190.0
    assert meta["AAPL"]["is_stale"] is True


def test_quote_cache_freshness_ignores_none_timestamp_without_type_error(monkeypatch):
    meta = _quote_meta_for_cached_asof(monkeypatch, None)

    assert meta == {}


def test_quote_lookup_uses_configured_intraday_chart_endpoint_before_eod_fallback(monkeypatch):
    _reset_quote_lookup_state()
    db = _db()
    calls: list[tuple[str, dict]] = []

    class FakeResponse:
        status_code = 200

        def json(self):
            return [{"date": "2026-07-01 15:59:00", "close": 213.5, "volume": 100179}]

    def fake_get(url, params=None, timeout=10):
        calls.append((url, dict(params or {})))
        return FakeResponse()

    monkeypatch.setenv("FMP_API_KEY", "secret-key")
    monkeypatch.setenv("FMP_PERSIST_USAGE_EVENTS", "0")
    monkeypatch.setattr(quote_lookup.requests, "get", fake_get)

    try:
        meta = quote_lookup.get_current_prices_meta_db(db, ["AAPL"], allow_cache_write=False)
    finally:
        db.close()

    assert meta["AAPL"]["price"] == 213.5
    assert meta["AAPL"]["asof_ts"] == datetime(2026, 7, 1, 15, 59, 0)
    assert calls
    assert calls[0][0].endswith("/stable/historical-chart/1min")
    assert calls[0][1]["symbol"] == "AAPL"
    assert calls[0][1]["from"]
    assert calls[0][1]["to"]
    assert all(not call[0].endswith("/stable/quote-short") for call in calls)


def test_fresh_memory_quote_cache_returns_without_provider_call(monkeypatch):
    _reset_quote_lookup_state()
    quote_lookup._cache_set_meta(
        "AAPL",
        {"symbol": "AAPL", "price": 213.5, "change": 1.2, "source": "live_provider"},
        lane="ticker_quote",
    )
    monkeypatch.setattr(
        quote_lookup.requests,
        "get",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("provider should not be called")),
    )

    meta = quote_lookup.get_current_prices_meta_db(SimpleNamespace(), ["AAPL"], lane="ticker_quote", allow_live_user_fetch=True)

    assert meta["AAPL"]["price"] == 213.5
    assert meta["AAPL"]["change"] == 1.2
    assert meta["AAPL"]["is_stale"] is False
    assert meta["AAPL"]["source"] in {"cache", "live_provider"}


def test_stale_db_quote_returns_without_blocking_live_fetch(monkeypatch):
    _reset_quote_lookup_state()
    asof_ts = datetime.now(timezone.utc) - timedelta(minutes=5)
    monkeypatch.setenv("QUOTE_FEED_TTL_SECONDS", "30")
    monkeypatch.setattr(
        quote_lookup,
        "quote_cache_get_many_with_age",
        lambda _db, _symbols: {"AAPL": (190.0, asof_ts)},
    )
    enqueued: list[str] = []
    monkeypatch.setattr(quote_lookup, "_enqueue_quote_refreshes", lambda symbols, **_kwargs: enqueued.extend(symbols))
    monkeypatch.setattr(
        quote_lookup.requests,
        "get",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("stale-while-revalidate should not block")),
    )

    meta = quote_lookup.get_current_prices_meta_db(SimpleNamespace(), ["AAPL"], lane="feed_quote", allow_live_user_fetch=True)

    assert meta["AAPL"]["price"] == 190.0
    assert meta["AAPL"]["is_stale"] is True
    assert meta["AAPL"]["source"] == "stale_cache"
    assert enqueued == ["AAPL"]


def test_missing_quote_calls_single_symbol_provider_when_budget_allows(monkeypatch):
    _reset_quote_lookup_state()
    calls: list[str] = []

    class FakeResponse:
        status_code = 200

        def json(self):
            return [{"symbol": "AAPL", "price": 214.25, "change": 1.1, "changesPercentage": 0.52, "volume": 12345, "marketCap": 3300000000000}]

    def fake_get(url, params=None, timeout=10):
        calls.append(dict(params or {}).get("symbol"))
        return FakeResponse()

    monkeypatch.setenv("FMP_API_KEY", "secret-key")
    monkeypatch.setenv("FMP_PERSIST_USAGE_EVENTS", "0")
    monkeypatch.setenv("FMP_QUOTE_PROCESS_CALLS_PER_MINUTE", "10")
    monkeypatch.setattr(quote_lookup.requests, "get", fake_get)

    db = _db()
    try:
        meta = quote_lookup.get_current_prices_meta_db(db, ["AAPL"], lane="ticker_quote", allow_live_user_fetch=True)
    finally:
        db.close()

    assert calls == ["AAPL"]
    assert meta["AAPL"]["price"] == 214.25
    assert meta["AAPL"]["change"] == 1.1
    assert meta["AAPL"]["change_percent"] == 0.52
    assert meta["AAPL"]["volume"] == 12345
    assert meta["AAPL"]["market_cap"] == 3300000000000
    assert meta["AAPL"]["source"] == "live_provider"


def test_force_quote_endpoint_uses_intraday_chart(monkeypatch):
    _reset_quote_lookup_state()
    calls: list[str] = []

    class FakeResponse:
        status_code = 200

        def json(self):
            return [
                {"date": "2026-07-01 15:59:00", "close": 214.25, "volume": 12345},
                {"date": "2026-07-01 09:30:00", "close": 200.0, "volume": 1000},
            ]

    def fake_get(url, params=None, timeout=10):
        calls.append(url)
        assert params["symbol"] == "AAPL"
        assert params["from"]
        assert params["to"]
        assert params["from"] < params["to"]
        return FakeResponse()

    monkeypatch.setenv("FMP_API_KEY", "secret-key")
    monkeypatch.setenv("FMP_PERSIST_USAGE_EVENTS", "0")
    monkeypatch.setenv("FMP_QUOTE_PROCESS_CALLS_PER_MINUTE", "10")
    monkeypatch.setattr(quote_lookup.requests, "get", fake_get)

    db = _db()
    try:
        meta = quote_lookup.get_current_prices_meta_db(
            db,
            ["AAPL"],
            lane="ticker_quote",
            allow_live_user_fetch=True,
            force_quote_endpoint=True,
        )
    finally:
        db.close()

    assert calls
    assert calls[0].endswith("/stable/historical-chart/1min")
    assert all("historical-price-eod/light" not in call for call in calls)
    assert meta["AAPL"]["price"] == 214.25
    assert meta["AAPL"]["asof_ts"] == datetime(2026, 7, 1, 15, 59, 0)
    assert meta["AAPL"]["volume"] == 12345
    assert meta["AAPL"]["source"] == "live_quote"


def test_force_quote_endpoint_fetches_multiple_symbols_with_small_parallelism(monkeypatch):
    _reset_quote_lookup_state()
    calls: list[str] = []

    class FakeResponse:
        status_code = 200

        def __init__(self, symbol: str):
            self.symbol = symbol

        def json(self):
            return [{"date": "2026-07-01 15:59:00", "close": 200.0 if self.symbol == "AAPL" else 300.0}]

    def fake_get(url, params=None, timeout=10):
        symbol = dict(params or {}).get("symbol")
        calls.append(symbol)
        __import__("time").sleep(0.05)
        return FakeResponse(symbol)

    monkeypatch.setenv("FMP_API_KEY", "secret-key")
    monkeypatch.setenv("FMP_PERSIST_USAGE_EVENTS", "0")
    monkeypatch.setenv("FMP_QUOTE_PROCESS_CALLS_PER_MINUTE", "10")
    monkeypatch.setenv("QUOTE_FORCE_ENDPOINT_MAX_WORKERS", "2")
    monkeypatch.setattr(quote_lookup.requests, "get", fake_get)

    db = _db()
    started = __import__("time").perf_counter()
    try:
        meta = quote_lookup.get_current_prices_meta_db(
            db,
            ["AAPL", "NVDA"],
            lane="ticker_quote",
            allow_live_user_fetch=True,
            force_quote_endpoint=True,
            allow_cache_write=False,
        )
    finally:
        db.close()
    elapsed = __import__("time").perf_counter() - started

    assert sorted(calls) == ["AAPL", "NVDA"]
    assert elapsed < 0.14
    assert meta["AAPL"]["price"] == 200.0
    assert meta["NVDA"]["price"] == 300.0


def test_force_quote_endpoint_skips_persistent_quote_cache(monkeypatch):
    _reset_quote_lookup_state()
    asof_ts = datetime.now(timezone.utc).replace(tzinfo=None)
    calls: list[str] = []

    class FakeResponse:
        status_code = 200

        def json(self):
            return [{"date": "2026-07-01 15:59:00", "close": 214.25}]

    def fake_get(url, params=None, timeout=10):
        calls.append(url)
        return FakeResponse()

    monkeypatch.setenv("FMP_API_KEY", "secret-key")
    monkeypatch.setenv("FMP_PERSIST_USAGE_EVENTS", "0")
    monkeypatch.setenv("FMP_QUOTE_PROCESS_CALLS_PER_MINUTE", "10")
    monkeypatch.setattr(quote_lookup.requests, "get", fake_get)
    monkeypatch.setattr(
        quote_lookup,
        "quote_cache_get_many_with_age",
        lambda _db, _symbols: {"AAPL": (142.02, asof_ts)},
    )

    meta = quote_lookup.get_current_prices_meta_db(
        SimpleNamespace(),
        ["AAPL"],
        lane="ticker_quote",
        allow_live_user_fetch=True,
        force_quote_endpoint=True,
    )

    assert calls
    assert meta["AAPL"]["price"] == 214.25
    assert meta["AAPL"]["price"] != 142.02


def test_quote_budget_exhausted_returns_stale_cache_without_provider_call(monkeypatch):
    _reset_quote_lookup_state()
    asof_ts = datetime.now(timezone.utc) - timedelta(minutes=5)
    monkeypatch.setenv("QUOTE_FEED_TTL_SECONDS", "30")
    monkeypatch.setenv("FMP_QUOTE_PROCESS_CALLS_PER_MINUTE", "1")
    quote_lookup._QUOTE_CALL_TIMESTAMPS.append(__import__("time").time())
    monkeypatch.setattr(
        quote_lookup,
        "quote_cache_get_many_with_age",
        lambda _db, _symbols: {"AAPL": (190.0, asof_ts)},
    )
    monkeypatch.setattr(
        quote_lookup.requests,
        "get",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("provider should not be called")),
    )

    meta = quote_lookup.get_current_prices_meta_db(
        SimpleNamespace(),
        ["AAPL"],
        lane="feed_quote",
        allow_live_user_fetch=True,
        stale_while_revalidate=False,
    )

    assert meta["AAPL"]["price"] == 190.0
    assert meta["AAPL"]["is_stale"] is True


def test_quote_circuit_open_returns_unavailable_without_provider_call(monkeypatch):
    _reset_quote_lookup_state()
    quote_lookup._disable_quotes(minutes=1, reason="rate_limited_429_configured_quote")
    monkeypatch.setenv("FMP_API_KEY", "secret-key")
    monkeypatch.setattr(quote_lookup, "quote_cache_get_many_with_age", lambda _db, _symbols: {})
    monkeypatch.setattr(
        quote_lookup.requests,
        "get",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("provider should not be called")),
    )

    meta = quote_lookup.get_current_prices_meta_db(SimpleNamespace(), ["AAPL"], lane="ticker_quote", allow_live_user_fetch=True)

    assert meta["AAPL"]["price"] is None
    assert meta["AAPL"]["status"] == "provider_429"


def test_quote_coalescing_prevents_duplicate_same_symbol_calls(monkeypatch):
    _reset_quote_lookup_state()
    calls: list[str] = []

    class FakeResponse:
        status_code = 200

        def json(self):
            return [{"symbol": "AAPL", "price": 214.25}]

    def fake_get(url, params=None, timeout=10):
        calls.append(dict(params or {}).get("symbol"))
        __import__("time").sleep(0.05)
        return FakeResponse()

    monkeypatch.setenv("FMP_API_KEY", "secret-key")
    monkeypatch.setenv("FMP_PERSIST_USAGE_EVENTS", "0")
    monkeypatch.setenv("FMP_QUOTE_PROCESS_CALLS_PER_MINUTE", "10")
    monkeypatch.setattr(quote_lookup.requests, "get", fake_get)

    results: list[dict] = []

    def run_lookup():
        db = _db()
        try:
            results.append(quote_lookup.get_current_prices_meta_db(db, ["AAPL"], lane="ticker_quote", allow_live_user_fetch=True))
        finally:
            db.close()

    import threading

    first = threading.Thread(target=run_lookup)
    second = threading.Thread(target=run_lookup)
    first.start()
    second.start()
    first.join()
    second.join()

    assert calls == ["AAPL"]
    assert len(results) == 2
    assert all(result["AAPL"]["price"] == 214.25 for result in results)
