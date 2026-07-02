from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db import Base, ensure_provider_control_schema
import app.services.quote_lookup as quote_lookup


def _reset_quote_lookup_state() -> None:
    quote_lookup._QUOTE_CACHE.clear()
    quote_lookup._MISS_CACHE.clear()
    quote_lookup._quotes_disabled_until = None
    quote_lookup._quotes_disable_reason = None


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
