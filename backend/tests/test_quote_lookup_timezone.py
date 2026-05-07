from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

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
