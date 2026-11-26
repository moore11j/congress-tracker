"""Deterministic minute-chart fixtures; no real provider requests."""
from datetime import date
from pathlib import Path
import json

import pytest

from test_massive_price_core import db, isolated
from app.models import PriceCache
from app.services.chart_session_returns import intraday_session_returns


def close(db, day, price, symbol="NBIS", **kwargs):
    row = PriceCache(symbol=symbol, date=day, close=price, price_source="massive",
                     adjustment_status="split_adjusted_price_return")
    for key, value in kwargs.items():
        setattr(row, key, value)
    db.add(row)
    db.commit()


def result(db, timestamp, price=98.08):
    return intraday_session_returns(db, "NBIS", {"date": timestamp, "close": price}, selected_massive=True)


@pytest.mark.parametrize("symbol", ["NBIS", "NVDA"])
@pytest.mark.parametrize("days", [1, 5])
def test_bundle_day_return_is_regular_close_to_close_not_last_two_minutes(monkeypatch, db, symbol, days):
    from app import main
    evidence = json.loads((Path(__file__).parent / "fixtures/chart_regular_closes_nov25.json").read_text())
    bars = [r for r in evidence["rows"] if r["symbol"] == symbol]
    previous, regular = [r["close"] for r in bars]
    for bar in bars:
        close(db, bar["date"], bar["close"], symbol, price_source=bar["price_source"])
    # Minute prices are synthetic to isolate the reported adjacent-minute bug.
    scale = previous / 100
    expected = (regular - previous) / previous * 100
    minute = [{"date": "2026-11-26T00:58:00Z", "close": 98 * scale},
              {"date": "2026-11-26T00:59:00Z", "close": 98.08 * scale}]
    points = ([{"date": "2026-11-24T23:59:00Z", "close": 150 * scale}] if days == 5 else []) + minute
    monkeypatch.setattr(main, "get_request_context", lambda: {})
    monkeypatch.setattr(main, "get_expected_latest_market_date", lambda: date(2026, 11, 25))
    monkeypatch.setattr(main, "is_price_history_stale", lambda *a, **k: {"is_stale": False})
    monkeypatch.setattr(main, "get_intraday_chart_series_with_fallback", lambda *a, **k: points)
    monkeypatch.setattr(main, "select_visible_ticker_events", lambda *a, **k: [])
    monkeypatch.setattr(main, "_query_unified_signals", lambda **k: [])
    monkeypatch.setattr(main, "_cached_ticker_fundamentals_row", lambda *a: None)
    monkeypatch.setattr(main, "enqueue_data_enrichment_job", lambda **k: None)
    bundle = main._build_ticker_chart_bundle(symbol, days, db)
    quote = bundle["quote"]
    assert bundle["resolution"] == "1min"
    assert quote["current_price"] == quote["latest_close"] == 98.08 * scale
    assert quote["previous_close"] == previous
    assert quote["day_change_pct"] == pytest.approx(expected)
    assert round(expected, 2) == {"NBIS": -2.66, "NVDA": -3.91}[symbol]
    assert quote["day_change_pct"] != pytest.approx((98.08 / 98 - 1) * 100)
    assert quote["extended_hours_change_pct"] == pytest.approx((98.08 * scale - regular) / regular * 100)
    assert quote["market_session"] == "after_hours"
    assert quote["regular_session_date"] == "2026-11-25"
    assert quote["previous_close_date"] == "2026-11-24"
    assert quote["price_kind"] == "minute_close" and quote["is_delayed"] is True
    assert quote["delay_minutes"] == 15 and quote["asof"] == minute[-1]["date"]
    assert quote["source_freshness"]["price_source"] == "minute_series"
    daily = main._build_ticker_chart_quote(db, symbol, [
        {"date": "2026-11-24", "close": previous},
        {"date": "2026-11-25", "close": regular}])
    assert daily["day_change_pct"] == quote["day_change_pct"]
    assert daily["price_kind"] == "daily_close" and daily["is_delayed"] is None


def test_regular_session_uses_live_minute_against_previous_close_not_future_eod(db):
    close(db, "2026-11-24", 100)
    close(db, "2026-11-25", 500)
    row = result(db, "2026-11-25T15:30:00Z", 101)
    assert row["market_session"] == "regular" and row["day_change_pct"] == 1
    assert row["extended_hours_change_pct"] is None


def test_after_hours_missing_current_close_does_not_use_last_regular_minute(db):
    close(db, "2026-11-24", 100)
    row = result(db, "2026-11-25T21:00:00Z")
    assert row["previous_close"] == 100
    assert row["day_change_pct"] is None and row["extended_hours_change_pct"] is None


@pytest.mark.parametrize("bad", [None, 0, -1, float("inf")])
def test_missing_or_invalid_previous_close_never_uses_older_session(db, bad):
    close(db, "2026-11-23", 100)
    if bad is not None:
        close(db, "2026-11-24", bad)
    close(db, "2026-11-25", 97.34)
    row = result(db, "2026-11-26T00:59:00Z")
    assert row["day_change_pct"] is None
    assert row["extended_hours_change_pct"] is not None


@pytest.mark.parametrize("field,value", [("price_source", "fmp"), ("adjustment_status", "unadjusted")])
def test_massive_comparison_rejects_mixed_provider_or_price_basis(db, field, value):
    close(db, "2026-11-24", 100, **{field: value})
    assert result(db, "2026-11-25T15:30:00Z")["day_change_pct"] is None


def test_premarket_is_separate_and_does_not_use_future_regular_close(db):
    close(db, "2026-11-24", 100)
    close(db, "2026-11-25", 500)
    row = result(db, "2026-11-25T13:00:00Z", 101)
    assert row["market_session"] == "pre_market" and row["day_change_pct"] is None
    assert row["extended_hours_change_pct"] == 1


@pytest.mark.parametrize("timestamp,day,prior,session", [
    ("2026-11-27T17:59:00Z", "2026-11-27", "2026-11-25", "regular"),
    ("2026-11-27T18:00:00Z", "2026-11-27", "2026-11-25", "after_hours"),
    ("2026-12-24T18:01:00Z", "2026-12-24", "2026-12-23", "after_hours"),
    ("2026-07-02T18:00:00Z", "2026-07-02", "2026-07-01", "regular"),
    ("2026-07-06T20:00:00Z", "2026-07-06", "2026-07-02", "after_hours"),
    ("2026-03-09T19:59:00Z", "2026-03-09", "2026-03-06", "regular"),
])
def test_holiday_weekend_early_close_and_dst(db, timestamp, day, prior, session):
    close(db, prior, 100)
    close(db, day, 101)
    row = result(db, timestamp, 101)
    assert row["market_session"] == session and row["previous_close_date"] == prior
    assert row["day_change_pct"] == 1


@pytest.mark.parametrize("timestamp", ["invalid", "2026-11-25T15:00:00", "2026-11-26T15:00:00Z"])
def test_invalid_or_closed_session_never_invents_return(db, timestamp):
    assert result(db, timestamp)["day_change_pct"] is None
