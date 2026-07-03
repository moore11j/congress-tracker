from datetime import datetime

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import app.main as app_main
from app.db import Base
from app.main import _build_market_quotes_response
from app.models import PriceCache, QuoteCache, TickerMeta
from app.request_priority import RoutePriority, classify_request


def _session():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    TestSession = sessionmaker(bind=engine, future=True)
    return TestSession()


def test_market_quotes_reads_cached_quotes_without_requiring_all_symbols():
    db = _session()
    db.add(TickerMeta(symbol="NVDA", company_name="NVIDIA Corp", exchange="NASDAQ"))
    db.add(QuoteCache(symbol="NVDA", price=123.45, asof_ts=datetime(2026, 6, 3, 20, 0, 0)))
    db.add(PriceCache(symbol="NVDA", date="2026-06-02", close=120.0))
    db.commit()

    response = _build_market_quotes_response("nvda,AAPL,INVALID!,NVDA", db)

    assert response["status"] == "partial"
    assert response["items"][0] == {
        "symbol": "NVDA",
        "company_name": "NVIDIA Corp",
        "current_price": 123.45,
        "day_change_pct": (123.45 - 120.0) / 120.0 * 100,
        "as_of": "2026-06-03T20:00:00",
    }
    assert response["items"][1] == {
        "symbol": "AAPL",
        "company_name": "AAPL",
        "current_price": None,
        "day_change_pct": None,
        "as_of": None,
    }


def test_market_quotes_use_live_user_quote_path(monkeypatch):
    db = _session()
    calls = []

    def fake_quotes(db_arg, symbols, **kwargs):
        calls.append((db_arg, symbols, kwargs))
        return {
            "NVDA": {
                "symbol": "NVDA",
                "price": 160.25,
                "change_percent": 1.75,
                "asof_ts": datetime(2026, 7, 3, 15, 30, 0),
                "is_stale": False,
            }
        }

    monkeypatch.setattr(app_main, "get_current_prices_meta_db", fake_quotes)

    response = _build_market_quotes_response("NVDA", db)

    assert response["status"] == "ok"
    assert response["items"][0]["current_price"] == 160.25
    assert response["items"][0]["day_change_pct"] == 1.75
    assert calls
    _, symbols, kwargs = calls[0]
    assert symbols == ["NVDA"]
    assert kwargs["allow_live_user_fetch"] is True
    assert kwargs["stale_while_revalidate"] is False
    assert kwargs["release_connection_before_fetch"] is True
    assert kwargs["force_quote_endpoint"] is True


def test_market_quotes_prefer_quote_change_percent_over_historical_close(monkeypatch):
    db = _session()
    db.add(PriceCache(symbol="AAPL", date="2026-07-02", close=100.0))
    db.commit()

    monkeypatch.setattr(
        app_main,
        "get_current_prices_meta_db",
        lambda *_args, **_kwargs: {
            "AAPL": {
                "symbol": "AAPL",
                "price": 210.0,
                "change_percent": 0.5,
                "asof_ts": datetime(2026, 7, 3, 15, 30, 0),
                "is_stale": False,
            }
        },
    )

    response = _build_market_quotes_response("AAPL", db)

    assert response["items"][0]["current_price"] == 210.0
    assert response["items"][0]["day_change_pct"] == 0.5
    assert response["items"][0]["day_change_pct"] != 110.0


def test_market_quotes_are_bounded_and_normal_priority():
    db = _session()
    symbols = ",".join(f"S{index}" for index in range(20))

    response = _build_market_quotes_response(symbols, db)

    assert len(response["items"]) == 12
    assert response["status"] == "unavailable"
    assert classify_request("/api/market/quotes", {"symbols": "NVDA,AAPL"}) == RoutePriority.NORMAL


def test_persisted_congress_leaderboard_is_not_global_heavy_priority():
    assert classify_request("/api/leaderboards/congress-traders", {"performance_model": "portfolio"}) == RoutePriority.NORMAL
    assert classify_request("/api/leaderboards/other-expensive-view", {}) == RoutePriority.HEAVY
