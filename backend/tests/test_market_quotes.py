from datetime import datetime

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

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
