from datetime import datetime, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.main import _build_ticker_chart_bundle
from app.models import Event, PriceCache


def _session():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    TestSession = sessionmaker(bind=engine, future=True)
    return TestSession()


def test_ticker_chart_bundle_uses_daily_prices_sp500_and_normalized_markers(monkeypatch):
    db = _session()
    for symbol, rows in {
        "AAPL": [
            ("2026-04-09", 190.0),
            ("2026-04-10", 195.0),
        ],
        "^GSPC": [
            ("2026-04-09", 5100.0),
            ("2026-04-10", 5150.0),
        ],
    }.items():
        for day, close in rows:
            db.add(PriceCache(symbol=symbol, date=day, close=close))

    db.add(
        Event(
            event_type="congress_trade",
            ts=datetime(2026, 4, 10, tzinfo=timezone.utc),
            event_date=datetime(2026, 4, 10, tzinfo=timezone.utc),
            symbol="AAPL",
            source="house",
            impact_score=1.0,
            payload_json='{"trade_date":"2026-04-10"}',
            member_name="Example Member",
            member_bioguide_id="E000001",
            chamber="House",
            party="D",
            trade_type="Purchase",
            amount_min=1000,
            amount_max=15000,
        )
    )
    db.commit()

    monkeypatch.setattr(
        "app.main._quote_snapshot_from_fmp",
        lambda symbol: {
            "price": 196.0,
            "previousClose": 195.0,
            "marketCap": 3_000_000_000,
            "avgVolume": 50_000_000,
            "pe": 28.5,
            "beta": 1.2,
        },
    )
    monkeypatch.setattr("app.main._query_unified_signals", lambda **kwargs: [])

    bundle = _build_ticker_chart_bundle("aapl", 30, db)

    assert bundle["resolution"] == "daily"
    assert bundle["benchmark"]["symbol"] == "^GSPC"
    assert bundle["benchmark"]["label"] == "S&P 500"
    assert bundle["prices"][-1] == {"date": "2026-04-10", "close": 195.0}
    assert bundle["benchmark"]["points"][-1]["close"] == 5150.0
    assert bundle["markers"][0]["kind"] == "congress"
    assert bundle["markers"][0]["date"] == "2026-04-10"
    assert bundle["quote"]["current_price"] == 196.0
    assert bundle["quote"]["day_change"] == 1.0
    assert bundle["quote"]["market_cap"] == 3_000_000_000
