from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.db import Base
from app.models import Event, PriceCache
from app.services.confirmation_score import (
    confirmation_band_for_score,
    get_confirmation_score_bundle_for_ticker,
)


def _event(
    *,
    event_id: int,
    symbol: str,
    event_type: str,
    trade_type: str,
    event_date: datetime,
    amount_max: int = 250_000,
):
    return Event(
        id=event_id,
        event_type=event_type,
        ts=event_date,
        event_date=event_date,
        symbol=symbol,
        source="test",
        payload_json=json.dumps({"symbol": symbol, "reporting_cik": "000123"}),
        trade_type=trade_type,
        amount_min=10_000,
        amount_max=amount_max,
    )


def _price(symbol: str, day: datetime, close: float) -> PriceCache:
    return PriceCache(symbol=symbol, date=day.date().isoformat(), close=close)


def test_confirmation_band_thresholds_match_product_contract():
    assert confirmation_band_for_score(0) == "inactive"
    assert confirmation_band_for_score(19) == "inactive"
    assert confirmation_band_for_score(20) == "weak"
    assert confirmation_band_for_score(40) == "moderate"
    assert confirmation_band_for_score(60) == "strong"
    assert confirmation_band_for_score(80) == "exceptional"


def test_confirmation_score_bundle_combines_insider_and_price_confirmation():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)

    now = datetime.now(timezone.utc)
    start = now - timedelta(days=29)
    recent = now - timedelta(days=5)

    with Session(engine) as db:
        db.add(
            _event(
                event_id=1,
                symbol="CRM",
                event_type="insider_trade",
                trade_type="sale",
                event_date=recent,
            )
        )
        db.add_all(
            [
                _price("CRM", start, 100),
                _price("CRM", recent, 90),
                _price("^GSPC", start, 100),
                _price("^GSPC", recent, 98),
            ]
        )
        db.commit()

        bundle = get_confirmation_score_bundle_for_ticker(db, "CRM", lookback_days=30)

        assert bundle["ticker"] == "CRM"
        assert bundle["band"] in {"weak", "moderate", "strong"}
        assert bundle["direction"] == "bearish"
        assert bundle["status"] == "2-source bearish confirmation"
        assert bundle["sources"]["insiders"]["present"] is True
        assert bundle["sources"]["price_volume"]["present"] is True
        assert bundle["sources"]["congress"]["present"] is False
        assert 0 <= bundle["score"] <= 100
        assert 2 <= len(bundle["drivers"]) <= 4


def test_confirmation_score_bundle_degrades_to_inactive_without_sources():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)

    with Session(engine) as db:
        bundle = get_confirmation_score_bundle_for_ticker(db, "ZZZ", lookback_days=30)

        assert bundle["score"] == 0
        assert bundle["band"] == "inactive"
        assert bundle["direction"] == "neutral"
        assert bundle["status"] == "Inactive"
        assert bundle["sources"]["congress"]["present"] is False
        assert bundle["sources"]["insiders"]["present"] is False
        assert bundle["sources"]["signals"]["present"] is False
        assert bundle["sources"]["price_volume"]["present"] is False
