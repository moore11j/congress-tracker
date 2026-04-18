from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.db import Base
from app.models import Event
from app.routers.signals import _query_unified_signals


def _event(
    *,
    event_id: int,
    symbol: str,
    event_date: datetime,
    amount_max: int,
) -> Event:
    return Event(
        id=event_id,
        event_type="congress_trade",
        ts=event_date,
        event_date=event_date,
        symbol=symbol,
        source="test",
        payload_json=json.dumps({"symbol": symbol}),
        member_name="Test Member",
        member_bioguide_id=f"{symbol}1",
        chamber="House",
        party="I",
        trade_type="purchase",
        amount_min=100,
        amount_max=amount_max,
    )


def _seed_unusual_congress_rows(db: Session) -> None:
    now = datetime.now(timezone.utc)
    event_id = 1
    for symbol in ("AAA", "BBB"):
        for days_back in (120, 100, 80):
            db.add(
                _event(
                    event_id=event_id,
                    symbol=symbol,
                    event_date=now - timedelta(days=days_back),
                    amount_max=100,
                )
            )
            event_id += 1
        db.add(
            _event(
                event_id=event_id,
                symbol=symbol,
                event_date=now - timedelta(days=3),
                amount_max=1_000,
            )
        )
        event_id += 1
    db.commit()


def test_unified_signals_can_filter_and_sort_by_confirmation_score(monkeypatch):
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)

    def fake_confirmation_bundles(_db, symbols, *, lookback_days=30):
        assert lookback_days == 30
        assert set(symbols) == {"AAA", "BBB"}
        return {
            "AAA": {
                "confirmation_score": 82,
                "confirmation_band": "exceptional",
                "confirmation_direction": "bullish",
                "confirmation_status": "2-source bullish confirmation",
                "confirmation_source_count": 2,
                "confirmation_explanation": "Congress buy-skewed",
                "is_multi_source": True,
                "signal_freshness": {
                    "ticker": "AAA",
                    "lookback_days": 30,
                    "freshness_score": 86,
                    "freshness_state": "fresh",
                    "freshness_label": "Fresh multi-source setup",
                    "explanation": "Recent Congress activity and smart signal remain tightly clustered.",
                    "timing": {
                        "freshest_source_days": 2,
                        "stalest_active_source_days": 6,
                        "active_source_count": 2,
                        "overlap_window_days": 4,
                    },
                },
            },
            "BBB": {
                "confirmation_score": 34,
                "confirmation_band": "weak",
                "confirmation_direction": "bullish",
                "confirmation_status": "Single-source bullish",
                "confirmation_source_count": 1,
                "confirmation_explanation": "Congress buy-skewed",
                "is_multi_source": False,
                "signal_freshness": {
                    "ticker": "BBB",
                    "lookback_days": 30,
                    "freshness_score": 62,
                    "freshness_state": "early",
                    "freshness_label": "Early setup",
                    "explanation": "A single recent source is active, but broader confirmation is still limited.",
                    "timing": {
                        "freshest_source_days": 3,
                        "stalest_active_source_days": 3,
                        "active_source_count": 1,
                        "overlap_window_days": 0,
                    },
                },
            },
        }

    monkeypatch.setattr("app.routers.signals.get_slim_confirmation_score_bundles_for_tickers", fake_confirmation_bundles)

    with Session(engine) as db:
        _seed_unusual_congress_rows(db)

        items = _query_unified_signals(
            db=db,
            mode="all",
            sort="confirmation",
            limit=10,
            offset=0,
            baseline_days=365,
            congress_recent_days=30,
            insider_recent_days=30,
            congress_min_baseline_count=3,
            insider_min_baseline_count=3,
            congress_multiple=1.5,
            insider_multiple=1.5,
            congress_min_amount=0,
            insider_min_amount=0,
            min_smart_score=None,
            side="all",
            symbol=None,
            confirmation_band="strong_plus",
            confirmation_direction="bullish",
            min_confirmation_sources=2,
        )

    assert [item.symbol for item in items] == ["AAA"]
    assert items[0].confirmation_score == 82
    assert items[0].confirmation_band == "exceptional"
    assert items[0].confirmation_source_count == 2
    assert items[0].is_multi_source is True
    assert items[0].signal_freshness is not None
    assert items[0].signal_freshness.freshness_state == "fresh"
    assert items[0].signal_freshness.freshness_score == 86
