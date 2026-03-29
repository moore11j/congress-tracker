from datetime import datetime, timezone
from types import SimpleNamespace
import json

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.db import Base
from app.models import Event, TradeOutcome
from app.routers.events import VISIBLE_INSIDER_TRADE_TYPES, _insider_trade_row
from app.routers.events import _event_cik, _load_insider_trade_outcomes


def _event(*, event_id: int, trade_type: str = "sale", amount_max: int = 10000):
    return SimpleNamespace(
        id=event_id,
        symbol="USNA",
        trade_type=trade_type,
        amount_min=amount_max,
        amount_max=amount_max,
        member_name=None,
        ts=datetime(2026, 1, 15, tzinfo=timezone.utc),
    )


def test_visible_insider_trade_types_include_aliases_used_by_filers():
    assert "sale" in VISIBLE_INSIDER_TRADE_TYPES
    assert "purchase" in VISIBLE_INSIDER_TRADE_TYPES
    assert "s-sale" in VISIBLE_INSIDER_TRADE_TYPES
    assert "p-purchase" in VISIBLE_INSIDER_TRADE_TYPES


def test_unscored_recent_trade_does_not_emit_pnl_or_smart_signal():
    row = _insider_trade_row(
        _event(event_id=1),
        {
            "symbol": "USNA",
            "transaction_date": "2026-01-14",
            "price": 95.0,
            "smart_score": 28,
            "smart_band": "mild",
        },
        outcome=None,
        fallback_pnl_pct=0.5,
    )

    assert row["pnl_pct"] is None
    assert row["pnl_source"] is None
    assert row["smart_score"] is None
    assert row["smart_band"] is None


def test_event_cik_uses_issuer_cik_not_reporting_owner_cik():
    payload = {
        "reporting_cik": "0000019617",
        "raw": {
            "reportingCik": "0000019617",
            "rptOwnerCik": "0000019617",
        },
    }
    assert _event_cik(payload) is None

    payload_with_issuer = {
        "reporting_cik": "0000019617",
        "raw": {
            "companyCik": "0000320193",
            "reportingCik": "0000019617",
        },
    }
    assert _event_cik(payload_with_issuer) == "0000320193"


def test_outcome_fallback_does_not_cross_match_by_symbol_only():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)

    with Session(engine) as db:
        ts = datetime(2026, 3, 20, tzinfo=timezone.utc)
        event = Event(
            id=101,
            event_type="insider_trade",
            ts=ts,
            event_date=ts,
            symbol="AAPL",
            source="fmp",
            trade_type="sale",
            payload_json=json.dumps(
                {
                    "symbol": "AAPL",
                    "transaction_date": "2026-03-18",
                    "reporting_cik": "0000019617",
                }
            ),
            amount_min=1000,
            amount_max=5000,
        )
        db.add(event)
        db.add(
            TradeOutcome(
                id=202,
                event_id=9999,
                member_id="0000100000",
                member_name="Unrelated Insider",
                symbol="AAPL",
                trade_type="sale",
                source="fmp",
                trade_date=datetime(2026, 3, 18, tzinfo=timezone.utc).date(),
                benchmark_symbol="^GSPC",
                return_pct=100.0,
                alpha_pct=103.8,
                amount_min=1000,
                amount_max=5000,
                scoring_status="ok",
                methodology_version="insider_v1",
            )
        )
        db.commit()

        by_event_id, ordered = _load_insider_trade_outcomes(
            db,
            [(event, json.loads(event.payload_json))],
            "0000019617",
            "^GSPC",
            90,
        )

        assert by_event_id == {}
        assert ordered == []
