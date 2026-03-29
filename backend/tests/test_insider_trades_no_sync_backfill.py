from datetime import datetime, timezone
import json

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.db import Base
from app.models import Event
from app.routers import events as events_router


def test_insider_trades_does_not_trigger_sync_outcome_backfill(monkeypatch):
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)

    ts = datetime(2026, 3, 20, tzinfo=timezone.utc)
    with Session(engine) as db:
        db.add(
            Event(
                id=1,
                event_type="insider_trade",
                ts=ts,
                event_date=ts,
                symbol="JPM",
                source="fmp",
                trade_type="sale",
                payload_json=json.dumps(
                    {
                        "symbol": "JPM",
                        "transaction_date": "2026-03-18",
                        "reporting_cik": "0000019617",
                        "insider_name": "Test Insider",
                    }
                ),
                amount_min=1000,
                amount_max=5000,
            )
        )
        db.commit()

        def _boom(*args, **kwargs):
            raise AssertionError("ensure_insider_trade_outcomes_for_cik should not be called by /trades")

        monkeypatch.setattr(events_router, "ensure_insider_trade_outcomes_for_cik", _boom)

        payload = events_router.insider_trades(
            reporting_cik="0000019617",
            db=db,
            lookback_days=90,
            limit=50,
        )

    assert payload["reporting_cik"] == "0000019617"
    assert len(payload["items"]) == 1
    assert payload["items"][0]["symbol"] == "JPM"
