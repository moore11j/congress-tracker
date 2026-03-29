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


def test_insider_alpha_summary_skips_sync_backfill_for_high_volume_insider(monkeypatch):
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)

    ts = datetime(2026, 3, 20, tzinfo=timezone.utc)
    with Session(engine) as db:
        bulk = [
            Event(
                id=idx,
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
            for idx in range(1, events_router.INSIDER_ALPHA_ENSURE_MAX_EVENTS + 6)
        ]
        db.add_all(bulk)
        db.commit()

        def _boom(*args, **kwargs):
            raise AssertionError("ensure_insider_trade_outcomes_for_cik should not be called for high-volume alpha-summary")

        monkeypatch.setattr(events_router, "ensure_insider_trade_outcomes_for_cik", _boom)

        payload = events_router.insider_alpha_summary(
            reporting_cik="0000019617",
            db=db,
            lookback_days=90,
        )

    assert payload["reporting_cik"] == "0000019617"
    assert payload["trades_analyzed"] == 0


def test_insider_alpha_summary_keeps_bounded_sync_backfill_for_small_missing_set(monkeypatch):
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

        calls = {"count": 0}

        def _count_calls(*args, **kwargs):
            calls["count"] += 1
            return {"scanned_events": 1, "computed": 0, "inserted": 0, "updated": 0}

        monkeypatch.setattr(events_router, "ensure_insider_trade_outcomes_for_cik", _count_calls)

        events_router.insider_alpha_summary(
            reporting_cik="0000019617",
            db=db,
            lookback_days=90,
        )

    assert calls["count"] == 1
