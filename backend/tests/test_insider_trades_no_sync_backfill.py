from datetime import datetime, timezone
import json

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.db import Base
from app.models import Event
from app.routers import events as events_router


def test_insider_trades_does_not_trigger_sync_outcome_backfill():
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

        payload = events_router.insider_trades(
            reporting_cik="0000019617",
            db=db,
            lookback_days=90,
            limit=50,
        )

    assert payload["reporting_cik"] == "0000019617"
    assert len(payload["items"]) == 1
    assert payload["items"][0]["symbol"] == "JPM"


def test_events_can_skip_price_enrichment_for_read_only_ticker_pages(monkeypatch):
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)

    ts = datetime(2026, 3, 20, tzinfo=timezone.utc)
    with Session(engine) as db:
        db.add(
            Event(
                id=1,
                event_type="congress_trade",
                ts=ts,
                event_date=ts,
                symbol="JPM",
                source="congress",
                trade_type="purchase",
                member_name="Test Member",
                member_bioguide_id="T000001",
                payload_json=json.dumps(
                    {
                        "symbol": "JPM",
                        "trade_date": "2026-03-18",
                    }
                ),
                amount_min=1000,
                amount_max=5000,
            )
        )
        db.commit()

        def _boom(*args, **kwargs):
            raise AssertionError("read-only ticker event fetch should not enrich prices")

        monkeypatch.setattr(events_router, "get_eod_close", _boom)
        monkeypatch.setattr(events_router, "get_current_prices_meta_db", _boom)

        payload = events_router.list_events(
            db=db,
            symbol="JPM",
            recent_days=365,
            limit=100,
            enrich_prices=False,
        )

    assert len(payload.items) == 1
    assert payload.items[0].symbol == "JPM"
    assert payload.items[0].estimated_price is None
    assert payload.items[0].current_price is None


def test_insider_alpha_summary_skips_sync_backfill_for_high_volume_insider():
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
        for idx in range(1, 206)
        ]
        db.add_all(bulk)
        db.commit()

        payload = events_router.insider_alpha_summary(
            reporting_cik="0000019617",
            db=db,
            lookback_days=90,
        )

    assert payload["reporting_cik"] == "0000019617"
    assert payload["trades_analyzed"] == 0


def test_insider_alpha_summary_does_not_sync_backfill_for_small_missing_set():
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

        payload = events_router.insider_alpha_summary(
            reporting_cik="0000019617",
            db=db,
            lookback_days=90,
        )

    assert payload["reporting_cik"] == "0000019617"
    assert payload["trades_analyzed"] == 0


def test_insider_trades_uses_persisted_payload_detail_fallbacks():
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
                symbol="ASX",
                source="fmp",
                trade_type="purchase",
                payload_json=json.dumps(
                    {
                        "symbol": "ASX",
                        "security_name": "ASE Technology Holding Co., Ltd.",
                        "transaction_date": "2026-03-18",
                        "reporting_cik": "0000019617",
                        "insider_name": "Jeffrey Chen",
                        "price": 12.34,
                        "trade_value": 123400,
                        "pnl_pct": 4.2,
                        "smart_score": 72,
                        "smart_band": "strong",
                    }
                ),
                amount_min=100000,
                amount_max=250000,
            )
        )
        db.commit()

        payload = events_router.insider_trades(
            reporting_cik="0000019617",
            db=db,
            lookback_days=90,
            limit=50,
        )

    row = payload["items"][0]
    assert row["symbol"] == "ASX"
    assert row["company_name"] == "ASE Technology Holding Co., Ltd."
    assert row["trade_date"] == "2026-03-18"
    assert row["price"] == 12.34
    assert row["trade_value"] == 123400
    assert row["pnl_pct"] == 4.2
    assert row["pnl_source"] == "persisted_payload"
    assert row["smart_score"] == 72
    assert row["smart_band"] == "strong"
