from datetime import datetime, timezone
import json

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from app.db import Base
from app.models import Event, TradeOutcome
from app.services.trade_outcomes import ensure_insider_trade_outcomes_for_cik


def _event(*, event_id: int, reporting_cik: str, trade_type: str = "sale") -> Event:
    ts = datetime(2026, 3, 20, tzinfo=timezone.utc)
    return Event(
        id=event_id,
        event_type="insider_trade",
        ts=ts,
        event_date=ts,
        symbol="USNA",
        source="fmp",
        trade_type=trade_type,
        transaction_type=trade_type,
        payload_json=json.dumps(
            {
                "symbol": "USNA",
                "reporting_cik": reporting_cik,
                "transaction_date": "2026-03-20",
                "price": 17.0,
                "is_market_trade": True,
            }
        ),
    )


def test_ensure_insider_trade_outcomes_for_cik_inserts_missing_row(monkeypatch):
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)

    with Session(engine) as db:
        db.add(_event(event_id=115121, reporting_cik="0001203302"))
        db.commit()

        def fake_compute(*, db, events, benchmark_symbol, max_symbols_per_request=None):
            assert benchmark_symbol == "^GSPC"
            assert [event.id for event in events] == [115121]
            return [
                {
                    "event_id": 115121,
                    "member_id": "0001203302",
                    "member_name": "Gilbert A Fuller",
                    "symbol": "USNA",
                    "trade_type": "sale",
                    "source": "fmp",
                    "trade_date": "2026-03-20",
                    "entry_price": 17.0,
                    "entry_price_date": "2026-03-20",
                    "current_price": 16.0,
                    "current_price_date": "2026-03-21",
                    "benchmark_symbol": "^GSPC",
                    "benchmark_entry_price": 5000.0,
                    "benchmark_current_price": 5050.0,
                    "return_pct": 5.88,
                    "benchmark_return_pct": 1.0,
                    "alpha_pct": 4.88,
                    "holding_days": 1,
                    "amount_min": None,
                    "amount_max": None,
                    "scoring_status": "ok",
                    "scoring_error": None,
                    "methodology_version": "insider_v1",
                }
            ]

        monkeypatch.setattr("app.services.trade_outcomes.compute_insider_trade_outcomes", fake_compute)

        report = ensure_insider_trade_outcomes_for_cik(
            db=db,
            reporting_cik="0001203302",
            lookback_days=90,
            benchmark_symbol="^GSPC",
        )

        assert report["scanned_events"] == 1
        assert report["computed"] == 1
        assert report["inserted"] == 1
        assert report["updated"] == 0

        row = db.execute(select(TradeOutcome).where(TradeOutcome.event_id == 115121)).scalar_one()
        assert row.member_id == "0001203302"
        assert row.scoring_status == "ok"
        assert row.methodology_version == "insider_v1"
        assert row.return_pct == 5.88
