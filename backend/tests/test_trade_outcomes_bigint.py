from datetime import datetime, timezone

from sqlalchemy import BigInteger, create_engine, select
from sqlalchemy.orm import sessionmaker

import app.compute_trade_outcomes as compute_module
from app.db import Base
from app.models import Event, TradeOutcome


LARGE_AMOUNT_MIN = 2_147_483_648
LARGE_AMOUNT_MAX = 3_000_000_000


def test_trade_outcome_amount_columns_are_bigint() -> None:
    assert isinstance(TradeOutcome.__table__.c.amount_min.type, BigInteger)
    assert isinstance(TradeOutcome.__table__.c.amount_max.type, BigInteger)


def test_event_amount_columns_are_bigint() -> None:
    assert isinstance(Event.__table__.c.amount_min.type, BigInteger)
    assert isinstance(Event.__table__.c.amount_max.type, BigInteger)


def test_compute_trade_outcomes_persists_amounts_above_postgres_int32(monkeypatch) -> None:
    engine = create_engine("sqlite:///:memory:", future=True)
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    Base.metadata.create_all(bind=engine, tables=[Event.__table__, TradeOutcome.__table__])

    event_ts = datetime(2026, 5, 1, tzinfo=timezone.utc)
    with SessionLocal() as db:
        db.add(
            Event(
                id=9001,
                event_type="congress_trade",
                ts=event_ts,
                event_date=event_ts,
                symbol="MEGA",
                source="test",
                payload_json="{}",
                member_name="Large Trade",
                member_bioguide_id="L000001",
                trade_type="purchase",
                transaction_type="purchase",
                amount_min=LARGE_AMOUNT_MIN,
                amount_max=LARGE_AMOUNT_MAX,
            )
        )
        db.commit()

    def fake_compute_congress_trade_outcomes(*, db, events, benchmark_symbol):
        assert benchmark_symbol == "^GSPC"
        assert [event.id for event in events] == [9001]
        return [
            {
                "event_id": 9001,
                "member_id": "L000001",
                "member_name": "Large Trade",
                "symbol": "MEGA",
                "trade_type": "purchase",
                "source": "test",
                "trade_date": "2026-05-01",
                "entry_price": 10.0,
                "entry_price_date": "2026-05-01",
                "current_price": 11.0,
                "current_price_date": "2026-05-02",
                "benchmark_symbol": "^GSPC",
                "benchmark_entry_price": 5000.0,
                "benchmark_current_price": 5050.0,
                "return_pct": 10.0,
                "benchmark_return_pct": 1.0,
                "alpha_pct": 9.0,
                "holding_days": 1,
                "amount_min": events[0].amount_min,
                "amount_max": events[0].amount_max,
                "scoring_status": "ok",
                "scoring_error": None,
                "methodology_version": "congress_v1",
            }
        ]

    monkeypatch.setattr(compute_module, "engine", engine)
    monkeypatch.setattr(compute_module, "SessionLocal", SessionLocal)
    monkeypatch.setattr(compute_module, "ensure_event_columns", lambda: None)
    monkeypatch.setattr(compute_module, "ensure_trade_outcomes_amount_bigint", lambda: None)
    monkeypatch.setattr(compute_module, "compute_congress_trade_outcomes", fake_compute_congress_trade_outcomes)

    report = compute_module.run_compute(
        replace=True,
        limit=None,
        member_id=None,
        event_type="congress_trade",
        benchmark_symbol="^GSPC",
        lookback_days=None,
        trade_date_after=None,
        only_missing=False,
        retry_failed_status=None,
        retry_failed_statuses=None,
    )

    assert report["inserted"] == 1
    with SessionLocal() as db:
        row = db.execute(select(TradeOutcome).where(TradeOutcome.event_id == 9001)).scalar_one()
        assert row.amount_min == LARGE_AMOUNT_MIN
        assert row.amount_max == LARGE_AMOUNT_MAX
