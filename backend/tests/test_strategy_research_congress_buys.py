from __future__ import annotations

import json
from datetime import date, datetime, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.models import Event, PriceCache
from app.strategy_research.congress_buys import (
    Lot,
    PriceBar,
    ResearchConfig,
    Signal,
    load_congress_purchase_signals,
    run_research,
    simulate_active_lot_portfolio,
)


def _session():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    Base.metadata.create_all(bind=engine)
    return SessionLocal()


def _price(db, symbol: str, day: str, adjusted_close: float) -> None:
    db.add(
        PriceCache(
            symbol=symbol,
            date=day,
            close=adjusted_close,
            adjusted_close=adjusted_close,
            raw_close=adjusted_close,
        )
    )


def _event(
    db,
    *,
    event_id: int = 1,
    payload: dict,
    event_date: datetime | None = None,
    trade_type: str = "purchase",
) -> None:
    db.add(
        Event(
            id=event_id,
            event_type="congress_trade",
            ts=datetime(2024, 1, 5, 12, 0, tzinfo=timezone.utc),
            event_date=event_date,
            symbol="AAPL",
            source="congress",
            trade_type=trade_type,
            amount_min=1000,
            amount_max=15000,
            member_name="Example Member",
            member_bioguide_id="E000001",
            payload_json=json.dumps(payload),
        )
    )


def test_congress_signal_uses_filing_date_not_transaction_date():
    db = _session()
    try:
        _event(
            db,
            payload={
                "symbol": "AAPL",
                "filing_date": "2024-01-05",
                "transaction_date": "2024-01-02",
            },
        )
        db.commit()

        signals = load_congress_purchase_signals(
            db,
            universe=("AAPL",),
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 31),
        )

        assert len(signals) == 1
        assert signals[0].disclosure_date == date(2024, 1, 5)
        assert signals[0].raw_entry_date == date(2024, 1, 6)
    finally:
        db.close()


def test_congress_signals_dedupe_duplicate_event_rows_without_doc_ids():
    db = _session()
    try:
        payload = {
            "symbol": "AAPL",
            "filing_date": "2024-01-05",
            "transaction_date": "2024-01-02",
        }
        _event(db, event_id=1, payload=payload)
        _event(db, event_id=2, payload=payload)
        db.commit()

        signals = load_congress_purchase_signals(
            db,
            universe=("AAPL",),
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 31),
        )

        assert len(signals) == 1
        assert signals[0].event_id == 1
    finally:
        db.close()


def test_research_requires_adjusted_prices_by_default():
    db = _session()
    try:
        _event(db, payload={"symbol": "AAPL", "filing_date": "2024-01-02"})
        db.add(PriceCache(symbol="AAPL", date="2024-01-03", close=100.0, adjusted_close=None))
        db.add(PriceCache(symbol="AAPL", date="2024-02-02", close=110.0, adjusted_close=None))
        _price(db, "SPY", "2024-01-03", 100.0)
        _price(db, "SPY", "2024-02-02", 101.0)
        db.commit()

        result = run_research(
            db,
            ResearchConfig(
                strategy_name="Congress Buys",
                universe=("AAPL",),
                benchmark="SPY",
                start_date=date(2024, 1, 1),
                end_date=date(2024, 3, 1),
                hold_days=(30,),
                weighting="equal",
                rebalance_frequency="event",
                slippage_bps=0.0,
                fee_bps=0.0,
                require_adjusted=True,
                min_lots=1,
            ),
        )

        assert result["runs"][0]["status"] == "insufficient_timeline"
        assert result["runs"][0]["lots"] == 0
    finally:
        db.close()


def test_run_research_reports_net_cost_adjusted_metrics():
    db = _session()
    try:
        _event(db, payload={"symbol": "AAPL", "filing_date": "2024-01-02"})
        _price(db, "AAPL", "2024-01-03", 100.0)
        _price(db, "AAPL", "2024-01-04", 101.0)
        _price(db, "AAPL", "2024-02-02", 110.0)
        _price(db, "SPY", "2024-01-03", 100.0)
        _price(db, "SPY", "2024-01-04", 100.0)
        _price(db, "SPY", "2024-02-02", 100.0)
        db.commit()

        result = run_research(
            db,
            ResearchConfig(
                strategy_name="Congress Buys",
                universe=("AAPL",),
                benchmark="SPY",
                start_date=date(2024, 1, 1),
                end_date=date(2024, 3, 1),
                hold_days=(30,),
                weighting="equal",
                rebalance_frequency="event",
                slippage_bps=5.0,
                fee_bps=0.0,
                require_adjusted=True,
                min_lots=1,
            ),
        )

        row = result["runs"][0]
        assert row["status"] == "ok"
        assert row["lots"] == 1
        assert row["total_return_pct"] > 0
        assert row["trade_count"] == 2
        assert result["metadata"]["execution_timing"] == "first trading day strictly after public disclosure date"
    finally:
        db.close()


def test_transaction_value_weighting_charges_normalized_turnover_costs():
    days = [date(2024, 1, day) for day in (2, 3, 4, 5)]
    price_maps = {
        "AAPL": {day: PriceBar(day=day, close=100.0, dollar_volume=None) for day in days},
        "MSFT": {day: PriceBar(day=day, close=100.0, dollar_volume=None) for day in days},
    }
    benchmark_prices = {day: PriceBar(day=day, close=100.0, dollar_volume=None) for day in days}
    small_signal = Signal(
        event_id=1,
        symbol="AAPL",
        disclosure_date=date(2024, 1, 1),
        raw_entry_date=date(2024, 1, 2),
        amount_min=1,
        amount_max=1,
        member_name=None,
        member_bioguide_id=None,
        chamber=None,
        party=None,
        source_filing_id=None,
        source_document_url=None,
    )
    large_signal = Signal(
        event_id=2,
        symbol="MSFT",
        disclosure_date=date(2024, 1, 2),
        raw_entry_date=date(2024, 1, 3),
        amount_min=1_000_000_000_000,
        amount_max=1_000_000_000_000,
        member_name=None,
        member_bioguide_id=None,
        chamber=None,
        party=None,
        source_filing_id=None,
        source_document_url=None,
    )
    lots = [
        Lot(small_signal, date(2024, 1, 2), date(2024, 1, 5), 100.0, 100.0, 0.0, 0.0),
        Lot(large_signal, date(2024, 1, 3), date(2024, 1, 5), 100.0, 100.0, 0.0, 0.0),
    ]

    simulation = simulate_active_lot_portfolio(
        lots,
        price_maps,
        benchmark_prices,
        weighting="transaction_value",
        per_side_cost_rate=0.0005,
    )

    assert min(simulation["daily_returns"]) > -0.01
    assert min(simulation["strategy_curve"]) > 99.0
