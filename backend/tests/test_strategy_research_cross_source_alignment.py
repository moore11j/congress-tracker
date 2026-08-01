from __future__ import annotations

import json
from datetime import date, datetime, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.models import Event, GovernmentContract, InsiderTransactionNormalized, PriceCache
from app.strategy_research.congress_buys import ResearchConfig, load_congress_purchase_signals
from app.strategy_research.cross_source_alignment import (
    CONTRACT_AWARD_DATE_PROXY_NOTE,
    build_alignment_signals,
    load_government_contract_signals,
    run_research,
)
from app.strategy_research.insider_buys import load_insider_open_market_purchase_signals


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


def _congress_event(db, *, event_id: int = 1, symbol: str = "AAPL", filing_date: str = "2024-01-10") -> None:
    db.add(
        Event(
            id=event_id,
            event_type="congress_trade",
            ts=datetime(2024, 1, 10, 12, 0, tzinfo=timezone.utc),
            event_date=datetime.fromisoformat(f"{filing_date}T12:00:00+00:00"),
            symbol=symbol,
            source="congress",
            trade_type="purchase",
            amount_min=1000,
            amount_max=15000,
            member_name="Example Member",
            member_bioguide_id="E000001",
            payload_json=json.dumps({"symbol": symbol, "filing_date": filing_date}),
        )
    )


def _insider_row(
    db,
    *,
    row_id: int = 1,
    symbol: str = "AAPL",
    filing_date: date = date(2024, 1, 9),
) -> None:
    db.add(
        InsiderTransactionNormalized(
            id=row_id,
            accession_number=f"0000000000-{row_id}",
            ticker_normalized=symbol,
            reporting_owner_cik="0001234567",
            reporting_owner_name="Example Insider",
            owner_relationship_json="{}",
            officer_title="Director",
            is_director=True,
            is_officer=False,
            is_ten_percent_owner=False,
            transaction_date=date(2024, 1, 5),
            filing_date=filing_date,
            transaction_code="P",
            transaction_type_normalized="open_market_purchase",
            shares=10.0,
            price=100.0,
            value=1000.0,
            is_derivative=False,
            footnotes_json="[]",
            ten_b5_1_flag=False,
            normalized_hash=f"hash-{row_id}",
            is_duplicate=False,
            parser_confidence=0.95,
        )
    )


def _contract_row(
    db,
    *,
    row_id: int = 1,
    symbol: str = "AAPL",
    award_date: date = date(2024, 1, 8),
    amount: float = 5_000_000.0,
) -> None:
    db.add(
        GovernmentContract(
            id=row_id,
            award_id=f"award-{row_id}",
            dedupe_key=f"contract-{row_id}",
            symbol=symbol,
            recipient_name="Example Recipient",
            award_date=award_date,
            award_amount=amount,
            awarding_agency="Department of Defense",
            source="usaspending",
        )
    )


def test_alignment_uses_later_primary_public_date_without_future_confirmation():
    db = _session()
    try:
        _insider_row(db, row_id=1, filing_date=date(2024, 1, 9))
        _insider_row(db, row_id=2, filing_date=date(2024, 1, 12))
        _congress_event(db, filing_date="2024-01-10")
        db.commit()

        insiders = load_insider_open_market_purchase_signals(
            db,
            universe=("AAPL",),
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 31),
            role="all",
        )
        congress = load_congress_purchase_signals(
            db,
            universe=("AAPL",),
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 31),
        )

        aligned = build_alignment_signals(
            congress,
            insiders,
            lookback_days=30,
            min_confirming_signals=1,
            primary_source="congress",
            confirming_source="insider",
        )

        assert len(aligned) == 1
        assert aligned[0].signal.disclosure_date == date(2024, 1, 10)
        assert aligned[0].signal.raw_entry_date == date(2024, 1, 11)
        assert aligned[0].confirming_count == 1
    finally:
        db.close()


def test_government_contract_alignment_is_lower_confidence_proxy():
    db = _session()
    try:
        _congress_event(db, filing_date="2024-01-10")
        _contract_row(db, award_date=date(2024, 1, 8))
        _price(db, "AAPL", "2024-01-11", 100.0)
        _price(db, "AAPL", "2024-02-12", 105.0)
        _price(db, "SPY", "2024-01-11", 100.0)
        _price(db, "SPY", "2024-02-12", 102.0)
        db.commit()

        contract_signals = load_government_contract_signals(
            db,
            universe=("AAPL",),
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 31),
            min_amount=1_000_000.0,
        )
        result = run_research(
            db,
            ResearchConfig(
                strategy_name="Congress + Government Contracts",
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
            pair="congress_contracts",
            lookback_days=30,
            min_confirming_signals=1,
            min_contract_amount=1_000_000.0,
        )

        assert len(contract_signals) == 1
        assert result["metadata"]["data_quality_confidence"] == "lower"
        assert result["metadata"]["data_quality_note"] == CONTRACT_AWARD_DATE_PROXY_NOTE
        assert result["runs"][0]["status"] == "ok"
    finally:
        db.close()
