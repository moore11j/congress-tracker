from __future__ import annotations

from datetime import date

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.models import InsiderTransaction, InsiderTransactionNormalized, PriceCache
from app.strategy_research.congress_buys import ResearchConfig
from app.strategy_research.insider_buys import (
    load_legacy_insider_purchase_signals,
    load_insider_open_market_purchase_signals,
    run_research,
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


def _insider_row(
    db,
    *,
    row_id: int = 1,
    symbol: str = "AAPL",
    transaction_type: str = "open_market_purchase",
    filing_date: date = date(2024, 1, 5),
    transaction_date: date = date(2024, 1, 2),
    officer_title: str | None = "Chief Executive Officer",
    is_director: bool = False,
    is_duplicate: bool = False,
) -> None:
    db.add(
        InsiderTransactionNormalized(
            id=row_id,
            accession_number=f"0000000000-{row_id}",
            ticker_normalized=symbol,
            reporting_owner_cik="0001234567",
            reporting_owner_name="Example Insider",
            owner_relationship_json="{}",
            officer_title=officer_title,
            is_director=is_director,
            is_officer=bool(officer_title),
            is_ten_percent_owner=False,
            transaction_date=transaction_date,
            filing_date=filing_date,
            transaction_code="P",
            transaction_type_normalized=transaction_type,
            shares=10.0,
            price=100.0,
            value=1000.0,
            is_derivative=False,
            footnotes_json="[]",
            ten_b5_1_flag=False,
            normalized_hash=f"hash-{row_id}",
            is_duplicate=is_duplicate,
            parser_confidence=0.95,
        )
    )


def _legacy_insider_row(
    db,
    *,
    row_id: int = 1,
    symbol: str = "AAPL",
    transaction_type: str = "purchase",
    filing_date: date = date(2024, 1, 5),
    transaction_date: date = date(2024, 1, 2),
    role: str = "CEO",
) -> None:
    db.add(
        InsiderTransaction(
            id=row_id,
            source="legacy",
            external_id=f"legacy-{row_id}",
            symbol=symbol,
            reporting_cik="0001234567",
            insider_name="Example Insider",
            transaction_type=transaction_type,
            role=role,
            ownership="direct",
            transaction_date=transaction_date,
            filing_date=filing_date,
            shares=10.0,
            price=100.0,
            payload_json="{}",
        )
    )


def test_insider_signals_use_form4_filing_date_and_open_market_purchase_only():
    db = _session()
    try:
        _insider_row(db, row_id=1)
        _insider_row(db, row_id=2, transaction_type="grant_award")
        db.commit()

        signals = load_insider_open_market_purchase_signals(
            db,
            universe=("AAPL",),
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 31),
            role="all",
        )

        assert len(signals) == 1
        assert signals[0].disclosure_date == date(2024, 1, 5)
        assert signals[0].raw_entry_date == date(2024, 1, 6)
        assert signals[0].amount_max == 1000
    finally:
        db.close()


def test_legacy_insider_signals_use_filing_date_and_exclude_future_transactions():
    db = _session()
    try:
        _legacy_insider_row(db, row_id=1)
        _legacy_insider_row(
            db,
            row_id=2,
            filing_date=date(2024, 1, 10),
            transaction_date=date(2030, 1, 1),
        )
        _legacy_insider_row(db, row_id=3, transaction_type="sale")
        db.commit()

        signals = load_legacy_insider_purchase_signals(
            db,
            universe=("AAPL",),
            start_date=date(2024, 1, 1),
            end_date=date(2026, 7, 31),
            role="all",
        )

        assert len(signals) == 1
        assert signals[0].disclosure_date == date(2024, 1, 5)
        assert signals[0].raw_entry_date == date(2024, 1, 6)
        assert signals[0].amount_max == 1000
    finally:
        db.close()


def test_insider_role_filter_ceo_excludes_director_only_rows():
    db = _session()
    try:
        _insider_row(db, row_id=1, officer_title="Chief Executive Officer", is_director=True)
        _insider_row(db, row_id=2, officer_title=None, is_director=True)
        db.commit()

        ceo_signals = load_insider_open_market_purchase_signals(
            db,
            universe=("AAPL",),
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 31),
            role="ceo",
        )
        director_signals = load_insider_open_market_purchase_signals(
            db,
            universe=("AAPL",),
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 31),
            role="director",
        )

        assert len(ceo_signals) == 1
        assert len(director_signals) == 2
    finally:
        db.close()


def test_legacy_run_research_reports_lower_confidence_metrics():
    db = _session()
    try:
        _legacy_insider_row(db)
        _price(db, "AAPL", "2024-01-08", 100.0)
        _price(db, "AAPL", "2024-02-07", 110.0)
        _price(db, "SPY", "2024-01-08", 100.0)
        _price(db, "SPY", "2024-02-07", 101.0)
        db.commit()

        result = run_research(
            db,
            ResearchConfig(
                strategy_name="Legacy Insider Open-Market Buys",
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
            role="all",
            source="legacy",
        )

        row = result["runs"][0]
        assert row["status"] == "ok"
        assert row["lots"] == 1
        assert result["metadata"]["data_quality_confidence"] == "lower"
        assert result["metadata"]["source"] == "legacy"
    finally:
        db.close()


def test_insider_run_research_reports_metrics():
    db = _session()
    try:
        _insider_row(db)
        _price(db, "AAPL", "2024-01-08", 100.0)
        _price(db, "AAPL", "2024-02-07", 110.0)
        _price(db, "SPY", "2024-01-08", 100.0)
        _price(db, "SPY", "2024-02-07", 101.0)
        db.commit()

        result = run_research(
            db,
            ResearchConfig(
                strategy_name="Insider Open-Market Buys",
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
            role="all",
        )

        row = result["runs"][0]
        assert row["status"] == "ok"
        assert row["lots"] == 1
        assert row["win_rate_pct"] == 100.0
        assert result["metadata"]["signal_source"] == "insider_transactions_normalized open_market_purchase rows"
    finally:
        db.close()
