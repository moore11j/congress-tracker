from __future__ import annotations

import json
from datetime import date, datetime, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import app.jobs.audit_historical_fundamentals_backfill as audit_module
from app.db import Base
from app.models import InsiderTransactionNormalized, TickerFinancialsCache


def _session():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    Base.metadata.create_all(bind=engine)
    return SessionLocal


def _payload() -> dict:
    return {
        "sections_present": ["income", "valuation"],
        "annual": [{"date": "2025-12-31", "revenue": 100, "eps": 2.0}],
        "quarterly": [
            {"date": "2025-03-31", "revenue": 20, "eps": 0.20, "freeCashFlow": 1, "grossMargin": 40},
            {"date": "2025-06-30", "revenue": 22, "eps": 0.22, "freeCashFlow": 2, "grossMargin": 41},
            {"date": "2025-09-30", "revenue": 24, "eps": 0.24, "freeCashFlow": 3, "grossMargin": 42},
            {"date": "2025-12-31", "revenue": 26, "eps": 0.26, "freeCashFlow": 4, "grossMargin": 43},
            {"date": "2026-03-31", "revenue": 30, "eps": 0.30, "freeCashFlow": 5, "grossMargin": 44},
        ],
    }


def test_payload_summary_counts_statement_rows_and_snapshot_candidates():
    row = TickerFinancialsCache(
        symbol="AAPL",
        status="ok",
        payload_json=json.dumps(_payload()),
        fetched_at=datetime(2026, 8, 1, tzinfo=timezone.utc),
    )

    summary = audit_module._payload_summary(row)

    assert summary["annual_rows"] == 1
    assert summary["quarterly_rows"] == 5
    assert summary["eligible_quarterly_snapshots"] == 5
    assert summary["earliest_statement_date"] == "2025-03-31"
    assert summary["latest_statement_date"] == "2026-03-31"


def test_audit_reports_cache_coverage_and_insider_overlap(monkeypatch):
    SessionLocal = _session()
    db = SessionLocal()
    try:
        db.add(
            TickerFinancialsCache(
                symbol="AAPL",
                status="ok",
                payload_json=json.dumps(_payload()),
                fetched_at=datetime(2026, 8, 1, tzinfo=timezone.utc),
            )
        )
        db.add(
            TickerFinancialsCache(
                symbol="MSFT",
                status="partial",
                payload_json=json.dumps({"sections_present": [], "annual": [], "quarterly": []}),
                fetched_at=datetime(2026, 8, 1, tzinfo=timezone.utc),
            )
        )
        db.add(
            InsiderTransactionNormalized(
                accession_number="0000000001-26-000001",
                normalized_hash="aapl-hash",
                reporting_owner_cik="1",
                reporting_owner_name="Owner",
                issuer_cik="2",
                issuer_name="Apple",
                ticker_raw="AAPL",
                ticker_normalized="AAPL",
                transaction_date=date(2026, 7, 1),
                filing_date=date(2026, 7, 2),
                transaction_type_normalized="open_market_purchase",
                is_duplicate=False,
            )
        )
        db.add(
            InsiderTransactionNormalized(
                accession_number="0000000003-26-000001",
                normalized_hash="tsla-hash",
                reporting_owner_cik="3",
                reporting_owner_name="Owner 2",
                issuer_cik="4",
                issuer_name="Tesla",
                ticker_raw="TSLA",
                ticker_normalized="TSLA",
                transaction_date=date(2026, 7, 1),
                filing_date=date(2026, 7, 2),
                transaction_type_normalized="open_market_purchase",
                is_duplicate=False,
            )
        )
        db.commit()
    finally:
        db.close()

    monkeypatch.setattr(audit_module, "SessionLocal", SessionLocal)

    result = audit_module.audit_historical_fundamentals_backfill(sample_limit=1)

    assert result["ticker_financials_cache"]["symbols"] == 2
    assert result["ticker_financials_cache"]["status_counts"] == {"ok": 1, "partial": 1}
    assert result["ticker_financials_cache"]["symbols_with_eligible_quarterly_statement_rows"] == 1
    assert result["ticker_financials_cache"]["estimated_quarterly_snapshot_rows_from_cache"] == 5
    assert result["strategy_universe_overlap"]["normalized_insider_purchase_symbols"] == 2
    assert result["strategy_universe_overlap"]["cached_financial_symbols_in_insider_purchase_universe"] == 1
    assert result["strategy_universe_overlap"]["insider_purchase_symbols_missing_financial_cache"] == 1
    assert len(result["ticker_financials_cache"]["sample"]) == 1
