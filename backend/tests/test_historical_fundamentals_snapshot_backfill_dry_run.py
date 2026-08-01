from __future__ import annotations

import json
from datetime import date, datetime, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import app.jobs.dry_run_historical_fundamentals_snapshot_backfill as backfill_module
from app.db import Base, ensure_fundamentals_snapshot_schema
from app.models import FundamentalsSnapshot, TickerFinancialsCache


def _session():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    Base.metadata.create_all(bind=engine)
    ensure_fundamentals_snapshot_schema(engine)
    return SessionLocal


def _payload() -> dict:
    return {
        "quarterly": [
            {"date": "2025-03-31", "revenue": 100.0, "eps": 1.00, "freeCashFlow": 10.0, "grossMargin": 40.0, "operatingMargin": 20.0},
            {"date": "2025-06-30", "revenue": 110.0, "eps": 1.10, "freeCashFlow": 11.0, "grossMargin": 41.0, "operatingMargin": 21.0},
            {"date": "2025-09-30", "revenue": 120.0, "eps": 1.20, "freeCashFlow": 12.0, "grossMargin": 42.0, "operatingMargin": 22.0},
            {"date": "2025-12-31", "revenue": 130.0, "eps": 1.30, "freeCashFlow": 13.0, "grossMargin": 43.0, "operatingMargin": 23.0},
            {"date": "2026-03-31", "revenue": 125.0, "eps": 1.25, "freeCashFlow": 15.0, "grossMargin": 44.0, "operatingMargin": 24.0},
        ]
    }


def _cache_row(symbol: str = "AAPL") -> TickerFinancialsCache:
    return TickerFinancialsCache(
        symbol=symbol,
        status="ok",
        payload_json=json.dumps(_payload()),
        fetched_at=datetime(2026, 8, 1, 12, 0, tzinfo=timezone.utc),
    )


def test_candidates_from_financials_row_uses_quarterly_rows_and_lagged_snapshot_dates():
    candidates = backfill_module.candidates_from_financials_row(_cache_row(), lag_days=45)

    assert len(candidates) == 5
    assert candidates[0].symbol == "AAPL"
    assert candidates[0].period_date == date(2025, 3, 31)
    assert candidates[0].snapshot_date == date(2025, 5, 15)
    assert candidates[0].gross_margin == 40.0
    assert candidates[0].eps_ttm is None
    assert candidates[4].revenue_growth == 25.0
    assert candidates[4].eps_growth == 25.0
    assert candidates[4].fcf_growth == 50.0
    assert candidates[4].eps_ttm == 4.85
    assert candidates[4].methodology_version == backfill_module.METHODOLOGY_VERSION


def test_dry_run_reports_new_rows_and_existing_snapshot_conflicts(monkeypatch):
    SessionLocal = _session()
    db = SessionLocal()
    try:
        db.add(_cache_row())
        db.add(
            FundamentalsSnapshot(
                symbol="AAPL",
                provider="fmp",
                snapshot_date=date(2025, 5, 15),
                observed_at=datetime(2026, 8, 1, tzinfo=timezone.utc),
                source_fetched_at=datetime(2026, 8, 1, tzinfo=timezone.utc),
                period_date=date(2025, 3, 31),
                status="ok",
            )
        )
        db.commit()
    finally:
        db.close()

    monkeypatch.setattr(backfill_module, "SessionLocal", SessionLocal)

    result = backfill_module.dry_run_historical_fundamentals_snapshot_backfill(symbols=["AAPL"], sample_limit=2)

    assert result["mode"] == "dry_run"
    assert result["rows_written"] == 0
    assert result["source_kind"] == "ticker_financials_cache_statement_proxy"
    assert result["data_quality_confidence"] == "medium_proxy"
    assert result["as_of_date"]
    assert result["cache_rows_seen"] == 1
    assert result["raw_candidate_rows"] == 5
    assert result["candidate_rows"] == 5
    assert result["candidate_symbols"] == 1
    assert result["existing_snapshot_key_conflicts"] == 1
    assert result["new_snapshot_key_candidates"] == 4
    assert result["estimated_candidate_json_bytes"] > 0
    assert result["snapshot_date_range"] == {"start": "2025-05-15", "end": "2026-05-15"}
    assert len(result["sample"]) == 2
    assert result["warnings"][0] == "No rows were written."


def test_dry_run_symbol_filter_limits_cache_rows(monkeypatch):
    SessionLocal = _session()
    db = SessionLocal()
    try:
        db.add(_cache_row("AAPL"))
        db.add(_cache_row("MSFT"))
        db.commit()
    finally:
        db.close()

    monkeypatch.setattr(backfill_module, "SessionLocal", SessionLocal)

    result = backfill_module.dry_run_historical_fundamentals_snapshot_backfill(symbols=["MSFT"], sample_limit=10)

    assert result["cache_rows_seen"] == 1
    assert result["candidate_symbols"] == 1
    assert result["top_symbols_by_candidate_rows"] == [{"symbol": "MSFT", "candidate_rows": 5}]


def test_dry_run_excludes_future_proxy_availability_dates(monkeypatch):
    SessionLocal = _session()
    db = SessionLocal()
    try:
        db.add(_cache_row("AAPL"))
        db.commit()
    finally:
        db.close()

    monkeypatch.setattr(backfill_module, "SessionLocal", SessionLocal)

    result = backfill_module.dry_run_historical_fundamentals_snapshot_backfill(
        symbols=["AAPL"],
        as_of=date(2025, 8, 1),
        sample_limit=10,
    )

    assert result["raw_candidate_rows"] == 5
    assert result["future_availability_candidate_rows_excluded"] == 4
    assert result["candidate_rows"] == 1
    assert result["snapshot_date_range"] == {"start": "2025-05-15", "end": "2025-05-15"}


def test_apply_writes_proxy_rows_once_with_metadata(monkeypatch):
    SessionLocal = _session()
    db = SessionLocal()
    try:
        db.add(_cache_row("AAPL"))
        db.commit()
    finally:
        db.close()

    monkeypatch.setattr(backfill_module, "SessionLocal", SessionLocal)

    first = backfill_module.dry_run_historical_fundamentals_snapshot_backfill(
        symbols=["AAPL"],
        apply=True,
        sample_limit=1,
    )
    second = backfill_module.dry_run_historical_fundamentals_snapshot_backfill(
        symbols=["AAPL"],
        apply=True,
        sample_limit=1,
    )

    assert first["mode"] == "apply"
    assert first["candidate_rows"] == 5
    assert first["rows_written"] == 5
    assert first["existing_snapshot_key_conflicts"] == 0
    assert first["new_snapshot_key_candidates"] == 5
    assert first["warnings"][0] == "Proxy historical fundamentals rows were written: 5."
    assert second["rows_written"] == 0
    assert second["existing_snapshot_key_conflicts"] == 5
    assert second["warnings"][0] == "Apply mode requested; no new proxy rows were written."

    db = SessionLocal()
    try:
        rows = db.query(FundamentalsSnapshot).order_by(FundamentalsSnapshot.snapshot_date.asc()).all()
        assert len(rows) == 5
        assert rows[0].source_kind == "ticker_financials_cache_statement_proxy"
        assert rows[0].data_quality_confidence == "medium_proxy"
        assert "statement period date plus 45 calendar days" in rows[0].availability_basis
        assert rows[0].methodology_version == backfill_module.METHODOLOGY_VERSION
    finally:
        db.close()
