from __future__ import annotations

from datetime import date, datetime, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import app.jobs.refresh_analyst_events as refresh_module
from app.db import Base, ensure_analyst_consensus_schema
from app.models import AnalystConsensusIngestionRun, AnalystConsensusSnapshot, AnalystSymbolBackfillStatus
from app.services.analyst_consensus import GRADE_DAILY_REFRESH_JOB, PRICE_TARGET_DAILY_REFRESH_JOB


def _session():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    Base.metadata.create_all(bind=engine)
    ensure_analyst_consensus_schema(engine)
    return SessionLocal


def _snapshot(symbol: str) -> AnalystConsensusSnapshot:
    return AnalystConsensusSnapshot(
        symbol=symbol,
        snapshot_date=date(2026, 8, 7),
        availability_status="available",
        provider_status="available",
        source="fmp",
        ingested_at=datetime(2026, 8, 7, tzinfo=timezone.utc),
        raw_payload_json="{}",
    )


def test_refresh_analyst_events_refreshes_grades_and_price_targets(monkeypatch):
    SessionLocal = _session()
    monkeypatch.setattr(refresh_module, "SessionLocal", SessionLocal)
    monkeypatch.setattr(refresh_module, "ensure_analyst_consensus_schema", lambda _engine: None)

    grade_symbols: list[str] = []
    target_symbols: list[str] = []

    def fake_grade(_db, symbol, **_kwargs):
        grade_symbols.append(symbol)
        return {"symbol": symbol, "status": "available", "rows_seen": 1, "inserted": 1, "updated": 0}

    def fake_target(_db, symbol, **_kwargs):
        target_symbols.append(symbol)
        return {"symbol": symbol, "status": "available", "rows_seen": 2, "inserted": 2, "updated": 0}

    monkeypatch.setattr(refresh_module, "ingest_symbol_grade_events", fake_grade)
    monkeypatch.setattr(refresh_module, "ingest_symbol_price_target_events", fake_target)

    db = SessionLocal()
    try:
        db.add_all([_snapshot("AAPL"), _snapshot("NVDA")])
        db.commit()
    finally:
        db.close()

    result = refresh_module.refresh_analyst_events(limit=1, sleep_seconds=0)

    db = SessionLocal()
    try:
        run = db.query(AnalystConsensusIngestionRun).one()
        attempts = {
            (row.job_name, row.symbol): row
            for row in db.query(AnalystSymbolBackfillStatus).order_by(AnalystSymbolBackfillStatus.job_name).all()
        }
        assert result["status"] == "success"
        assert result["symbols_attempted"] == 2
        assert result["records_inserted"] == 3
        assert result["grades"]["symbols_attempted"] == 1
        assert result["price_targets"]["symbols_attempted"] == 1
        assert grade_symbols == ["AAPL"]
        assert target_symbols == ["AAPL"]
        assert run.job_name == "analyst_events_daily_refresh"
        assert run.symbols_attempted == 2
        assert run.records_inserted == 3
        assert attempts[(GRADE_DAILY_REFRESH_JOB, "AAPL")].rows_seen == 1
        assert attempts[(PRICE_TARGET_DAILY_REFRESH_JOB, "AAPL")].rows_seen == 2
    finally:
        db.close()
