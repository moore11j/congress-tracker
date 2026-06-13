from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

import app.services.data_enrichment_queue as queue_module
from app.db import Base
from app.models import DataEnrichmentJob
from app.services.data_enrichment_queue import (
    enqueue_data_enrichment_job,
    is_valid_enrichment_symbol,
    process_data_enrichment_jobs,
    skip_invalid_symbol_jobs,
)


def _session_factory():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False)


def test_enrichment_queue_rejects_placeholder_symbols(monkeypatch):
    Session = _session_factory()
    monkeypatch.setattr(queue_module, "SessionLocal", Session)

    assert not is_valid_enrichment_symbol("[SYMBOL]")
    assert not enqueue_data_enrichment_job(job_type="ticker_meta", symbol="[SYMBOL]", reason="test")
    assert not enqueue_data_enrichment_job(job_type="ticker_meta", symbol="UNKNOWN", reason="test")
    assert not enqueue_data_enrichment_job(job_type="ticker_meta", symbol=None, reason="test")

    db = Session()
    try:
        assert db.execute(select(DataEnrichmentJob)).scalars().all() == []
    finally:
        db.close()


def test_skip_invalid_symbol_jobs_marks_existing_pending_rows():
    Session = _session_factory()
    db = Session()
    try:
        db.add_all(
            [
                DataEnrichmentJob(
                    job_type="ticker_meta",
                    symbol="[SYMBOL]",
                    dedupe_key="ticker_meta|[SYMBOL]||",
                    priority=10,
                    status="queued",
                    attempts=0,
                    max_attempts=3,
                    source="test",
                    reason="test",
                    next_run_at=datetime.now(timezone.utc),
                ),
                DataEnrichmentJob(
                    job_type="ticker_meta",
                    symbol="NBIS",
                    dedupe_key="ticker_meta|NBIS||",
                    priority=10,
                    status="queued",
                    attempts=0,
                    max_attempts=3,
                    source="test",
                    reason="test",
                    next_run_at=datetime.now(timezone.utc),
                ),
            ]
        )
        db.commit()

        assert skip_invalid_symbol_jobs(db) == 1

        rows = {row.symbol: row for row in db.execute(select(DataEnrichmentJob)).scalars()}
        assert rows["[SYMBOL]"].status == "skipped"
        assert rows["[SYMBOL]"].reason == "invalid_symbol"
        assert rows["NBIS"].status == "queued"
    finally:
        db.close()


def test_timeout_result_is_retryable_failure_not_success(monkeypatch):
    Session = _session_factory()
    monkeypatch.setattr(queue_module, "SessionLocal", Session)

    def timeout_result(*_args, **_kwargs):
        return {
            "symbol": "PXD",
            "status": "unavailable",
            "reason": "provider_timeout",
            "items": [],
        }

    monkeypatch.setattr("app.services.fmp_news.get_stock_news", timeout_result)

    db = Session()
    try:
        db.add(
            DataEnrichmentJob(
                job_type="news_stock",
                symbol="PXD",
                dedupe_key="news_stock|PXD||",
                priority=10,
                status="queued",
                attempts=0,
                max_attempts=3,
                source="test",
                reason="test",
                next_run_at=datetime.now(timezone.utc),
            )
        )
        db.commit()
    finally:
        db.close()

    summary = process_data_enrichment_jobs(limit=1)

    db = Session()
    try:
        row = db.execute(select(DataEnrichmentJob)).scalar_one()
        assert summary == {"processed": 1, "succeeded": 0, "failed": 1, "skipped": 0}
        assert row.status == "queued"
        assert row.reason == "provider_timeout"
        assert row.error == "provider_timeout"
        assert row.attempts == 1
    finally:
        db.close()
