from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

import app.services.data_enrichment_queue as queue_module
from app.db import Base
from app.models import DataEnrichmentJob
from app.services.data_enrichment_queue import (
    enqueue_data_enrichment_job,
    enqueue_priority_ticker_prewarm_jobs,
    is_valid_enrichment_symbol,
    process_data_enrichment_jobs,
    skip_invalid_symbol_jobs,
)
from app.request_priority import get_request_context, reset_request_context, set_request_context
from app.services.provider_usage import provider_usage_summary, reset_provider_usage


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
    monkeypatch.setenv("ENRICHMENT_QUEUE_ENABLED", "true")

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


def test_profile_enrichment_runs_with_background_context_when_page_fetch_blocked(monkeypatch):
    Session = _session_factory()
    monkeypatch.setattr(queue_module, "SessionLocal", Session)
    monkeypatch.setenv("ENRICHMENT_QUEUE_ENABLED", "true")
    monkeypatch.setenv("FMP_API_KEY", "test-key")
    monkeypatch.setenv("FMP_ALLOW_SYNC_USER_FETCH", "false")
    monkeypatch.setenv("FMP_PERSIST_USAGE_EVENTS", "0")
    reset_provider_usage()
    calls = {"count": 0}

    def fake_profile(symbol):
        calls["count"] += 1
        assert symbol == "AAPL"
        assert get_request_context().get("path") == "background"
        return {"symbol": "AAPL", "companyName": "Apple Inc."}

    monkeypatch.setattr("app.main._company_profile_snapshot_from_fmp", fake_profile)

    db = Session()
    try:
        db.add(
            DataEnrichmentJob(
                job_type="profile",
                symbol="AAPL",
                dedupe_key="profile|AAPL||",
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

    token = set_request_context({"path": "/api/tickers/AAPL", "priority": "heavy"})
    try:
        summary = process_data_enrichment_jobs(limit=1)
    finally:
        reset_request_context(token)

    usage = provider_usage_summary()
    assert summary == {"processed": 1, "succeeded": 1, "failed": 0, "skipped": 0}
    assert calls["count"] == 1
    assert not any(row["reason"] == "page_fetch_blocked" for row in usage["fallback_reasons"])
    reset_provider_usage()


def test_priority_ticker_prewarm_core_mode_prioritizes_hot_ticker_data(monkeypatch):
    Session = _session_factory()
    monkeypatch.setattr(queue_module, "SessionLocal", Session)
    monkeypatch.setenv("PRIORITY_TICKER_PREWARM_LANDING_SYMBOLS", "AAPL")

    db = Session()
    try:
        result = enqueue_priority_ticker_prewarm_jobs(db, symbol_limit=1, mode="core")

        rows = db.execute(select(DataEnrichmentJob).order_by(DataEnrichmentJob.priority)).scalars().all()
        job_types = [row.job_type for row in rows]
        assert result["prewarm_mode"] == "core"
        assert result["symbols"] == ["AAPL"]
        assert result["attempted"] == 7
        assert set(job_types) == {
            "quote",
            "ticker_meta",
            "fundamentals",
            "price_series",
            "technical_indicators",
            "ticker_context_bundle",
        }
        assert job_types.count("price_series") == 2
        assert job_types.count("ticker_context_bundle") == 1
        assert "ticker_financials" not in job_types
        assert "news_stock" not in job_types
        assert "press_releases" not in job_types
        assert "sec_filings" not in job_types
    finally:
        db.close()


def test_enrichment_queue_yields_when_pressure_guard_trips(monkeypatch):
    Session = _session_factory()
    monkeypatch.setattr(queue_module, "SessionLocal", Session)
    monkeypatch.setenv("ENRICHMENT_QUEUE_ENABLED", "true")

    guard_calls = {"count": 0}
    processed_symbols: list[str] = []

    def fake_guard(_db):
        guard_calls["count"] += 1
        if guard_calls["count"] == 1:
            return SimpleNamespace(proceed=True, reason="ok", to_dict=lambda: {"reason": "ok"})
        return SimpleNamespace(
            proceed=False,
            reason="db_active_connection_pressure",
            to_dict=lambda: {"reason": "db_active_connection_pressure"},
        )

    def fake_process_one(_db, job):
        assert get_request_context()["priority"] == "background"
        processed_symbols.append(job.symbol)

    monkeypatch.setattr(queue_module, "_check_enrichment_queue_pressure", fake_guard)
    monkeypatch.setattr(queue_module, "_process_one", fake_process_one)

    db = Session()
    try:
        db.add_all(
            [
                DataEnrichmentJob(
                    job_type="quote",
                    symbol="AAPL",
                    dedupe_key="quote|AAPL||",
                    priority=10,
                    status="queued",
                    attempts=0,
                    max_attempts=3,
                    source="test",
                    reason="test",
                    next_run_at=datetime.now(timezone.utc),
                ),
                DataEnrichmentJob(
                    job_type="quote",
                    symbol="MSFT",
                    dedupe_key="quote|MSFT||",
                    priority=20,
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
    finally:
        db.close()

    summary = process_data_enrichment_jobs(limit=2)

    db = Session()
    try:
        rows = {row.symbol: row for row in db.execute(select(DataEnrichmentJob)).scalars()}
        assert summary == {"processed": 1, "succeeded": 1, "failed": 0, "skipped": 1}
        assert processed_symbols == ["AAPL"]
        assert rows["AAPL"].status == "done"
        assert rows["MSFT"].status == "queued"
    finally:
        db.close()
