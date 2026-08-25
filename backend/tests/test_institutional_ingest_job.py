from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest
from fastapi import HTTPException
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool
from starlette.requests import Request

from app import ingest_institutional_activity as ingest_module
from app.auth import SESSION_COOKIE_NAME, sign_session_payload
from app.clients.fmp import FMPClientError
from app.db import Base
from app.models import Event, InstitutionalIngestJobRun, InstitutionalIngestJobState, UserAccount
from app.routers import institutional_ingest_admin as admin_router
from app.services import institutional_ingest_job as job_module


def _session_factory():
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
        future=True,
    )
    Base.metadata.create_all(bind=engine)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False)


@pytest.fixture()
def job_env(monkeypatch):
    Session = _session_factory()
    monkeypatch.setattr(job_module, "SessionLocal", Session)
    monkeypatch.setattr(job_module, "ensure_institutional_activity_schema", lambda *_args, **_kwargs: None)
    monkeypatch.setenv("INSTITUTIONAL_SCHEDULED_INGEST_ENABLED", "false")
    monkeypatch.setenv("INSTITUTIONAL_SCHEDULED_INGEST_START_PAGE", "9")
    return Session


def _fake_result(**overrides):
    result = {
        "status": "ok",
        "start_page": 9,
        "pages": 1,
        "pages_scanned": 1,
        "first_empty_page_seen": None,
        "max_filings_reached": 0,
        "scanned": 25,
        "parsed": 25,
        "parse_failed": 0,
        "already_processed_skipped": 0,
        "processed_filings": 3,
        "empty_extract_retryable": 0,
        "empty_extract_processed_no_holdings": 0,
        "skipped": 0,
        "errors": 0,
        "position_rows": 25,
        "position_changes": 20,
        "summaries": 10,
        "activity_events": 4,
        "feed_events": 1,
    }
    result.update(overrides)
    return result


def _seed_state(Session, **kwargs):
    db = Session()
    try:
        state = InstitutionalIngestJobState(
            job_name=job_module.LATEST_FILINGS_JOB_NAME,
            enabled=kwargs.pop("enabled", False),
            cursor_page=kwargs.pop("cursor_page", 9),
            pages_per_run=kwargs.pop("pages_per_run", 1),
            limit=kwargs.pop("limit", 25),
            max_filings_per_run=kwargs.pop("max_filings_per_run", 5),
            last_status=kwargs.pop("last_status", "idle"),
            last_started_at=kwargs.pop("last_started_at", None),
        )
        for key, value in kwargs.items():
            setattr(state, key, value)
        db.add(state)
        db.commit()
    finally:
        db.close()


def _seed_historical_state(Session, **kwargs):
    db = Session()
    try:
        state = InstitutionalIngestJobState(
            job_name=job_module.HISTORICAL_13F_JOB_NAME,
            enabled=kwargs.pop("enabled", False),
            cursor_page=kwargs.pop("cursor_page", 0),
            pages_per_run=1,
            limit=25,
            max_filings_per_run=kwargs.pop("max_filings_per_run", 1),
            last_status=kwargs.pop("last_status", "idle"),
            metadata_json=json.dumps(
                kwargs.pop(
                    "metadata",
                    {"start_year": 2024, "end_year": 2026, "holder_ciks": ["0000000001", "0000000002"]},
                )
            ),
            last_started_at=kwargs.pop("last_started_at", None),
        )
        for key, value in kwargs.items():
            setattr(state, key, value)
        db.add(state)
        db.commit()
    finally:
        db.close()


def _state(Session):
    db = Session()
    try:
        return db.get(InstitutionalIngestJobState, job_module.LATEST_FILINGS_JOB_NAME)
    finally:
        db.close()


def _historical_state(Session):
    db = Session()
    try:
        return db.get(InstitutionalIngestJobState, job_module.HISTORICAL_13F_JOB_NAME)
    finally:
        db.close()


def _runs(Session):
    db = Session()
    try:
        return list(db.query(InstitutionalIngestJobRun).order_by(InstitutionalIngestJobRun.id).all())
    finally:
        db.close()


def test_job_state_initialization_defaults_disabled(job_env):
    db = job_env()
    try:
        state = job_module.get_or_create_latest_job_state(db)
        assert state.enabled is False
        assert state.cursor_page == 9
        assert state.pages_per_run == 1
    assert state.limit == 5
        assert state.max_filings_per_run == 5
    finally:
        db.close()


def test_scheduled_latest_once_does_nothing_when_disabled(job_env, monkeypatch):
    called = False

    def fake_ingest(**_kwargs):
        nonlocal called
        called = True
        return _fake_result()

    monkeypatch.setattr(ingest_module, "ingest_latest_institutional_filings", fake_ingest)

    result = job_module.run_scheduled_latest_once()

    assert result["status"] == "paused"
    assert called is False
    assert _runs(job_env)[0].status == "paused"


def test_scheduled_latest_once_processes_fixed_window_and_advances_cursor(job_env, monkeypatch):
    _seed_state(job_env, cursor_page=9, pages_per_run=5, max_filings_per_run=50, enabled=True)
    monkeypatch.setenv("INSTITUTIONAL_SCHEDULED_INGEST_ENABLED", "true")
    calls = []

    def fake_ingest(**kwargs):
        calls.append(kwargs)
        return _fake_result()

    monkeypatch.setattr(ingest_module, "ingest_latest_institutional_filings", fake_ingest)

    result = job_module.run_scheduled_latest_once()

    assert result["status"] == "success"
    assert calls == [{"start_page": 9, "pages": 1, "limit": 5, "max_filings": 5}]
    state = _state(job_env)
    assert state.cursor_page == 10
    assert state.pages_per_run == 1
    assert state.max_filings_per_run == 5
    assert state.total_filings_processed == 3
    assert state.total_position_rows == 25
    run = _runs(job_env)[0]
    assert run.status == "success"
    assert run.next_cursor_page == 10


def test_scheduled_latest_once_can_reset_cursor_to_start_page_each_run(job_env, monkeypatch):
    _seed_state(job_env, cursor_page=42, first_empty_page=99, enabled=True)
    monkeypatch.setenv("INSTITUTIONAL_SCHEDULED_INGEST_ENABLED", "true")
    monkeypatch.setenv("INSTITUTIONAL_SCHEDULED_INGEST_START_PAGE", "0")
    monkeypatch.setenv("INSTITUTIONAL_SCHEDULED_INGEST_RESET_CURSOR_EACH_RUN", "true")
    calls = []

    def fake_ingest(**kwargs):
        calls.append(kwargs)
        return _fake_result(start_page=0)

    monkeypatch.setattr(ingest_module, "ingest_latest_institutional_filings", fake_ingest)

    result = job_module.run_scheduled_latest_once()

    assert result["status"] == "success"
    assert calls == [{"start_page": 0, "pages": 1, "limit": 5, "max_filings": 5}]
    state = _state(job_env)
    assert state.cursor_page == 1
    assert state.first_empty_page is None


def test_scheduled_latest_once_retains_cursor_when_reset_is_not_enabled(job_env, monkeypatch):
    _seed_state(job_env, cursor_page=42, first_empty_page=99, enabled=True)
    monkeypatch.setenv("INSTITUTIONAL_SCHEDULED_INGEST_ENABLED", "true")
    monkeypatch.setenv("INSTITUTIONAL_SCHEDULED_INGEST_START_PAGE", "0")
    calls = []

    def fake_ingest(**kwargs):
        calls.append(kwargs)
        return _fake_result(start_page=42)

    monkeypatch.setattr(ingest_module, "ingest_latest_institutional_filings", fake_ingest)

    result = job_module.run_scheduled_latest_once()

    assert result["status"] == "success"
    assert calls == [{"start_page": 42, "pages": 1, "limit": 5, "max_filings": 5}]
    state = _state(job_env)
    assert state.cursor_page == 43
    assert state.first_empty_page == 99


def test_job_cursor_advances_after_successful_all_skipped_window(job_env, monkeypatch):
    _seed_state(job_env, cursor_page=9, enabled=True)
    monkeypatch.setenv("INSTITUTIONAL_SCHEDULED_INGEST_ENABLED", "true")
    monkeypatch.setattr(
        ingest_module,
        "ingest_latest_institutional_filings",
        lambda **_kwargs: _fake_result(processed_filings=0, already_processed_skipped=25, skipped=25, position_rows=0, position_changes=0, summaries=0, activity_events=0, feed_events=0),
    )

    result = job_module.run_scheduled_latest_once()

    assert result["status"] == "success"
    assert _state(job_env).cursor_page == 10


def test_job_empty_page_marks_first_empty_and_wraps_to_first_page(job_env, monkeypatch):
    _seed_state(job_env, cursor_page=12, enabled=True)
    monkeypatch.setenv("INSTITUTIONAL_SCHEDULED_INGEST_ENABLED", "true")
    monkeypatch.setattr(
        ingest_module,
        "ingest_latest_institutional_filings",
        lambda **_kwargs: _fake_result(pages_scanned=0, scanned=0, parsed=0, processed_filings=0, first_empty_page_seen=12, position_rows=0, position_changes=0, summaries=0, activity_events=0, feed_events=0),
    )

    result = job_module.run_scheduled_latest_once()

    assert result["status"] == "success"
    state = _state(job_env)
    assert state.cursor_page == 0
    assert state.first_empty_page == 12
    assert state.enabled is True
    assert state.last_status == "success"
    run = _runs(job_env)[0]
    assert run.next_cursor_page == 0
    assert (json.loads(run.metadata_json or "{}")).get("stop_reason") == "empty_page_wrapped"


def test_job_duplicate_failure_pauses_and_disables(job_env, monkeypatch):
    _seed_state(job_env, cursor_page=9, enabled=True)
    monkeypatch.setenv("INSTITUTIONAL_SCHEDULED_INGEST_ENABLED", "true")
    monkeypatch.setattr(ingest_module, "ingest_latest_institutional_filings", lambda **_kwargs: _fake_result())
    monkeypatch.setattr(
        job_module,
        "institutional_ingest_duplicate_checks",
        lambda _db: {
            "filings_by_accession": 0,
            "active_canonical_filing_duplicates": 1,
            "positions_by_canonical_filing_security_put_call": 0,
            "position_changes": 0,
            "summaries": 0,
            "activity_events": 0,
            "feed_events": 0,
        },
    )

    result = job_module.run_scheduled_latest_once()

    assert result["status"] == "failed"
    state = _state(job_env)
    assert state.enabled is False
    assert state.cursor_page == 9
    assert "duplicate checks failed" in (state.last_error or "")


def test_feed_event_threshold_warns_without_pausing_clean_run(job_env, monkeypatch):
    _seed_state(job_env, cursor_page=20, enabled=True)
    monkeypatch.setenv("INSTITUTIONAL_SCHEDULED_INGEST_ENABLED", "true")
    monkeypatch.setenv("INSTITUTIONAL_SCHEDULED_INGEST_FEED_EVENTS_WARNING_THRESHOLD", "100")
    monkeypatch.setattr(
        ingest_module,
        "ingest_latest_institutional_filings",
        lambda **_kwargs: _fake_result(processed_filings=5, activity_events=149, feed_events=109),
    )

    result = job_module.run_scheduled_latest_once()

    state = _state(job_env)
    run = _runs(job_env)[0]
    metadata = json.loads(run.metadata_json or "{}")
    assert result["status"] == "success"
    assert state.enabled is True
    assert state.cursor_page == 21
    assert state.last_status == "success"
    assert state.last_error is None
    assert run.status == "success"
    assert run.next_cursor_page == 21
    assert metadata["feed_events_warning"] == {"feed_events": 109, "threshold": 100}


def test_latest_provider_timeout_is_retryable_and_keeps_scheduler_enabled(job_env, monkeypatch):
    _seed_state(job_env, cursor_page=24, enabled=True)
    monkeypatch.setenv("INSTITUTIONAL_SCHEDULED_INGEST_ENABLED", "true")

    def raise_timeout(**_kwargs):
        raise FMPClientError(
            "FMP API request failed for institutional-ownership/latest: "
            "HTTPSConnectionPool(host='financialmodelingprep.com', port=443): "
            "Read timed out. (read timeout=30)"
        )

    monkeypatch.setattr(ingest_module, "ingest_latest_institutional_filings", raise_timeout)

    result = job_module.run_scheduled_latest_once()

    state = _state(job_env)
    run = _runs(job_env)[0]
    assert result["status"] == "retryable"
    assert state.enabled is True
    assert state.cursor_page == 24
    assert state.last_status == "retryable"
    assert "Read timed out" in (state.last_error or "")
    assert run.status == "retryable"
    assert run.next_cursor_page == 24


def test_latest_provider_auth_failure_still_disables_scheduler(job_env, monkeypatch):
    _seed_state(job_env, cursor_page=24, enabled=True)
    monkeypatch.setenv("INSTITUTIONAL_SCHEDULED_INGEST_ENABLED", "true")
    monkeypatch.setattr(
        ingest_module,
        "ingest_latest_institutional_filings",
        lambda **_kwargs: (_ for _ in ()).throw(
            FMPClientError("FMP API auth failed (401) for institutional-ownership/latest: unauthorized")
        ),
    )

    result = job_module.run_scheduled_latest_once()

    state = _state(job_env)
    run = _runs(job_env)[0]
    assert result["status"] == "failed"
    assert state.enabled is False
    assert state.cursor_page == 24
    assert state.last_status == "failed"
    assert run.status == "failed"


def test_scheduled_latest_overlapping_run_returns_skipped_locked(job_env, monkeypatch):
    _seed_state(job_env, enabled=True, last_status="running", last_started_at=datetime.now(timezone.utc))
    monkeypatch.setenv("INSTITUTIONAL_SCHEDULED_INGEST_ENABLED", "true")
    monkeypatch.setattr(ingest_module, "ingest_latest_institutional_filings", lambda **_kwargs: pytest.fail("overlap should not ingest"))

    result = job_module.run_scheduled_latest_once()

    assert result["status"] == "skipped_locked"
    assert _runs(job_env)[0].status == "skipped_locked"


def test_scheduled_latest_stale_running_state_can_recover(job_env, monkeypatch):
    _seed_state(job_env, enabled=True, last_status="running", last_started_at=datetime.now(timezone.utc) - timedelta(hours=3))
    monkeypatch.setenv("INSTITUTIONAL_SCHEDULED_INGEST_ENABLED", "true")
    monkeypatch.setattr(ingest_module, "ingest_latest_institutional_filings", lambda **_kwargs: _fake_result())

    result = job_module.run_scheduled_latest_once()

    assert result["status"] == "success"
    assert _state(job_env).cursor_page == 10


def test_retryable_empty_extract_does_not_block_cursor(job_env, monkeypatch):
    _seed_state(job_env, cursor_page=9, enabled=True)
    monkeypatch.setenv("INSTITUTIONAL_SCHEDULED_INGEST_ENABLED", "true")
    monkeypatch.setattr(
        ingest_module,
        "ingest_latest_institutional_filings",
        lambda **_kwargs: _fake_result(pages_scanned=1, processed_filings=0, empty_extract_retryable=1, scanned=25, parsed=25, position_rows=0, position_changes=0, summaries=0, activity_events=0, feed_events=0),
    )

    result = job_module.run_scheduled_latest_once()

    assert result["status"] == "success"
    assert _state(job_env).cursor_page == 10


def test_max_filings_reached_advances_cursor_after_clean_page_run(job_env, monkeypatch):
    _seed_state(job_env, cursor_page=9, enabled=True)
    monkeypatch.setenv("INSTITUTIONAL_SCHEDULED_INGEST_ENABLED", "true")
    monkeypatch.setattr(
        ingest_module,
        "ingest_latest_institutional_filings",
        lambda **_kwargs: _fake_result(pages_scanned=1, processed_filings=25, max_filings_reached=1),
    )

    result = job_module.run_scheduled_latest_once()

    assert result["status"] == "success"
    assert _state(job_env).cursor_page == 10


def test_scheduled_latest_once_env_disabled_overrides_persisted_enabled(job_env, monkeypatch):
    _seed_state(job_env, cursor_page=9, enabled=True)
    monkeypatch.setenv("INSTITUTIONAL_SCHEDULED_INGEST_ENABLED", "false")
    monkeypatch.setattr(ingest_module, "ingest_latest_institutional_filings", lambda **_kwargs: pytest.fail("env-disabled scheduler must not ingest"))

    result = job_module.run_scheduled_latest_once()

    assert result["status"] == "paused"
    state = _state(job_env)
    assert state.cursor_page == 9
    assert "environment" in (result["run"]["error_message"] or "")


def test_scheduled_latest_once_persisted_disabled_overrides_env_enabled(job_env, monkeypatch):
    _seed_state(job_env, cursor_page=9, enabled=False)
    monkeypatch.setenv("INSTITUTIONAL_SCHEDULED_INGEST_ENABLED", "true")
    monkeypatch.setattr(ingest_module, "ingest_latest_institutional_filings", lambda **_kwargs: pytest.fail("state-disabled scheduler must not ingest"))

    result = job_module.run_scheduled_latest_once()

    assert result["status"] == "paused"
    state = _state(job_env)
    assert state.enabled is False
    assert state.cursor_page == 9
    assert "disabled" in (result["run"]["error_message"] or "")


def test_scheduled_latest_enabled_check_requires_env_and_persisted_state(job_env, monkeypatch):
    _seed_state(job_env, cursor_page=14, enabled=False)
    monkeypatch.setenv("INSTITUTIONAL_SCHEDULED_INGEST_ENABLED", "true")

    disabled = job_module.scheduled_latest_enabled_check()
    assert disabled["enabled"] is False
    assert disabled["env_enabled"] is True
    assert disabled["state_enabled"] is False
    assert disabled["state"]["cursor_page"] == 14

    db = job_env()
    try:
        state = db.get(InstitutionalIngestJobState, job_module.LATEST_FILINGS_JOB_NAME)
        state.enabled = True
        db.commit()
    finally:
        db.close()

    enabled = job_module.scheduled_latest_enabled_check()
    assert enabled["enabled"] is True
    assert enabled["env_enabled"] is True
    assert enabled["state_enabled"] is True


def test_scheduled_latest_once_cli_invokes_scheduler(monkeypatch, capsys):
    calls = []

    def fake_run_once():
        calls.append(True)
        return {"status": "success", "run": {"id": 1}}

    monkeypatch.setattr(ingest_module.sys, "argv", ["prog", "--scheduled-latest-once"])
    monkeypatch.setattr(job_module, "run_scheduled_latest_once", fake_run_once)

    ingest_module.main()

    assert calls == [True]
    assert "success" in capsys.readouterr().out


def test_historical_job_initialization_defaults_disabled(job_env):
    db = job_env()
    try:
        state = job_module.get_or_create_historical_job_state(db)
        payload = job_module.historical_job_state_payload(state)
        assert state.enabled is False
        assert state.cursor_page == 0
        assert state.max_filings_per_run == 1
        assert payload["holder_count"] >= 10
        assert payload["current_holder_cik"]
    finally:
        db.close()


def test_historical_job_run_once_pauses_when_disabled(job_env, monkeypatch):
    _seed_historical_state(job_env, enabled=False)
    monkeypatch.setattr(
        ingest_module,
        "backfill_institutional_historical_batch",
        lambda **_kwargs: pytest.fail("disabled historical job should not run backfill"),
    )

    result = job_module.run_historical_backfill_once()

    assert result["status"] == "paused"
    assert _runs(job_env)[0].status == "paused"
    assert _historical_state(job_env).cursor_page == 0


def test_historical_job_config_cli_updates_symbol_batch_size_only(job_env, monkeypatch, capsys):
    _seed_historical_state(job_env, enabled=False, max_filings_per_run=1)
    monkeypatch.setattr(ingest_module, "SessionLocal", job_env)

    monkeypatch.setattr(
        ingest_module.sys,
        "argv",
        ["prog", "--historical-job-config", "--historical-symbol-batch-size", "100"],
    )

    ingest_module.main()

    state = _historical_state(job_env)
    metadata = json.loads(state.metadata_json or "{}")
    assert state.max_filings_per_run == 1
    assert metadata["symbol_batch_size"] == 100
    assert "symbol_batch_size" in capsys.readouterr().out


def test_historical_job_processes_current_holder_and_keeps_cursor(job_env, monkeypatch):
    _seed_historical_state(job_env, enabled=True, cursor_page=0, max_filings_per_run=2)
    calls = []

    def fake_backfill(**kwargs):
        calls.append(kwargs)
        return {
            "status": "ok",
            "candidate_filings": 4,
            "selected_filings": 2,
            "skipped_existing": 0,
            "processed_filings": 2,
            "position_rows": 0,
            "position_changes": 20,
            "summaries": 10,
            "activity_events": 8,
            "feed_events": 0,
            "errors": 0,
        }

    monkeypatch.setattr(ingest_module, "backfill_institutional_historical_batch", fake_backfill)

    result = job_module.run_historical_backfill_once()

    assert result["status"] == "success"
    assert calls == [
        {
            "holder_ciks": ["0000000001"],
            "start_year": 2024,
            "end_year": 2026,
            "max_holders": 1,
            "max_filings_total": 2,
            "max_filings_per_holder": 2,
            "apply": True,
            "target_existing_filing_id": None,
            "symbol_cursor": None,
            "symbol_batch_size": 100,
        }
    ]
    state = _historical_state(job_env)
    assert state.cursor_page == 0
    assert state.total_filings_processed == 2
    assert state.total_activity_events == 8
    run = _runs(job_env)[0]
    assert run.job_name == job_module.HISTORICAL_13F_JOB_NAME
    assert run.next_cursor_page == 0
    assert run.position_changes == 20


def test_historical_job_stores_partial_filing_symbol_cursor(job_env, monkeypatch):
    _seed_historical_state(job_env, enabled=True, cursor_page=0, max_filings_per_run=1)
    calls = []

    def fake_backfill(**kwargs):
        calls.append(kwargs)
        return {
            "status": "ok",
            "candidate_filings": 1,
            "selected_filings": 1,
            "skipped_existing": 0,
            "processed_filings": 0,
            "processed_existing_position_filings": 0,
            "partial_filings": 1,
            "active_filing_id": 123,
            "symbols_processed": 100,
            "symbols_total": 5000,
            "next_symbol_cursor": "AAPL",
            "position_rows": 0,
            "position_changes": 100,
            "summaries": 100,
            "activity_events": 50,
            "feed_events": 0,
            "errors": 0,
        }

    monkeypatch.setattr(ingest_module, "backfill_institutional_historical_batch", fake_backfill)

    result = job_module.run_historical_backfill_once()

    state = _historical_state(job_env)
    metadata = json.loads(state.metadata_json or "{}")
    assert result["status"] == "success"
    assert state.cursor_page == 0
    assert metadata["active_filing_id"] == 123
    assert metadata["symbol_cursor"] == "AAPL"
    assert calls[0]["target_existing_filing_id"] is None
    assert calls[0]["symbol_cursor"] is None
    run = _runs(job_env)[0]
    assert run.processed_filings == 0
    assert run.position_changes == 100


def test_historical_job_config_updates_symbol_batch_size(job_env):
    db = job_env()
    try:
        state = job_module.update_historical_job_config(db, symbol_batch_size=250)
        db.commit()
        metadata = json.loads(state.metadata_json or "{}")
        assert metadata["symbol_batch_size"] == 250

        state = job_module.update_historical_job_config(db, symbol_batch_size=999)
        db.commit()
        metadata = json.loads(state.metadata_json or "{}")
        assert metadata["symbol_batch_size"] == 500
    finally:
        db.close()


def test_historical_job_resumes_partial_filing_symbol_cursor(job_env, monkeypatch):
    _seed_historical_state(
        job_env,
        enabled=True,
        cursor_page=0,
        max_filings_per_run=1,
        metadata={
            "start_year": 2024,
            "end_year": 2026,
            "holder_ciks": ["0000000001", "0000000002"],
            "active_filing_id": 123,
            "symbol_cursor": "AAPL",
            "symbol_batch_size": 25,
        },
    )
    calls = []

    def fake_backfill(**kwargs):
        calls.append(kwargs)
        return {
            "status": "ok",
            "candidate_filings": 1,
            "selected_filings": 1,
            "skipped_existing": 0,
            "processed_filings": 1,
            "processed_existing_position_filings": 1,
            "partial_filings": 0,
            "active_filing_id": 123,
            "symbols_processed": 25,
            "symbols_total": 125,
            "next_symbol_cursor": None,
            "position_rows": 0,
            "position_changes": 25,
            "summaries": 25,
            "activity_events": 12,
            "feed_events": 0,
            "errors": 0,
        }

    monkeypatch.setattr(ingest_module, "backfill_institutional_historical_batch", fake_backfill)

    result = job_module.run_historical_backfill_once()

    state = _historical_state(job_env)
    metadata = json.loads(state.metadata_json or "{}")
    assert result["status"] == "success"
    assert state.cursor_page == 0
    assert metadata["active_filing_id"] is None
    assert metadata["symbol_cursor"] is None
    assert calls == [
        {
            "holder_ciks": ["0000000001"],
            "start_year": 2024,
            "end_year": 2026,
            "max_holders": 1,
            "max_filings_total": 1,
            "max_filings_per_holder": 1,
            "apply": True,
            "target_existing_filing_id": 123,
            "symbol_cursor": "AAPL",
            "symbol_batch_size": 25,
        }
    ]


def test_historical_job_advances_cursor_when_holder_has_no_work(job_env, monkeypatch):
    _seed_historical_state(job_env, enabled=True, cursor_page=0)
    monkeypatch.setattr(
        ingest_module,
        "backfill_institutional_historical_batch",
        lambda **_kwargs: {
            "status": "ok",
            "candidate_filings": 3,
            "selected_filings": 0,
            "skipped_existing": 3,
            "processed_filings": 0,
            "position_rows": 0,
            "position_changes": 0,
            "summaries": 0,
            "activity_events": 0,
            "feed_events": 0,
            "errors": 0,
        },
    )

    result = job_module.run_historical_backfill_once()

    assert result["status"] == "success"
    assert _historical_state(job_env).cursor_page == 1
    assert _runs(job_env)[0].next_cursor_page == 1


def test_historical_job_marks_complete_after_last_holder(job_env, monkeypatch):
    _seed_historical_state(
        job_env,
        enabled=True,
        cursor_page=1,
        metadata={"start_year": 2024, "end_year": 2026, "holder_ciks": ["0000000001", "0000000002"]},
    )
    monkeypatch.setattr(
        ingest_module,
        "backfill_institutional_historical_batch",
        lambda **_kwargs: {
            "status": "ok",
            "candidate_filings": 0,
            "selected_filings": 0,
            "skipped_existing": 0,
            "processed_filings": 0,
            "position_rows": 0,
            "position_changes": 0,
            "summaries": 0,
            "activity_events": 0,
            "feed_events": 0,
            "errors": 0,
        },
    )

    result = job_module.run_historical_backfill_once()

    state = _historical_state(job_env)
    assert result["status"] == "success"
    assert state.cursor_page == 2
    assert state.enabled is False
    assert state.last_status == "complete"


def test_historical_job_run_once_cli_invokes_runner(monkeypatch, capsys):
    calls = []

    def fake_run_once(*, require_enabled: bool = True):
        calls.append(require_enabled)
        return {"status": "success", "run": {"id": 2}}

    monkeypatch.setattr(ingest_module.sys, "argv", ["prog", "--historical-job-run-once"])
    monkeypatch.setattr(job_module, "run_historical_backfill_once", fake_run_once)

    ingest_module.main()

    assert calls == [True]
    assert "success" in capsys.readouterr().out


def test_scheduled_historical_once_cli_exits_nonzero_on_failed_run(monkeypatch, capsys):
    calls = []

    def fake_run_once(*, require_enabled: bool = True):
        calls.append(require_enabled)
        return {"status": "failed", "error": "boom"}

    monkeypatch.setattr(ingest_module.sys, "argv", ["prog", "--scheduled-historical-once"])
    monkeypatch.setattr(job_module, "run_historical_backfill_once", fake_run_once)

    with pytest.raises(SystemExit) as exc:
        ingest_module.main()

    assert exc.value.code == 1
    assert calls == [True]
    assert "failed" in capsys.readouterr().out


def test_scheduled_historical_once_cli_treats_paused_as_clean_skip(monkeypatch, capsys):
    calls = []

    def fake_run_once(*, require_enabled: bool = True):
        calls.append(require_enabled)
        return {"status": "paused"}

    monkeypatch.setattr(ingest_module.sys, "argv", ["prog", "--scheduled-historical-once"])
    monkeypatch.setattr(job_module, "run_historical_backfill_once", fake_run_once)

    with pytest.raises(SystemExit) as exc:
        ingest_module.main()

    assert exc.value.code == 0
    assert calls == [True]
    assert "paused" in capsys.readouterr().out


def _request_for_user(user: UserAccount) -> Request:
    token = sign_session_payload({"uid": user.id, "email": user.email})
    return Request({"type": "http", "method": "POST", "path": "/", "headers": [(b"cookie", f"{SESSION_COOKIE_NAME}={token}".encode())]})


def _user(db, email: str, *, role: str = "user") -> UserAccount:
    user = UserAccount(email=email, role=role, entitlement_tier="free")
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


def test_admin_endpoints_require_admin(job_env):
    db = job_env()
    try:
        user = _user(db, "not-admin@example.com")
        with pytest.raises(HTTPException) as exc:
            admin_router.admin_institutional_ingest_status(_request_for_user(user), db)
        assert exc.value.status_code == 403
    finally:
        db.close()


def test_admin_can_enable_disable_and_configure_job(job_env):
    db = job_env()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        request = _request_for_user(admin)

        enabled = admin_router.admin_enable_institutional_ingest(request, db)
        assert enabled["state"]["enabled"] is True

        configured = admin_router.admin_configure_institutional_ingest(
            admin_router.ConfigPayload(pages_per_run=3, max_filings_per_run=50, limit=25),
            request,
            db,
        )
        assert configured["state"]["pages_per_run"] == 3
        assert configured["state"]["max_filings_per_run"] == 50

        moved = admin_router.admin_set_institutional_ingest_cursor(admin_router.CursorPayload(cursor_page=21), request, db)
        assert moved["state"]["cursor_page"] == 21

        disabled = admin_router.admin_disable_institutional_ingest(request, db)
        assert disabled["state"]["enabled"] is False
    finally:
        db.close()


def test_admin_can_configure_historical_job(job_env):
    db = job_env()
    try:
        admin = _user(db, "historical-admin@example.com", role="admin")
        request = _request_for_user(admin)

        initialized = admin_router.admin_init_historical_institutional_ingest(request, db)
        assert initialized["state"]["job_name"] == job_module.HISTORICAL_13F_JOB_NAME

        configured = admin_router.admin_configure_historical_institutional_ingest(
            admin_router.HistoricalConfigPayload(
                start_year=2024,
                end_year=2026,
                holder_ciks=["0000000001", "0000000002"],
                max_filings_per_run=2,
                symbol_batch_size=250,
            ),
            request,
            db,
        )
        assert configured["state"]["metadata"]["start_year"] == 2024
        assert configured["state"]["metadata"]["symbol_batch_size"] == 250
        assert configured["state"]["holder_count"] == 2
        assert configured["state"]["max_filings_per_run"] == 2

        enabled = admin_router.admin_enable_historical_institutional_ingest(request, db)
        assert enabled["state"]["enabled"] is True

        moved = admin_router.admin_set_historical_institutional_ingest_cursor(admin_router.CursorPayload(cursor_page=1), request, db)
        assert moved["state"]["cursor_page"] == 1
        assert moved["state"]["current_holder_cik"] == "0000000002"

        disabled = admin_router.admin_disable_historical_institutional_ingest(request, db)
        assert disabled["state"]["enabled"] is False
    finally:
        db.close()


def test_duplicate_check_reports_feed_event_duplicates(job_env):
    db = job_env()
    try:
        db.add_all(
            [
                Event(event_type="cluster_accumulation", ts=datetime.now(timezone.utc), source="13F filing", symbol="AAPL", payload_json='{"a":1}'),
                Event(event_type="cluster_accumulation", ts=datetime.now(timezone.utc), source="13F filing", symbol="AAPL", payload_json='{"a":1}'),
            ]
        )
        db.commit()
        report = job_module.institutional_ingest_duplicate_checks(db)
        assert report["feed_events"] == 1
    finally:
        db.close()
