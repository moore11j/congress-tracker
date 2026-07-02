from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from fastapi import HTTPException
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool
from starlette.requests import Request

from app import ingest_institutional_activity as ingest_module
from app.auth import SESSION_COOKIE_NAME, sign_session_payload
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
    monkeypatch.setenv("INSTITUTIONAL_LATEST_JOB_ENABLED", "false")
    monkeypatch.setenv("INSTITUTIONAL_LATEST_JOB_START_PAGE", "9")
    monkeypatch.setenv("INSTITUTIONAL_LATEST_JOB_PAGES_PER_RUN", "2")
    monkeypatch.setenv("INSTITUTIONAL_LATEST_JOB_LIMIT", "25")
    monkeypatch.setenv("INSTITUTIONAL_LATEST_JOB_MAX_FILINGS", "25")
    monkeypatch.setenv("INSTITUTIONAL_LATEST_JOB_STOP_AT_EMPTY_PAGE", "true")
    return Session


def _fake_result(**overrides):
    result = {
        "status": "ok",
        "start_page": 9,
        "pages": 2,
        "pages_scanned": 2,
        "first_empty_page_seen": None,
        "max_filings_reached": 0,
        "scanned": 50,
        "parsed": 50,
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
            pages_per_run=kwargs.pop("pages_per_run", 2),
            limit=kwargs.pop("limit", 25),
            max_filings_per_run=kwargs.pop("max_filings_per_run", 25),
            last_status=kwargs.pop("last_status", "idle"),
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
        assert state.pages_per_run == 2
        assert state.limit == 25
        assert state.max_filings_per_run == 25
    finally:
        db.close()


def test_job_run_once_requires_enabled_when_requested(job_env, monkeypatch):
    called = False

    def fake_ingest(**_kwargs):
        nonlocal called
        called = True
        return _fake_result()

    monkeypatch.setattr(ingest_module, "ingest_latest_institutional_filings", fake_ingest)

    result = job_module.run_latest_ingest_job_once(require_enabled=True)

    assert result["status"] == "paused"
    assert called is False
    assert _runs(job_env)[0].status == "paused"


def test_job_run_once_processes_window_and_advances_cursor(job_env, monkeypatch):
    _seed_state(job_env, cursor_page=9, pages_per_run=2, enabled=False)
    calls = []

    def fake_ingest(**kwargs):
        calls.append(kwargs)
        return _fake_result()

    monkeypatch.setattr(ingest_module, "ingest_latest_institutional_filings", fake_ingest)

    result = job_module.run_latest_ingest_job_once()

    assert result["status"] == "success"
    assert calls == [{"start_page": 9, "pages": 2, "limit": 25, "max_filings": 25}]
    state = _state(job_env)
    assert state.cursor_page == 11
    assert state.total_filings_processed == 3
    assert state.total_position_rows == 25
    run = _runs(job_env)[0]
    assert run.status == "success"
    assert run.next_cursor_page == 11


def test_job_cursor_advances_after_successful_all_skipped_window(job_env, monkeypatch):
    _seed_state(job_env, cursor_page=9)
    monkeypatch.setattr(
        ingest_module,
        "ingest_latest_institutional_filings",
        lambda **_kwargs: _fake_result(processed_filings=0, already_processed_skipped=50, skipped=50, position_rows=0, position_changes=0, summaries=0, activity_events=0, feed_events=0),
    )

    result = job_module.run_latest_ingest_job_once()

    assert result["status"] == "success"
    assert _state(job_env).cursor_page == 11


def test_job_empty_page_marks_first_empty_and_pauses(job_env, monkeypatch):
    _seed_state(job_env, cursor_page=12, enabled=True)
    monkeypatch.setattr(
        ingest_module,
        "ingest_latest_institutional_filings",
        lambda **_kwargs: _fake_result(pages_scanned=0, scanned=0, parsed=0, processed_filings=0, first_empty_page_seen=12, position_rows=0, position_changes=0, summaries=0, activity_events=0, feed_events=0),
    )

    result = job_module.run_latest_ingest_job_once()

    assert result["status"] == "success"
    state = _state(job_env)
    assert state.cursor_page == 12
    assert state.first_empty_page == 12
    assert state.enabled is False
    assert state.last_status == "paused"


def test_job_duplicate_failure_pauses_and_disables(job_env, monkeypatch):
    _seed_state(job_env, cursor_page=9, enabled=True)
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

    result = job_module.run_latest_ingest_job_once()

    assert result["status"] == "failed"
    state = _state(job_env)
    assert state.enabled is False
    assert state.cursor_page == 9
    assert "duplicate checks failed" in (state.last_error or "")


def test_job_overlapping_run_returns_skipped_locked(job_env, monkeypatch):
    _seed_state(job_env, last_status="running", last_started_at=datetime.now(timezone.utc))
    monkeypatch.setattr(ingest_module, "ingest_latest_institutional_filings", lambda **_kwargs: pytest.fail("overlap should not ingest"))

    result = job_module.run_latest_ingest_job_once()

    assert result["status"] == "skipped_locked"
    assert _runs(job_env)[0].status == "skipped_locked"


def test_job_stale_running_state_can_recover(job_env, monkeypatch):
    _seed_state(job_env, last_status="running", last_started_at=datetime.now(timezone.utc) - timedelta(hours=3))
    monkeypatch.setattr(ingest_module, "ingest_latest_institutional_filings", lambda **_kwargs: _fake_result())

    result = job_module.run_latest_ingest_job_once()

    assert result["status"] == "success"
    assert _state(job_env).cursor_page == 11


def test_retryable_empty_extract_does_not_block_cursor(job_env, monkeypatch):
    _seed_state(job_env, cursor_page=9)
    monkeypatch.setattr(
        ingest_module,
        "ingest_latest_institutional_filings",
        lambda **_kwargs: _fake_result(pages_scanned=1, processed_filings=0, empty_extract_retryable=1, scanned=25, parsed=25, position_rows=0, position_changes=0, summaries=0, activity_events=0, feed_events=0),
    )

    result = job_module.run_latest_ingest_job_once()

    assert result["status"] == "success"
    assert _state(job_env).cursor_page == 10


def test_max_filings_reached_keeps_cursor_on_same_page(job_env, monkeypatch):
    _seed_state(job_env, cursor_page=9)
    monkeypatch.setattr(
        ingest_module,
        "ingest_latest_institutional_filings",
        lambda **_kwargs: _fake_result(pages_scanned=1, processed_filings=25, max_filings_reached=1),
    )

    result = job_module.run_latest_ingest_job_once()

    assert result["status"] == "success"
    assert _state(job_env).cursor_page == 9


def test_job_run_once_cli_invokes_durable_runner(monkeypatch, capsys):
    calls = []

    def fake_run_once(*, require_enabled: bool = False):
        calls.append(require_enabled)
        return {"status": "success", "run": {"id": 1}}

    monkeypatch.setattr(ingest_module.sys, "argv", ["prog", "--job-run-once", "--require-job-enabled"])
    monkeypatch.setattr(job_module, "run_latest_ingest_job_once", fake_run_once)

    ingest_module.main()

    assert calls == [True]
    assert "success" in capsys.readouterr().out


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
