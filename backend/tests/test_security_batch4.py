from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.auth import SESSION_COOKIE_NAME, sign_session_payload
from app.db import Base, get_db
from app.main import app
from app.models import Event, UserAccount
from app.security.redaction import redact_database_url, redact_secret_value, redact_url, safe_config_for_log


def _session_factory():
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False)


async def _call_app(path: str, *, method: str = "GET", headers: list[tuple[bytes, bytes]] | None = None) -> tuple[int, dict]:
    raw_path, _, query_string = path.partition("?")
    messages = []

    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message):
        messages.append(message)

    await app(
        {
            "type": "http",
            "http_version": "1.1",
            "method": method,
            "path": raw_path,
            "raw_path": raw_path.encode("ascii"),
            "query_string": query_string.encode("ascii"),
            "headers": [(b"host", b"testserver"), *(headers or [])],
            "client": ("203.0.113.10", 12345),
            "server": ("testserver", 80),
            "scheme": "http",
        },
        receive,
        send,
    )
    started = next(message for message in messages if message["type"] == "http.response.start")
    body = b"".join(message.get("body", b"") for message in messages if message["type"] == "http.response.body")
    return int(started["status"]), json.loads(body or b"{}")


def _install_db_override(Session):
    def override_get_db():
        db = Session()
        try:
            yield db
        finally:
            db.close()

    app.dependency_overrides[get_db] = override_get_db


def _seed_event_and_admin(Session) -> UserAccount:
    db = Session()
    try:
        admin = UserAccount(email="admin@example.com", role="admin", entitlement_tier="free")
        event = Event(
            event_type="congress_trade",
            ts=datetime(2026, 1, 1, tzinfo=timezone.utc),
            event_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
            symbol="AAPL",
            source="test",
            impact_score=1.0,
            payload_json='{"ticker":"AAPL","transaction_type":"purchase"}',
            member_name="Test Member",
            member_bioguide_id="T000001",
            chamber="house",
            party="democrat",
            trade_type="purchase",
            transaction_type="purchase",
            amount_min=1000,
            amount_max=15000,
        )
        db.add_all([admin, event])
        db.commit()
        db.refresh(admin)
        return admin
    finally:
        db.close()


def test_debug_ticker_meta_rejects_unauthenticated_query_token_in_production(monkeypatch):
    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.setenv("APP_SESSION_SECRET", "x" * 48)
    monkeypatch.setenv("ADMIN_TOKEN", "legacy-query-token")
    monkeypatch.setenv("RATE_LIMIT_ENABLED", "false")
    Session = _session_factory()
    _install_db_override(Session)
    try:
        status, body = asyncio.run(_call_app("/api/debug/ticker-meta?symbol=AAPL&token=legacy-query-token"))
    finally:
        app.dependency_overrides.clear()

    assert status in {401, 403}
    assert body.get("meta") is None


def test_debug_ticker_meta_allows_admin_session_in_production(monkeypatch):
    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.setenv("APP_SESSION_SECRET", "x" * 48)
    monkeypatch.delenv("FMP_API_KEY", raising=False)
    monkeypatch.setenv("RATE_LIMIT_ENABLED", "false")
    Session = _session_factory()
    _install_db_override(Session)
    admin = _seed_event_and_admin(Session)
    token = sign_session_payload({"uid": admin.id, "email": admin.email})
    try:
        status, body = asyncio.run(
            _call_app(
                "/api/debug/ticker-meta?symbol=AAPL",
                headers=[(b"cookie", f"{SESSION_COOKIE_NAME}={token}".encode("ascii"))],
            )
        )
    finally:
        app.dependency_overrides.clear()

    assert status == 200
    assert body["symbol_normalized"] == "AAPL"
    assert body["stable_preview"] == {"error": "missing_api_key"}


def test_admin_ensure_data_rejects_legacy_query_token(monkeypatch):
    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.setenv("APP_SESSION_SECRET", "x" * 48)
    monkeypatch.setenv("ADMIN_TOKEN", "legacy-query-token")
    Session = _session_factory()
    _install_db_override(Session)
    try:
        status, body = asyncio.run(_call_app("/admin/ensure_data?token=legacy-query-token", method="POST"))
    finally:
        app.dependency_overrides.clear()

    assert status in {401, 403}
    assert "transactions" not in body


def test_events_debug_true_is_suppressed_for_public_production_request(monkeypatch):
    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.setenv("APP_SESSION_SECRET", "x" * 48)
    monkeypatch.setenv("RATE_LIMIT_ENABLED", "false")
    Session = _session_factory()
    _install_db_override(Session)
    _seed_event_and_admin(Session)
    try:
        status, body = asyncio.run(_call_app("/api/events?debug=true&limit=1&enrich_prices=false"))
    finally:
        app.dependency_overrides.clear()

    assert status == 200
    assert "debug" not in body
    assert len(body["items"]) == 1


def test_events_debug_metadata_is_available_to_admin_in_production(monkeypatch):
    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.setenv("APP_SESSION_SECRET", "x" * 48)
    monkeypatch.setenv("RATE_LIMIT_ENABLED", "false")
    Session = _session_factory()
    _install_db_override(Session)
    admin = _seed_event_and_admin(Session)
    token = sign_session_payload({"uid": admin.id, "email": admin.email})
    try:
        status, body = asyncio.run(
            _call_app(
                "/api/events?debug=true&limit=1&enrich_prices=false",
                headers=[(b"cookie", f"{SESSION_COOKIE_NAME}={token}".encode("ascii"))],
            )
        )
    finally:
        app.dependency_overrides.clear()

    assert status == 200
    assert body["debug"]["count_after_filters"] == 1
    assert "applied_filters" in body["debug"]


def test_redaction_helper_redacts_database_url_userinfo_and_query():
    raw = "postgresql://db_user:s3cr3t@example.internal:5432/appdb?sslmode=require&apikey=abc"

    redacted = redact_database_url(raw)

    assert redacted == "postgresql://example.internal:5432/appdb"
    assert "db_user" not in redacted
    assert "s3cr3t" not in redacted
    assert "apikey" not in redacted


def test_redaction_helper_handles_empty_none_and_malformed_values():
    assert redact_url(None) is None
    assert redact_url("") == ""
    assert redact_url("not a url?password=secret") == "not a url?[REDACTED]"
    assert redact_secret_value(None) is None
    assert redact_secret_value("") == ""
    assert redact_secret_value("secret") == "[REDACTED]"
    assert safe_config_for_log({"DATABASE_URL": "postgres://u:p@h/db", "INGEST_LIMIT": 10}) == {
        "DATABASE_URL": "postgres://h/db",
        "INGEST_LIMIT": 10,
    }
