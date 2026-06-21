from __future__ import annotations

import asyncio
import json

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.auth import SESSION_COOKIE_NAME, sign_session_payload
from app.db import Base, get_db
from app.main import app
from app.models import UserAccount


async def _call_app(
    path: str,
    *,
    method: str = "GET",
    headers: list[tuple[bytes, bytes]] | None = None,
) -> tuple[int, dict]:
    raw_path, _, query_string = path.partition("?")
    messages: list[dict] = []

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


def _session_factory():
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False)


def _install_db_override(Session) -> None:
    def override_get_db():
        db = Session()
        try:
            yield db
        finally:
            db.close()

    app.dependency_overrides[get_db] = override_get_db


def _seed_admin(Session) -> UserAccount:
    db = Session()
    try:
        admin = UserAccount(email="admin@example.com", role="admin", entitlement_tier="free")
        db.add(admin)
        db.commit()
        db.refresh(admin)
        return admin
    finally:
        db.close()


def _cookie_header(token: str = "dummy-session") -> tuple[bytes, bytes]:
    return (b"cookie", f"{SESSION_COOKIE_NAME}={token}".encode("ascii"))


def test_cookie_auth_post_from_allowed_origin_succeeds(monkeypatch):
    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.setenv("FRONTEND_ORIGINS", "https://app.walnutmarkets.com")

    status, body = asyncio.run(
        _call_app(
            "/api/auth/logout",
            method="POST",
            headers=[_cookie_header(), (b"origin", b"https://app.walnutmarkets.com")],
        )
    )

    assert status == 200
    assert body["status"] == "ok"


def test_cookie_auth_post_from_disallowed_origin_fails(monkeypatch):
    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.setenv("FRONTEND_ORIGINS", "https://app.walnutmarkets.com")

    status, body = asyncio.run(
        _call_app(
            "/api/auth/logout",
            method="POST",
            headers=[_cookie_header(), (b"origin", b"https://evil.example")],
        )
    )

    assert status == 403
    assert body == {"detail": "Forbidden"}


def test_cookie_auth_post_without_origin_or_referer_fails(monkeypatch):
    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.setenv("FRONTEND_ORIGINS", "https://app.walnutmarkets.com")

    status, body = asyncio.run(_call_app("/api/auth/logout", method="POST", headers=[_cookie_header()]))

    assert status == 403
    assert body == {"detail": "Forbidden"}


def test_cookie_auth_post_allows_trusted_referer_when_origin_missing(monkeypatch):
    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.setenv("FRONTEND_ORIGINS", "https://app.walnutmarkets.com")

    status, body = asyncio.run(
        _call_app(
            "/api/auth/logout",
            method="POST",
            headers=[
                _cookie_header(),
                (b"referer", b"https://app.walnutmarkets.com/account"),
            ],
        )
    )

    assert status == 200
    assert body["status"] == "ok"


def test_auth_entrypoints_without_session_cookie_are_not_origin_blocked(monkeypatch):
    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.setenv("FRONTEND_ORIGINS", "https://app.walnutmarkets.com")

    login_status, _ = asyncio.run(_call_app("/api/auth/login", method="POST"))
    register_status, _ = asyncio.run(_call_app("/api/auth/register", method="POST"))
    google_status, _ = asyncio.run(_call_app("/api/auth/google/callback", method="POST", headers=[_cookie_header()]))

    assert login_status != 403
    assert register_status != 403
    assert google_status != 403


def test_get_routes_and_options_preflight_are_not_blocked(monkeypatch):
    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.setenv("FRONTEND_ORIGINS", "https://app.walnutmarkets.com")

    get_status, get_body = asyncio.run(_call_app("/health", method="GET", headers=[_cookie_header()]))
    options_status, _ = asyncio.run(_call_app("/api/auth/logout", method="OPTIONS", headers=[_cookie_header()]))

    assert get_status == 200
    assert get_body == {"status": "ok"}
    assert options_status != 403


def test_stripe_webhook_is_exempt_from_origin_guard(monkeypatch):
    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.setenv("FRONTEND_ORIGINS", "https://app.walnutmarkets.com")

    status, _ = asyncio.run(
        _call_app("/api/billing/stripe/webhook", method="POST", headers=[_cookie_header()])
    )

    assert status != 403


def test_admin_mutation_from_allowed_origin_succeeds(monkeypatch):
    monkeypatch.setenv("APP_ENV", "development")
    Session = _session_factory()
    _install_db_override(Session)
    admin = _seed_admin(Session)
    token = sign_session_payload({"uid": admin.id, "email": admin.email})
    try:
        status, body = asyncio.run(
            _call_app(
                "/admin/seed-demo",
                method="POST",
                headers=[_cookie_header(token), (b"origin", b"http://localhost:3000")],
            )
        )
    finally:
        app.dependency_overrides.clear()

    assert status == 200
    assert body["status"] == "ok"


def test_admin_mutation_from_disallowed_origin_fails(monkeypatch):
    monkeypatch.setenv("APP_ENV", "development")
    Session = _session_factory()
    _install_db_override(Session)
    admin = _seed_admin(Session)
    token = sign_session_payload({"uid": admin.id, "email": admin.email})
    try:
        status, body = asyncio.run(
            _call_app(
                "/admin/seed-demo",
                method="POST",
                headers=[_cookie_header(token), (b"origin", b"https://evil.example")],
            )
        )
    finally:
        app.dependency_overrides.clear()

    assert status == 403
    assert body == {"detail": "Forbidden"}


def test_localhost_origin_is_trusted_only_outside_production(monkeypatch):
    monkeypatch.setenv("APP_ENV", "development")
    dev_status, _ = asyncio.run(
        _call_app(
            "/api/auth/logout",
            method="POST",
            headers=[_cookie_header(), (b"origin", b"http://localhost:3000")],
        )
    )

    monkeypatch.setenv("APP_ENV", "production")
    prod_status, prod_body = asyncio.run(
        _call_app(
            "/api/auth/logout",
            method="POST",
            headers=[_cookie_header(), (b"origin", b"http://localhost:3000")],
        )
    )

    assert dev_status == 200
    assert prod_status == 403
    assert prod_body == {"detail": "Forbidden"}
