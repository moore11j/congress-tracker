from __future__ import annotations

import asyncio
import json
import logging

import pytest
from fastapi import HTTPException
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool
from starlette.requests import Request

from app.auth import sign_session_payload
from app.db import Base
from app.models import UserAccount
from app.rate_limit import (
    RATE_LIMIT_MESSAGE,
    rate_limit_admin_mutation,
    rate_limit_auth_login,
    rate_limit_export,
    rate_limit_password_reset_confirm,
    rate_limit_password_reset_request,
    rate_limit_register,
    reset_rate_limiter_for_tests,
)


def _session():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    Base.metadata.create_all(engine)
    return Session()


def _user(db, email: str, *, role: str = "user") -> UserAccount:
    user = UserAccount(email=email, role=role, entitlement_tier="premium")
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


def _request(
    *,
    method: str = "POST",
    path: str = "/",
    payload: dict | None = None,
    ip: str = "203.0.113.10",
    user: UserAccount | None = None,
) -> Request:
    body = json.dumps(payload or {}).encode("utf-8")
    headers = [(b"content-type", b"application/json")]
    if user is not None:
        token = sign_session_payload({"uid": user.id, "email": user.email})
        headers.append((b"authorization", f"Bearer {token}".encode("utf-8")))

    async def receive():
        return {"type": "http.request", "body": body, "more_body": False}

    return Request(
        {
            "type": "http",
            "method": method,
            "path": path,
            "headers": headers,
            "client": (ip, 12345),
        },
        receive,
    )


@pytest.fixture(autouse=True)
def _reset_limiter(monkeypatch):
    monkeypatch.setenv("APP_ENV", "test")
    monkeypatch.setenv("RATE_LIMIT_ENABLED", "true")
    reset_rate_limiter_for_tests()
    yield
    reset_rate_limiter_for_tests()


def _assert_429(exc: HTTPException) -> None:
    assert exc.status_code == 429
    assert exc.detail == RATE_LIMIT_MESSAGE
    assert int(exc.headers["Retry-After"]) > 0


def test_login_limiter_returns_429_after_threshold(caplog):
    caplog.set_level(logging.WARNING, logger="app.rate_limit")
    for _ in range(5):
        asyncio.run(rate_limit_auth_login(_request(payload={"email": "Reader@Example.com", "password": "wrong"})))

    with pytest.raises(HTTPException) as raised:
        asyncio.run(rate_limit_auth_login(_request(payload={"email": "reader@example.com", "password": "wrong"})))

    _assert_429(raised.value)
    logged = "\n".join(record.getMessage() for record in caplog.records)
    assert "reader@example.com" not in logged
    assert "wrong" not in logged
    assert "auth_login_ip_email" in logged


def test_login_route_returns_429_over_asgi_stack():
    from app.db import get_db
    from app.main import app

    engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False)

    def override_get_db():
        db = Session()
        try:
            yield db
        finally:
            db.close()

    async def call_login() -> int:
        body = json.dumps({"email": "nobody@example.com", "password": "wrongpass123"}).encode("utf-8")
        messages = []
        sent = False

        async def receive():
            nonlocal sent
            if not sent:
                sent = True
                return {"type": "http.request", "body": body, "more_body": False}
            return {"type": "http.disconnect"}

        async def send(message):
            messages.append(message)

        await app(
            {
                "type": "http",
                "http_version": "1.1",
                "method": "POST",
                "path": "/api/auth/login",
                "raw_path": b"/api/auth/login",
                "query_string": b"",
                "headers": [(b"host", b"testserver"), (b"content-type", b"application/json")],
                "client": ("203.0.113.55", 12345),
                "server": ("testserver", 80),
                "scheme": "http",
            },
            receive,
            send,
        )
        started = next(message for message in messages if message["type"] == "http.response.start")
        return int(started["status"])

    app.dependency_overrides[get_db] = override_get_db
    try:
        statuses = [asyncio.run(call_login()) for _ in range(6)]
    finally:
        app.dependency_overrides.clear()

    assert statuses == [401, 401, 401, 401, 401, 429]


def test_password_reset_limiter_is_generic_and_does_not_leak_inputs():
    payload = {"email": "victim@example.com", "password": "not-used", "token": "secret-token"}
    for _ in range(3):
        asyncio.run(rate_limit_password_reset_request(_request(payload=payload)))

    with pytest.raises(HTTPException) as raised:
        asyncio.run(rate_limit_password_reset_request(_request(payload=payload)))

    _assert_429(raised.value)
    response_text = json.dumps({"detail": raised.value.detail, "headers": raised.value.headers})
    assert "victim@example.com" not in response_text
    assert "secret-token" not in response_text
    assert "not-used" not in response_text


def test_password_reset_confirm_limiter_hashes_token_key():
    payload = {"token": "raw-reset-token-value", "password": "Newpass1!"}
    for _ in range(5):
        asyncio.run(rate_limit_password_reset_confirm(_request(payload=payload)))

    with pytest.raises(HTTPException) as raised:
        asyncio.run(rate_limit_password_reset_confirm(_request(payload=payload)))

    _assert_429(raised.value)
    response_text = json.dumps({"detail": raised.value.detail, "headers": raised.value.headers})
    assert "raw-reset-token-value" not in response_text
    assert "Newpass1!" not in response_text


def test_register_limiter_returns_429_after_threshold():
    for _ in range(5):
        rate_limit_register(_request(payload={"email": "new@example.com"}))

    with pytest.raises(HTTPException) as raised:
        rate_limit_register(_request(payload={"email": "another@example.com"}))

    _assert_429(raised.value)


def test_export_limiter_uses_authenticated_user_id_not_shared_ip():
    db = _session()
    try:
        first = _user(db, "first@example.com")
        second = _user(db, "second@example.com")

        for _ in range(10):
            rate_limit_export(_request(method="GET", path="/api/screener/export.csv", user=first), db)
        for _ in range(10):
            rate_limit_export(_request(method="GET", path="/api/screener/export.csv", user=second), db)

        with pytest.raises(HTTPException) as raised:
            rate_limit_export(_request(method="GET", path="/api/screener/export.csv", user=first), db)

        _assert_429(raised.value)
    finally:
        db.close()


def test_admin_mutation_limiter_stops_rapid_admin_actions():
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        for _ in range(30):
            rate_limit_admin_mutation(_request(method="PATCH", path="/api/admin/settings/oauth", user=admin), db)

        with pytest.raises(HTTPException) as raised:
            rate_limit_admin_mutation(_request(method="PATCH", path="/api/admin/settings/oauth", user=admin), db)

        _assert_429(raised.value)
    finally:
        db.close()


def test_health_endpoint_has_no_rate_limit_dependency():
    from app.main import app

    health_routes = [route for route in app.routes if getattr(route, "path", None) == "/health"]
    assert len(health_routes) == 1
    dependant = getattr(health_routes[0], "dependant", None)
    assert dependant is not None
    assert dependant.dependencies == []
