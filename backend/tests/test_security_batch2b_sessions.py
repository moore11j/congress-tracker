from __future__ import annotations

from datetime import datetime, timezone

import pytest
from fastapi import HTTPException
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from starlette.requests import Request
from starlette.responses import Response

import app.auth as auth_module
from app.auth import (
    SESSION_COOKIE_NAME,
    current_user,
    sign_session_payload,
    validate_session_secret_config,
    verify_session_token,
)
from app.db import Base
from app.models import UserAccount
from app.routers.accounts import LoginPayload, RegisterPayload, login, logout, me, register


def _session():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    Base.metadata.create_all(engine)
    return Session()


def _user(db, email: str, *, password_hash: str | None = None, role: str = "user") -> UserAccount:
    user = UserAccount(email=email, password_hash=password_hash, role=role, entitlement_tier="free")
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


def _register_payload(email: str, *, password: str = "Password123!") -> RegisterPayload:
    return RegisterPayload(
        first_name="Reader",
        last_name="One",
        email=email,
        password=password,
        country="US",
        state_province="CA",
        postal_code="94105",
        city="San Francisco",
        address_line1="1 Market St",
        address_line2="Suite 200",
    )


def _request(headers: list[tuple[bytes, bytes]]) -> Request:
    return Request({"type": "http", "method": "GET", "path": "/", "headers": headers})


def test_session_token_has_expiration_and_rejects_expired_or_malformed(monkeypatch):
    monkeypatch.setenv("APP_ENV", "test")
    token = sign_session_payload({"uid": 1, "email": "reader@example.com"})

    parsed = verify_session_token(token)

    assert parsed is not None
    assert parsed["uid"] == 1
    assert isinstance(parsed["iat"], int)
    assert isinstance(parsed["exp"], int)
    assert parsed["exp"] > parsed["iat"]
    assert verify_session_token("not-a-token") is None

    expired = sign_session_payload(
        {
            "uid": 1,
            "email": "reader@example.com",
            "exp": int(datetime.now(timezone.utc).timestamp()) - 1,
        }
    )
    assert verify_session_token(expired) is None


def test_login_sets_secure_httponly_session_cookie_in_production(monkeypatch):
    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.setenv("APP_SESSION_SECRET", "x" * 48)
    monkeypatch.setenv("APP_SESSION_COOKIE_SAMESITE", "none")
    db = _session()
    try:
        registered = register(_register_payload("cookie-login@example.com"), db)
        assert registered["authenticated"] is True
        assert "token" not in registered
        response = Response()

        signed_in = login(LoginPayload(email="cookie-login@example.com", password="Password123!"), response, db)

        assert signed_in["user"]["email"] == "cookie-login@example.com"
        assert signed_in["authenticated"] is True
        assert "token" not in signed_in
        cookie = response.headers["set-cookie"].lower()
        assert f"{SESSION_COOKIE_NAME}=" in cookie
        assert "httponly" in cookie
        assert "secure" in cookie
        assert "samesite=none" in cookie
        assert "max-age=2592000" in cookie
        assert "expires=" in cookie
    finally:
        db.close()


def test_register_sets_session_cookie(monkeypatch):
    monkeypatch.setenv("APP_ENV", "test")
    db = _session()
    try:
        response = Response()

        created = register(_register_payload("cookie-register@example.com"), response, db)

        assert created["user"]["email"] == "cookie-register@example.com"
        assert created["authenticated"] is True
        assert "token" not in created
        cookie = response.headers["set-cookie"].lower()
        assert f"{SESSION_COOKIE_NAME}=" in cookie
        assert "httponly" in cookie
        assert "samesite=lax" in cookie
    finally:
        db.close()


def test_current_user_uses_cookie_and_rejects_bearer_by_default(monkeypatch):
    monkeypatch.setenv("APP_ENV", "test")
    monkeypatch.delenv("APP_ALLOW_BEARER_SESSION_AUTH", raising=False)
    db = _session()
    try:
        cookie_user = _user(db, "cookie@example.com")
        bearer_user = _user(db, "bearer@example.com")
        cookie_token = sign_session_payload({"uid": cookie_user.id, "email": cookie_user.email})
        bearer_token = sign_session_payload({"uid": bearer_user.id, "email": bearer_user.email})

        cookie_request = _request([(b"cookie", f"{SESSION_COOKIE_NAME}={cookie_token}".encode())])
        bearer_request = _request([(b"authorization", f"Bearer {bearer_token}".encode())])
        both_request = _request(
            [
                (b"cookie", f"{SESSION_COOKIE_NAME}={cookie_token}".encode()),
                (b"authorization", f"Bearer {bearer_token}".encode()),
            ]
        )

        assert current_user(db, cookie_request, required=True).email == cookie_user.email
        assert current_user(db, bearer_request, required=False) is None
        with pytest.raises(HTTPException) as exc_info:
            current_user(db, bearer_request, required=True)
        assert exc_info.value.status_code == 401
        assert current_user(db, both_request, required=True).email == cookie_user.email
    finally:
        db.close()


def test_bearer_session_auth_requires_nonproduction_opt_in(monkeypatch):
    monkeypatch.setenv("APP_ENV", "test")
    monkeypatch.setenv("APP_ALLOW_BEARER_SESSION_AUTH", "1")
    db = _session()
    try:
        bearer_user = _user(db, "bearer-dev@example.com")
        bearer_token = sign_session_payload({"uid": bearer_user.id, "email": bearer_user.email})

        bearer_request = _request([(b"authorization", f"Bearer {bearer_token}".encode())])

        assert current_user(db, bearer_request, required=True).email == bearer_user.email
    finally:
        db.close()


def test_current_user_reuses_request_local_resolution(monkeypatch):
    monkeypatch.setenv("APP_ENV", "test")
    db = _session()
    try:
        user = _user(db, "request-cache@example.com")
        request = _request([(b"cookie", f"{SESSION_COOKIE_NAME}=signed-token".encode())])
        calls = {"verify": 0}

        def fake_verify(token: str | None):
            calls["verify"] += 1
            assert token == "signed-token"
            return {"uid": user.id, "email": user.email}

        monkeypatch.setattr(auth_module, "verify_session_token", fake_verify)

        first = current_user(db, request, required=True)
        second = current_user(db, request, required=True)

        assert first is second
        assert first.email == user.email
        assert calls["verify"] == 1
    finally:
        db.close()


def test_bearer_session_auth_is_rejected_in_production(monkeypatch):
    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.setenv("APP_SESSION_SECRET", "x" * 48)
    monkeypatch.setenv("APP_ALLOW_BEARER_SESSION_AUTH", "1")
    db = _session()
    try:
        bearer_user = _user(db, "bearer-prod@example.com")
        bearer_token = sign_session_payload({"uid": bearer_user.id, "email": bearer_user.email})

        bearer_request = _request([(b"authorization", f"Bearer {bearer_token}".encode())])

        assert current_user(db, bearer_request, required=False) is None
    finally:
        db.close()


def test_auth_me_uses_cookie_session_and_returns_unauthenticated_without_cookie(monkeypatch):
    monkeypatch.setenv("APP_ENV", "test")
    db = _session()
    try:
        user = _user(db, "cookie-me@example.com")
        token = sign_session_payload({"uid": user.id, "email": user.email})

        cookie_request = _request([(b"cookie", f"{SESSION_COOKIE_NAME}={token}".encode())])
        cookie_response = me(cookie_request, db)
        assert cookie_response["user"]["email"] == "cookie-me@example.com"
        assert cookie_response["entitlements"]["tier"] == "free"

        anonymous_response = me(_request([]), db)
        assert anonymous_response["user"] is None
        assert anonymous_response["entitlements"]["tier"] == "free"
    finally:
        db.close()


def test_logout_clears_session_cookie(monkeypatch):
    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.setenv("APP_SESSION_SECRET", "x" * 48)
    monkeypatch.setenv("APP_SESSION_COOKIE_SAMESITE", "none")
    response = Response()

    result = logout(response)

    assert result == {"status": "ok", "clear_cookie": SESSION_COOKIE_NAME}
    cookie = response.headers["set-cookie"].lower()
    assert f"{SESSION_COOKIE_NAME}=" in cookie
    assert "max-age=0" in cookie
    assert "httponly" in cookie
    assert "secure" in cookie
    assert "samesite=none" in cookie


def test_production_requires_strong_app_session_secret(monkeypatch):
    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.delenv("APP_SESSION_SECRET", raising=False)
    monkeypatch.delenv("ADMIN_TOKEN", raising=False)

    with pytest.raises(RuntimeError, match="APP_SESSION_SECRET is required"):
        validate_session_secret_config()

    monkeypatch.setenv("APP_SESSION_SECRET", "short")
    with pytest.raises(RuntimeError, match="at least 32 characters"):
        validate_session_secret_config()

    monkeypatch.setenv("APP_SESSION_SECRET", "x" * 48)
    validate_session_secret_config()
