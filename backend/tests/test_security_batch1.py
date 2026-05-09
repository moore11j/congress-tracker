from __future__ import annotations

from fastapi import HTTPException
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from starlette.requests import Request

from app.auth import sign_session_payload
from app.db import Base
from app.main import _cors_allowed_origins, seed_demo
from app.models import Member, UserAccount


def _session():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    Base.metadata.create_all(engine)
    return Session()


def _anonymous_request() -> Request:
    return Request({"type": "http", "method": "POST", "path": "/admin/seed-demo", "headers": []})


def _request_for_user(user: UserAccount) -> Request:
    token = sign_session_payload({"uid": user.id, "email": user.email})
    return Request(
        {"type": "http", "method": "POST", "path": "/admin/seed-demo", "headers": [(b"authorization", f"Bearer {token}".encode())]}
    )


def _user(db, email: str, *, role: str = "user") -> UserAccount:
    user = UserAccount(email=email, role=role, entitlement_tier="free")
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


def test_seed_demo_rejects_unauthenticated_without_mutation(monkeypatch):
    monkeypatch.setenv("APP_ENV", "development")
    db = _session()
    try:
        before = db.execute(select(Member)).scalars().all()
        try:
            seed_demo(_anonymous_request(), db)
        except HTTPException as exc:
            assert exc.status_code == 401
        else:
            raise AssertionError("Expected unauthenticated seed-demo rejection")
        after = db.execute(select(Member)).scalars().all()
        assert before == after == []
    finally:
        db.close()


def test_seed_demo_disabled_in_production_even_for_admin(monkeypatch):
    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.setenv("APP_SESSION_SECRET", "x" * 48)
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        try:
            seed_demo(_request_for_user(admin), db)
        except HTTPException as exc:
            assert exc.status_code == 404
        else:
            raise AssertionError("Expected production seed-demo to be disabled")
        assert db.execute(select(Member)).scalars().all() == []
    finally:
        db.close()


def test_production_cors_rejects_wildcard_and_keeps_explicit_origins(monkeypatch):
    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.setenv("CORS_ALLOW_ORIGINS", "*,http://localhost:3000")
    monkeypatch.delenv("FRONTEND_ORIGINS", raising=False)
    monkeypatch.delenv("FRONTEND_URL", raising=False)

    origins = _cors_allowed_origins()

    assert "*" not in origins
    assert "http://localhost:3000" in origins
    assert "https://congress-tracker-two.vercel.app" in origins


def test_production_cors_missing_config_uses_only_production_default(monkeypatch):
    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.delenv("CORS_ALLOW_ORIGINS", raising=False)
    monkeypatch.delenv("FRONTEND_ORIGINS", raising=False)
    monkeypatch.delenv("FRONTEND_URL", raising=False)

    origins = _cors_allowed_origins()

    assert origins == ["https://congress-tracker-two.vercel.app"]


def test_configured_frontend_origin_is_allowed(monkeypatch):
    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.setenv("FRONTEND_ORIGINS", "https://preview.example.com")
    monkeypatch.delenv("CORS_ALLOW_ORIGINS", raising=False)
    monkeypatch.delenv("FRONTEND_URL", raising=False)

    origins = _cors_allowed_origins()

    assert "https://preview.example.com" in origins
    assert "https://congress-tracker-two.vercel.app" in origins
    assert "*" not in origins
