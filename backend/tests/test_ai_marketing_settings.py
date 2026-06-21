from __future__ import annotations

import json

import pytest
from fastapi import HTTPException
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from starlette.requests import Request

from app.auth import sign_session_payload
from app.db import Base, ensure_ai_marketing_schema
from app.models import UserAccount
from app.routers.ai_marketing import (
    SettingsPatchPayload,
    admin_ai_marketing_settings,
    admin_ai_marketing_update_settings,
)
from app.services.ai_marketing import (
    AI_MARKETING_MODEL,
    OPENAI_API_KEY,
    REDDIT_CLIENT_ID,
    REDDIT_CLIENT_SECRET,
    REDDIT_USER_AGENT,
    config_status,
    resolved_setting_value,
)


def _session():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    ensure_ai_marketing_schema(engine)
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    return Session()


def _request_for_user(user: UserAccount) -> Request:
    token = sign_session_payload({"uid": user.id, "email": user.email})
    return Request({"type": "http", "method": "PATCH", "path": "/", "headers": [(b"authorization", f"Bearer {token}".encode())]})


def _user(db, email: str, *, role: str = "user") -> UserAccount:
    user = UserAccount(email=email, role=role, entitlement_tier="premium")
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


def _items_by_key(payload: dict):
    return {item["key"]: item for item in payload["items"]}


def _clear_env(monkeypatch):
    for key in (OPENAI_API_KEY, AI_MARKETING_MODEL, REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET, REDDIT_USER_AGENT):
        monkeypatch.delenv(key, raising=False)


def test_admin_can_save_non_secret_model_setting(monkeypatch):
    _clear_env(monkeypatch)
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        payload = admin_ai_marketing_update_settings(
            SettingsPatchPayload(updates={AI_MARKETING_MODEL: "gpt-test-mini"}),
            _request_for_user(admin),
            db,
        )
        item = _items_by_key(payload)[AI_MARKETING_MODEL]

        assert item["value"] == "gpt-test-mini"
        assert item["configured"] is True
        assert item["source"] == "admin_settings"
        assert item["source_label"] == "Configured in admin settings"
        assert resolved_setting_value(db, AI_MARKETING_MODEL) == "gpt-test-mini"
    finally:
        db.close()


def test_admin_can_save_secret_openai_key_and_get_masks_value(monkeypatch):
    _clear_env(monkeypatch)
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        payload = admin_ai_marketing_update_settings(
            SettingsPatchPayload(updates={OPENAI_API_KEY: "sk-secret-value"}),
            _request_for_user(admin),
            db,
        )
        get_payload = admin_ai_marketing_settings(_request_for_user(admin), db)
        openai_item = _items_by_key(get_payload)[OPENAI_API_KEY]

        assert _items_by_key(payload)[OPENAI_API_KEY]["configured"] is True
        assert openai_item["configured"] is True
        assert openai_item["source"] == "admin_settings"
        assert openai_item["masked_value"]
        assert "value" not in openai_item
        assert "sk-secret-value" not in json.dumps(get_payload)
    finally:
        db.close()


def test_non_admin_cannot_read_or_update_settings(monkeypatch):
    _clear_env(monkeypatch)
    db = _session()
    try:
        user = _user(db, "reader@example.com", role="user")
        request = _request_for_user(user)

        with pytest.raises(HTTPException) as read_exc:
            admin_ai_marketing_settings(request, db)
        assert read_exc.value.status_code == 403

        with pytest.raises(HTTPException) as update_exc:
            admin_ai_marketing_update_settings(SettingsPatchPayload(updates={AI_MARKETING_MODEL: "gpt-test-mini"}), request, db)
        assert update_exc.value.status_code == 403
    finally:
        db.close()


def test_config_resolver_prefers_db_setting_over_env(monkeypatch):
    _clear_env(monkeypatch)
    monkeypatch.setenv(OPENAI_API_KEY, "sk-env-value")
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        admin_ai_marketing_update_settings(
            SettingsPatchPayload(updates={OPENAI_API_KEY: "sk-db-value"}),
            _request_for_user(admin),
            db,
        )

        assert resolved_setting_value(db, OPENAI_API_KEY) == "sk-db-value"
        assert _items_by_key(admin_ai_marketing_settings(_request_for_user(admin), db))[OPENAI_API_KEY]["source"] == "admin_settings"
    finally:
        db.close()


def test_config_resolver_falls_back_to_env_when_db_missing(monkeypatch):
    _clear_env(monkeypatch)
    monkeypatch.setenv(REDDIT_USER_AGENT, "walnut-market-terminal/1.0")
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        payload = admin_ai_marketing_settings(_request_for_user(admin), db)
        item = _items_by_key(payload)[REDDIT_USER_AGENT]

        assert resolved_setting_value(db, REDDIT_USER_AGENT) == "walnut-market-terminal/1.0"
        assert item["value"] == "walnut-market-terminal/1.0"
        assert item["source"] == "server_env"
        assert item["source_label"] == "Configured via server env"
    finally:
        db.close()


def test_missing_credentials_produce_helpful_warnings(monkeypatch):
    _clear_env(monkeypatch)
    db = _session()
    try:
        warnings = config_status(db)["warnings"]

        assert "OpenAI API key missing" in warnings
        assert "Reddit client ID missing" in warnings
        assert "Reddit client secret missing" in warnings
        assert "Reddit user agent missing" in warnings
    finally:
        db.close()
