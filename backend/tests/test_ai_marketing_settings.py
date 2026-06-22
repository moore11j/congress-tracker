from __future__ import annotations

import json

import pytest
from fastapi import HTTPException
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from starlette.requests import Request

from app.auth import SESSION_COOKIE_NAME, sign_session_payload
from app.db import Base, ensure_ai_marketing_schema
from app.models import AiMarketingSetting, UserAccount
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
    test_openai_connection as run_openai_connection_test,
    test_reddit_connection as run_reddit_connection_test,
)


def _session():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    ensure_ai_marketing_schema(engine)
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    return Session()


def _request_for_user(user: UserAccount) -> Request:
    token = sign_session_payload({"uid": user.id, "email": user.email})
    return Request({"type": "http", "method": "PATCH", "path": "/", "headers": [(b"cookie", f"{SESSION_COOKIE_NAME}={token}".encode())]})


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


def _deprecated_provider_setting(db, key: str, value: str, *, is_secret: bool = True) -> None:
    db.add(AiMarketingSetting(key=key, value=value, is_secret=is_secret))
    db.commit()


@pytest.mark.parametrize("key", [OPENAI_API_KEY, AI_MARKETING_MODEL, REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET, REDDIT_USER_AGENT])
def test_admin_cannot_save_provider_env_only_settings(monkeypatch, key):
    _clear_env(monkeypatch)
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        with pytest.raises(HTTPException) as exc:
            admin_ai_marketing_update_settings(
                SettingsPatchPayload(updates={key: "raw-provider-value"}),
                _request_for_user(admin),
                db,
            )
        assert exc.value.status_code == 422
        assert exc.value.detail == "Provider credentials are managed through server environment variables."
    finally:
        db.close()


@pytest.mark.parametrize("key", [OPENAI_API_KEY, AI_MARKETING_MODEL, REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET, REDDIT_USER_AGENT])
def test_admin_cannot_clear_provider_env_only_settings(monkeypatch, key):
    _clear_env(monkeypatch)
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        with pytest.raises(HTTPException) as exc:
            admin_ai_marketing_update_settings(
                SettingsPatchPayload(clear=[key]),
                _request_for_user(admin),
                db,
            )
        assert exc.value.status_code == 422
        assert exc.value.detail == "Provider credentials are managed through server environment variables."
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


def test_get_settings_returns_provider_status_without_raw_values(monkeypatch):
    _clear_env(monkeypatch)
    monkeypatch.setenv(OPENAI_API_KEY, "sk-env-openai")
    monkeypatch.setenv(AI_MARKETING_MODEL, "gpt-env-model")
    monkeypatch.setenv(REDDIT_CLIENT_ID, "reddit-env-client")
    monkeypatch.setenv(REDDIT_CLIENT_SECRET, "reddit-env-secret")
    monkeypatch.setenv(REDDIT_USER_AGENT, "walnut-market-terminal/1.0")
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        _deprecated_provider_setting(db, OPENAI_API_KEY, "sk-db-openai")
        _deprecated_provider_setting(db, REDDIT_CLIENT_SECRET, "reddit-db-secret")

        payload = admin_ai_marketing_settings(_request_for_user(admin), db)
        items = _items_by_key(payload)
        serialized = json.dumps(payload)

        assert items[OPENAI_API_KEY]["configured"] is True
        assert items[OPENAI_API_KEY]["source"] == "server_env"
        assert items[OPENAI_API_KEY]["source_label"] == "Configured via server env"
        assert items[OPENAI_API_KEY]["masked_value"] is None
        assert "value" not in items[OPENAI_API_KEY]
        assert "Deprecated DB-stored provider credentials detected; ignored." in payload["config"]["warnings"]
        assert "sk-env-openai" not in serialized
        assert "sk-db-openai" not in serialized
        assert "reddit-env-client" not in serialized
        assert "reddit-env-secret" not in serialized
        assert "reddit-db-secret" not in serialized
        assert "walnut-market-terminal/1.0" not in serialized
    finally:
        db.close()


def test_config_resolver_ignores_db_provider_setting_and_uses_env(monkeypatch):
    _clear_env(monkeypatch)
    monkeypatch.setenv(OPENAI_API_KEY, "sk-env-value")
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        _deprecated_provider_setting(db, OPENAI_API_KEY, "sk-db-value")

        payload = admin_ai_marketing_settings(_request_for_user(admin), db)

        assert resolved_setting_value(db, OPENAI_API_KEY) == "sk-env-value"
        assert _items_by_key(payload)[OPENAI_API_KEY]["source"] == "server_env"
        assert "Deprecated DB-stored provider credentials detected; ignored." in payload["config"]["warnings"]
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
        assert "value" not in item
        assert item["source"] == "server_env"
        assert item["source_label"] == "Configured via server env"
    finally:
        db.close()


def test_model_uses_default_when_env_missing_and_db_ignored(monkeypatch):
    _clear_env(monkeypatch)
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        _deprecated_provider_setting(db, AI_MARKETING_MODEL, "gpt-db-model", is_secret=False)
        payload = admin_ai_marketing_settings(_request_for_user(admin), db)
        item = _items_by_key(payload)[AI_MARKETING_MODEL]

        assert resolved_setting_value(db, AI_MARKETING_MODEL) == "gpt-5.4-mini"
        assert item["configured"] is True
        assert item["source"] == "default"
        assert item["source_label"] == "Default"
        assert "value" not in item
        assert "Deprecated DB-stored provider credentials detected; ignored." in payload["config"]["warnings"]
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


def test_openai_connection_uses_env_key_not_db(monkeypatch):
    _clear_env(monkeypatch)
    monkeypatch.setenv(OPENAI_API_KEY, "sk-env-openai")
    monkeypatch.setenv(AI_MARKETING_MODEL, "gpt-env-model")
    captured = {}

    class FakeResponse:
        status_code = 200

    def fake_get(url, **kwargs):
        captured["url"] = url
        captured["headers"] = kwargs.get("headers")
        return FakeResponse()

    monkeypatch.setattr("app.services.ai_marketing.requests.get", fake_get)
    db = _session()
    try:
        _deprecated_provider_setting(db, OPENAI_API_KEY, "sk-db-openai")

        result = run_openai_connection_test(db)

        assert result["ok"] is True
        assert captured["headers"]["Authorization"] == "Bearer sk-env-openai"
        assert captured["url"].endswith("/gpt-env-model")
    finally:
        db.close()


def test_reddit_connection_uses_env_credentials_not_db(monkeypatch):
    _clear_env(monkeypatch)
    monkeypatch.setenv(REDDIT_CLIENT_ID, "reddit-env-client")
    monkeypatch.setenv(REDDIT_CLIENT_SECRET, "reddit-env-secret")
    monkeypatch.setenv(REDDIT_USER_AGENT, "walnut-env-agent")
    captured = {}

    def fake_access_token(client_id, client_secret, user_agent):
        captured["client_id"] = client_id
        captured["client_secret"] = client_secret
        captured["user_agent"] = user_agent
        return "token"

    monkeypatch.setattr("app.services.ai_marketing.RedditSourceAdapter._access_token", staticmethod(fake_access_token))
    db = _session()
    try:
        _deprecated_provider_setting(db, REDDIT_CLIENT_ID, "reddit-db-client")
        _deprecated_provider_setting(db, REDDIT_CLIENT_SECRET, "reddit-db-secret")
        _deprecated_provider_setting(db, REDDIT_USER_AGENT, "reddit-db-agent", is_secret=False)

        result = run_reddit_connection_test(db)

        assert result["ok"] is True
        assert captured == {
            "client_id": "reddit-env-client",
            "client_secret": "reddit-env-secret",
            "user_agent": "walnut-env-agent",
        }
    finally:
        db.close()
