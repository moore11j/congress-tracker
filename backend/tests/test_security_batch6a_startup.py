from __future__ import annotations

import asyncio
import logging
import subprocess

import pytest
from fastapi import HTTPException
from starlette.requests import Request

from app.routers.accounts import stripe_webhook
from app.security.startup_checks import (
    StartupSecurityError,
    billing_enabled,
    log_stripe_billing_readiness,
    stripe_webhook_enabled,
    is_production,
    validate_startup_security_config,
)


_ENV_KEYS = (
    "APP_ENV",
    "ENV",
    "NODE_ENV",
    "FLY_APP_NAME",
    "APP_SESSION_SECRET",
    "APP_SESSION_COOKIE_SAMESITE",
    "APP_ALLOW_BEARER_SESSION_AUTH",
    "ADMIN_TOKEN",
    "FRONTEND_ORIGINS",
    "CORS_ALLOW_ORIGINS",
    "FRONTEND_URL",
    "CT_ALLOW_ENTITLEMENT_HEADER",
    "CT_ALLOW_INSECURE_RESET_LINK_RESPONSE",
    "BILLING_ENABLED",
    "STRIPE_ENABLED",
    "CT_BILLING_ENABLED",
    "CT_STRIPE_ENABLED",
    "STRIPE_SECRET_KEY",
    "STRIPE_WEBHOOK_SECRET",
    "STRIPE_PRICE_ID_PREMIUM_MONTHLY",
    "STRIPE_PRICE_ID_PREMIUM_ANNUAL",
    "STRIPE_PRICE_ID_PRO_MONTHLY",
    "STRIPE_PRICE_ID_PRO_ANNUAL",
    "SMTP_HOST",
    "CT_ALLOW_ADMIN_QUERY_TOKEN",
    "CT_ALLOW_DEBUG_QUERY_TOKEN",
    "CT_ENABLE_ADMIN_TOKEN_QUERY_AUTH",
    "CT_ENABLE_DEBUG_TOKEN_QUERY_AUTH",
    "AUTO_REPAIR_EVENTS_ON_STARTUP",
    "AUTOHEAL_ON_STARTUP",
    "AUTO_BACKFILL_EVENTS_ON_STARTUP",
    "STARTUP_OPTIONAL_TASK_TIMEOUT_SECONDS",
    "STARTUP_EVENT_REPAIR_LIMIT",
    "STARTUP_EVENT_BACKFILL_LIMIT",
)


def _clear_security_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in _ENV_KEYS:
        monkeypatch.delenv(key, raising=False)


def _safe_production_env(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_security_env(monkeypatch)
    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.setenv("APP_SESSION_SECRET", "x" * 48)
    monkeypatch.setenv("APP_SESSION_COOKIE_SAMESITE", "none")
    monkeypatch.setenv("SMTP_HOST", "smtp.example.test")


def test_production_missing_app_session_secret_fails_validation(monkeypatch):
    _safe_production_env(monkeypatch)
    monkeypatch.delenv("APP_SESSION_SECRET", raising=False)

    with pytest.raises(StartupSecurityError, match="APP_SESSION_SECRET is required"):
        validate_startup_security_config()


def test_production_weak_app_session_secret_fails_validation(monkeypatch):
    _safe_production_env(monkeypatch)
    monkeypatch.setenv("APP_SESSION_SECRET", "short")

    with pytest.raises(StartupSecurityError, match="at least 32 characters"):
        validate_startup_security_config()


def test_production_app_session_secret_equal_admin_token_fails_validation(monkeypatch):
    _safe_production_env(monkeypatch)
    shared = "shared-secret-value-that-is-long-enough"
    monkeypatch.setenv("APP_SESSION_SECRET", shared)
    monkeypatch.setenv("ADMIN_TOKEN", shared)

    with pytest.raises(StartupSecurityError, match="must not reuse ADMIN_TOKEN"):
        validate_startup_security_config()


def test_cross_site_production_requires_samesite_none(monkeypatch):
    _safe_production_env(monkeypatch)
    monkeypatch.setenv("APP_SESSION_COOKIE_SAMESITE", "lax")
    monkeypatch.setenv("FRONTEND_ORIGINS", "https://app.walnutmarkets.com")

    with pytest.raises(StartupSecurityError, match="APP_SESSION_COOKIE_SAMESITE=none"):
        validate_startup_security_config()


def test_bearer_session_auth_enabled_in_production_fails_validation(monkeypatch):
    _safe_production_env(monkeypatch)
    monkeypatch.setenv("APP_ALLOW_BEARER_SESSION_AUTH", "1")

    with pytest.raises(StartupSecurityError, match="APP_ALLOW_BEARER_SESSION_AUTH"):
        validate_startup_security_config()


def test_wildcard_cors_in_production_fails_validation(monkeypatch):
    _safe_production_env(monkeypatch)
    monkeypatch.setenv("CORS_ALLOW_ORIGINS", "*,https://congress-tracker-two.vercel.app")

    with pytest.raises(StartupSecurityError, match="wildcard origins"):
        validate_startup_security_config()


def test_entitlement_override_header_enabled_in_production_fails_validation(monkeypatch):
    _safe_production_env(monkeypatch)
    monkeypatch.setenv("CT_ALLOW_ENTITLEMENT_HEADER", "1")

    with pytest.raises(StartupSecurityError, match="CT_ALLOW_ENTITLEMENT_HEADER"):
        validate_startup_security_config()


def test_reset_token_return_enabled_in_production_fails_validation(monkeypatch):
    _safe_production_env(monkeypatch)
    monkeypatch.setenv("CT_ALLOW_INSECURE_RESET_LINK_RESPONSE", "true")

    with pytest.raises(StartupSecurityError, match="Reset-token responses"):
        validate_startup_security_config()


def test_password_reset_email_missing_warns_without_secret_values(monkeypatch, caplog):
    _safe_production_env(monkeypatch)
    monkeypatch.delenv("SMTP_HOST", raising=False)

    with caplog.at_level(logging.WARNING, logger="app.security.startup_checks"):
        validate_startup_security_config()

    messages = [record.getMessage() for record in caplog.records]
    assert any("password_reset_email_delivery_unavailable" in message for message in messages)
    assert all("APP_SESSION_SECRET" not in message for message in messages)
    assert all("x" * 48 not in message for message in messages)


def test_stripe_disabled_missing_stripe_secrets_does_not_fail_startup(monkeypatch):
    _safe_production_env(monkeypatch)
    monkeypatch.delenv("STRIPE_SECRET_KEY", raising=False)
    monkeypatch.delenv("STRIPE_WEBHOOK_SECRET", raising=False)

    validate_startup_security_config()


def test_stripe_disabled_webhook_fails_closed(monkeypatch):
    _safe_production_env(monkeypatch)
    request = Request({"type": "http", "method": "POST", "path": "/api/billing/stripe/webhook", "headers": []})

    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(stripe_webhook(request, None))

    assert exc_info.value.status_code == 503
    assert exc_info.value.detail == "Stripe billing is disabled."


def test_stripe_billing_auto_enabled_by_canonical_env(monkeypatch, caplog):
    _safe_production_env(monkeypatch)
    monkeypatch.setenv("STRIPE_SECRET_KEY", "sk_live_redacted")
    monkeypatch.setenv("STRIPE_WEBHOOK_SECRET", "whsec_redacted")
    monkeypatch.setenv("STRIPE_PRICE_ID_PREMIUM_MONTHLY", "price_premium_monthly")
    monkeypatch.setenv("STRIPE_PRICE_ID_PREMIUM_ANNUAL", "price_premium_annual")
    monkeypatch.setenv("STRIPE_PRICE_ID_PRO_MONTHLY", "price_pro_monthly")
    monkeypatch.setenv("STRIPE_PRICE_ID_PRO_ANNUAL", "price_pro_annual")

    with caplog.at_level(logging.INFO, logger="app.security.startup_checks"):
        readiness = log_stripe_billing_readiness(context="test")

    assert billing_enabled() is True
    assert readiness["billing_enabled"] is True
    assert readiness["missing_env_vars"] == []
    assert any("billing_enabled=True" in record.getMessage() for record in caplog.records)
    assert all("sk_live_redacted" not in record.getMessage() for record in caplog.records)
    assert all("whsec_redacted" not in record.getMessage() for record in caplog.records)


def test_webhook_ready_with_secret_pair_without_price_or_frontend_config(monkeypatch):
    _safe_production_env(monkeypatch)
    monkeypatch.setenv("STRIPE_SECRET_KEY", "sk_live_redacted")
    monkeypatch.setenv("STRIPE_WEBHOOK_SECRET", "whsec_redacted")

    assert stripe_webhook_enabled() is True
    assert billing_enabled() is False


def test_webhook_gate_allows_secret_pair_before_signature_validation(monkeypatch):
    _safe_production_env(monkeypatch)
    monkeypatch.setenv("STRIPE_SECRET_KEY", "sk_live_redacted")
    monkeypatch.setenv("STRIPE_WEBHOOK_SECRET", "whsec_redacted")

    async def receive():
        return {"type": "http.request", "body": b"{}", "more_body": False}

    request = Request(
        {"type": "http", "method": "POST", "path": "/api/billing/stripe/webhook", "headers": []},
        receive,
    )

    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(stripe_webhook(request, None))

    assert exc_info.value.status_code == 400
    assert exc_info.value.detail == "Missing Stripe signature."


def test_stripe_enabled_missing_webhook_secret_fails_validation(monkeypatch):
    _safe_production_env(monkeypatch)
    monkeypatch.setenv("BILLING_ENABLED", "1")
    monkeypatch.setenv("STRIPE_SECRET_KEY", "sk_live_redacted")
    monkeypatch.delenv("STRIPE_WEBHOOK_SECRET", raising=False)

    with pytest.raises(StartupSecurityError, match="STRIPE_WEBHOOK_SECRET"):
        validate_startup_security_config()


def test_stripe_enabled_missing_canonical_price_fails_validation(monkeypatch):
    _safe_production_env(monkeypatch)
    monkeypatch.setenv("BILLING_ENABLED", "1")
    monkeypatch.setenv("STRIPE_SECRET_KEY", "sk_live_redacted")
    monkeypatch.setenv("STRIPE_WEBHOOK_SECRET", "whsec_redacted")
    monkeypatch.setenv("STRIPE_PRICE_ID_PREMIUM_MONTHLY", "price_premium_monthly")

    with pytest.raises(StartupSecurityError, match="STRIPE_PRICE_ID_PREMIUM_ANNUAL"):
        validate_startup_security_config()


def test_local_test_mode_remains_usable(monkeypatch):
    _clear_security_env(monkeypatch)
    monkeypatch.setenv("APP_ENV", "test")
    monkeypatch.setenv("CORS_ALLOW_ORIGINS", "*")
    monkeypatch.setenv("CT_ALLOW_ENTITLEMENT_HEADER", "1")
    monkeypatch.setenv("CT_ALLOW_INSECURE_RESET_LINK_RESPONSE", "1")
    monkeypatch.setenv("BILLING_ENABLED", "1")

    assert is_production() is False
    validate_startup_security_config()


def test_fly_app_name_implies_production_without_local_override(monkeypatch):
    _clear_security_env(monkeypatch)
    monkeypatch.setenv("FLY_APP_NAME", "congress-tracker-api")

    assert is_production() is True


def test_fastapi_startup_schedules_optional_maintenance_without_running_inline(monkeypatch, caplog):
    import app.main as main_module

    _clear_security_env(monkeypatch)
    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.setenv("AUTO_REPAIR_EVENTS_ON_STARTUP", "1")
    monkeypatch.setenv("AUTOHEAL_ON_STARTUP", "1")
    monkeypatch.setenv("AUTO_BACKFILL_EVENTS_ON_STARTUP", "1")

    calls: list[str] = []
    scheduled: list[str] = []

    monkeypatch.setattr(main_module, "validate_startup_security_config", lambda: calls.append("security"))
    monkeypatch.setattr(main_module, "_create_all_with_startup_limits", lambda: calls.append("create_all"))
    monkeypatch.setattr(main_module, "_seed_plan_and_provider_config", lambda: calls.append("seed_plan_provider"))
    monkeypatch.setattr(main_module, "_seed_email_templates", lambda: calls.append("seed_email_templates"))
    monkeypatch.setattr(main_module, "_log_startup_maintenance_config", lambda: calls.append("maintenance_config"))

    for attr in (
        "ensure_email_notification_schema",
        "ensure_price_cache_volume_columns",
        "ensure_fundamentals_cache_schema",
        "ensure_ticker_meta_identity_schema",
        "ensure_search_and_insights_schema",
        "ensure_ticker_content_cache_schema",
        "ensure_ticker_financials_cache_schema",
        "ensure_user_account_billing_schema",
        "ensure_page_analytics_schema",
        "ensure_provider_usage_schema",
        "ensure_provider_control_schema",
        "ensure_data_enrichment_jobs_schema",
        "ensure_ai_marketing_schema",
        "ensure_event_columns",
        "ensure_monitoring_alert_columns",
        "ensure_house_annual_disclosure_schema",
        "ensure_trade_outcomes_amount_bigint",
        "ensure_government_contracts_schema",
    ):
        monkeypatch.setattr(main_module, attr, lambda *args, _attr=attr, **kwargs: calls.append(_attr))

    monkeypatch.setattr(main_module, "_schedule_startup_maintenance", lambda name, fn: scheduled.append(name))
    monkeypatch.setattr(
        main_module,
        "_run_startup_event_repair",
        lambda: pytest.fail("event repair should be scheduled, not run inline"),
    )
    monkeypatch.setattr(
        main_module,
        "_run_startup_autoheal",
        lambda: pytest.fail("autoheal should be scheduled, not run inline"),
    )
    monkeypatch.setattr(
        main_module,
        "_run_startup_auto_backfill",
        lambda: pytest.fail("auto-backfill should be scheduled, not run inline"),
    )

    with caplog.at_level(logging.INFO, logger="app.main"):
        main_module._startup_create_tables()

    assert calls[0:2] == ["security", "create_all"]
    assert "seed_plan_provider" in calls
    assert "seed_email_templates" in calls
    assert scheduled == ["startup_event_repair", "startup_autoheal", "startup_auto_backfill"]
    messages = [record.getMessage() for record in caplog.records]
    assert any("startup_step_begin name=startup_security_config" in message for message in messages)
    assert any("startup_step_complete name=database_base_metadata_create_all" in message for message in messages)


def test_production_startup_maintenance_defaults_disabled(monkeypatch):
    import app.main as main_module

    _clear_security_env(monkeypatch)
    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.delenv("AUTO_REPAIR_EVENTS_ON_STARTUP", raising=False)
    monkeypatch.delenv("AUTOHEAL_ON_STARTUP", raising=False)
    monkeypatch.delenv("AUTO_BACKFILL_EVENTS_ON_STARTUP", raising=False)

    assert main_module._startup_maintenance_enabled("AUTO_REPAIR_EVENTS_ON_STARTUP") is False
    assert main_module._startup_maintenance_enabled("AUTOHEAL_ON_STARTUP") is False
    assert main_module._startup_maintenance_enabled("AUTO_BACKFILL_EVENTS_ON_STARTUP") is False


def test_startup_run_module_returns_timeout_result(monkeypatch):
    import app.main as main_module

    def timeout_run(*_args, **_kwargs):
        raise subprocess.TimeoutExpired(cmd=["python", "-m", "app.fake"], timeout=1, output="out", stderr="err")

    monkeypatch.setattr(main_module.subprocess, "run", timeout_run)

    result = main_module._run_module("app.fake", timeout_seconds=1)

    assert result["module"] == "app.fake"
    assert result["returncode"] == -1
    assert result["timed_out"] is True
    assert result["timeout_seconds"] == 1
