from __future__ import annotations

import logging
import os
from typing import Any
from urllib.parse import urlparse

STRIPE_BILLING_ENABLE_FLAGS = (
    "BILLING_ENABLED",
    "STRIPE_ENABLED",
    "CT_BILLING_ENABLED",
    "CT_STRIPE_ENABLED",
)
STRIPE_WEBHOOK_REQUIRED_ENV_VARS = (
    "STRIPE_SECRET_KEY",
    "STRIPE_WEBHOOK_SECRET",
)
STRIPE_CANONICAL_PRICE_ENV_VARS = (
    "STRIPE_PRICE_ID_PREMIUM_MONTHLY",
    "STRIPE_PRICE_ID_PREMIUM_ANNUAL",
    "STRIPE_PRICE_ID_PRO_MONTHLY",
    "STRIPE_PRICE_ID_PRO_ANNUAL",
)
STRIPE_ADMIN_FREE_PRICE_ENV_VARS = (
    "STRIPE_PREMIUM_ADMIN_FREE_PRICE_ID",
    "STRIPE_PRO_ADMIN_FREE_PRICE_ID",
)
STRIPE_LEGACY_PRICE_ENV_VARS = (
    "STRIPE_PRICE_ID",
    "STRIPE_PRICE_ID_MONTHLY",
    "STRIPE_PRICE_ID_ANNUAL",
    "STRIPE_PRO_PRICE_ID",
    "STRIPE_PRO_PRICE_ID_MONTHLY",
    "STRIPE_PRO_PRICE_ID_ANNUAL",
)
STRIPE_BILLING_REQUIRED_ENV_VARS = (
    *STRIPE_WEBHOOK_REQUIRED_ENV_VARS,
    *STRIPE_CANONICAL_PRICE_ENV_VARS,
)
STRIPE_RECOMMENDED_WEBHOOK_EVENTS = (
    "checkout.session.completed",
    "customer.deleted",
    "customer.subscription.created",
    "customer.subscription.updated",
    "customer.subscription.deleted",
    "customer.subscription.paused",
    "invoice.paid",
    "invoice.payment_succeeded",
    "invoice.payment.paid",
    "invoice.payment_failed",
    "invoice.payment_action_required",
    "invoice.voided",
    "invoice.marked_uncollectible",
    "charge.refunded",
    "refund.created",
    "refund.updated",
)
AUTH_APP_FRONTEND_HOST = "app.walnutmarkets.com"
AUTH_APP_FRONTEND_DEFAULT_URL = f"https://{AUTH_APP_FRONTEND_HOST}"
UNSAFE_LIVE_CHECKOUT_HOSTS = {
    "localhost",
    "127.0.0.1",
    "::1",
    "walnut-intel.com",
    "www.walnut-intel.com",
    "app.walnut-intel.com",
}
_NONPRODUCTION_ENVS = {"local", "dev", "development", "test", "testing", "ci"}


def _env(name: str) -> str:
    return os.getenv(name, "").strip()


def _env_url(name: str) -> str | None:
    value = _env(name).rstrip("/")
    return value or None


def _url_host(value: str | None) -> str:
    if not value:
        return ""
    return (urlparse(value).hostname or "").strip().lower()


def _env_price_id(*names: str) -> str | None:
    for name in names:
        value = _env(name)
        if value.startswith("price_"):
            return value
    return None


def stripe_secret_key_mode() -> str:
    secret = _env("STRIPE_SECRET_KEY")
    if not secret:
        return "missing"
    if secret.startswith("sk_live"):
        return "live"
    if secret.startswith("sk_test"):
        return "test"
    return "unknown"


def stripe_publishable_key_mode() -> str:
    key = _env("NEXT_PUBLIC_STRIPE_PUBLISHABLE_KEY")
    if not key:
        return "missing"
    if key.startswith("pk_live"):
        return "live"
    if key.startswith("pk_test"):
        return "test"
    return "unknown"


def _live_secret_configured() -> bool:
    return stripe_secret_key_mode() == "live"


def _configured_legacy_price_env_vars() -> list[str]:
    return [name for name in STRIPE_LEGACY_PRICE_ENV_VARS if _env(name)]


def _normalize_tier(value: str | None) -> str:
    return "pro" if (value or "").strip().lower() == "pro" else "premium"


def _price_config_key(billing_interval: str | None = None, tier: str | None = None) -> str:
    interval = "annual" if (billing_interval or "").strip().lower() == "annual" else "monthly"
    return f"{_normalize_tier(tier)}_{interval}"


def stripe_price_env_name(billing_interval: str | None = None, tier: str | None = None) -> str:
    interval = "annual" if (billing_interval or "").strip().lower() == "annual" else "monthly"
    return f"STRIPE_PRICE_ID_{_normalize_tier(tier).upper()}_{interval.upper()}"


def stripe_price_label(billing_interval: str | None = None, tier: str | None = None) -> str:
    interval = "annual" if (billing_interval or "").strip().lower() == "annual" else "monthly"
    return f"{_normalize_tier(tier).capitalize()} {interval}"


def stripe_price_id(billing_interval: str | None = None, tier: str | None = None) -> str | None:
    interval = (billing_interval or "").strip().lower()
    normalized_tier = _normalize_tier(tier)
    live_mode = _live_secret_configured()
    if normalized_tier == "pro":
        if interval == "annual":
            return _env_price_id("STRIPE_PRICE_ID_PRO_ANNUAL") if live_mode else _env_price_id("STRIPE_PRICE_ID_PRO_ANNUAL", "STRIPE_PRO_PRICE_ID_ANNUAL", "STRIPE_PRO_PRICE_ID")
        if interval == "monthly":
            return _env_price_id("STRIPE_PRICE_ID_PRO_MONTHLY") if live_mode else _env_price_id("STRIPE_PRICE_ID_PRO_MONTHLY", "STRIPE_PRO_PRICE_ID_MONTHLY", "STRIPE_PRO_PRICE_ID")
        return _env_price_id("STRIPE_PRICE_ID_PRO_MONTHLY") if live_mode else _env_price_id("STRIPE_PRICE_ID_PRO_MONTHLY", "STRIPE_PRO_PRICE_ID", "STRIPE_PRO_PRICE_ID_MONTHLY")
    if interval == "annual":
        return _env_price_id("STRIPE_PRICE_ID_PREMIUM_ANNUAL") if live_mode else _env_price_id("STRIPE_PRICE_ID_PREMIUM_ANNUAL", "STRIPE_PRICE_ID_ANNUAL", "STRIPE_PRICE_ID")
    if interval == "monthly":
        return _env_price_id("STRIPE_PRICE_ID_PREMIUM_MONTHLY") if live_mode else _env_price_id("STRIPE_PRICE_ID_PREMIUM_MONTHLY", "STRIPE_PRICE_ID_MONTHLY", "STRIPE_PRICE_ID")
    return _env_price_id("STRIPE_PRICE_ID_PREMIUM_MONTHLY") if live_mode else _env_price_id("STRIPE_PRICE_ID_PREMIUM_MONTHLY", "STRIPE_PRICE_ID", "STRIPE_PRICE_ID_MONTHLY")


def _billing_price_readiness() -> dict[str, dict[str, Any]]:
    prices: dict[str, dict[str, Any]] = {}
    for tier in ("premium", "pro"):
        for interval in ("monthly", "annual"):
            key = _price_config_key(interval, tier)
            price_id = stripe_price_id(interval, tier)
            prices[key] = {
                "label": stripe_price_label(interval, tier),
                "tier": tier,
                "billing_interval": interval,
                "env_name": stripe_price_env_name(interval, tier),
                "price_id": price_id or "missing",
                "configured": bool(price_id),
            }
    return prices


def _admin_free_price_readiness() -> dict[str, dict[str, Any]]:
    prices: dict[str, dict[str, Any]] = {}
    for tier, env_name in (
        ("premium", "STRIPE_PREMIUM_ADMIN_FREE_PRICE_ID"),
        ("pro", "STRIPE_PRO_ADMIN_FREE_PRICE_ID"),
    ):
        prices[tier] = {
            "label": f"{tier.capitalize()} admin free grant",
            "tier": tier,
            "env_name": env_name,
            "configured": bool(_env_price_id(env_name)),
        }
    return prices


def _dedupe_missing_env_vars(values: list[str]) -> list[str]:
    missing: list[str] = []
    for value in values:
        if value and value not in missing:
            missing.append(value)
    return missing


def _app_environment() -> str:
    return (_env("APP_ENV") or _env("ENV") or _env("NODE_ENV")).lower()


def _frontend_base_url() -> str:
    return os.getenv("FRONTEND_BASE_URL", "http://localhost:3000").strip().rstrip("/") or "http://localhost:3000"


def _authenticated_app_frontend_base_url() -> str:
    for name in ("FRONTEND_APP_URL", "APP_BASE_URL", "NEXT_PUBLIC_APP_BASE_URL", "NEXT_PUBLIC_APP_URL", "FRONTEND_BASE_URL"):
        value = _env_url(name)
        if _url_host(value) == AUTH_APP_FRONTEND_HOST:
            return value
    if _app_environment() in _NONPRODUCTION_ENVS:
        return _frontend_base_url()
    return AUTH_APP_FRONTEND_DEFAULT_URL


def checkout_success_url() -> str:
    return f"{_authenticated_app_frontend_base_url()}/account/billing?checkout=success"


def checkout_cancel_url() -> str:
    return f"{_authenticated_app_frontend_base_url()}/pricing?checkout=cancelled"


def customer_portal_return_url() -> str:
    configured = _env_url("STRIPE_CUSTOMER_PORTAL_RETURN_URL")
    if _url_host(configured) == AUTH_APP_FRONTEND_HOST:
        return configured
    return f"{_authenticated_app_frontend_base_url()}/account/billing?portal_return=1"


def _unsafe_live_checkout_url(url: str) -> dict[str, str] | None:
    host = _url_host(url)
    if not host:
        return {"host": "", "reason": "missing_host"}
    if host in {"localhost", "127.0.0.1", "::1"}:
        return {"host": host, "reason": "localhost"}
    if host == "walnut-intel.com" or host.endswith(".walnut-intel.com"):
        return {"host": host, "reason": "walnut-intel.com"}
    if host in UNSAFE_LIVE_CHECKOUT_HOSTS:
        return {"host": host, "reason": "unsafe_host"}
    return None


def _checkout_url_readiness() -> dict[str, Any]:
    urls = {
        "success_url": checkout_success_url(),
        "cancel_url": checkout_cancel_url(),
        "customer_portal_return_url": customer_portal_return_url(),
    }
    unsafe = []
    for name in ("success_url", "cancel_url"):
        issue = _unsafe_live_checkout_url(urls[name])
        if issue:
            unsafe.append({"name": name, **issue})
    return {
        **urls,
        "unsafe_live_checkout_urls": unsafe,
        "ready_for_live_mode": not unsafe,
    }


def _stripe_live_mode_errors(full_missing: list[str], url_readiness: dict[str, Any]) -> list[dict[str, Any]]:
    if not _live_secret_configured():
        return []
    errors: list[dict[str, Any]] = []
    missing_live = [name for name in STRIPE_BILLING_REQUIRED_ENV_VARS if name in full_missing]
    if missing_live:
        errors.append({"code": "live_missing_required_env_vars", "missing_env_vars": missing_live})
    legacy_price_env_vars = _configured_legacy_price_env_vars()
    if legacy_price_env_vars:
        errors.append({"code": "live_legacy_price_env_vars_present", "env_vars": legacy_price_env_vars})
    if stripe_publishable_key_mode() == "test":
        errors.append({"code": "live_backend_test_publishable_key", "env_var": "NEXT_PUBLIC_STRIPE_PUBLISHABLE_KEY"})
    unsafe_urls = list(url_readiness.get("unsafe_live_checkout_urls") or [])
    if unsafe_urls:
        errors.append({"code": "live_unsafe_checkout_return_urls", "urls": unsafe_urls})
    return errors


def billing_readiness(
    *,
    checkout_tier: str | None = None,
    checkout_interval: str | None = None,
) -> dict[str, Any]:
    secret = _env("STRIPE_SECRET_KEY")
    webhook = _env("STRIPE_WEBHOOK_SECRET")
    prices = _billing_price_readiness()
    admin_free_prices = _admin_free_price_readiness()
    price_ids = {key: value["price_id"] for key, value in prices.items()}
    missing_price_env_vars = [value["env_name"] for value in prices.values() if not value["configured"]]
    missing_admin_free_price_env_vars = [
        value["env_name"] for value in admin_free_prices.values() if not value["configured"]
    ]
    full_missing = _dedupe_missing_env_vars(
        ([] if secret else ["STRIPE_SECRET_KEY"])
        + ([] if webhook else ["STRIPE_WEBHOOK_SECRET"])
        + missing_price_env_vars
    )
    webhook_missing = _dedupe_missing_env_vars(
        ([] if secret else ["STRIPE_SECRET_KEY"]) + ([] if webhook else ["STRIPE_WEBHOOK_SECRET"])
    )

    selected_checkout_key: str | None = None
    if checkout_tier is not None or checkout_interval is not None:
        selected_checkout_key = _price_config_key(checkout_interval, checkout_tier)
        checkout_price_keys = [selected_checkout_key]
    else:
        checkout_price_keys = list(prices.keys())
    checkout_missing = [] if secret else ["STRIPE_SECRET_KEY"]
    for key in checkout_price_keys:
        price = prices[key]
        if not price["configured"]:
            checkout_missing.append(price["env_name"])
    checkout_missing = _dedupe_missing_env_vars(checkout_missing)

    selected_price = prices.get(selected_checkout_key) if selected_checkout_key else None
    url_readiness = _checkout_url_readiness()
    key_mode = stripe_secret_key_mode()
    publishable_key_mode = stripe_publishable_key_mode()
    live_mode_errors = _stripe_live_mode_errors(full_missing, url_readiness)
    return {
        "overall": {
            "ready": not full_missing,
            "status": "ready" if not full_missing else "not_ready",
            "missing_env_vars": full_missing,
        },
        "checkout": {
            "ready": not checkout_missing,
            "status": "ready" if not checkout_missing else "not_ready",
            "missing_env_vars": checkout_missing,
            "selected_plan": selected_price["tier"] if selected_price else None,
            "selected_interval": selected_price["billing_interval"] if selected_price else None,
            "selected_price_id": selected_price["price_id"] if selected_price else None,
        },
        "webhooks": {
            "ready": not webhook_missing,
            "status": "ready" if not webhook_missing else "not_ready",
            "missing_env_vars": webhook_missing,
            "recommended_events": list(STRIPE_RECOMMENDED_WEBHOOK_EVENTS),
        },
        "stripe_api": {
            "ready": bool(secret),
            "status": "ready" if secret else "not_ready",
            "missing_env_vars": [] if secret else ["STRIPE_SECRET_KEY"],
        },
        "price_ids": price_ids,
        "prices": prices,
        "admin_free_grants": {
            "ready": not missing_admin_free_price_env_vars,
            "status": "ready" if not missing_admin_free_price_env_vars else "not_ready",
            "missing_env_vars": missing_admin_free_price_env_vars,
            "prices": admin_free_prices,
        },
        "urls": url_readiness,
        "missing_env_vars": full_missing,
        "missing_price_env_vars": missing_price_env_vars,
        "missing_admin_free_price_env_vars": missing_admin_free_price_env_vars,
        "missing_price_ids": [key for key, value in prices.items() if not value["configured"]],
        "billing_enabled": not full_missing,
        "required_env_vars": list(STRIPE_BILLING_REQUIRED_ENV_VARS),
        "admin_free_price_env_vars": list(STRIPE_ADMIN_FREE_PRICE_ENV_VARS),
        "secret_key_mode": key_mode,
        "publishable_key_mode": publishable_key_mode,
        "live_mode": key_mode == "live",
        "live_mode_ready": not live_mode_errors,
        "live_mode_errors": live_mode_errors,
        "legacy_price_env_vars_present": _configured_legacy_price_env_vars(),
    }


def missing_stripe_billing_env_vars() -> list[str]:
    return list(billing_readiness()["missing_env_vars"])


def missing_stripe_webhook_env_vars() -> list[str]:
    return list(billing_readiness()["webhooks"]["missing_env_vars"])


def log_billing_readiness(logger: logging.Logger, *, context: str, readiness: dict[str, Any] | None = None) -> dict[str, Any]:
    result = readiness or billing_readiness()
    logger.info(
        "stripe_billing_readiness context=%s billing_enabled=%s secret_key_mode=%s live_mode_ready=%s admin_free_grants_ready=%s missing_env_vars=%s",
        context,
        result["billing_enabled"],
        result.get("secret_key_mode"),
        result.get("live_mode_ready"),
        result.get("admin_free_grants", {}).get("ready"),
        result["missing_env_vars"],
    )
    if result.get("live_mode_errors"):
        logger.error(
            "stripe_live_readiness_error context=%s error_codes=%s",
            context,
            [error.get("code") for error in result.get("live_mode_errors", [])],
        )
    return result
