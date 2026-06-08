from __future__ import annotations

import logging
import os
from typing import Any

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
STRIPE_BILLING_REQUIRED_ENV_VARS = (
    *STRIPE_WEBHOOK_REQUIRED_ENV_VARS,
    *STRIPE_CANONICAL_PRICE_ENV_VARS,
)


def _env(name: str) -> str:
    return os.getenv(name, "").strip()


def _env_price_id(*names: str) -> str | None:
    for name in names:
        value = _env(name)
        if value.startswith("price_"):
            return value
    return None


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
    if normalized_tier == "pro":
        if interval == "annual":
            return _env_price_id("STRIPE_PRICE_ID_PRO_ANNUAL", "STRIPE_PRO_PRICE_ID_ANNUAL", "STRIPE_PRO_PRICE_ID")
        if interval == "monthly":
            return _env_price_id("STRIPE_PRICE_ID_PRO_MONTHLY", "STRIPE_PRO_PRICE_ID_MONTHLY", "STRIPE_PRO_PRICE_ID")
        return _env_price_id("STRIPE_PRICE_ID_PRO_MONTHLY", "STRIPE_PRO_PRICE_ID", "STRIPE_PRO_PRICE_ID_MONTHLY")
    if interval == "annual":
        return _env_price_id("STRIPE_PRICE_ID_PREMIUM_ANNUAL", "STRIPE_PRICE_ID_ANNUAL", "STRIPE_PRICE_ID")
    if interval == "monthly":
        return _env_price_id("STRIPE_PRICE_ID_PREMIUM_MONTHLY", "STRIPE_PRICE_ID_MONTHLY", "STRIPE_PRICE_ID")
    return _env_price_id("STRIPE_PRICE_ID_PREMIUM_MONTHLY", "STRIPE_PRICE_ID", "STRIPE_PRICE_ID_MONTHLY")


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


def _dedupe_missing_env_vars(values: list[str]) -> list[str]:
    missing: list[str] = []
    for value in values:
        if value and value not in missing:
            missing.append(value)
    return missing


def billing_readiness(
    *,
    checkout_tier: str | None = None,
    checkout_interval: str | None = None,
) -> dict[str, Any]:
    secret = _env("STRIPE_SECRET_KEY")
    webhook = _env("STRIPE_WEBHOOK_SECRET")
    prices = _billing_price_readiness()
    price_ids = {key: value["price_id"] for key, value in prices.items()}
    missing_price_env_vars = [value["env_name"] for value in prices.values() if not value["configured"]]
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
        },
        "stripe_api": {
            "ready": bool(secret),
            "status": "ready" if secret else "not_ready",
            "missing_env_vars": [] if secret else ["STRIPE_SECRET_KEY"],
        },
        "price_ids": price_ids,
        "prices": prices,
        "missing_env_vars": full_missing,
        "missing_price_env_vars": missing_price_env_vars,
        "missing_price_ids": [key for key, value in prices.items() if not value["configured"]],
        "billing_enabled": not full_missing,
        "required_env_vars": list(STRIPE_BILLING_REQUIRED_ENV_VARS),
    }


def missing_stripe_billing_env_vars() -> list[str]:
    return list(billing_readiness()["missing_env_vars"])


def missing_stripe_webhook_env_vars() -> list[str]:
    return list(billing_readiness()["webhooks"]["missing_env_vars"])


def log_billing_readiness(logger: logging.Logger, *, context: str, readiness: dict[str, Any] | None = None) -> dict[str, Any]:
    result = readiness or billing_readiness()
    logger.info(
        "stripe_billing_readiness context=%s billing_enabled=%s missing_env_vars=%s",
        context,
        result["billing_enabled"],
        result["missing_env_vars"],
    )
    return result
