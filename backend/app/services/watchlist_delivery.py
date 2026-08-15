"""Shared watchlist alert-category and delivery-mode rules.

The first generation of watchlist email preferences used two global switches and
a list of trigger chips.  Keep reading those values for existing subscribers,
but make the matrix the single source of truth for new saves and all delivery
decisions.
"""
from __future__ import annotations

import json
from typing import Any, Literal

from app.models import NotificationSubscription

DeliveryMode = Literal["off", "daily", "intraday", "both"]
DeliveryKind = Literal["daily", "intraday"]

WATCHLIST_ALERT_CATEGORIES = (
    "bullish_bearish_monitor",
    "congress",
    "conviction_threshold",
    "cross_source",
    "fundamentals",
    "government_contracts",
    "insiders",
    "institutional_activity",
    "large_trade_contract",
    "news",
    "press_releases",
)

VALID_DELIVERY_MODES = {"off", "daily", "intraday", "both"}

CATEGORY_TRIGGER = {
    "bullish_bearish_monitor": "monitor_state",
    "congress": "congress_activity",
    "conviction_threshold": "smart_score_threshold",
    "cross_source": "cross_source_confirmation",
    "fundamentals": "fundamentals",
    "government_contracts": "government_contract",
    "insiders": "insider_activity",
    "institutional_activity": "institutional_activity",
    "large_trade_contract": "large_trade_threshold",
    "news": "news",
    "press_releases": "press_releases",
}


def subscription_payload(subscription: NotificationSubscription | None) -> dict[str, Any]:
    if subscription is None:
        return {}
    try:
        value = json.loads(subscription.source_payload_json or "{}")
    except Exception:
        return {}
    return value if isinstance(value, dict) else {}


def normalize_delivery_modes(value: Any) -> dict[str, DeliveryMode]:
    raw = value if isinstance(value, dict) else {}
    return {
        category: raw.get(category) if raw.get(category) in VALID_DELIVERY_MODES else "off"
        for category in WATCHLIST_ALERT_CATEGORIES
    }


def delivery_modes_from_payload(payload: dict[str, Any]) -> dict[str, DeliveryMode] | None:
    raw = payload.get("alert_delivery_modes")
    if not isinstance(raw, dict):
        return None
    return normalize_delivery_modes(raw)


def delivery_mode(subscription: NotificationSubscription | None, category: str) -> DeliveryMode:
    if category not in WATCHLIST_ALERT_CATEGORIES or subscription is None or not subscription.active:
        return "off"
    payload = subscription_payload(subscription)
    matrix = delivery_modes_from_payload(payload)
    if matrix is not None:
        return matrix[category]

    # Legacy subscriptions continue to behave exactly as their prior two global
    # toggles and selected chips specified until the user next edits the matrix.
    try:
        selected = {str(item).strip().lower() for item in json.loads(subscription.alert_triggers_json or "[]")}
    except Exception:
        selected = set()
    trigger = CATEGORY_TRIGGER[category]
    if trigger not in selected:
        return "off"
    daily = bool(payload.get("daily_digest_enabled", subscription.active))
    intraday = bool(payload.get("intraday_alerts_enabled", subscription.active))
    if daily and intraday:
        return "both"
    if daily:
        return "daily"
    if intraday:
        return "intraday"
    return "off"


def is_delivery_enabled(subscription: NotificationSubscription | None, category: str, kind: DeliveryKind) -> bool:
    mode = delivery_mode(subscription, category)
    return mode == "both" or mode == kind


def categories_for_event(event_type: str | None, payload: dict[str, Any] | None = None) -> set[str]:
    event_key = (event_type or "").strip().lower()
    values = payload or {}
    categories: set[str] = set()
    if event_key in {"news", "news_article", "market_news"}:
        return {"news"}
    if event_key in {"press_release", "press_releases", "issuer_press_release"}:
        return {"press_releases"}
    if event_key.startswith("congress_trade"):
        categories.add("congress")
    if event_key.startswith("insider_trade"):
        categories.add("insiders")
    if event_key.startswith("government_contract") or event_key in {"contract_award", "government_exposure"}:
        categories.add("government_contracts")
    if event_key.startswith("institutional") or event_key in {"institutional_activity", "institutional_activity_change"}:
        categories.add("institutional_activity")
    if event_key in {"fundamental_change", "fundamentals_change", "fundamentals_flip"}:
        categories.add("fundamentals")
    if event_key in {
        "entered_bullish_monitor", "entered_bearish_monitor", "exited_bullish_monitor", "exited_bearish_monitor",
        "direction_flipped", "price_volume_change", "price_volume_signal", "unusual_price_volume", "volume_surge",
        "technical_breakout", "technical_breakdown", "price_volume_flip",
    }:
        categories.add("bullish_bearish_monitor")
    if event_key in {"smart_score_threshold", "score_change"} or _numeric(values.get("smart_score") or values.get("signal_score") or values.get("confirmation_score")) is not None:
        categories.add("conviction_threshold")
    if values.get("cross_source") or values.get("cross_source_confirmation") or values.get("cross_source_confirmed") or values.get("source_count"):
        categories.add("cross_source")
    if event_key in {"large_trade", "large_trade_contract"} or values.get("large_trade"):
        categories.add("large_trade_contract")
    return categories


def category_for_trigger(trigger: str | None, event_type: str | None = None, payload: dict[str, Any] | None = None) -> str | None:
    key = (trigger or "").strip().lower()
    reverse = {value: category for category, value in CATEGORY_TRIGGER.items()}
    if key in reverse:
        return reverse[key]
    categories = categories_for_event(event_type, payload)
    return next(iter(categories), None) if len(categories) == 1 else None


def _numeric(value: Any) -> int | None:
    try:
        return int(float(value)) if value is not None and not isinstance(value, bool) else None
    except (TypeError, ValueError):
        return None
