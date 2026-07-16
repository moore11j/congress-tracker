from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Literal

from fastapi import HTTPException, Request
from sqlalchemy.exc import OperationalError
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.auth import current_user, is_admin_user
from app.models import AppSetting, FeatureGate, PlanLimit, PlanPrice, SavedScreen, UserAccount, Watchlist
from app.services.billing_readiness import stripe_price_id

TierName = Literal["free", "premium", "pro", "admin"]
PlanTierName = Literal["free", "premium", "pro"]
BillingInterval = Literal["monthly", "annual"]
EffectivePlanSource = Literal["stripe_subscription", "admin_subscription", "manual_override", "free", "suspended", "admin"]
FeatureKey = Literal[
    "signals",
    "ticker_confirmation",
    "premium_feed_metrics",
    "leaderboards",
    "backtesting",
    "screener",
    "screener_intelligence",
    "screener_presets",
    "screener_saved_screens",
    "screener_monitoring",
    "screener_csv_export",
    "screener_results",
    "watchlists",
    "watchlist_tickers",
    "saved_views",
    "notification_digests",
    "monitoring_sources",
    "event_calendar",
    "inbox_alerts",
    "inbox_alert_retention",
    "government_contracts_feed",
    "government_contracts_filters",
    "insider_feed",
    "congress_feed",
    "options_flow_feed",
    "options_flow_filters",
    "institutional_feed",
    "institutional_filters",
    "macro_positioning",
    "market_pressure",
    "api_webhooks",
]

PLAN_TIERS: tuple[PlanTierName, ...] = ("free", "premium", "pro")
PLAN_RANKS: dict[TierName, int] = {"free": 0, "premium": 10, "pro": 20, "admin": 100}
HARD_MINIMUM_FEATURE_TIERS: dict[FeatureKey, TierName] = {
    "options_flow_feed": "pro",
    "options_flow_filters": "pro",
    "institutional_feed": "pro",
    "institutional_filters": "pro",
    "macro_positioning": "pro",
    "market_pressure": "pro",
}


@dataclass(frozen=True)
class TierEntitlements:
    tier: TierName
    rank: int
    limits: dict[FeatureKey, int]
    features: frozenset[FeatureKey]

    def limit(self, feature: FeatureKey) -> int:
        return self.limits[feature]

    def has_feature(self, feature: FeatureKey) -> bool:
        return feature in self.features


ENTITLEMENTS: dict[TierName, TierEntitlements] = {
    "free": TierEntitlements(
        tier="free",
        rank=0,
        limits={
            "signals": 0,
            "ticker_confirmation": 0,
            "premium_feed_metrics": 0,
            "leaderboards": 0,
            "backtesting": 0,
            "screener": 0,
            "screener_intelligence": 0,
            "screener_presets": 0,
            "screener_saved_screens": 1,
            "screener_monitoring": 0,
            "screener_csv_export": 0,
            "screener_results": 5,
            "watchlists": 1,
            "watchlist_tickers": 5,
            "saved_views": 1,
            "notification_digests": 0,
            "monitoring_sources": 3,
            "event_calendar": 0,
            "inbox_alerts": 1,
            "inbox_alert_retention": 14,
            "government_contracts_feed": 1,
            "government_contracts_filters": 0,
            "insider_feed": 1,
            "congress_feed": 1,
            "options_flow_feed": 0,
            "options_flow_filters": 0,
            "institutional_feed": 0,
            "institutional_filters": 0,
            "macro_positioning": 0,
            "market_pressure": 0,
            "api_webhooks": 0,
        },
        features=frozenset(
            {
                "screener",
                "screener_saved_screens",
                "screener_results",
                "watchlists",
                "watchlist_tickers",
                "saved_views",
                "monitoring_sources",
                "inbox_alerts",
                "government_contracts_feed",
                "insider_feed",
                "congress_feed",
            }
        ),
    ),
    "premium": TierEntitlements(
        tier="premium",
        rank=10,
        limits={
            "signals": 1,
            "ticker_confirmation": 1,
            "premium_feed_metrics": 1,
            "leaderboards": 1,
            "backtesting": 1,
            "screener": 1,
            "screener_intelligence": 1,
            "screener_presets": 1,
            "screener_saved_screens": 5,
            "screener_monitoring": 1,
            "screener_csv_export": 1,
            "screener_results": 25,
            "watchlists": 5,
            "watchlist_tickers": 25,
            "saved_views": 10,
            "notification_digests": 25,
            "monitoring_sources": 10,
            "event_calendar": 1,
            "inbox_alerts": 1,
            "inbox_alert_retention": 90,
            "government_contracts_feed": 1,
            "government_contracts_filters": 1,
            "insider_feed": 1,
            "congress_feed": 1,
            "options_flow_feed": 0,
            "options_flow_filters": 0,
            "institutional_feed": 0,
            "institutional_filters": 0,
            "macro_positioning": 0,
            "market_pressure": 0,
            "api_webhooks": 0,
        },
        features=frozenset(
            {
                "signals",
                "ticker_confirmation",
                "premium_feed_metrics",
                "leaderboards",
                "backtesting",
                "screener",
                "screener_intelligence",
                "screener_presets",
                "screener_saved_screens",
                "screener_monitoring",
                "screener_results",
                "watchlists",
                "watchlist_tickers",
                "saved_views",
                "notification_digests",
                "monitoring_sources",
                "event_calendar",
                "inbox_alerts",
                "government_contracts_feed",
                "government_contracts_filters",
                "insider_feed",
                "congress_feed",
            }
        ),
    ),
    "pro": TierEntitlements(
        tier="pro",
        rank=20,
        limits={
            "signals": 1,
            "ticker_confirmation": 1,
            "premium_feed_metrics": 1,
            "leaderboards": 1,
            "backtesting": 1,
            "screener": 1,
            "screener_intelligence": 1,
            "screener_presets": 1,
            "screener_saved_screens": 25,
            "screener_monitoring": 1,
            "screener_csv_export": 1,
            "screener_results": 100,
            "watchlists": 25,
            "watchlist_tickers": 100,
            "saved_views": 25,
            "notification_digests": 100,
            "monitoring_sources": 25,
            "event_calendar": 1,
            "inbox_alerts": 1,
            "inbox_alert_retention": 365,
            "government_contracts_feed": 1,
            "government_contracts_filters": 1,
            "insider_feed": 1,
            "congress_feed": 1,
            "options_flow_feed": 1,
            "options_flow_filters": 1,
            "institutional_feed": 1,
            "institutional_filters": 1,
            "macro_positioning": 1,
            "market_pressure": 1,
            "api_webhooks": 1,
        },
        features=frozenset(
            {
                "signals",
                "ticker_confirmation",
                "premium_feed_metrics",
                "leaderboards",
                "backtesting",
                "screener",
                "screener_intelligence",
                "screener_presets",
                "screener_saved_screens",
                "screener_monitoring",
                "screener_csv_export",
                "screener_results",
                "watchlists",
                "watchlist_tickers",
                "saved_views",
                "notification_digests",
                "monitoring_sources",
                "event_calendar",
                "inbox_alerts",
                "government_contracts_feed",
                "government_contracts_filters",
                "insider_feed",
                "congress_feed",
                "options_flow_feed",
                "options_flow_filters",
                "institutional_feed",
                "institutional_filters",
                "macro_positioning",
                "market_pressure",
                "api_webhooks",
            }
        ),
    ),
}

DEFAULT_FEATURE_GATES: dict[FeatureKey, dict[str, str]] = {
    "signals": {
        "required_tier": "premium",
        "description": "Signal conviction screens and unusual activity research.",
    },
    "ticker_confirmation": {
        "required_tier": "premium",
        "description": "Ticker confirmation score, active-source readout, and freshness setup details.",
    },
    "premium_feed_metrics": {
        "required_tier": "premium",
        "description": "Visible gain/loss percentages and signal scores in feed and watchlist activity cards.",
    },
    "leaderboards": {
        "required_tier": "premium",
        "description": "Congress and insider performance leaderboards.",
    },
    "backtesting": {
        "required_tier": "premium",
        "description": "Historical backtests for watchlists, saved screens, Congress portfolios, and insider portfolios.",
    },
    "screener": {
        "required_tier": "free",
        "description": "Core stock screener access with base market and company filters.",
    },
    "screener_intelligence": {
        "required_tier": "premium",
        "description": "Congress, insider, confirmation, Why Now, and freshness screener intelligence.",
    },
    "screener_presets": {
        "required_tier": "premium",
        "description": "Starter screener presets built around Walnut intelligence.",
    },
    "screener_saved_screens": {
        "required_tier": "free",
        "description": "Save reusable screener setups.",
    },
    "screener_monitoring": {
        "required_tier": "premium",
        "description": "Saved screen monitoring events and inbox updates.",
    },
    "screener_csv_export": {
        "required_tier": "pro",
        "description": "Export screener results to CSV.",
    },
    "screener_results": {
        "required_tier": "free",
        "description": "Maximum screener results available per query.",
    },
    "watchlists": {
        "required_tier": "free",
        "description": "Create named ticker watchlists.",
    },
    "watchlist_tickers": {
        "required_tier": "free",
        "description": "Add tickers to a watchlist.",
    },
    "saved_views": {
        "required_tier": "free",
        "description": "Save reusable feed, signal, and watchlist filter views.",
    },
    "notification_digests": {
        "required_tier": "premium",
        "description": "Email digests and alert-trigger subscriptions.",
    },
    "monitoring_sources": {
        "required_tier": "free",
        "description": "Monitor watchlists and saved screens in the inbox.",
    },
    "event_calendar": {
        "required_tier": "premium",
        "description": "Earnings, dividends, IPOs, stock splits, and economic calendar overlays.",
    },
    "inbox_alerts": {
        "required_tier": "free",
        "description": "Inbox alerts for monitored watchlists and saved screens.",
    },
    "inbox_alert_retention": {
        "required_tier": "free",
        "description": "Inbox alert retention window in days.",
    },
    "government_contracts_feed": {
        "required_tier": "free",
        "description": "Government contracts feed access.",
    },
    "government_contracts_filters": {
        "required_tier": "premium",
        "description": "Advanced government contracts filters.",
    },
    "insider_feed": {
        "required_tier": "free",
        "description": "Insider activity feed access.",
    },
    "congress_feed": {
        "required_tier": "free",
        "description": "Congress trading feed access.",
    },
    "options_flow_feed": {
        "required_tier": "pro",
        "description": "Options flow feed access.",
    },
    "options_flow_filters": {
        "required_tier": "pro",
        "description": "Options flow filters in discovery workflows.",
    },
    "institutional_feed": {
        "required_tier": "pro",
        "description": "Institutional Activity and 13F filing access.",
    },
    "institutional_filters": {
        "required_tier": "pro",
        "description": "Institutional activity filters in discovery workflows.",
    },
    "macro_positioning": {
        "required_tier": "pro",
        "description": "Institutional macro positioning intelligence for ticker confirmation.",
    },
    "market_pressure": {
        "required_tier": "pro",
        "description": "Sector-organized pressure maps for price movement and Walnut confirmation alignment.",
    },
    "api_webhooks": {
        "required_tier": "pro",
        "description": "API and webhook access placeholder for future workflow automation.",
    },
}

ENTITLEMENTS["admin"] = TierEntitlements(
    tier="admin",
    rank=PLAN_RANKS["admin"],
    limits={key: max(int(value), 100000) for key, value in ENTITLEMENTS["pro"].limits.items()},
    features=frozenset(DEFAULT_FEATURE_GATES.keys()),
)

PLAN_FEATURES: dict[FeatureKey, dict[str, Any]] = {
    "signals": {
        "label": "Signals",
        "kind": "feature",
        "unit_singular": "",
        "unit_plural": "",
        "sort_order": 10,
        "pricing_description": "Premium signal screens for unusual Congress and insider activity.",
    },
    "ticker_confirmation": {
        "label": "Ticker confirmation",
        "kind": "feature",
        "unit_singular": "",
        "unit_plural": "",
        "sort_order": 11,
        "pricing_description": "Ticker confirmation score, active-source readout, and freshness setup details.",
    },
    "premium_feed_metrics": {
        "label": "Feed and watchlist metrics",
        "kind": "feature",
        "unit_singular": "",
        "unit_plural": "",
        "sort_order": 12,
        "pricing_description": "Gain/loss percentages and signal scores in feed and watchlist activity cards.",
    },
    "leaderboards": {
        "label": "Leaderboards",
        "kind": "feature",
        "unit_singular": "",
        "unit_plural": "",
        "sort_order": 20,
        "pricing_description": "Performance leaderboards for deeper political and insider intelligence.",
    },
    "backtesting": {
        "label": "Portfolio backtesting",
        "kind": "feature",
        "unit_singular": "",
        "unit_plural": "",
        "sort_order": 22,
        "pricing_description": "Historical backtests across watchlists, saved screens, Congress disclosures, and insider filings.",
    },
    "screener": {
        "label": "Stock screener",
        "kind": "feature",
        "unit_singular": "",
        "unit_plural": "",
        "sort_order": 25,
        "pricing_description": "Core market and company filters across the stock screener.",
    },
    "screener_intelligence": {
        "label": "Screener intelligence filters",
        "kind": "feature",
        "unit_singular": "",
        "unit_plural": "",
        "sort_order": 26,
        "pricing_description": "Congress, insider, confirmation, Why Now, and freshness filters inside the screener.",
    },
    "screener_presets": {
        "label": "Screener starter presets",
        "kind": "feature",
        "unit_singular": "",
        "unit_plural": "",
        "sort_order": 27,
        "pricing_description": "One-click starter screens for higher-conviction discovery workflows.",
    },
    "screener_saved_screens": {
        "label": "Saved screens",
        "kind": "limit",
        "unit_singular": "screen",
        "unit_plural": "screens",
        "sort_order": 28,
        "pricing_description": "Saved screener setups you can revisit from the discovery workflow.",
    },
    "screener_monitoring": {
        "label": "Saved screen monitoring",
        "kind": "feature",
        "unit_singular": "",
        "unit_plural": "",
        "sort_order": 29,
        "pricing_description": "Monitoring events when names enter, exit, or upgrade inside a saved screen.",
    },
    "screener_csv_export": {
        "label": "Screener CSV export",
        "kind": "feature",
        "unit_singular": "",
        "unit_plural": "",
        "sort_order": 30,
        "pricing_description": "Download screener results as CSV for offline workflow and triage.",
    },
    "screener_results": {
        "label": "Screener results",
        "kind": "limit",
        "unit_singular": "result",
        "unit_plural": "results",
        "sort_order": 31,
        "pricing_description": "Maximum screener results returned per query.",
    },
    "watchlists": {
        "label": "Watchlists",
        "kind": "limit",
        "unit_singular": "watchlist",
        "unit_plural": "watchlists",
        "sort_order": 40,
        "pricing_description": "Saved research lists for monitoring symbols, filings, and alerts.",
    },
    "watchlist_tickers": {
        "label": "Tickers per watchlist",
        "kind": "limit",
        "unit_singular": "ticker",
        "unit_plural": "tickers",
        "sort_order": 50,
        "pricing_description": "Ticker capacity inside each watchlist.",
    },
    "notification_digests": {
        "label": "Alerts and digests",
        "kind": "feature",
        "unit_singular": "",
        "unit_plural": "",
        "sort_order": 60,
        "pricing_description": "Email digests and alert-trigger subscriptions for monitored research.",
    },
    "saved_views": {
        "label": "Saved views",
        "kind": "limit",
        "unit_singular": "view",
        "unit_plural": "views",
        "sort_order": 70,
        "pricing_description": "Reusable feed, signal, and watchlist filters.",
    },
    "monitoring_sources": {
        "label": "Monitoring sources",
        "kind": "limit",
        "unit_singular": "source",
        "unit_plural": "sources",
        "sort_order": 80,
        "pricing_description": "Watchlists and saved screens monitored in the inbox.",
    },
    "event_calendar": {
        "label": "Event calendar",
        "kind": "feature",
        "unit_singular": "",
        "unit_plural": "",
        "sort_order": 81,
        "pricing_description": "Calendar overlays for earnings, dividends, IPOs, stock splits, and economic releases.",
    },
    "inbox_alerts": {
        "label": "Inbox and alerts",
        "kind": "feature",
        "unit_singular": "",
        "unit_plural": "",
        "sort_order": 82,
        "pricing_description": "Inbox alerts for monitored watchlists and saved screens.",
    },
    "inbox_alert_retention": {
        "label": "Alert retention",
        "kind": "limit",
        "unit_singular": "day",
        "unit_plural": "days",
        "sort_order": 84,
        "pricing_description": "How long inbox alert history remains available.",
    },
    "congress_feed": {
        "label": "Congress feed",
        "kind": "feature",
        "unit_singular": "",
        "unit_plural": "",
        "sort_order": 90,
        "pricing_description": "Congress trading disclosures in the main feed.",
    },
    "insider_feed": {
        "label": "Insider feed",
        "kind": "feature",
        "unit_singular": "",
        "unit_plural": "",
        "sort_order": 92,
        "pricing_description": "Insider filings and trading activity in the main feed.",
    },
    "government_contracts_feed": {
        "label": "Government contracts feed",
        "kind": "feature",
        "unit_singular": "",
        "unit_plural": "",
        "sort_order": 94,
        "pricing_description": "Government contract awards and modifications in market context.",
    },
    "government_contracts_filters": {
        "label": "Government contracts filters",
        "kind": "feature",
        "unit_singular": "",
        "unit_plural": "",
        "sort_order": 96,
        "pricing_description": "Filter and triage contract activity by richer contract attributes.",
    },
    "options_flow_feed": {
        "label": "Options Flow Feed",
        "kind": "feature",
        "unit_singular": "",
        "unit_plural": "",
        "sort_order": 100,
        "pricing_description": "Options flow overlay and feed access.",
    },
    "options_flow_filters": {
        "label": "Options Flow Filters",
        "kind": "feature",
        "unit_singular": "",
        "unit_plural": "",
        "sort_order": 102,
        "pricing_description": "Options flow filters for screeners and intelligence workflows.",
    },
    "institutional_feed": {
        "label": "Institutional Feed",
        "kind": "feature",
        "unit_singular": "",
        "unit_plural": "",
        "sort_order": 110,
        "pricing_description": "Institutional Activity and 13F filing access.",
    },
    "institutional_filters": {
        "label": "Institutional Filters",
        "kind": "feature",
        "unit_singular": "",
        "unit_plural": "",
        "sort_order": 112,
        "pricing_description": "Institutional activity filters for screeners and intelligence workflows.",
    },
    "macro_positioning": {
        "label": "Macro Positioning Intelligence",
        "kind": "feature",
        "unit_singular": "",
        "unit_plural": "",
        "sort_order": 114,
        "pricing_description": "Institutional Macro Positioning for ticker confirmation.",
    },
    "market_pressure": {
        "label": "Pressure Map",
        "kind": "feature",
        "unit_singular": "",
        "unit_plural": "",
        "sort_order": 116,
        "pricing_description": "Sector-organized pressure maps for price movement and Walnut confirmation alignment.",
    },
    "api_webhooks": {
        "label": "API and webhooks",
        "kind": "feature",
        "unit_singular": "",
        "unit_plural": "",
        "sort_order": 120,
        "pricing_description": "API and webhook workflow automation placeholder.",
    },
}

PLAN_LIMIT_SETTING_KEYS: dict[tuple[TierName, FeatureKey], str] = {
    ("free", "saved_views"): "saved_views_free_limit",
    ("premium", "saved_views"): "saved_views_premium_limit",
    ("pro", "saved_views"): "saved_views_pro_limit",
    ("free", "screener_saved_screens"): "saved_screens_free_limit",
    ("premium", "screener_saved_screens"): "saved_screens_premium_limit",
    ("pro", "screener_saved_screens"): "saved_screens_pro_limit",
    ("free", "monitoring_sources"): "free_monitoring_sources_limit",
    ("premium", "monitoring_sources"): "premium_monitoring_sources_limit",
    ("pro", "monitoring_sources"): "pro_monitoring_sources_limit",
}

LEGACY_PLAN_LIMIT_SETTING_KEYS: dict[tuple[TierName, FeatureKey], tuple[str, ...]] = {
    ("free", "saved_views"): ("free_saved_views_limit",),
    ("free", "screener_saved_screens"): ("free_saved_views_limit",),
}

DEFAULT_PLAN_PRICES: dict[TierName, dict[BillingInterval, dict[str, Any]]] = {
    "free": {
        "monthly": {"amount_cents": 0, "currency": "USD"},
        "annual": {"amount_cents": 0, "currency": "USD"},
    },
    "premium": {
        "monthly": {"amount_cents": 2495, "currency": "USD"},
        "annual": {"amount_cents": 24950, "currency": "USD"},
    },
    "pro": {
        "monthly": {"amount_cents": 3995, "currency": "USD"},
        "annual": {"amount_cents": 39995, "currency": "USD"},
    },
}

PAID_SUBSCRIPTION_STATUSES = {"active", "trialing"}
CANCELED_PAID_THROUGH_STATUSES = {"canceled", "cancelled"}
PAYMENT_GRACE_SUBSCRIPTION_STATUSES = {"past_due", "payment_failed", "payment_action_required"}
REVOKED_SUBSCRIPTION_STATUSES = {
    "deleted",
    "incomplete_expired",
    "paused",
    "refunded",
    "uncollectible",
    "unpaid",
    "void",
    "voided",
}


def _env_price_id(name: str) -> str | None:
    value = os.getenv(name, "").strip()
    return value if value.startswith("price_") else None


def _configured_admin_free_price_tier(price_id: str | None) -> PlanTierName | None:
    cleaned = str(price_id or "").strip()
    if not cleaned:
        return None
    admin_free_prices = {
        _env_price_id("STRIPE_PREMIUM_ADMIN_FREE_PRICE_ID"): "premium",
        _env_price_id("STRIPE_PRO_ADMIN_FREE_PRICE_ID"): "pro",
    }
    mapped = admin_free_prices.get(cleaned)
    return mapped if mapped in {"premium", "pro"} else None


def configured_stripe_price_tier(price_id: str | None) -> PlanTierName | None:
    cleaned = str(price_id or "").strip()
    if not cleaned:
        return None
    for tier in ("premium", "pro"):
        for interval in ("monthly", "annual"):
            if cleaned == stripe_price_id(interval, tier):
                return tier
    return _configured_admin_free_price_tier(cleaned)


def stripe_payment_failure_grace_days() -> int:
    raw = os.getenv("STRIPE_PAYMENT_FAILURE_GRACE_DAYS", "0").strip()
    try:
        return max(0, min(int(raw), 30))
    except (TypeError, ValueError):
        return 0


def _aware_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def stripe_managed_subscription(user: UserAccount) -> bool:
    return bool(user.stripe_customer_id or user.stripe_subscription_id or user.subscription_status or user.subscription_plan)


def subscription_policy_tier(user: UserAccount, *, now: datetime | None = None) -> PlanTierName:
    """Resolve paid access from Stripe lifecycle fields only."""
    now = now or datetime.now(timezone.utc)
    status = (user.subscription_status or "").strip().lower()
    paid_through = _aware_utc(user.access_expires_at)
    subscription_tier = normalize_tier(user.subscription_plan)
    if subscription_tier not in {"premium", "pro"}:
        price_tier = configured_stripe_price_tier(user.stripe_price_id)
        if price_tier in {"premium", "pro"}:
            subscription_tier = price_tier
    if subscription_tier not in {"premium", "pro"}:
        entitlement_tier = normalize_tier(user.entitlement_tier)
        if entitlement_tier in {"premium", "pro"}:
            subscription_tier = entitlement_tier
    if subscription_tier not in {"premium", "pro"}:
        return "free"
    if status in REVOKED_SUBSCRIPTION_STATUSES:
        return "free"
    if bool(user.subscription_cancel_at_period_end) and paid_through is not None and paid_through <= now:
        return "free"
    if status in PAID_SUBSCRIPTION_STATUSES:
        return subscription_tier
    if status in CANCELED_PAID_THROUGH_STATUSES and bool(user.subscription_cancel_at_period_end) and paid_through is not None and paid_through > now:
        return subscription_tier
    if status in PAYMENT_GRACE_SUBSCRIPTION_STATUSES:
        grace_days = stripe_payment_failure_grace_days()
        if grace_days > 0 and paid_through is not None and paid_through > now and paid_through <= now + timedelta(days=grace_days, minutes=5):
            return subscription_tier
    return "free"


def normalize_tier(value: str | None) -> TierName:
    normalized = (value or "").strip().lower()
    if normalized == "admin":
        return "admin"
    if normalized == "pro":
        return "pro"
    if normalized == "premium":
        return "premium"
    return "free"


def _rank(tier: TierName) -> int:
    return PLAN_RANKS.get(tier, 0)


def _effective_required_tier(feature_key: FeatureKey, configured_tier: str | None) -> TierName:
    normalized = normalize_tier(configured_tier)
    hard_minimum = HARD_MINIMUM_FEATURE_TIERS.get(feature_key)
    if hard_minimum and _rank(normalized) < _rank(hard_minimum):
        return hard_minimum
    return normalized


def required_tier_for_feature(db: Session | None, feature_key: FeatureKey) -> TierName:
    configured_tier = DEFAULT_FEATURE_GATES[feature_key]["required_tier"]
    if db is not None:
        try:
            row = db.get(FeatureGate, feature_key)
        except OperationalError:
            row = None
        if row and row.required_tier:
            configured_tier = row.required_tier
    return _effective_required_tier(feature_key, configured_tier)


def seed_feature_gates(db: Session) -> None:
    changed = False
    for feature_key, config in DEFAULT_FEATURE_GATES.items():
        existing = db.get(FeatureGate, feature_key)
        if existing:
            continue
        db.add(
            FeatureGate(
                feature_key=feature_key,
                required_tier=config["required_tier"],
                description=config["description"],
            )
        )
        changed = True
    if changed:
        db.commit()


def seed_plan_limits(db: Session) -> None:
    changed = False
    for tier, entitlements in ENTITLEMENTS.items():
        for feature_key, limit_value in entitlements.limits.items():
            existing = db.get(PlanLimit, {"tier": tier, "feature_key": feature_key})
            if existing:
                continue
            db.add(PlanLimit(tier=tier, feature_key=feature_key, limit_value=limit_value))
            changed = True
    if changed:
        db.commit()


def _plan_limit_setting_key(tier: TierName, feature_key: FeatureKey) -> str | None:
    return PLAN_LIMIT_SETTING_KEYS.get((tier, feature_key))


def _plan_limit_setting_fallback_keys(tier: TierName, feature_key: FeatureKey) -> tuple[str, ...]:
    return LEGACY_PLAN_LIMIT_SETTING_KEYS.get((tier, feature_key), ())


def _plan_limit_setting_value(db: Session, setting_key: str) -> int | None:
    try:
        row = db.get(AppSetting, setting_key)
    except OperationalError:
        return None
    if row is None or row.value is None:
        return None
    try:
        return max(int(str(row.value).strip()), 0)
    except (TypeError, ValueError):
        return None


def _resolved_plan_limit_setting_value(db: Session, tier: TierName, feature_key: FeatureKey) -> int | None:
    setting_key = _plan_limit_setting_key(tier, feature_key)
    if setting_key:
        value = _plan_limit_setting_value(db, setting_key)
        if value is not None:
            return value
    for fallback_key in _plan_limit_setting_fallback_keys(tier, feature_key):
        value = _plan_limit_setting_value(db, fallback_key)
        if value is not None:
            return value
    return None


def _set_plan_limit_setting(db: Session, setting_key: str, limit_value: int) -> None:
    row = db.get(AppSetting, setting_key)
    if row is None:
        row = AppSetting(key=setting_key)
        db.add(row)
    row.value = str(max(int(limit_value), 0))


def seed_plan_prices(db: Session) -> None:
    changed = False
    for tier, intervals in DEFAULT_PLAN_PRICES.items():
        for billing_interval, price in intervals.items():
            existing = db.get(PlanPrice, {"tier": tier, "billing_interval": billing_interval})
            if existing:
                continue
            db.add(
                PlanPrice(
                    tier=tier,
                    billing_interval=billing_interval,
                    amount_cents=int(price["amount_cents"]),
                    currency=str(price["currency"]).upper(),
                )
            )
            changed = True
    if changed:
        db.commit()


def seed_plan_config(db: Session) -> None:
    seed_feature_gates(db)
    seed_plan_limits(db)
    seed_plan_prices(db)


def feature_gate_rows(db: Session) -> list[FeatureGate]:
    seed_feature_gates(db)
    return db.execute(select(FeatureGate).order_by(FeatureGate.feature_key.asc())).scalars().all()


def feature_gate_payloads(db: Session) -> list[dict[str, str]]:
    payloads = [
        {
            "feature_key": row.feature_key,
            "required_tier": _effective_required_tier(row.feature_key, row.required_tier),  # type: ignore[arg-type]
            "description": row.description or DEFAULT_FEATURE_GATES.get(row.feature_key, {}).get("description", ""),
        }
        for row in feature_gate_rows(db)
    ]
    return sorted(payloads, key=lambda item: str(item["feature_key"]))


def set_feature_gate(db: Session, *, feature_key: FeatureKey, required_tier: TierName) -> FeatureGate:
    if feature_key not in DEFAULT_FEATURE_GATES:
        raise HTTPException(status_code=404, detail="Unknown feature key.")
    row = db.get(FeatureGate, feature_key)
    if not row:
        row = FeatureGate(
            feature_key=feature_key,
            description=DEFAULT_FEATURE_GATES[feature_key]["description"],
        )
        db.add(row)
    row.required_tier = _effective_required_tier(feature_key, required_tier)
    db.commit()
    db.refresh(row)
    return row


def plan_limit_rows(db: Session) -> list[PlanLimit]:
    seed_plan_limits(db)
    return (
        db.execute(select(PlanLimit).order_by(PlanLimit.feature_key.asc(), PlanLimit.tier.asc()))
        .scalars()
        .all()
    )


def plan_limit_payloads(db: Session) -> list[dict[str, Any]]:
    payloads: list[dict[str, Any]] = []
    for tier in PLAN_TIERS:
        for feature_key, limit_value in _limits_for_tier(db, tier).items():
            if feature_key not in DEFAULT_FEATURE_GATES:
                continue
            payloads.append(
                {
                    "feature_key": feature_key,
                    "tier": tier,
                    "limit_value": int(limit_value or 0),
                    "label": PLAN_FEATURES.get(feature_key, {}).get("label", feature_key),
                    "unit_singular": PLAN_FEATURES.get(feature_key, {}).get("unit_singular", ""),
                    "unit_plural": PLAN_FEATURES.get(feature_key, {}).get("unit_plural", ""),
                    "sort_order": int(PLAN_FEATURES.get(feature_key, {}).get("sort_order", 999)),
                }
            )
    return sorted(
        payloads,
        key=lambda item: (int(item["sort_order"]), PLAN_RANKS.get(item["tier"], 999), str(item["feature_key"])),
    )


def set_plan_limit(db: Session, *, feature_key: FeatureKey, tier: TierName, limit_value: int) -> PlanLimit:
    if feature_key not in DEFAULT_FEATURE_GATES:
        raise HTTPException(status_code=404, detail="Unknown feature key.")
    normalized_limit = max(int(limit_value), 0)
    row = db.get(PlanLimit, {"tier": tier, "feature_key": feature_key})
    if not row:
        row = PlanLimit(tier=tier, feature_key=feature_key)
        db.add(row)
    row.limit_value = normalized_limit

    setting_key = _plan_limit_setting_key(tier, feature_key)
    if setting_key:
        _set_plan_limit_setting(db, setting_key, normalized_limit)
    db.commit()
    db.refresh(row)
    return row


def plan_price_rows(db: Session) -> list[PlanPrice]:
    seed_plan_prices(db)
    return (
        db.execute(select(PlanPrice).order_by(PlanPrice.tier.asc(), PlanPrice.billing_interval.asc()))
        .scalars()
        .all()
    )


def plan_price_payloads(db: Session) -> list[dict[str, Any]]:
    return [
        {
            "tier": normalize_tier(row.tier),
            "billing_interval": "annual" if row.billing_interval == "annual" else "monthly",
            "amount_cents": int(row.amount_cents or 0),
            "currency": (row.currency or "USD").upper(),
        }
        for row in plan_price_rows(db)
    ]


def set_plan_price(
    db: Session,
    *,
    tier: TierName,
    billing_interval: BillingInterval,
    amount_cents: int,
    currency: str = "USD",
) -> PlanPrice:
    row = db.get(PlanPrice, {"tier": tier, "billing_interval": billing_interval})
    if not row:
        row = PlanPrice(tier=tier, billing_interval=billing_interval)
        db.add(row)
    row.amount_cents = max(int(amount_cents), 0)
    row.currency = (currency or "USD").strip().upper()[:8] or "USD"
    db.commit()
    db.refresh(row)
    return row


def _limits_for_tier(db: Session | None, tier: TierName) -> dict[FeatureKey, int]:
    defaults = dict(ENTITLEMENTS[tier].limits)
    if db is None:
        return defaults
    rows = plan_limit_rows(db)
    for row in rows:
        feature_key = row.feature_key
        if row.tier == tier and feature_key in DEFAULT_FEATURE_GATES:
            defaults[feature_key] = int(row.limit_value or 0)  # type: ignore[literal-required]
    for (mapped_tier, feature_key), _setting_key in PLAN_LIMIT_SETTING_KEYS.items():
        if mapped_tier != tier:
            continue
        value = _resolved_plan_limit_setting_value(db, tier, feature_key)
        if value is not None:
            defaults[feature_key] = value
    return defaults


def plan_config_payload(db: Session) -> dict[str, Any]:
    gate_rows = db.execute(select(FeatureGate)).scalars().all()
    gates_by_key: dict[str, dict[str, str]] = {
        feature_key: {
            "feature_key": feature_key,
            "required_tier": _effective_required_tier(feature_key, str(config["required_tier"])),
            "description": str(config["description"]),
        }
        for feature_key, config in DEFAULT_FEATURE_GATES.items()
    }
    for row in gate_rows:
        if row.feature_key not in DEFAULT_FEATURE_GATES:
            continue
        gates_by_key[row.feature_key] = {
            "feature_key": row.feature_key,
            "required_tier": _effective_required_tier(row.feature_key, row.required_tier),  # type: ignore[arg-type]
            "description": row.description or DEFAULT_FEATURE_GATES.get(row.feature_key, {}).get("description", ""),
        }

    limits_by_tier: dict[str, dict[str, int]] = {
        tier: {key: int(value) for key, value in ENTITLEMENTS[tier].limits.items()}
        for tier in PLAN_TIERS
    }
    plan_limit_rows = db.execute(select(PlanLimit)).scalars().all()
    for row in plan_limit_rows:
        if row.tier in PLAN_TIERS and row.feature_key in DEFAULT_FEATURE_GATES:
            limits_by_tier[row.tier][row.feature_key] = int(row.limit_value or 0)  # type: ignore[index]

    setting_keys = set(PLAN_LIMIT_SETTING_KEYS.values())
    for fallback_keys in LEGACY_PLAN_LIMIT_SETTING_KEYS.values():
        setting_keys.update(fallback_keys)
    setting_rows = (
        db.execute(select(AppSetting).where(AppSetting.key.in_(sorted(setting_keys)))).scalars().all()
        if setting_keys
        else []
    )
    setting_values: dict[str, int] = {}
    for row in setting_rows:
        if row.value is None:
            continue
        try:
            setting_values[row.key] = max(int(str(row.value).strip()), 0)
        except (TypeError, ValueError):
            continue

    for (tier, feature_key), setting_key in PLAN_LIMIT_SETTING_KEYS.items():
        if tier not in PLAN_TIERS or feature_key not in DEFAULT_FEATURE_GATES:
            continue
        value = setting_values.get(setting_key)
        if value is None:
            for fallback_key in LEGACY_PLAN_LIMIT_SETTING_KEYS.get((tier, feature_key), ()):
                value = setting_values.get(fallback_key)
                if value is not None:
                    break
        if value is not None:
            limits_by_tier[tier][feature_key] = value

    prices_by_tier: dict[str, dict[str, dict[str, Any]]] = {tier: {} for tier in PLAN_TIERS}
    for tier, intervals in DEFAULT_PLAN_PRICES.items():
        if tier not in PLAN_TIERS:
            continue
        for billing_interval, price in intervals.items():
            prices_by_tier[tier][billing_interval] = {
                "tier": tier,
                "billing_interval": billing_interval,
                "amount_cents": int(price["amount_cents"]),
                "currency": str(price["currency"]).upper(),
            }
    price_rows = db.execute(select(PlanPrice)).scalars().all()
    for row in price_rows:
        tier = normalize_tier(row.tier)
        billing_interval = "annual" if row.billing_interval == "annual" else "monthly"
        if tier not in PLAN_TIERS:
            continue
        prices_by_tier.setdefault(tier, {})[billing_interval] = {
            "tier": tier,
            "billing_interval": billing_interval,
            "amount_cents": int(row.amount_cents or 0),
            "currency": (row.currency or "USD").upper(),
        }

    prices = sorted(
        [price for tier_prices in prices_by_tier.values() for price in tier_prices.values()],
        key=lambda item: (PLAN_RANKS.get(item["tier"], 999), item["billing_interval"]),
    )
    plan_limits = sorted(
        [
            {
                "feature_key": feature_key,
                "tier": tier,
                "limit_value": int(limit_value or 0),
                "label": PLAN_FEATURES.get(feature_key, {}).get("label", feature_key),
                "unit_singular": PLAN_FEATURES.get(feature_key, {}).get("unit_singular", ""),
                "unit_plural": PLAN_FEATURES.get(feature_key, {}).get("unit_plural", ""),
                "sort_order": int(PLAN_FEATURES.get(feature_key, {}).get("sort_order", 999)),
            }
            for tier in PLAN_TIERS
            for feature_key, limit_value in limits_by_tier[tier].items()
            if feature_key in DEFAULT_FEATURE_GATES
        ],
        key=lambda item: (int(item["sort_order"]), PLAN_RANKS.get(item["tier"], 999), str(item["feature_key"])),
    )
    feature_gates = sorted(gates_by_key.values(), key=lambda item: str(item["feature_key"]))

    features = []
    for feature_key, meta in sorted(PLAN_FEATURES.items(), key=lambda item: int(item[1]["sort_order"])):
        gate = gates_by_key.get(feature_key)
        required_tier = _effective_required_tier(
            feature_key,
            gate.get("required_tier") if gate else DEFAULT_FEATURE_GATES[feature_key]["required_tier"],
        )
        features.append(
            {
                "feature_key": feature_key,
                "label": meta["label"],
                "kind": meta["kind"],
                "description": meta["pricing_description"],
                "required_tier": required_tier,
                "unit_singular": meta["unit_singular"],
                "unit_plural": meta["unit_plural"],
                "sort_order": int(meta["sort_order"]),
                "limits": {
                    "free": limits_by_tier["free"].get(feature_key, 0),
                    "premium": limits_by_tier["premium"].get(feature_key, 0),
                    "pro": limits_by_tier["pro"].get(feature_key, 0),
                },
            }
        )

    return {
        "tiers": [
            {
                "tier": "free",
                "name": "Free",
                "description": "For casual research and a focused starter watchlist.",
                "limits": limits_by_tier["free"],
                "prices": prices_by_tier.get("free", {}),
            },
            {
                "tier": "premium",
                "name": "Premium",
                "description": "For daily monitoring, premium research signals, alerts, and deeper market-political intelligence.",
                "limits": limits_by_tier["premium"],
                "prices": prices_by_tier.get("premium", {}),
            },
            {
                "tier": "pro",
                "name": "Pro",
                "description": "For serious investors and operators who need higher limits, Pro-only data sets, and workflow automation readiness.",
                "limits": limits_by_tier["pro"],
                "prices": prices_by_tier.get("pro", {}),
            },
        ],
        "features": features,
        "feature_gates": feature_gates,
        "plan_limits": plan_limits,
        "plan_prices": prices,
    }


def effective_user_tier(user: UserAccount | None) -> TierName:
    if user is None:
        return normalize_tier(os.getenv("CT_DEFAULT_TIER"))
    if is_admin_user(user):
        return "admin"
    if user.is_suspended:
        return "free"
    if stripe_managed_subscription(user):
        subscription_tier = subscription_policy_tier(user)
        if subscription_tier in {"premium", "pro"}:
            return subscription_tier
        if user.manual_tier_override is None:
            return "free"
    if user.manual_tier_override is not None:
        manual_tier = normalize_tier(user.manual_tier_override)
        if manual_tier in {"premium", "pro"}:
            return manual_tier
        return "free"
    if user.password_hash and user.email_verified_at is None:
        return "free"
    entitlement_tier = normalize_tier(user.entitlement_tier)
    if entitlement_tier in {"premium", "pro"}:
        return entitlement_tier
    if (user.subscription_status or "").strip().lower() in PAID_SUBSCRIPTION_STATUSES:
        subscription_tier = normalize_tier(user.subscription_plan)
        if subscription_tier in {"premium", "pro"}:
            return subscription_tier
        if user.subscription_plan and subscription_tier == "free":
            return "free"
        return "premium"
    return "free"


def effective_user_plan_source(user: UserAccount | None) -> EffectivePlanSource:
    if user is None:
        return "free"
    if is_admin_user(user):
        return "admin"
    if user.is_suspended:
        return "suspended"
    if stripe_managed_subscription(user):
        subscription_tier = subscription_policy_tier(user)
        if subscription_tier in {"premium", "pro"}:
            return "admin_subscription" if _configured_admin_free_price_tier(user.stripe_price_id) else "stripe_subscription"
    if user.manual_tier_override is not None and normalize_tier(user.manual_tier_override) in {"premium", "pro"}:
        return "manual_override"
    return "free"


def entitlements_for_user(db: Session | None, user: UserAccount) -> TierEntitlements:
    tier = effective_user_tier(user)
    limits = _limits_for_tier(db, "pro" if is_admin_user(user) else tier)
    if is_admin_user(user):
        limits = {
            feature: 1 if int(value or 0) <= 1 else max(int(value or 0), 1_000_000)
            for feature, value in limits.items()
        }
    return TierEntitlements(
        tier=tier,
        rank=ENTITLEMENTS[tier].rank,
        limits=limits,
        features=_features_for_tier(db, tier, is_admin=is_admin_user(user)),
    )


def _features_for_tier(db: Session | None, tier: TierName, *, is_admin: bool = False) -> frozenset[FeatureKey]:
    if is_admin:
        return frozenset(DEFAULT_FEATURE_GATES.keys())
    if db is None:
        return ENTITLEMENTS[tier].features
    rows = feature_gate_rows(db)
    return frozenset(
        row.feature_key
        for row in rows
        if row.feature_key in DEFAULT_FEATURE_GATES
        and _rank(tier) >= _rank(_effective_required_tier(row.feature_key, row.required_tier))  # type: ignore[arg-type]
    )


def current_entitlements(request: Request, db: Session | None = None) -> TierEntitlements:
    user = current_user(db, request, required=False) if db is not None else None
    if user:
        return entitlements_for_user(db, user)

    header_tier = request.headers.get("x-ct-entitlement-tier")
    if header_tier and os.getenv("CT_ALLOW_ENTITLEMENT_HEADER", "0").strip().lower() in {"1", "true", "yes"}:
        tier = normalize_tier(header_tier)
        return TierEntitlements(
            tier=tier,
            rank=ENTITLEMENTS[tier].rank,
            limits=_limits_for_tier(db, tier),
            features=_features_for_tier(db, tier),
        )

    tier = normalize_tier(os.getenv("CT_DEFAULT_TIER"))
    return TierEntitlements(
        tier=tier,
        rank=ENTITLEMENTS[tier].rank,
        limits=_limits_for_tier(db, tier),
        features=_features_for_tier(db, tier),
    )


def entitlement_payload(entitlements: TierEntitlements, *, user: UserAccount | None = None) -> dict[str, Any]:
    return {
        "plan": entitlements.tier,
        "tier": entitlements.tier,
        "effective_tier": entitlements.tier,
        "source": effective_user_plan_source(user),
        "is_admin": is_admin_user(user),
        "limits": entitlements.limits,
        "features": sorted(entitlements.features),
        "upgrade_url": "/pricing",
        "user": (
            {
                "id": user.id,
                "email": user.email,
                "name": user.name,
                "role": user.role,
                "is_admin": is_admin_user(user),
                "is_suspended": user.is_suspended,
                "subscription_status": user.subscription_status,
            }
            if user
            else None
        ),
    }


def premium_required_error(*, feature: FeatureKey, message: str, entitlements: TierEntitlements) -> HTTPException:
    return HTTPException(
        status_code=402,
        detail={
            "code": "premium_required",
            "feature": feature,
            "tier": entitlements.tier,
            "limit": entitlements.limit(feature),
            "message": message,
            "upgrade_url": "/pricing",
        },
    )


def require_feature(entitlements: TierEntitlements, feature: FeatureKey, *, message: str) -> None:
    if entitlements.has_feature(feature):
        return
    raise premium_required_error(feature=feature, message=message, entitlements=entitlements)


def enforce_limit(entitlements: TierEntitlements, feature: FeatureKey, *, current_count: int, message: str) -> None:
    if current_count < entitlements.limit(feature):
        return
    raise premium_required_error(feature=feature, message=message, entitlements=entitlements)


def monitored_source_ids(
    db: Session,
    *,
    user_id: int,
    entitlements: TierEntitlements,
) -> dict[str, frozenset[int]]:
    source_limit = max(int(entitlements.limit("monitoring_sources") or 0), 0)
    if source_limit <= 0:
        return {"watchlist_ids": frozenset(), "saved_screen_ids": frozenset()}

    watchlist_ids = db.execute(
        select(Watchlist.id)
        .where(Watchlist.owner_user_id == user_id)
        .order_by(Watchlist.id.asc())
    ).scalars().all()
    allowed_watchlist_ids = tuple(int(watchlist_id) for watchlist_id in watchlist_ids[:source_limit])
    remaining_slots = max(source_limit - len(allowed_watchlist_ids), 0)

    saved_screen_ids: list[int] = []
    if remaining_slots > 0:
        saved_screen_ids = db.execute(
            select(SavedScreen.id)
            .where(SavedScreen.user_id == user_id)
            .order_by(SavedScreen.id.asc())
            .limit(remaining_slots)
        ).scalars().all()

    return {
        "watchlist_ids": frozenset(allowed_watchlist_ids),
        "saved_screen_ids": frozenset(int(saved_screen_id) for saved_screen_id in saved_screen_ids),
    }


def require_monitored_watchlist_source(
    db: Session,
    *,
    user_id: int,
    watchlist_id: int,
    entitlements: TierEntitlements,
) -> None:
    allowed_ids = monitored_source_ids(db, user_id=user_id, entitlements=entitlements)["watchlist_ids"]
    if watchlist_id in allowed_ids:
        return
    raise premium_required_error(
        feature="monitoring_sources",
        message="Your current plan can monitor fewer watchlists and saved screens. Upgrade to monitor more sources.",
        entitlements=entitlements,
    )


def require_monitored_saved_screen_source(
    db: Session,
    *,
    user_id: int,
    saved_screen_id: int,
    entitlements: TierEntitlements,
) -> None:
    allowed_ids = monitored_source_ids(db, user_id=user_id, entitlements=entitlements)["saved_screen_ids"]
    if saved_screen_id in allowed_ids:
        return
    raise premium_required_error(
        feature="monitoring_sources",
        message="Your current plan can monitor fewer watchlists and saved screens. Upgrade to monitor more sources.",
        entitlements=entitlements,
    )
