from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Literal

from fastapi import HTTPException, Request
from sqlalchemy.exc import OperationalError
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.auth import current_user, is_admin_user
from app.models import AppSetting, FeatureGate, PlanLimit, PlanPrice, SavedScreen, UserAccount, Watchlist

TierName = Literal["free", "premium", "pro", "admin"]
PlanTierName = Literal["free", "premium", "pro"]
BillingInterval = Literal["monthly", "annual"]
FeatureKey = Literal[
    "signals",
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
    "api_webhooks",
]

PLAN_TIERS: tuple[PlanTierName, ...] = ("free", "premium", "pro")
PLAN_RANKS: dict[TierName, int] = {"free": 0, "premium": 10, "pro": 20, "admin": 100}


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
            "leaderboards": 0,
            "backtesting": 0,
            "screener": 0,
            "screener_intelligence": 0,
            "screener_presets": 0,
            "screener_saved_screens": 3,
            "screener_monitoring": 0,
            "screener_csv_export": 0,
            "screener_results": 25,
            "watchlists": 1,
            "watchlist_tickers": 10,
            "saved_views": 3,
            "notification_digests": 0,
            "monitoring_sources": 2,
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
            "leaderboards": 1,
            "backtesting": 1,
            "screener": 1,
            "screener_intelligence": 1,
            "screener_presets": 1,
            "screener_saved_screens": 10,
            "screener_monitoring": 1,
            "screener_csv_export": 1,
            "screener_results": 250,
            "watchlists": 10,
            "watchlist_tickers": 30,
            "saved_views": 50,
            "notification_digests": 25,
            "monitoring_sources": 5,
            "inbox_alerts": 1,
            "inbox_alert_retention": 90,
            "government_contracts_feed": 1,
            "government_contracts_filters": 1,
            "insider_feed": 1,
            "congress_feed": 1,
            "options_flow_feed": 1,
            "options_flow_filters": 1,
            "institutional_feed": 0,
            "institutional_filters": 0,
            "api_webhooks": 0,
        },
        features=frozenset(
            {
                "signals",
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
                "inbox_alerts",
                "government_contracts_feed",
                "government_contracts_filters",
                "insider_feed",
                "congress_feed",
                "options_flow_feed",
                "options_flow_filters",
            }
        ),
    ),
    "pro": TierEntitlements(
        tier="pro",
        rank=20,
        limits={
            "signals": 1,
            "leaderboards": 1,
            "backtesting": 1,
            "screener": 1,
            "screener_intelligence": 1,
            "screener_presets": 1,
            "screener_saved_screens": 50,
            "screener_monitoring": 1,
            "screener_csv_export": 1,
            "screener_results": 1000,
            "watchlists": 25,
            "watchlist_tickers": 100,
            "saved_views": 50,
            "notification_digests": 100,
            "monitoring_sources": 15,
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
            "api_webhooks": 1,
        },
        features=frozenset(
            {
                "signals",
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
                "inbox_alerts",
                "government_contracts_feed",
                "government_contracts_filters",
                "insider_feed",
                "congress_feed",
                "options_flow_feed",
                "options_flow_filters",
                "institutional_feed",
                "institutional_filters",
                "api_webhooks",
            }
        ),
    ),
}

DEFAULT_FEATURE_GATES: dict[FeatureKey, dict[str, str]] = {
    "signals": {
        "required_tier": "premium",
        "description": "Smart money signal screens and unusual activity research.",
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
        "description": "Starter screener presets built around Capitol Ledger intelligence.",
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
        "required_tier": "premium",
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
        "required_tier": "premium",
        "description": "Options flow feed access when provider data is available.",
    },
    "options_flow_filters": {
        "required_tier": "premium",
        "description": "Options flow filters in discovery workflows.",
    },
    "institutional_feed": {
        "required_tier": "pro",
        "description": "Institutional activity feed access when provider data is available.",
    },
    "institutional_filters": {
        "required_tier": "pro",
        "description": "Institutional activity filters in discovery workflows.",
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
        "pricing_description": "Options flow overlay and feed access. Coming soon where provider data is not yet enabled.",
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
        "pricing_description": "Institutional activity feed access. Coming soon where provider data is not yet enabled.",
    },
    "institutional_filters": {
        "label": "Institutional Filters",
        "kind": "feature",
        "unit_singular": "",
        "unit_plural": "",
        "sort_order": 112,
        "pricing_description": "Institutional activity filters for screeners and intelligence workflows.",
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
        "monthly": {"amount_cents": 1995, "currency": "USD"},
        "annual": {"amount_cents": 19995, "currency": "USD"},
    },
    "pro": {
        "monthly": {"amount_cents": 4995, "currency": "USD"},
        "annual": {"amount_cents": 49995, "currency": "USD"},
    },
}

PAID_SUBSCRIPTION_STATUSES = {"active", "trialing"}


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
            "required_tier": normalize_tier(row.required_tier),
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
    row.required_tier = required_tier
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
    seed_plan_config(db)
    gates_by_key = {row["feature_key"]: row for row in feature_gate_payloads(db)}
    limits_by_tier: dict[str, dict[str, int]] = {
        tier: {key: int(value) for key, value in _limits_for_tier(db, tier).items()}
        for tier in PLAN_TIERS
    }
    prices = plan_price_payloads(db)
    prices_by_tier: dict[str, dict[str, dict[str, Any]]] = {tier: {} for tier in PLAN_TIERS}
    for price in prices:
        prices_by_tier.setdefault(price["tier"], {})[price["billing_interval"]] = price

    features = []
    for feature_key, meta in sorted(PLAN_FEATURES.items(), key=lambda item: int(item[1]["sort_order"])):
        gate = gates_by_key.get(feature_key)
        required_tier = normalize_tier(gate.get("required_tier") if gate else DEFAULT_FEATURE_GATES[feature_key]["required_tier"])
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
        "feature_gates": feature_gate_payloads(db),
        "plan_limits": plan_limit_payloads(db),
        "plan_prices": prices,
    }


def effective_user_tier(user: UserAccount | None) -> TierName:
    if user is None:
        return normalize_tier(os.getenv("CT_DEFAULT_TIER"))
    if is_admin_user(user):
        return "admin"
    if user.manual_tier_override:
        return normalize_tier(user.manual_tier_override)
    entitlement_tier = normalize_tier(user.entitlement_tier)
    if entitlement_tier in {"premium", "pro"}:
        return entitlement_tier
    access_expires_at = user.access_expires_at
    if access_expires_at and access_expires_at.tzinfo is None:
        access_expires_at = access_expires_at.replace(tzinfo=timezone.utc)
    if access_expires_at and access_expires_at > datetime.now(timezone.utc):
        return "premium"
    if (user.subscription_status or "").strip().lower() in PAID_SUBSCRIPTION_STATUSES:
        subscription_tier = normalize_tier(user.subscription_plan)
        return subscription_tier if subscription_tier in {"premium", "pro"} else "premium"
    return "free"


def entitlements_for_user(db: Session | None, user: UserAccount) -> TierEntitlements:
    tier = effective_user_tier(user)
    return TierEntitlements(
        tier=tier,
        rank=ENTITLEMENTS[tier].rank,
        limits=_limits_for_tier(db, tier),
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
        if row.feature_key in DEFAULT_FEATURE_GATES and _rank(tier) >= _rank(normalize_tier(row.required_tier))
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
        "tier": entitlements.tier,
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
                "manual_tier_override": user.manual_tier_override,
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
