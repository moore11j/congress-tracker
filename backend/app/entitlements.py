from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Literal

from fastapi import HTTPException, Request
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.auth import current_user, is_admin_user
from app.models import FeatureGate, PlanLimit, PlanPrice, UserAccount

TierName = Literal["free", "premium"]
BillingInterval = Literal["monthly", "annual"]
FeatureKey = Literal[
    "signals",
    "leaderboards",
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
]


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
            "screener": 0,
            "screener_intelligence": 0,
            "screener_presets": 0,
            "screener_saved_screens": 1,
            "screener_monitoring": 0,
            "screener_csv_export": 0,
            "screener_results": 25,
            "watchlists": 1,
            "watchlist_tickers": 10,
            "saved_views": 5,
            "notification_digests": 0,
            "monitoring_sources": 8,
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
            }
        ),
    ),
    "premium": TierEntitlements(
        tier="premium",
        rank=10,
        limits={
            "signals": 1,
            "leaderboards": 1,
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
            "monitoring_sources": 100,
        },
        features=frozenset(
            {
                "signals",
                "leaderboards",
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
        "description": "Monitor watchlists and saved views in the inbox.",
    },
}

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
        "label": "Monitoring inbox",
        "kind": "limit",
        "unit_singular": "source",
        "unit_plural": "sources",
        "sort_order": 80,
        "pricing_description": "Watchlists and saved views monitored in the inbox.",
    },
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
}

PAID_SUBSCRIPTION_STATUSES = {"active", "trialing"}


def normalize_tier(value: str | None) -> TierName:
    normalized = (value or "").strip().lower()
    if normalized == "premium":
        return "premium"
    return "free"


def _rank(tier: TierName) -> int:
    return ENTITLEMENTS[tier].rank


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
    return [
        {
            "feature_key": row.feature_key,
            "required_tier": normalize_tier(row.required_tier),
            "description": row.description or DEFAULT_FEATURE_GATES.get(row.feature_key, {}).get("description", ""),
        }
        for row in feature_gate_rows(db)
    ]


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
    return [
        {
            "feature_key": row.feature_key,
            "tier": normalize_tier(row.tier),
            "limit_value": int(row.limit_value or 0),
            "label": PLAN_FEATURES.get(row.feature_key, {}).get("label", row.feature_key),
            "unit_singular": PLAN_FEATURES.get(row.feature_key, {}).get("unit_singular", ""),
            "unit_plural": PLAN_FEATURES.get(row.feature_key, {}).get("unit_plural", ""),
            "sort_order": int(PLAN_FEATURES.get(row.feature_key, {}).get("sort_order", 999)),
        }
        for row in plan_limit_rows(db)
        if row.feature_key in DEFAULT_FEATURE_GATES
    ]


def set_plan_limit(db: Session, *, feature_key: FeatureKey, tier: TierName, limit_value: int) -> PlanLimit:
    if feature_key not in DEFAULT_FEATURE_GATES:
        raise HTTPException(status_code=404, detail="Unknown feature key.")
    row = db.get(PlanLimit, {"tier": tier, "feature_key": feature_key})
    if not row:
        row = PlanLimit(tier=tier, feature_key=feature_key)
        db.add(row)
    row.limit_value = max(int(limit_value), 0)
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
    return defaults


def plan_config_payload(db: Session) -> dict[str, Any]:
    seed_plan_config(db)
    gates_by_key = {row["feature_key"]: row for row in feature_gate_payloads(db)}
    limits_by_tier: dict[str, dict[str, int]] = {
        tier: {key: int(value) for key, value in _limits_for_tier(db, tier).items()}
        for tier in ("free", "premium")
    }
    prices = plan_price_payloads(db)
    prices_by_tier: dict[str, dict[str, dict[str, Any]]] = {"free": {}, "premium": {}}
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
        return "premium"
    if user.manual_tier_override:
        return normalize_tier(user.manual_tier_override)
    if normalize_tier(user.entitlement_tier) == "premium":
        return "premium"
    access_expires_at = user.access_expires_at
    if access_expires_at and access_expires_at.tzinfo is None:
        access_expires_at = access_expires_at.replace(tzinfo=timezone.utc)
    if access_expires_at and access_expires_at > datetime.now(timezone.utc):
        return "premium"
    if (user.subscription_status or "").strip().lower() in PAID_SUBSCRIPTION_STATUSES:
        return "premium"
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
