from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Literal

from fastapi import HTTPException, Request
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.auth import current_user, is_admin_user
from app.models import FeatureGate, UserAccount

TierName = Literal["free", "premium"]
FeatureKey = Literal[
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
            "watchlists": 3,
            "watchlist_tickers": 15,
            "saved_views": 5,
            "notification_digests": 0,
            "monitoring_sources": 8,
        },
        features=frozenset({"watchlists", "watchlist_tickers", "saved_views", "monitoring_sources"}),
    ),
    "premium": TierEntitlements(
        tier="premium",
        rank=10,
        limits={
            "watchlists": 25,
            "watchlist_tickers": 100,
            "saved_views": 50,
            "notification_digests": 25,
            "monitoring_sources": 100,
        },
        features=frozenset(
            {
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


def effective_user_tier(user: UserAccount | None) -> TierName:
    if user is None:
        return normalize_tier(os.getenv("CT_DEFAULT_TIER"))
    if is_admin_user(user):
        return "premium"
    if user.manual_tier_override:
        return normalize_tier(user.manual_tier_override)
    if normalize_tier(user.entitlement_tier) == "premium":
        return "premium"
    if (user.subscription_status or "").strip().lower() in PAID_SUBSCRIPTION_STATUSES:
        return "premium"
    return "free"


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
        tier = effective_user_tier(user)
        return TierEntitlements(
            tier=tier,
            rank=ENTITLEMENTS[tier].rank,
            limits=ENTITLEMENTS[tier].limits,
            features=_features_for_tier(db, tier, is_admin=is_admin_user(user)),
        )

    header_tier = request.headers.get("x-ct-entitlement-tier")
    if header_tier and os.getenv("CT_ALLOW_ENTITLEMENT_HEADER", "0").strip().lower() in {"1", "true", "yes"}:
        tier = normalize_tier(header_tier)
        return TierEntitlements(
            tier=tier,
            rank=ENTITLEMENTS[tier].rank,
            limits=ENTITLEMENTS[tier].limits,
            features=_features_for_tier(db, tier),
        )

    tier = normalize_tier(os.getenv("CT_DEFAULT_TIER"))
    return TierEntitlements(
        tier=tier,
        rank=ENTITLEMENTS[tier].rank,
        limits=ENTITLEMENTS[tier].limits,
        features=_features_for_tier(db, tier),
    )


def entitlement_payload(entitlements: TierEntitlements, *, user: UserAccount | None = None) -> dict[str, Any]:
    return {
        "tier": entitlements.tier,
        "limits": entitlements.limits,
        "features": sorted(entitlements.features),
        "upgrade_url": "/account/billing",
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
            "upgrade_url": "/account/billing",
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
