"""Opt-in strategy following and idempotent event-delivery queueing."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import StrategyDefinition, StrategyEvent, StrategyEventDelivery, StrategySubscription, UserAccount

ALLOWED_EVENT_TYPES = {"trade_added", "trade_exited", "position_rebalanced", "rebalance_completed"}


def _event_types(value: str | None) -> list[str]:
    try:
        parsed = json.loads(value or "[]")
    except (TypeError, ValueError):
        return []
    return [str(item) for item in parsed] if isinstance(parsed, list) else []


def _payload(subscription: StrategySubscription) -> dict[str, Any]:
    return {
        "id": int(subscription.id),
        "strategyId": int(subscription.strategy_id),
        "isActive": bool(subscription.is_active),
        "emailEnabled": bool(subscription.email_enabled),
        "deliveryMode": subscription.delivery_mode,
        "eventTypes": _event_types(subscription.event_types_json),
        "createdAt": subscription.created_at.isoformat() if subscription.created_at else None,
        "updatedAt": subscription.updated_at.isoformat() if subscription.updated_at else None,
    }


def _published_strategy(db: Session, slug: str) -> StrategyDefinition:
    strategy = db.execute(
        select(StrategyDefinition).where(StrategyDefinition.slug == slug, StrategyDefinition.status == "published")
    ).scalars().first()
    if strategy is None:
        raise HTTPException(status_code=404, detail="Strategy not found.")
    return strategy


def get_strategy_subscription(db: Session, *, user_id: int, slug: str) -> dict[str, Any] | None:
    strategy = _published_strategy(db, slug)
    subscription = db.execute(
        select(StrategySubscription).where(
            StrategySubscription.user_id == user_id,
            StrategySubscription.strategy_id == strategy.id,
        )
    ).scalars().first()
    return _payload(subscription) if subscription else None


def upsert_strategy_subscription(
    db: Session,
    *,
    user_id: int,
    slug: str,
    email_enabled: bool,
    delivery_mode: str,
    event_types: list[str],
) -> dict[str, Any]:
    if delivery_mode not in {"realtime", "daily"}:
        raise HTTPException(status_code=422, detail="Unsupported strategy delivery mode.")
    normalized_types = sorted({str(event_type) for event_type in event_types})
    if not normalized_types or any(event_type not in ALLOWED_EVENT_TYPES for event_type in normalized_types):
        raise HTTPException(status_code=422, detail="Unsupported strategy event type.")
    strategy = _published_strategy(db, slug)
    subscription = db.execute(
        select(StrategySubscription).where(
            StrategySubscription.user_id == user_id,
            StrategySubscription.strategy_id == strategy.id,
        )
    ).scalars().first()
    if subscription is None:
        subscription = StrategySubscription(user_id=user_id, strategy_id=strategy.id)
        db.add(subscription)
    subscription.is_active = True
    subscription.email_enabled = bool(email_enabled)
    subscription.delivery_mode = delivery_mode
    subscription.event_types_json = json.dumps(normalized_types, sort_keys=True, separators=(",", ":"))
    db.commit()
    db.refresh(subscription)
    return _payload(subscription)


def unsubscribe_strategy(db: Session, *, user_id: int, slug: str) -> None:
    strategy = _published_strategy(db, slug)
    subscription = db.execute(
        select(StrategySubscription).where(
            StrategySubscription.user_id == user_id,
            StrategySubscription.strategy_id == strategy.id,
        )
    ).scalars().first()
    if subscription is None:
        return
    subscription.is_active = False
    subscription.email_enabled = False
    db.commit()


def queue_strategy_event_deliveries(
    db: Session,
    *,
    events: list[StrategyEvent],
) -> dict[str, int]:
    """Queue delivery rows only. Sending is intentionally handled by a later worker."""
    queued = 0
    skipped = 0
    for event in events:
        subscriptions = db.execute(
            select(StrategySubscription, UserAccount)
            .join(UserAccount, UserAccount.id == StrategySubscription.user_id)
            .where(
                StrategySubscription.strategy_id == event.strategy_id,
                StrategySubscription.is_active.is_(True),
                StrategySubscription.email_enabled.is_(True),
                UserAccount.email_notifications_enabled.is_(True),
                StrategySubscription.created_at <= event.created_at,
            )
        ).all()
        for subscription, _user in subscriptions:
            if subscription.delivery_mode != "realtime" or event.event_type not in _event_types(subscription.event_types_json):
                skipped += 1
                continue
            key = f"strategy-event:{event.id}:subscription:{subscription.id}:email"
            existing = db.execute(
                select(StrategyEventDelivery.id).where(StrategyEventDelivery.idempotency_key == key)
            ).scalar_one_or_none()
            if existing is not None:
                skipped += 1
                continue
            db.add(
                StrategyEventDelivery(
                    strategy_event_id=event.id,
                    subscription_id=subscription.id,
                    channel="email",
                    idempotency_key=key,
                    status="pending",
                )
            )
            queued += 1
    db.commit()
    return {"queued": queued, "skipped": skipped}


def queue_recent_strategy_event_deliveries(db: Session, *, limit: int = 100, lookback_hours: int = 48) -> dict[str, Any]:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=max(1, lookback_hours))
    events = db.execute(
        select(StrategyEvent)
        .where(StrategyEvent.created_at >= cutoff)
        .order_by(StrategyEvent.created_at.asc(), StrategyEvent.id.asc())
        .limit(max(1, min(limit, 500)))
    ).scalars().all()
    result = queue_strategy_event_deliveries(db, events=events)
    return {"events": len(events), **result}
