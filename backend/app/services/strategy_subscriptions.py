"""Opt-in strategy following and idempotent event-delivery queueing."""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import AppSetting, StrategyDefinition, StrategyEvent, StrategyEventDelivery, StrategySubscription, UserAccount
from app.services.email_delivery import email_delivery_enabled, send_email

ALLOWED_EVENT_TYPES = {"trade_added", "trade_exited", "position_rebalanced", "rebalance_completed"}
STRATEGY_EMAIL_TEMPLATE_KEY = "alerts.strategy_event"
STRATEGY_EMAIL_STATUS_KEY = "strategy_email_delivery_status"


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


def _truthy_env(name: str, default: str = "false") -> bool:
    return os.getenv(name, default).strip().lower() in {"1", "true", "yes", "on"}


def strategy_email_delivery_enabled() -> bool:
    return _truthy_env("STRATEGY_EMAIL_DELIVERY_ENABLED")


def _max_delivery_attempts() -> int:
    try:
        return max(1, min(10, int(os.getenv("STRATEGY_EMAIL_DELIVERY_MAX_ATTEMPTS", "3") or 3)))
    except ValueError:
        return 3


def _retry_delay_seconds(attempts: int) -> int:
    return min(3600, 60 * (2 ** max(0, attempts - 1)))


def _loads_object(value: str | None) -> dict[str, Any]:
    try:
        parsed = json.loads(value or "{}")
    except (TypeError, ValueError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _write_delivery_status(db: Session, payload: dict[str, Any]) -> None:
    row = db.get(AppSetting, STRATEGY_EMAIL_STATUS_KEY)
    if row is None:
        row = AppSetting(key=STRATEGY_EMAIL_STATUS_KEY)
        db.add(row)
    row.value = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    db.commit()


def strategy_delivery_status(db: Session) -> dict[str, Any]:
    row = db.get(AppSetting, STRATEGY_EMAIL_STATUS_KEY)
    return {
        "enabled": strategy_email_delivery_enabled() and email_delivery_enabled(),
        "strategyEnabled": strategy_email_delivery_enabled(),
        "providerEnabled": email_delivery_enabled(),
        "maxAttempts": _max_delivery_attempts(),
        "lastRun": _loads_object(row.value if row else None) or None,
    }


def _is_due(delivery: StrategyEventDelivery, now: datetime) -> bool:
    last_attempt_at = delivery.last_attempt_at
    if last_attempt_at is not None and last_attempt_at.tzinfo is None:
        last_attempt_at = last_attempt_at.replace(tzinfo=timezone.utc)
    if delivery.status == "pending":
        return True
    if delivery.status == "processing":
        return last_attempt_at is None or last_attempt_at <= now - timedelta(minutes=15)
    if delivery.status != "retry":
        return False
    return last_attempt_at is None or last_attempt_at <= now - timedelta(seconds=_retry_delay_seconds(int(delivery.attempts or 0)))


def _first_name(user: UserAccount) -> str:
    if (user.first_name or "").strip():
        return str(user.first_name).strip()
    if (user.name or "").strip():
        return str(user.name).strip().split()[0]
    return "there"


def _strategy_event_context(*, strategy: StrategyDefinition, event: StrategyEvent, user: UserAccount) -> dict[str, str]:
    event_label = {"trade_added": "New position", "trade_exited": "Position exited", "position_rebalanced": "Position rebalanced", "rebalance_completed": "Strategy rebalance complete"}.get(event.event_type, "Strategy update")
    symbol = (event.ticker_at_time or event.symbol or "").strip().upper()
    base_url = os.getenv("APP_BASE_URL", "https://app.walnutmarkets.com").strip().rstrip("/")
    event_description = f"Walnut recorded a new position in {symbol} for {strategy.name}." if event.event_type == "trade_added" and symbol else f"Walnut recorded a {event_label.lower()} for {strategy.name}."
    subject = f"{strategy.name}: new position in {symbol}" if event.event_type == "trade_added" and symbol else f"{strategy.name}: {event_label.lower()}"
    return {
        "first_name": _first_name(user), "strategy_name": strategy.name, "strategy_slug": strategy.slug,
        "event_label": event_label,
        "event_description": event_description,
        "symbol": symbol or "Portfolio-level update", "event_time": event.occurred_at.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "strategy_url": f"{base_url}/strategies/{strategy.slug}", "email_subject": subject,
    }


def _mark_delivery_result(db: Session, delivery_id: int, *, status: str, error: str | None = None, provider_message_id: str | None = None) -> None:
    delivery = db.get(StrategyEventDelivery, delivery_id)
    if delivery is None:
        return
    delivery.status = status
    delivery.error = error[:500] if error else None
    if provider_message_id:
        delivery.provider_message_id = provider_message_id
    if status == "delivered":
        delivery.delivered_at = datetime.now(timezone.utc)
    db.commit()


def process_pending_strategy_event_deliveries(db: Session, *, limit: int = 50, now: datetime | None = None) -> dict[str, Any]:
    """Deliver queued strategy emails only when both delivery kill switches are enabled."""
    run_at = now or datetime.now(timezone.utc)
    if run_at.tzinfo is None:
        run_at = run_at.replace(tzinfo=timezone.utc)
    payload: dict[str, Any] = {"scheduledFor": run_at.isoformat(), "limit": max(1, min(int(limit), 250)), "maxAttempts": _max_delivery_attempts(), "processed": 0, "delivered": 0, "retried": 0, "failed": 0, "skipped": 0}
    if not strategy_email_delivery_enabled():
        payload.update({"status": "disabled", "reason": "strategy_email_delivery_disabled"})
        _write_delivery_status(db, payload)
        return payload
    if not email_delivery_enabled():
        payload.update({"status": "disabled", "reason": "global_email_delivery_disabled"})
        _write_delivery_status(db, payload)
        return payload

    rows = db.execute(
        select(StrategyEventDelivery).where(StrategyEventDelivery.channel == "email").where(StrategyEventDelivery.status.in_(("pending", "retry", "processing"))).order_by(StrategyEventDelivery.created_at.asc(), StrategyEventDelivery.id.asc()).limit(max(1, min(int(limit) * 3, 750)))
    ).scalars().all()
    due_ids = [int(row.id) for row in rows if _is_due(row, run_at)][: payload["limit"]]
    for delivery_id in due_ids:
        delivery = db.get(StrategyEventDelivery, delivery_id)
        if delivery is None or not _is_due(delivery, run_at):
            continue
        delivery.status = "processing"
        delivery.attempts = int(delivery.attempts or 0) + 1
        delivery.last_attempt_at = run_at
        db.commit()
        payload["processed"] += 1
        joined = db.execute(
            select(StrategyEventDelivery, StrategyEvent, StrategyDefinition, StrategySubscription, UserAccount)
            .join(StrategyEvent, StrategyEvent.id == StrategyEventDelivery.strategy_event_id)
            .join(StrategyDefinition, StrategyDefinition.id == StrategyEvent.strategy_id)
            .join(StrategySubscription, StrategySubscription.id == StrategyEventDelivery.subscription_id)
            .join(UserAccount, UserAccount.id == StrategySubscription.user_id)
            .where(StrategyEventDelivery.id == delivery_id)
        ).first()
        if joined is None:
            _mark_delivery_result(db, delivery_id, status="failed", error="Strategy delivery references are unavailable.")
            payload["failed"] += 1
            continue
        current, event, strategy, subscription, user = joined
        if strategy.status != "published" or not subscription.is_active or not subscription.email_enabled or not user.email_notifications_enabled:
            _mark_delivery_result(db, delivery_id, status="skipped", error="Recipient or strategy is no longer eligible for strategy email.")
            payload["skipped"] += 1
            continue
        try:
            result = send_email(db, to_email=user.email, template_key=STRATEGY_EMAIL_TEMPLATE_KEY, context=_strategy_event_context(strategy=strategy, event=event, user=user), user_id=int(user.id), category="strategy_alerts", idempotency_key=f"strategy-email:{delivery_id}:attempt:{current.attempts}", provider_idempotency_key=current.idempotency_key)
        except Exception as exc:
            result = {"status": "failed", "error": f"{exc.__class__.__name__}: {str(exc)[:400]}"}
        status = str(result.get("status") or "failed")
        if status == "sent":
            _mark_delivery_result(db, delivery_id, status="delivered", provider_message_id=str(result.get("provider_message_id") or "") or None)
            payload["delivered"] += 1
        elif status in {"skipped", "log_only"}:
            _mark_delivery_result(db, delivery_id, status="skipped", error=str(result.get("error") or status))
            payload["skipped"] += 1
        else:
            attempts = int(current.attempts or 0)
            exhausted = attempts >= _max_delivery_attempts()
            _mark_delivery_result(db, delivery_id, status="failed" if exhausted else "retry", error=str(result.get("error") or "Email provider delivery failed."))
            payload["failed" if exhausted else "retried"] += 1
    payload["status"] = "partial" if payload["failed"] else "ok"
    payload["due"] = len(due_ids)
    _write_delivery_status(db, payload)
    return payload


def list_strategy_event_deliveries(db: Session, *, strategy_slug: str | None = None, limit: int = 50) -> dict[str, Any]:
    query = select(StrategyEventDelivery, StrategyEvent, StrategyDefinition, StrategySubscription, UserAccount).join(StrategyEvent, StrategyEvent.id == StrategyEventDelivery.strategy_event_id).join(StrategyDefinition, StrategyDefinition.id == StrategyEvent.strategy_id).join(StrategySubscription, StrategySubscription.id == StrategyEventDelivery.subscription_id).join(UserAccount, UserAccount.id == StrategySubscription.user_id)
    if strategy_slug:
        query = query.where(StrategyDefinition.slug == strategy_slug)
    rows = db.execute(query.order_by(StrategyEventDelivery.created_at.desc(), StrategyEventDelivery.id.desc()).limit(max(1, min(int(limit), 200)))).all()
    return {"items": [{"id": int(delivery.id), "strategySlug": strategy.slug, "strategyName": strategy.name, "eventType": event.event_type, "symbol": event.ticker_at_time or event.symbol, "recipientEmail": user.email, "status": delivery.status, "attempts": int(delivery.attempts or 0), "providerMessageId": delivery.provider_message_id, "error": delivery.error, "createdAt": delivery.created_at.isoformat() if delivery.created_at else None, "lastAttemptAt": delivery.last_attempt_at.isoformat() if delivery.last_attempt_at else None, "deliveredAt": delivery.delivered_at.isoformat() if delivery.delivered_at else None} for delivery, event, strategy, _subscription, user in rows], "worker": strategy_delivery_status(db)}
