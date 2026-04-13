from __future__ import annotations

from typing import Any, Literal

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.auth import current_user, normalize_email
from app.db import get_db
from app.entitlements import current_entitlements, require_feature
from app.models import NotificationDelivery, NotificationSubscription, Watchlist
from app.services.notifications import (
    notification_delivery_payload,
    notification_subscription_payload,
    run_due_digests,
    upsert_subscription,
)

router = APIRouter(tags=["notifications"])


class NotificationSubscriptionPayload(BaseModel):
    email: str | None = Field(default=None, min_length=3, max_length=320)
    source_type: Literal["watchlist", "saved_view"]
    source_id: str = Field(min_length=1, max_length=160)
    source_name: str = Field(min_length=1, max_length=160)
    source_payload: dict[str, Any] | None = None
    frequency: Literal["daily"] = "daily"
    only_if_new: bool = True
    active: bool = True
    alert_triggers: list[
        Literal[
            "cross_source_confirmation",
            "smart_score_threshold",
            "large_trade_threshold",
            "congress_activity",
            "insider_activity",
        ]
    ] = []
    min_smart_score: int | None = Field(default=None, ge=0, le=100)
    large_trade_amount: int | None = Field(default=None, ge=0)


@router.get("/notification-subscriptions")
def list_notification_subscriptions(
    db: Session = Depends(get_db),
    source_type: str | None = None,
    source_id: str | None = None,
    email: str | None = None,
):
    q = select(NotificationSubscription).order_by(NotificationSubscription.updated_at.desc(), NotificationSubscription.id.desc())
    normalized_source_type = source_type.strip().lower() if source_type else None
    if normalized_source_type:
        q = q.where(NotificationSubscription.source_type == normalized_source_type)
    if source_id:
        q = q.where(NotificationSubscription.source_id == source_id.strip())
    if email:
        q = q.where(NotificationSubscription.email == email.strip())
    rows = db.execute(q).scalars().all()
    return {"items": [notification_subscription_payload(row) for row in rows]}


@router.put("/notification-subscriptions")
def put_notification_subscription(
    payload: NotificationSubscriptionPayload,
    request: Request,
    db: Session = Depends(get_db),
):
    require_feature(
        current_entitlements(request, db),
        "notification_digests",
        message="Email digests and high-signal alerts are included with Premium.",
    )
    user = current_user(db, request, required=False)
    resolved_email = normalize_email(user.email) if user and payload.source_type == "watchlist" else normalize_email(payload.email)
    if "@" not in resolved_email:
        if payload.source_type == "watchlist":
            raise HTTPException(status_code=401, detail="Sign in required.")
        raise HTTPException(status_code=422, detail="A valid email is required.")
    if payload.source_type == "watchlist":
        try:
            watchlist_id = int(payload.source_id)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail="Watchlist source_id must be numeric.") from exc
        watchlist = db.execute(select(Watchlist).where(Watchlist.id == watchlist_id)).scalar_one_or_none()
        if not watchlist:
            raise HTTPException(status_code=404, detail="Watchlist not found.")

    subscription = upsert_subscription(
        db,
        email=resolved_email,
        source_type=payload.source_type,
        source_id=payload.source_id,
        source_name=payload.source_name,
        source_payload=payload.source_payload,
        frequency=payload.frequency,
        only_if_new=payload.only_if_new,
        active=payload.active,
        alert_triggers=list(payload.alert_triggers),
        min_smart_score=payload.min_smart_score,
        large_trade_amount=payload.large_trade_amount,
        match_email=not (payload.source_type == "watchlist" and user is not None),
    )
    return notification_subscription_payload(subscription)


@router.delete("/notification-subscriptions/{subscription_id}", status_code=204)
def delete_notification_subscription(subscription_id: int, db: Session = Depends(get_db)):
    subscription = db.execute(
        select(NotificationSubscription).where(NotificationSubscription.id == subscription_id)
    ).scalar_one_or_none()
    if not subscription:
        raise HTTPException(status_code=404, detail="Subscription not found.")
    db.delete(subscription)
    db.commit()
    return None


@router.post("/notifications/digest/run")
def run_notification_digests(
    db: Session = Depends(get_db),
    send: bool = Query(False),
    limit: int = Query(10, ge=1, le=25),
):
    deliveries = run_due_digests(db, send=send, limit=limit)
    return {"items": [notification_delivery_payload(delivery) for delivery in deliveries]}


@router.get("/notifications/deliveries")
def list_notification_deliveries(
    db: Session = Depends(get_db),
    subscription_id: int | None = None,
    limit: int = Query(25, ge=1, le=100),
):
    q = select(NotificationDelivery).order_by(NotificationDelivery.created_at.desc(), NotificationDelivery.id.desc()).limit(limit)
    if subscription_id is not None:
        q = q.where(NotificationDelivery.subscription_id == subscription_id)
    rows = db.execute(q).scalars().all()
    return {"items": [notification_delivery_payload(row) for row in rows]}
