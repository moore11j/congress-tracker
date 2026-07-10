from __future__ import annotations

from typing import Any, Literal

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.auth import current_user, is_admin_user, normalize_email, require_admin_user
from app.db import get_db
from app.entitlements import current_entitlements, require_feature
from app.models import NotificationDelivery, NotificationSubscription, UserAccount, Watchlist
from app.rate_limit import rate_limit_admin_digest_run, rate_limit_notification_mutation
from app.services.notifications import (
    notification_delivery_payload,
    notification_subscription_payload,
    run_due_digests,
    upsert_subscription,
)

router = APIRouter(tags=["notifications"])


class NotificationSubscriptionPayload(BaseModel):
    email: str | None = Field(default=None, min_length=3, max_length=320)
    source_type: Literal["watchlist", "saved_view", "event_calendar"]
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
            "government_contract",
            "institutional_activity",
            "price_volume",
            "fundamentals",
            "event_calendar",
        ]
    ] = []
    min_smart_score: int | None = Field(default=None, ge=0, le=100)
    large_trade_amount: int | None = Field(default=None, ge=0)


def _require_watchlist_owner(db: Session, user: UserAccount, source_id: str) -> Watchlist:
    try:
        watchlist_id = int(source_id)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail="Watchlist source_id must be numeric.") from exc
    watchlist = db.execute(
        select(Watchlist).where(Watchlist.id == watchlist_id, Watchlist.owner_user_id == user.id)
    ).scalar_one_or_none()
    if not watchlist:
        raise HTTPException(status_code=404, detail="Watchlist not found.")
    return watchlist


def _subscription_owned_by_user(db: Session, subscription: NotificationSubscription, user: UserAccount) -> bool:
    if normalize_email(subscription.email) != normalize_email(user.email):
        return False
    if subscription.source_type == "watchlist":
        try:
            _require_watchlist_owner(db, user, subscription.source_id)
        except HTTPException:
            return False
    if subscription.source_type == "event_calendar" and subscription.source_id not in {"all", "watchlist", "none"}:
        return False
    return True


@router.get("/notification-subscriptions")
def list_notification_subscriptions(
    request: Request,
    db: Session = Depends(get_db),
    source_type: str | None = None,
    source_id: str | None = None,
    email: str | None = None,
):
    user = current_user(db, request, required=True)
    q = select(NotificationSubscription).order_by(NotificationSubscription.updated_at.desc(), NotificationSubscription.id.desc())
    normalized_source_type = source_type.strip().lower() if source_type else None
    if normalized_source_type and normalized_source_type not in {"watchlist", "saved_view", "event_calendar"}:
        raise HTTPException(status_code=422, detail="Unsupported source_type.")
    if normalized_source_type:
        q = q.where(NotificationSubscription.source_type == normalized_source_type)
    if source_id:
        q = q.where(NotificationSubscription.source_id == source_id.strip())
    if is_admin_user(user):
        if email:
            q = q.where(NotificationSubscription.email == normalize_email(email))
    else:
        if email and normalize_email(email) != normalize_email(user.email):
            raise HTTPException(status_code=403, detail="Not authorized.")
        q = q.where(NotificationSubscription.email == normalize_email(user.email))
        if normalized_source_type == "watchlist" and source_id:
            _require_watchlist_owner(db, user, source_id)
    rows = db.execute(q).scalars().all()
    return {"items": [notification_subscription_payload(row) for row in rows]}


@router.put("/notification-subscriptions", dependencies=[Depends(rate_limit_notification_mutation)])
def put_notification_subscription(
    payload: NotificationSubscriptionPayload,
    request: Request,
    db: Session = Depends(get_db),
):
    user = current_user(db, request, required=True)
    require_feature(
        current_entitlements(request, db),
        "notification_digests",
        message="Email digests and high-signal alerts are included with Premium.",
    )
    resolved_email = normalize_email(user.email)
    if "@" not in resolved_email:
        raise HTTPException(status_code=422, detail="A valid account email is required.")
    if payload.source_type == "watchlist":
        _require_watchlist_owner(db, user, payload.source_id)
    if payload.source_type == "event_calendar":
        require_feature(
            current_entitlements(request, db),
            "event_calendar",
            message="Earnings and event calendar overlays are included with Premium.",
        )
        if payload.source_id not in {"all", "watchlist", "none"}:
            raise HTTPException(status_code=422, detail="Unsupported event calendar alert scope.")

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
        match_email=True,
    )
    return notification_subscription_payload(subscription)


@router.delete(
    "/notification-subscriptions/{subscription_id}",
    status_code=204,
    dependencies=[Depends(rate_limit_notification_mutation)],
)
def delete_notification_subscription(subscription_id: int, request: Request, db: Session = Depends(get_db)):
    user = current_user(db, request, required=True)
    subscription = db.execute(
        select(NotificationSubscription).where(NotificationSubscription.id == subscription_id)
    ).scalar_one_or_none()
    if not subscription:
        raise HTTPException(status_code=404, detail="Subscription not found.")
    if not is_admin_user(user) and not _subscription_owned_by_user(db, subscription, user):
        raise HTTPException(status_code=404, detail="Subscription not found.")
    db.delete(subscription)
    db.commit()
    return None


@router.post("/notifications/digest/run", dependencies=[Depends(rate_limit_admin_digest_run)])
def run_notification_digests(
    request: Request,
    db: Session = Depends(get_db),
    send: bool = Query(False),
    limit: int = Query(10, ge=1, le=25),
):
    require_admin_user(db, request)
    deliveries = run_due_digests(db, send=send, limit=limit)
    return {"items": [notification_delivery_payload(delivery) for delivery in deliveries]}


@router.get("/notifications/deliveries")
def list_notification_deliveries(
    request: Request,
    db: Session = Depends(get_db),
    subscription_id: int | None = None,
    limit: int = Query(25, ge=1, le=100),
):
    require_admin_user(db, request)
    q = select(NotificationDelivery).order_by(NotificationDelivery.created_at.desc(), NotificationDelivery.id.desc()).limit(limit)
    if subscription_id is not None:
        q = q.where(NotificationDelivery.subscription_id == subscription_id)
    rows = db.execute(q).scalars().all()
    return {"items": [notification_delivery_payload(row) for row in rows]}
