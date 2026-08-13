from __future__ import annotations

from datetime import date

from fastapi import APIRouter, Depends, Query, Request, Response
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.auth import current_user, require_admin_user
from app.db import get_db
from app.entitlements import current_entitlements, require_feature
from app.services.strategies import list_strategy_cards, set_strategy_publication, strategy_detail
from app.services.strategy_versions import (
    activate_strategy_version,
    approve_strategy_version,
    create_strategy_version,
    list_strategy_versions,
    preview_strategy_version,
)
from app.services.strategy_scheduler import scheduler_status
from app.services.strategy_subscriptions import get_strategy_subscription, list_strategy_event_deliveries, strategy_delivery_status, unsubscribe_strategy, upsert_strategy_subscription
from app.services.strategy_subscriptions import process_pending_strategy_event_deliveries, queue_recent_strategy_event_deliveries
from app.services.strategy_scheduler import run_active_strategy_evaluations

router = APIRouter(tags=["strategies"])


class StrategyPublicationRequest(BaseModel):
    published: bool


class StrategyVersionCreateRequest(BaseModel):
    rules: dict = Field(default_factory=dict)
    parameters: dict = Field(default_factory=dict)
    universe: dict = Field(default_factory=dict)
    methodology: str | None = None
    effective_from: date | None = None


class StrategySubscriptionRequest(BaseModel):
    email_enabled: bool = True
    event_types: list[str] = Field(default_factory=lambda: ["trade_added", "trade_exited", "rebalance_completed"])


class StrategyOperationRequest(BaseModel):
    operation: str = Field(pattern="^(evaluate|queue|deliver)$")
    limit: int = Field(default=50, ge=1, le=250)


@router.get("/strategies")
def strategies(
    request: Request,
    response: Response,
    category: str | None = Query(default=None, max_length=80),
    period: str = Query(default="max", max_length=20),
    sort: str = Query(default="cagr", max_length=40),
    db: Session = Depends(get_db),
):
    response.headers["Cache-Control"] = "private, max-age=60"
    current_user(db, request, required=False)
    return list_strategy_cards(
        db,
        entitlements=current_entitlements(request, db),
        category=category,
        period=period,
        sort=sort,
        include_drafts=False,
    )


@router.get("/strategies/{slug}")
def strategy(
    slug: str,
    request: Request,
    response: Response,
    period: str = Query(default="max", max_length=20),
    equity_limit: int = Query(default=1500, ge=1, le=5000),
    holdings_offset: int = Query(default=0, ge=0),
    holdings_limit: int = Query(default=20, ge=1, le=100),
    db: Session = Depends(get_db),
):
    response.headers["Cache-Control"] = "private, max-age=60"
    current_user(db, request, required=False)
    return strategy_detail(
        db,
        slug=slug,
        entitlements=current_entitlements(request, db),
        period=period,
        equity_limit=equity_limit,
        holdings_offset=holdings_offset,
        holdings_limit=holdings_limit,
        include_drafts=False,
    )


@router.get("/strategies/{slug}/subscription")
def strategy_subscription(slug: str, request: Request, db: Session = Depends(get_db)):
    user = current_user(db, request, required=True)
    return {"subscription": get_strategy_subscription(db, user_id=int(user.id), slug=slug)}


@router.put("/strategies/{slug}/subscription")
def put_strategy_subscription(
    slug: str,
    payload: StrategySubscriptionRequest,
    request: Request,
    db: Session = Depends(get_db),
):
    user = current_user(db, request, required=True)
    require_feature(
        current_entitlements(request, db),
        "notification_digests",
        message="Strategy alerts are included with Premium.",
    )
    return {"subscription": upsert_strategy_subscription(
        db,
        user_id=int(user.id),
        slug=slug,
        email_enabled=payload.email_enabled,
        event_types=payload.event_types,
    )}


@router.delete("/strategies/{slug}/subscription", status_code=204)
def delete_strategy_subscription(slug: str, request: Request, db: Session = Depends(get_db)):
    user = current_user(db, request, required=True)
    unsubscribe_strategy(db, user_id=int(user.id), slug=slug)
    return None


@router.get("/admin/strategies")
def admin_strategies(
    request: Request,
    response: Response,
    category: str | None = Query(default=None, max_length=80),
    period: str = Query(default="max", max_length=20),
    sort: str = Query(default="cagr", max_length=40),
    db: Session = Depends(get_db),
):
    response.headers["Cache-Control"] = "private, no-store"
    require_admin_user(db, request)
    return list_strategy_cards(
        db,
        entitlements=current_entitlements(request, db),
        category=category,
        period=period,
        sort=sort,
        include_drafts=True,
    )


@router.get("/admin/strategies/{slug}")
def admin_strategy(
    slug: str,
    request: Request,
    response: Response,
    period: str = Query(default="max", max_length=20),
    equity_limit: int = Query(default=1500, ge=1, le=5000),
    holdings_offset: int = Query(default=0, ge=0),
    holdings_limit: int = Query(default=20, ge=1, le=100),
    db: Session = Depends(get_db),
):
    response.headers["Cache-Control"] = "private, no-store"
    require_admin_user(db, request)
    return strategy_detail(
        db,
        slug=slug,
        entitlements=current_entitlements(request, db),
        period=period,
        equity_limit=equity_limit,
        holdings_offset=holdings_offset,
        holdings_limit=holdings_limit,
        include_drafts=True,
    )


@router.patch("/admin/strategies/{slug}/publication")
def admin_set_strategy_publication(
    slug: str,
    payload: StrategyPublicationRequest,
    request: Request,
    db: Session = Depends(get_db),
):
    require_admin_user(db, request)
    return set_strategy_publication(
        db,
        slug=slug,
        published=payload.published,
        entitlements=current_entitlements(request, db),
    )


@router.get("/admin/strategies/{slug}/versions")
def admin_strategy_versions(slug: str, request: Request, db: Session = Depends(get_db)):
    require_admin_user(db, request)
    return list_strategy_versions(db, slug=slug)


@router.post("/admin/strategies/{slug}/versions")
def admin_create_strategy_version(
    slug: str,
    payload: StrategyVersionCreateRequest,
    request: Request,
    db: Session = Depends(get_db),
):
    require_admin_user(db, request)
    return create_strategy_version(
        db,
        slug=slug,
        rules=payload.rules,
        parameters=payload.parameters,
        universe=payload.universe,
        methodology=payload.methodology,
        effective_from=payload.effective_from,
        created_by="admin_strategy_console",
    )


@router.post("/admin/strategies/{slug}/versions/{version_id}/approve")
def admin_approve_strategy_version(
    slug: str,
    version_id: int,
    request: Request,
    db: Session = Depends(get_db),
):
    require_admin_user(db, request)
    return approve_strategy_version(db, slug=slug, version_id=version_id)


@router.post("/admin/strategies/{slug}/versions/{version_id}/activate")
def admin_activate_strategy_version(
    slug: str,
    version_id: int,
    request: Request,
    db: Session = Depends(get_db),
):
    require_admin_user(db, request)
    return activate_strategy_version(db, slug=slug, version_id=version_id)


@router.get("/admin/strategy-scheduler/status")
def admin_strategy_scheduler_status(request: Request, db: Session = Depends(get_db)):
    require_admin_user(db, request)
    return scheduler_status(db)


@router.get("/admin/strategy-deliveries")
def admin_strategy_deliveries(request: Request, strategy_slug: str | None = Query(default=None, max_length=160), limit: int = Query(default=50, ge=1, le=200), db: Session = Depends(get_db)):
    require_admin_user(db, request)
    return list_strategy_event_deliveries(db, strategy_slug=strategy_slug, limit=limit)


@router.get("/admin/strategy-delivery-worker/status")
def admin_strategy_delivery_worker_status(request: Request, db: Session = Depends(get_db)):
    require_admin_user(db, request)
    return strategy_delivery_status(db)


@router.post("/admin/strategy-operations/run")
def admin_run_strategy_operation(payload: StrategyOperationRequest, request: Request, db: Session = Depends(get_db)):
    require_admin_user(db, request)
    if payload.operation == "evaluate":
        return {"operation": "evaluate", "result": run_active_strategy_evaluations(db)}
    if payload.operation == "queue":
        return {"operation": "queue", "result": queue_recent_strategy_event_deliveries(db, limit=payload.limit)}
    return {"operation": "deliver", "result": process_pending_strategy_event_deliveries(db, limit=payload.limit)}


@router.get("/admin/strategies/{slug}/versions/{version_id}/preview")
def admin_preview_strategy_version(
    slug: str,
    version_id: int,
    request: Request,
    evaluation_date: date = Query(...),
    db: Session = Depends(get_db),
):
    require_admin_user(db, request)
    return preview_strategy_version(db, slug=slug, version_id=version_id, evaluation_date=evaluation_date)
