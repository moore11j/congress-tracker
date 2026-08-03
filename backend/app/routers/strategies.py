from __future__ import annotations

from fastapi import APIRouter, Depends, Query, Request, Response
from sqlalchemy.orm import Session

from app.auth import current_user, require_admin_user
from app.db import get_db
from app.entitlements import current_entitlements
from app.services.strategies import list_strategy_cards, strategy_detail

router = APIRouter(tags=["strategies"])


@router.get("/strategies")
def strategies(
    request: Request,
    response: Response,
    category: str | None = Query(default=None, max_length=80),
    period: str = Query(default="max", max_length=20),
    sort: str = Query(default="walnut_score", max_length=40),
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
        include_drafts=False,
    )


@router.get("/admin/strategies")
def admin_strategies(
    request: Request,
    response: Response,
    category: str | None = Query(default=None, max_length=80),
    period: str = Query(default="max", max_length=20),
    sort: str = Query(default="walnut_score", max_length=40),
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
        include_drafts=True,
    )
