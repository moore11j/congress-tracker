from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from sqlalchemy.orm import Session

from app.auth import current_user
from app.db import get_db
from app.entitlements import current_entitlements
from app.services.market_pressure import build_market_pressure_capabilities_response, build_market_pressure_response

router = APIRouter(tags=["market-pressure"])


@router.get("/market-pressure/capabilities")
def market_pressure_capabilities(
    request: Request,
    response: Response,
    db: Session = Depends(get_db),
):
    response.headers["Cache-Control"] = "private, no-store"
    user = current_user(db, request, required=False)
    entitlements = current_entitlements(request, db)
    try:
        return build_market_pressure_capabilities_response(db, entitlements=entitlements, user=user)
    except HTTPException as exc:
        headers = dict(exc.headers or {})
        headers["Cache-Control"] = "private, no-store"
        raise HTTPException(status_code=exc.status_code, detail=exc.detail, headers=headers) from exc


@router.get("/market-pressure")
def market_pressure(
    request: Request,
    response: Response,
    universe: str | None = Query(default="sp500"),
    period: str | None = Query(default="1d"),
    view: str | None = Query(default="market_pressure"),
    db: Session = Depends(get_db),
):
    response.headers["Cache-Control"] = "private, no-store"
    user = current_user(db, request, required=False)
    entitlements = current_entitlements(request, db)
    try:
        return build_market_pressure_response(
            db,
            universe=universe,
            period=period,
            view=view,
            entitlements=entitlements,
            user=user,
        )
    except HTTPException as exc:
        headers = dict(exc.headers or {})
        headers["Cache-Control"] = "private, no-store"
        raise HTTPException(status_code=exc.status_code, detail=exc.detail, headers=headers) from exc
