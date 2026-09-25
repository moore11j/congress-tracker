"""Read-only ticker surface for prepared operational evidence."""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.db import get_db
from app.auth import current_user
from app.entitlements import current_entitlements, require_feature
from app.models import Security
from app.services.operational_intelligence import operational_intelligence_enabled, ticker_operational_intelligence
from app.utils.symbols import normalize_symbol

router = APIRouter(prefix="/tickers", tags=["operational-intelligence"])


@router.get("/{symbol}/operational-intelligence")
def operational_intelligence(symbol: str, request: Request, response: Response, db: Session = Depends(get_db)):
    if not operational_intelligence_enabled():
        raise HTTPException(status_code=404, detail="Operational intelligence is not enabled.")
    current_user(db, request, required=True)
    require_feature(current_entitlements(request, db), "view_research_memory", message="Company developments are available with Premium.")
    response.headers["Cache-Control"] = "private, no-store"
    response.headers["Vary"] = "Cookie, Authorization"
    normalized = normalize_symbol(symbol)
    if not normalized:
        raise HTTPException(status_code=422, detail="Ticker symbol is required.")
    security = db.execute(select(Security).where(func.upper(Security.symbol) == normalized)).scalar_one_or_none()
    if not security:
        raise HTTPException(status_code=404, detail="Ticker security not found.")
    return ticker_operational_intelligence(db, security=security)
