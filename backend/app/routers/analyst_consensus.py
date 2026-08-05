from __future__ import annotations

from datetime import date

from fastapi import APIRouter, Depends, Query, Request, Response
from sqlalchemy.orm import Session

from app.db import get_db
from app.entitlements import current_entitlements, require_feature
from app.services.analyst_consensus import (
    compare_consensus_payload,
    current_consensus_payload,
    events_payload,
    history_payload,
)
from app.services.analyst_consensus_shadow_review import shadow_review_payload
from app.utils.symbols import normalize_symbol

router = APIRouter(tags=["analyst-consensus"])


def _cache_headers(response: Response, *, seconds: int = 300) -> None:
    response.headers["Cache-Control"] = f"private, max-age={seconds}"


@router.get("/tickers/{symbol}/consensus")
def ticker_consensus(symbol: str, request: Request, response: Response, db: Session = Depends(get_db)):
    entitlements = current_entitlements(request, db)
    _cache_headers(response)
    return current_consensus_payload(
        db,
        symbol,
        include_details=entitlements.has_feature("analyst_consensus_history"),
    )


@router.get("/tickers/{symbol}/consensus/history")
def ticker_consensus_history(
    symbol: str,
    request: Request,
    response: Response,
    days: int | None = Query(default=None, ge=1),
    start_date: date | None = None,
    end_date: date | None = None,
    db: Session = Depends(get_db),
):
    entitlements = current_entitlements(request, db)
    require_feature(
        entitlements,
        "analyst_consensus_history",
        message="Analyst consensus history is included with Premium.",
    )
    _cache_headers(response)
    return history_payload(db, symbol, days=days, start_date=start_date, end_date=end_date)


@router.get("/tickers/{symbol}/consensus/events")
def ticker_consensus_events(
    symbol: str,
    request: Request,
    response: Response,
    limit: int = Query(default=100, ge=1, le=500),
    start_date: date | None = None,
    end_date: date | None = None,
    action: str | None = None,
    db: Session = Depends(get_db),
):
    entitlements = current_entitlements(request, db)
    require_feature(
        entitlements,
        "analyst_consensus_history",
        message="Analyst grade-event history is included with Premium.",
    )
    _cache_headers(response)
    return events_payload(db, symbol, limit=limit, start_date=start_date, end_date=end_date, action=action)


@router.get("/compare/consensus")
def compare_consensus(
    response: Response,
    request: Request,
    symbols: str = Query(..., min_length=1),
    db: Session = Depends(get_db),
):
    entitlements = current_entitlements(request, db)
    parsed = [normalize_symbol(part) for part in symbols.split(",")]
    payload = compare_consensus_payload(
        db,
        [symbol for symbol in parsed if symbol],
        include_details=entitlements.has_feature("analyst_consensus_history"),
    )
    _cache_headers(response)
    return payload


@router.get("/analyst-consensus/shadow-review")
def analyst_consensus_shadow_review(
    response: Response,
    request: Request,
    symbols: str | None = Query(default=None),
    days: int = Query(default=365, ge=1, le=1825),
    horizon_days: int = Query(default=30, ge=1, le=365),
    max_snapshots: int = Query(default=5000, ge=1, le=25000),
    db: Session = Depends(get_db),
):
    entitlements = current_entitlements(request, db)
    require_feature(
        entitlements,
        "analyst_consensus_history",
        message="Analyst consensus shadow review is included with Premium.",
    )
    parsed = [normalize_symbol(part) for part in (symbols or "").split(",")]
    _cache_headers(response, seconds=900)
    return shadow_review_payload(
        db,
        symbols=[symbol for symbol in parsed if symbol],
        days=days,
        horizon_days=horizon_days,
        max_snapshots=max_snapshots,
    )
