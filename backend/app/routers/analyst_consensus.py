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
from app.utils.symbols import normalize_symbol

router = APIRouter(tags=["analyst-consensus"])


def _cache_headers(response: Response, *, seconds: int = 300) -> None:
    response.headers["Cache-Control"] = f"private, max-age={seconds}"


@router.get("/tickers/{symbol}/consensus")
def ticker_consensus(symbol: str, response: Response, db: Session = Depends(get_db)):
    _cache_headers(response)
    return current_consensus_payload(db, symbol)


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
    symbols: str = Query(..., min_length=1),
    db: Session = Depends(get_db),
):
    parsed = [normalize_symbol(part) for part in symbols.split(",")]
    payload = compare_consensus_payload(db, [symbol for symbol in parsed if symbol])
    _cache_headers(response)
    return payload
