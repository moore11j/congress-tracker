from __future__ import annotations

from datetime import date, timedelta
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sqlalchemy.orm import Session

from app.auth import current_user
from app.db import get_db
from app.entitlements import current_entitlements, require_feature
from app.services.event_calendar import fetch_event_calendar, watchlist_symbols_for_user

router = APIRouter(tags=["event-calendar"])

CalendarScope = Literal["watchlist", "all"]
MAX_CALENDAR_WINDOW_DAYS = 120


@router.get("/monitoring/event-calendar")
def get_monitoring_event_calendar(
    request: Request,
    db: Session = Depends(get_db),
    start: date = Query(...),
    end: date = Query(...),
    scope: CalendarScope = "watchlist",
):
    user = current_user(db, request, required=True)
    require_feature(
        current_entitlements(request, db),
        "event_calendar",
        message="Earnings and event calendar overlays are included with Premium.",
    )
    if end < start:
        raise HTTPException(status_code=422, detail="end must be on or after start.")
    if end - start > timedelta(days=MAX_CALENDAR_WINDOW_DAYS):
        raise HTTPException(status_code=422, detail=f"Calendar windows are limited to {MAX_CALENDAR_WINDOW_DAYS} days.")

    result = fetch_event_calendar(db, user, start=start, end=end, scope=scope, source="page_load", allow_live_fetch=True)
    watchlist_symbols = watchlist_symbols_for_user(db, user.id)
    return {
        "items": result.items,
        "errors": result.errors,
        "status": "partial" if result.errors and result.items else "unavailable" if result.errors else "ok",
        "scope": scope,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "watchlist_symbols": watchlist_symbols,
    }
