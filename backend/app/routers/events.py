from __future__ import annotations

import json
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import and_, or_, select
from sqlalchemy.orm import Session

from app.db import get_db
from app.models import Event, Security, WatchlistItem
from app.schemas import EventOut, EventsPage

router = APIRouter(tags=["events"])

DEFAULT_LIMIT = 50
MAX_LIMIT = 200


def _normalize_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _parse_iso_datetime(value: str) -> datetime:
    cleaned = value.strip()
    if cleaned.endswith("Z"):
        cleaned = cleaned[:-1] + "+00:00"
    parsed = datetime.fromisoformat(cleaned)
    return _normalize_datetime(parsed)


def _parse_since(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return _parse_iso_datetime(value)
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Invalid since datetime.") from exc


def _parse_cursor(cursor: str) -> tuple[datetime, int]:
    try:
        ts_str, id_str = cursor.split("|", 1)
        cursor_id = int(id_str)
        cursor_ts = _parse_iso_datetime(ts_str)
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Invalid cursor format. Expected ts|id") from exc
    return cursor_ts, cursor_id


def _parse_csv(value: str | None) -> list[str]:
    if not value:
        return []
    return [part.strip() for part in value.split(",") if part.strip()]


def _event_payload(event: Event) -> EventOut:
    try:
        payload = json.loads(event.payload_json)
        if not isinstance(payload, dict):
            payload = {}
    except Exception:
        payload = {}

    return EventOut(
        id=event.id,
        event_type=event.event_type,
        ts=event.ts,
        ticker=event.ticker,
        source=event.source,
        headline=event.headline,
        summary=event.summary,
        url=event.url,
        impact_score=event.impact_score,
        payload=payload,
    )


def _build_events_query(
    *,
    tickers: list[str],
    types: list[str],
    since: datetime | None,
    cursor: str | None,
    limit: int,
):
    q = select(Event)

    if tickers:
        q = q.where(Event.ticker.in_(tickers))

    if types:
        q = q.where(Event.event_type.in_(types))

    if since is not None:
        q = q.where(Event.ts >= since)

    if cursor:
        cursor_ts, cursor_id = _parse_cursor(cursor)
        q = q.where(
            or_(
                Event.ts < cursor_ts,
                and_(Event.ts == cursor_ts, Event.id < cursor_id),
            )
        )

    q = q.order_by(Event.ts.desc(), Event.id.desc()).limit(limit + 1)
    return q


def _fetch_events_page(db: Session, q, limit: int) -> EventsPage:
    rows = db.execute(q).scalars().all()
    items = [_event_payload(event) for event in rows[:limit]]

    next_cursor = None
    if len(rows) > limit:
        last = rows[limit - 1]
        next_cursor = f"{last.ts.isoformat()}|{last.id}"

    return EventsPage(items=items, next_cursor=next_cursor)


@router.get("/events", response_model=EventsPage)
def list_events(
    db: Session = Depends(get_db),
    tickers: str | None = None,
    types: str | None = None,
    since: str | None = None,
    cursor: str | None = None,
    limit: int = Query(DEFAULT_LIMIT, ge=1, le=MAX_LIMIT),
):
    ticker_list = [ticker.upper() for ticker in _parse_csv(tickers)]
    type_list = [event_type.strip().lower() for event_type in _parse_csv(types)]
    since_dt = _parse_since(since)

    q = _build_events_query(
        tickers=ticker_list,
        types=type_list,
        since=since_dt,
        cursor=cursor,
        limit=limit,
    )
    return _fetch_events_page(db, q, limit)


@router.get("/tickers/{symbol}/events", response_model=EventsPage)
def list_ticker_events(
    symbol: str,
    db: Session = Depends(get_db),
    types: str | None = None,
    since: str | None = None,
    cursor: str | None = None,
    limit: int = Query(DEFAULT_LIMIT, ge=1, le=MAX_LIMIT),
):
    ticker_list = [symbol.strip().upper()]
    type_list = [event_type.strip().lower() for event_type in _parse_csv(types)]
    since_dt = _parse_since(since)

    q = _build_events_query(
        tickers=ticker_list,
        types=type_list,
        since=since_dt,
        cursor=cursor,
        limit=limit,
    )
    return _fetch_events_page(db, q, limit)


@router.get("/watchlists/{watchlist_id}/events", response_model=EventsPage)
def list_watchlist_events(
    watchlist_id: int,
    db: Session = Depends(get_db),
    types: str | None = None,
    since: str | None = None,
    cursor: str | None = None,
    limit: int = Query(DEFAULT_LIMIT, ge=1, le=MAX_LIMIT),
):
    symbols = (
        db.execute(
            select(Security.symbol)
            .join(WatchlistItem, WatchlistItem.security_id == Security.id)
            .where(WatchlistItem.watchlist_id == watchlist_id)
        )
        .scalars()
        .all()
    )

    if not symbols:
        return EventsPage(items=[], next_cursor=None)

    ticker_list = [symbol.upper() for symbol in symbols if symbol]
    type_list = [event_type.strip().lower() for event_type in _parse_csv(types)]
    since_dt = _parse_since(since)

    q = _build_events_query(
        tickers=ticker_list,
        types=type_list,
        since=since_dt,
        cursor=cursor,
        limit=limit,
    )
    return _fetch_events_page(db, q, limit)
