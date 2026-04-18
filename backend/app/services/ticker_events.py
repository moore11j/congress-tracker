from __future__ import annotations

from datetime import datetime

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.models import Event
from app.services.event_activity_filters import insider_visibility_clause
from app.utils.symbols import normalize_symbol

TICKER_EVENT_TYPES = ("congress_trade", "insider_trade")


def ticker_event_timestamp_expr():
    return func.coalesce(Event.event_date, Event.ts)


def ticker_event_date_key(event: Event) -> str | None:
    value = event.event_date or event.ts
    if value is None:
        return None
    return value.date().isoformat() if isinstance(value, datetime) else str(value)[:10]


def ticker_event_visibility_clause():
    return insider_visibility_clause()


def select_visible_ticker_events(
    db: Session,
    *,
    symbol: str,
    since: datetime,
    limit: int,
) -> list[Event]:
    normalized_symbol = normalize_symbol(symbol)
    if not normalized_symbol:
        return []

    event_ts = ticker_event_timestamp_expr()
    return db.execute(
        select(Event)
        .where(Event.symbol.is_not(None))
        .where(func.upper(Event.symbol) == normalized_symbol)
        .where(Event.event_type.in_(TICKER_EVENT_TYPES))
        .where(ticker_event_visibility_clause())
        .where(event_ts >= since)
        .order_by(event_ts.desc(), Event.id.desc())
        .limit(limit)
    ).scalars().all()

