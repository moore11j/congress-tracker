from __future__ import annotations

from datetime import datetime

from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session

from app.models import Event, GovernmentContractAction
from app.services.event_activity_filters import insider_visibility_clause
from app.utils.symbols import normalize_symbol

GOVERNMENT_CONTRACT_EVENT_TYPES = (
    "government_contract",
    "government_contract_award",
    "contract_award",
    "government_exposure",
)
TICKER_EVENT_TYPES = ("congress_trade", "insider_trade", *GOVERNMENT_CONTRACT_EVENT_TYPES)


def ticker_event_timestamp_expr():
    return func.coalesce(Event.event_date, Event.ts)


def ticker_event_date_key(event: Event) -> str | None:
    value = event.event_date or event.ts
    if value is None:
        return None
    return value.date().isoformat() if isinstance(value, datetime) else str(value)[:10]


def ticker_event_visibility_clause():
    action_event_ids = select(GovernmentContractAction.event_id).where(GovernmentContractAction.event_id.is_not(None))
    return (
        insider_visibility_clause(),
        or_(
            Event.event_type.notin_(GOVERNMENT_CONTRACT_EVENT_TYPES),
            Event.id.in_(action_event_ids),
        ),
    )


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
        .where(*ticker_event_visibility_clause())
        .where(event_ts >= since)
        .order_by(event_ts.desc(), Event.id.desc())
        .limit(limit)
    ).scalars().all()
