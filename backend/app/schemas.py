from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel


class EventOut(BaseModel):
    id: int
    event_type: str
    ts: datetime
    symbol: str | None
    source: str
    member_name: str | None = None
    member_bioguide_id: str | None = None
    party: str | None = None
    chamber: str | None = None
    trade_type: str | None = None
    amount_min: int | None = None
    amount_max: int | None = None
    impact_score: float
    payload: dict


class EventsPage(BaseModel):
    items: list[EventOut]
    next_cursor: str | None


class EventsDebug(BaseModel):
    received_params: dict
    applied_filters: list[str]
    count_after_filters: int
    sql_hint: str | None = None


class EventsPageDebug(EventsPage):
    debug: EventsDebug | None = None
