from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel


class EventOut(BaseModel):
    id: int
    event_type: str
    ts: datetime
    ticker: str
    source: str
    headline: str | None
    summary: str | None
    url: str | None
    impact_score: float
    payload: dict


class EventsPage(BaseModel):
    items: list[EventOut]
    next_cursor: str | None
