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
    estimated_price: float | None = None
    current_price: float | None = None
    pnl_pct: float | None = None


class EventsPage(BaseModel):
    items: list[EventOut]
    next_cursor: str | None = None
    total: int | None = None
    limit: int | None = None
    offset: int | None = None


class EventsDebug(BaseModel):
    received_params: dict
    applied_filters: list[str]
    count_after_filters: int
    sql_hint: str | None = None


class EventsPageDebug(EventsPage):
    debug: EventsDebug | None = None


class UnusualSignalOut(BaseModel):
    event_id: int
    ts: datetime
    symbol: str | None
    member_name: str | None = None
    member_bioguide_id: str | None = None
    party: str | None = None
    chamber: str | None = None
    trade_type: str | None = None
    amount_min: int | None = None
    amount_max: int | None = None
    baseline_median_amount_max: float
    baseline_count: int
    unusual_multiple: float
    source: str


class UnusualSignalsDebug(BaseModel):
    total_hits: int | None = None
    sort: str | None = None
    offset: int | None = None
    mode: str
    applied_preset: str | None
    preset_input: str | None
    overrides: dict[str, int | float]
    baseline_days_clamped: bool
    effective_params: dict
    adaptive_applied: bool
    baseline_events_count: int
    median_rows_count: int
    recent_events_count: int
    symbols_passing_min_baseline_count: int
    final_hits_count: int


class UnusualSignalsResponseDebug(BaseModel):
    items: list[UnusualSignalOut]
    debug: UnusualSignalsDebug
