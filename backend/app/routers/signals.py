from __future__ import annotations

from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, Query
from sqlalchemy import Integer, cast, func, select
from sqlalchemy.orm import Session

from app.db import get_db
from app.models import Event
from app.schemas import UnusualSignalOut

router = APIRouter(tags=["signals"])

DEFAULT_RECENT_DAYS = 14
DEFAULT_BASELINE_DAYS = 60
DEFAULT_MULTIPLE = 5.0
DEFAULT_MIN_AMOUNT = 10_000
DEFAULT_LIMIT = 100
MAX_LIMIT = 500


def _baseline_median_subquery(baseline_since: datetime):
    baseline_events = (
        select(
            Event.symbol.label("symbol"),
            Event.amount_max.label("amount_max"),
            func.row_number()
            .over(partition_by=Event.symbol, order_by=Event.amount_max)
            .label("rn"),
            func.count().over(partition_by=Event.symbol).label("cnt"),
        )
        .where(Event.event_type == "congress_trade")
        .where(Event.amount_max.is_not(None))
        .where(Event.symbol.is_not(None))
        .where(Event.ts >= baseline_since)
        .subquery()
    )

    lower_index = cast((baseline_events.c.cnt + 1) / 2, Integer)
    upper_index = cast((baseline_events.c.cnt + 2) / 2, Integer)

    return (
        select(
            baseline_events.c.symbol.label("symbol"),
            func.avg(baseline_events.c.amount_max).label("median_amount_max"),
            func.max(baseline_events.c.cnt).label("baseline_count"),
        )
        .where(baseline_events.c.rn.in_([lower_index, upper_index]))
        .group_by(baseline_events.c.symbol)
        .subquery()
    )


@router.get("/signals/unusual", response_model=list[UnusualSignalOut])
def list_unusual_signals(
    db: Session = Depends(get_db),
    recent_days: int = Query(DEFAULT_RECENT_DAYS, ge=1),
    baseline_days: int = Query(DEFAULT_BASELINE_DAYS, ge=1),
    min_baseline_count: int = Query(10, ge=1),
    multiple: float = Query(DEFAULT_MULTIPLE, ge=1.0),
    min_amount: float = Query(DEFAULT_MIN_AMOUNT, ge=0),
    limit: int = Query(DEFAULT_LIMIT, ge=1, le=MAX_LIMIT),
):
    """Return congress trades with unusually large flows relative to baseline."""
    now = datetime.now(timezone.utc)
    recent_since = now - timedelta(days=recent_days)
    baseline_since = now - timedelta(days=baseline_days)

    median_subquery = _baseline_median_subquery(baseline_since)
    unusual_multiple = (Event.amount_max / median_subquery.c.median_amount_max).label(
        "unusual_multiple"
    )

    query = (
        select(
            Event.id.label("event_id"),
            Event.ts,
            Event.symbol,
            Event.member_name,
            Event.member_bioguide_id,
            Event.party,
            Event.chamber,
            Event.trade_type,
            Event.amount_min,
            Event.amount_max,
            Event.source,
            median_subquery.c.median_amount_max.label("baseline_median_amount_max"),
            median_subquery.c.baseline_count,
            unusual_multiple,
        )
        .join(median_subquery, median_subquery.c.symbol == Event.symbol)
        .where(Event.event_type == "congress_trade")
        .where(Event.ts >= recent_since)
        .where(Event.amount_max.is_not(None))
        .where(Event.amount_max >= min_amount)
        .where(median_subquery.c.median_amount_max.is_not(None))
        .where(median_subquery.c.median_amount_max > 0)
        .where(median_subquery.c.baseline_count >= min_baseline_count)
        .where(unusual_multiple >= multiple)
        .order_by(unusual_multiple.desc(), Event.ts.desc())
        .limit(limit)
    )

    rows = db.execute(query).all()
    return [
        UnusualSignalOut(
            event_id=row.event_id,
            ts=row.ts,
            symbol=row.symbol,
            member_name=row.member_name,
            member_bioguide_id=row.member_bioguide_id,
            party=row.party,
            chamber=row.chamber,
            trade_type=row.trade_type,
            amount_min=row.amount_min,
            amount_max=row.amount_max,
            baseline_median_amount_max=row.baseline_median_amount_max,
            baseline_count=row.baseline_count,
            unusual_multiple=row.unusual_multiple,
            source=row.source,
        )
        for row in rows
    ]
