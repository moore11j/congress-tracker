from __future__ import annotations

from datetime import datetime, timedelta, timezone
import logging

from fastapi import APIRouter, Depends, Query
from sqlalchemy import Float, Integer, String, bindparam, func, select, text
from sqlalchemy.orm import Session

from app.db import get_db
from app.models import Event
from app.schemas import UnusualSignalOut

router = APIRouter(tags=["signals"])
logger = logging.getLogger(__name__)

DEFAULT_RECENT_DAYS = 14
DEFAULT_BASELINE_DAYS = 60
DEFAULT_MULTIPLE = 5.0
DEFAULT_MIN_AMOUNT = 10_000
DEFAULT_LIMIT = 100
MAX_LIMIT = 500


def _baseline_median_subquery(baseline_since: datetime):
    median_cte = text(
        """
        WITH baseline AS (
            SELECT
                symbol,
                amount_max,
                ROW_NUMBER() OVER (
                    PARTITION BY symbol
                    ORDER BY amount_max
                ) AS rn,
                COUNT(*) OVER (
                    PARTITION BY symbol
                ) AS cnt
            FROM events
            WHERE event_type = 'congress_trade'
              AND amount_max IS NOT NULL
              AND symbol IS NOT NULL
              AND ts >= :baseline_since
        ),
        median AS (
            SELECT
                symbol,
                AVG(amount_max) AS median_amount_max,
                MAX(cnt) AS baseline_count
            FROM baseline
            WHERE rn IN (
                CAST((cnt + 1) / 2 AS INT),
                CAST((cnt + 2) / 2 AS INT)
            )
            GROUP BY symbol
        )
        SELECT symbol, median_amount_max, baseline_count
        FROM median
        """
    ).bindparams(bindparam("baseline_since", baseline_since))

    return median_cte.columns(
        symbol=String,
        median_amount_max=Float,
        baseline_count=Integer,
    ).subquery()


def _query_unusual_signals(
    *,
    db: Session,
    recent_days: int,
    baseline_days: int,
    min_baseline_count: int,
    multiple: float,
    min_amount: float,
    limit: int,
) -> list[UnusualSignalOut]:
    """Return congress trades with unusually large flows relative to baseline."""
    now = datetime.now(timezone.utc)
    recent_since = now - timedelta(days=recent_days)
    baseline_since = now - timedelta(days=baseline_days)

    median_subquery = _baseline_median_subquery(baseline_since)
    unusual_multiple = (Event.amount_max / median_subquery.c.median_amount_max).label(
        "unusual_multiple"
    )

    baseline_events_count = (
        db.execute(
            select(func.count())
            .select_from(Event)
            .where(Event.event_type == "congress_trade")
            .where(Event.amount_max.is_not(None))
            .where(Event.symbol.is_not(None))
            .where(Event.ts >= baseline_since)
        )
        .scalar_one()
    )
    median_rows_count = (
        db.execute(select(func.count()).select_from(median_subquery)).scalar_one()
    )
    recent_events_count = (
        db.execute(
            select(func.count())
            .select_from(Event)
            .where(Event.event_type == "congress_trade")
            .where(Event.amount_max.is_not(None))
            .where(Event.symbol.is_not(None))
            .where(Event.ts >= recent_since)
            .where(Event.amount_max >= min_amount)
        )
        .scalar_one()
    )

    logger.info(
        "unusual_signals recent_since=%s baseline_since=%s baseline_events=%s "
        "median_rows=%s recent_events=%s",
        recent_since,
        baseline_since,
        baseline_events_count,
        median_rows_count,
        recent_events_count,
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


@router.get("/signals/unusual", response_model=list[UnusualSignalOut])
def list_unusual_signals(
    db: Session = Depends(get_db),
    recent_days: int = Query(DEFAULT_RECENT_DAYS, ge=1),
    baseline_days: int = Query(DEFAULT_BASELINE_DAYS, ge=1),
    min_baseline_count: int = Query(3, ge=1),
    multiple: float = Query(DEFAULT_MULTIPLE, ge=1.0),
    min_amount: float = Query(DEFAULT_MIN_AMOUNT, ge=0),
    limit: int = Query(DEFAULT_LIMIT, ge=1, le=MAX_LIMIT),
):
    return _query_unusual_signals(
        db=db,
        recent_days=recent_days,
        baseline_days=baseline_days,
        min_baseline_count=min_baseline_count,
        multiple=multiple,
        min_amount=min_amount,
        limit=limit,
    )
