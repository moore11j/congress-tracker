from __future__ import annotations

from datetime import datetime, timedelta, timezone
import logging

from fastapi import APIRouter, Depends, Query
from sqlalchemy import Float, Integer, String, bindparam, func, select, text
from sqlalchemy.orm import Session

from app.db import get_db
from app.models import Event
from app.schemas import (
    UnusualSignalOut,
    UnusualSignalsDebug,
    UnusualSignalsResponseDebug,
)

router = APIRouter(tags=["signals"])
logger = logging.getLogger(__name__)

MAX_LIMIT = 500
PRESET_DEFAULT = "balanced"
PRESETS = {
    "discovery": {
        "baseline_days": 365,
        "recent_days": 365,
        "multiple": 1.02,
        "min_amount": 0,
        "min_baseline_count": 1,
        "limit": 100,
    },
    "balanced": {
        "baseline_days": 365,
        "recent_days": 180,
        "multiple": 2.1,
        "min_amount": 0,
        "min_baseline_count": 1,
        "limit": 100,
    },
    "strict": {
        "baseline_days": 365,
        "recent_days": 90,
        "multiple": 4.0,
        "min_amount": 10_000,
        "min_baseline_count": 3,
        "limit": 100,
    },
}


def _baseline_median_subquery(baseline_since: datetime):
    median_cte = text(
        """
        SELECT
            symbol,
            AVG(amount_max) AS median_amount_max,
            COUNT(*) AS baseline_count
        FROM events
        WHERE event_type = 'congress_trade'
          AND amount_max IS NOT NULL
          AND symbol IS NOT NULL
          AND ts >= :baseline_since
        GROUP BY symbol
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
) -> tuple[list[UnusualSignalOut], dict[str, int]]:
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
    symbols_passing_min_baseline_count = (
        db.execute(
            select(func.count())
            .select_from(median_subquery)
            .where(median_subquery.c.baseline_count >= min_baseline_count)
        ).scalar_one()
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
    items = [
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
    return items, {
        "baseline_events_count": baseline_events_count,
        "median_rows_count": median_rows_count,
        "recent_events_count": recent_events_count,
        "symbols_passing_min_baseline_count": symbols_passing_min_baseline_count,
        "final_hits_count": len(items),
    }


@router.get(
    "/signals/unusual",
    response_model=UnusualSignalsResponseDebug | list[UnusualSignalOut],
)
def list_unusual_signals(
    db: Session = Depends(get_db),
    preset: str | None = Query(None, pattern="^(discovery|balanced|strict)$"),
    debug: bool = Query(False),
    adaptive_baseline: bool = Query(False),
    recent_days: int | None = Query(None, ge=1),
    baseline_days: int | None = Query(None, ge=1),
    min_baseline_count: int | None = Query(None, ge=1),
    multiple: float | None = Query(None, ge=1.0),
    min_amount: float | None = Query(None, ge=0),
    limit: int | None = Query(None, ge=1, le=MAX_LIMIT),
):
    preset_input = preset

    # Only these count as SIGNAL overrides (they change scoring / filtering).
    # IMPORTANT: limit/debug/adaptive_baseline should NOT force "custom" mode.
    signal_overrides = {
        key: value
        for key, value in {
            "recent_days": recent_days,
            "baseline_days": baseline_days,
            "min_baseline_count": min_baseline_count,
            "multiple": multiple,
            "min_amount": min_amount,
        }.items()
        if value is not None
    }

    mode = "custom" if signal_overrides else "preset"
    applied_preset = (preset_input or PRESET_DEFAULT) if mode == "preset" else "custom"

    # In preset mode, use the chosen preset.
    # In custom mode (no preset), start from DEFAULT preset and apply signal overrides.
    base_preset = (preset_input or PRESET_DEFAULT) if mode == "preset" else PRESET_DEFAULT
    preset_values = PRESETS[base_preset]

    effective_recent_days = (
        recent_days if recent_days is not None else preset_values["recent_days"]
    )
    effective_baseline_days = (
        baseline_days if baseline_days is not None else preset_values["baseline_days"]
    )
    min_baseline_explicit = min_baseline_count is not None
    effective_min_baseline_count = (
        min_baseline_count
        if min_baseline_explicit
        else preset_values["min_baseline_count"]
    )
    effective_multiple = multiple if multiple is not None else preset_values["multiple"]
    effective_min_amount = (
        min_amount if min_amount is not None else preset_values["min_amount"]
    )

    effective_limit = limit or preset_values["limit"]
    effective_limit = min(effective_limit, MAX_LIMIT)

    baseline_days_clamped = False
    if effective_baseline_days < effective_recent_days:
        effective_baseline_days = effective_recent_days
        baseline_days_clamped = True

    median_rows_count = None
    adaptive_applied = False
    if mode == "custom" and adaptive_baseline and not min_baseline_explicit:
        min_baseline_before_adaptive = effective_min_baseline_count
        baseline_since = datetime.now(timezone.utc) - timedelta(days=effective_baseline_days)
        median_subquery = _baseline_median_subquery(baseline_since)
        median_rows_count = (
            db.execute(select(func.count()).select_from(median_subquery)).scalar_one()
        )
        if median_rows_count < 50:
            effective_min_baseline_count = 1
        elif median_rows_count < 200:
            effective_min_baseline_count = 3
        adaptive_applied = effective_min_baseline_count != min_baseline_before_adaptive

    logger.info(
        "unusual_signals mode=%s preset=%s recent_days=%s baseline_days=%s "
        "min_baseline_count=%s multiple=%s min_amount=%s limit=%s adaptive_baseline=%s",
        mode,
        applied_preset,
        effective_recent_days,
        effective_baseline_days,
        effective_min_baseline_count,
        effective_multiple,
        effective_min_amount,
        effective_limit,
        adaptive_baseline,
    )

    items, counts = _query_unusual_signals(
        db=db,
        recent_days=effective_recent_days,
        baseline_days=effective_baseline_days,
        min_baseline_count=effective_min_baseline_count,
        multiple=effective_multiple,
        min_amount=effective_min_amount,
        limit=effective_limit,
    )
    if not debug:
        return items

    return UnusualSignalsResponseDebug(
        items=items,
        debug=UnusualSignalsDebug(
            mode=mode,
            applied_preset=(preset_input or PRESET_DEFAULT) if mode == "preset" else "custom",
            preset_input=preset_input,
            overrides=signal_overrides,  # <-- signal overrides only
            baseline_days_clamped=baseline_days_clamped,
            effective_params={
                "recent_days": effective_recent_days,
                "baseline_days": effective_baseline_days,
                "min_baseline_count": effective_min_baseline_count,
                "multiple": effective_multiple,
                "min_amount": effective_min_amount,
                "limit": effective_limit,
                "preset": (preset_input or PRESET_DEFAULT) if mode == "preset" else "custom",
                "adaptive_baseline": adaptive_baseline,
            },
            adaptive_applied=adaptive_applied,
            **counts,
        ),
    )
