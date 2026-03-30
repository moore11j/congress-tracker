from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import logging

from fastapi import APIRouter, Depends, Query
from sqlalchemy import Float, Integer, String, bindparam, func, literal, select, text, union_all
from sqlalchemy.orm import Session

from app.db import get_db
from app.models import Event
from app.schemas import (
    InsiderSignalOut,
    UnifiedSignalOut,
    UnusualSignalOut,
    UnusualSignalsDebug,
    UnusualSignalsResponseDebug,
)
from app.services.signal_score import calculate_smart_score
from app.services.confirmation_metrics import get_confirmation_metrics_for_symbols
from app.services.ticker_meta import normalize_cik

router = APIRouter(tags=["signals"])
logger = logging.getLogger(__name__)

MAX_LIMIT = 500
PRESET_DEFAULT = "balanced"
PRESETS = {
    "discovery": {
        "baseline_days": 365,
        "recent_days": 60,
        "multiple": 1.25,
        "min_amount": 2_500,
        "min_baseline_count": 1,
        "limit": 100,
    },
    "balanced": {
        "baseline_days": 365,
        "recent_days": 180,
        "multiple": 1.75,
        "min_amount": 10_000,
        "min_baseline_count": 3,
        "limit": 100,
    },
    "strict": {
        "baseline_days": 365,
        "recent_days": 90,
        "multiple": 2.8,
        "min_amount": 50_000,
        "min_baseline_count": 5,
        "limit": 100,
    },
}

INSIDER_DEFAULTS = {
    "recent_days": 60,
    "multiple": 1.5,
    "min_amount": 10_000,
    "min_baseline_count": 3,
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




def _insider_baseline_median_subquery(baseline_since: datetime):
    median_cte = text(
        """
        SELECT
            symbol,
            AVG(amount_max) AS median_amount_max,
            COUNT(*) AS baseline_count
        FROM events
        WHERE event_type = 'insider_trade'
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


def _insider_reporting_name(payload_json: str | None) -> str | None:
    if not payload_json:
        return None
    try:
        payload = json.loads(payload_json)
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    v = payload.get("insider_name")
    if isinstance(v, str) and v.strip():
        return v.strip()
    raw = payload.get("raw")
    if isinstance(raw, dict):
        rn = raw.get("reportingName")
        if isinstance(rn, str) and rn.strip():
            return rn.strip()
    return None


def _insider_position(payload_json: str | None) -> str | None:
    if not payload_json:
        return None
    try:
        payload = json.loads(payload_json)
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    v = payload.get("role")
    if isinstance(v, str) and v.strip():
        return v.strip()
    raw = payload.get("raw")
    if isinstance(raw, dict):
        for key in ("officerTitle", "insiderRole", "position", "typeOfOwner"):
            vv = raw.get(key)
            if isinstance(vv, str) and vv.strip():
                return vv.strip()
    return None


def _insider_reporting_cik(payload_json: str | None) -> str | None:
    if not payload_json:
        return None
    try:
        payload = json.loads(payload_json)
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    raw = payload.get("raw") if isinstance(payload.get("raw"), dict) else {}
    return normalize_cik(
        payload.get("reporting_cik")
        or payload.get("reportingCik")
        or raw.get("reportingCik")
        or raw.get("reportingCIK")
        or raw.get("rptOwnerCik")
    )


def _query_insider_signals(
    *,
    db: Session,
    recent_days: int,
    baseline_days: int,
    min_baseline_count: int,
    multiple: float,
    min_amount: float,
    limit: int,
    offset: int = 0,
    sort: str = "multiple",
) -> list[InsiderSignalOut]:
    now = datetime.now(timezone.utc)
    recent_since = now - timedelta(days=recent_days)
    baseline_since = now - timedelta(days=baseline_days)

    median_subquery = _insider_baseline_median_subquery(baseline_since)
    unusual_multiple = (Event.amount_max / median_subquery.c.median_amount_max).label(
        "unusual_multiple"
    )

    base = (
        select(
            Event.id.label("event_id"),
            Event.ts,
            Event.symbol,
            Event.member_name,
            Event.trade_type,
            Event.amount_min,
            Event.amount_max,
            Event.source,
            Event.payload_json,
            median_subquery.c.median_amount_max.label("baseline_median_amount_max"),
            median_subquery.c.baseline_count,
            unusual_multiple,
        )
        .join(median_subquery, median_subquery.c.symbol == Event.symbol)
        .where(Event.event_type == "insider_trade")
        .where(Event.ts >= recent_since)
        .where(Event.amount_max.is_not(None))
        .where(Event.amount_max >= min_amount)
        .where(median_subquery.c.median_amount_max.is_not(None))
        .where(median_subquery.c.median_amount_max > 0)
        .where(median_subquery.c.baseline_count >= min_baseline_count)
        .where(unusual_multiple >= multiple)
    )

    if sort == "recent":
        ordered = base.order_by(Event.ts.desc(), unusual_multiple.desc())
    elif sort == "amount":
        ordered = base.order_by(Event.amount_max.desc(), unusual_multiple.desc(), Event.ts.desc())
    else:  # "multiple"
        ordered = base.order_by(unusual_multiple.desc(), Event.ts.desc())

    rows = db.execute(ordered.offset(offset).limit(limit)).all()

    items = []
    for row in rows:
        smart_score, smart_band = calculate_smart_score(
            unusual_multiple=row.unusual_multiple,
            amount_max=row.amount_max,
            ts=row.ts,
        )
        insider_name = _insider_reporting_name(row.payload_json) or row.member_name
        items.append(
            InsiderSignalOut(
                event_id=row.event_id,
                ts=row.ts,
                symbol=row.symbol,
                insider_name=insider_name,
                trade_type=row.trade_type,
                amount_min=row.amount_min,
                amount_max=row.amount_max,
                baseline_median_amount_max=row.baseline_median_amount_max,
                baseline_count=row.baseline_count,
                unusual_multiple=row.unusual_multiple,
                smart_score=smart_score,
                smart_band=smart_band,
                source=row.source,
            )
        )
    return items


def _query_unified_signals(
    *,
    db: Session,
    mode: str,
    sort: str,
    limit: int,
    offset: int,
    baseline_days: int,
    congress_recent_days: int,
    insider_recent_days: int,
    congress_min_baseline_count: int,
    insider_min_baseline_count: int,
    congress_multiple: float,
    insider_multiple: float,
    congress_min_amount: float,
    insider_min_amount: float,
    min_smart_score: int | None,
    side: str,
    symbol: str | None,
) -> list[UnifiedSignalOut]:
    now = datetime.now(timezone.utc)
    baseline_since = now - timedelta(days=baseline_days)
    congress_recent_since = now - timedelta(days=congress_recent_days)
    insider_recent_since = now - timedelta(days=insider_recent_days)

    congress_baseline = _baseline_median_subquery(baseline_since)
    insider_baseline = _insider_baseline_median_subquery(baseline_since)

    congress_unusual_multiple = (
        Event.amount_max / congress_baseline.c.median_amount_max
    ).label("unusual_multiple")
    insider_unusual_multiple = (
        Event.amount_max / insider_baseline.c.median_amount_max
    ).label("unusual_multiple")

    congress_select = (
        select(
            literal("congress").label("kind"),
            Event.id.label("event_id"),
            Event.ts.label("ts"),
            Event.symbol.label("symbol"),
            Event.member_name.label("who"),
            Event.member_bioguide_id.label("member_bioguide_id"),
            Event.party.label("party"),
            Event.chamber.label("chamber"),
            Event.trade_type.label("trade_type"),
            Event.amount_min.label("amount_min"),
            Event.amount_max.label("amount_max"),
            congress_baseline.c.median_amount_max.label("baseline_median_amount_max"),
            congress_baseline.c.baseline_count.label("baseline_count"),
            congress_unusual_multiple.label("unusual_multiple"),
            Event.source.label("source"),
            Event.payload_json.label("payload_json"),
            literal(None).cast(String).label("reporting_cik"),
        )
        .join(congress_baseline, congress_baseline.c.symbol == Event.symbol)
        .where(Event.event_type == "congress_trade")
        .where(Event.ts >= congress_recent_since)
        .where(Event.amount_max.is_not(None))
        .where(Event.amount_max >= congress_min_amount)
        .where(congress_baseline.c.median_amount_max.is_not(None))
        .where(congress_baseline.c.median_amount_max > 0)
        .where(congress_baseline.c.baseline_count >= congress_min_baseline_count)
        .where(congress_unusual_multiple >= congress_multiple)
    )

    if symbol:
        congress_select = congress_select.where(func.upper(Event.symbol) == symbol)

    insider_select = (
        select(
            literal("insider").label("kind"),
            Event.id.label("event_id"),
            Event.ts.label("ts"),
            Event.symbol.label("symbol"),
            Event.member_name.label("who"),
            literal(None).cast(String).label("member_bioguide_id"),
            literal(None).cast(String).label("party"),
            literal(None).cast(String).label("chamber"),
            Event.trade_type.label("trade_type"),
            Event.amount_min.label("amount_min"),
            Event.amount_max.label("amount_max"),
            insider_baseline.c.median_amount_max.label("baseline_median_amount_max"),
            insider_baseline.c.baseline_count.label("baseline_count"),
            insider_unusual_multiple.label("unusual_multiple"),
            Event.source.label("source"),
            Event.payload_json.label("payload_json"),
            literal(None).cast(String).label("reporting_cik"),
        )
        .join(insider_baseline, insider_baseline.c.symbol == Event.symbol)
        .where(Event.event_type == "insider_trade")
        .where(Event.ts >= insider_recent_since)
        .where(Event.amount_max.is_not(None))
        .where(Event.amount_max >= insider_min_amount)
        .where(insider_baseline.c.median_amount_max.is_not(None))
        .where(insider_baseline.c.median_amount_max > 0)
        .where(insider_baseline.c.baseline_count >= insider_min_baseline_count)
        .where(insider_unusual_multiple >= insider_multiple)
    )

    if symbol:
        insider_select = insider_select.where(func.upper(Event.symbol) == symbol)

    union_sq = union_all(congress_select, insider_select).subquery()

    query = select(union_sq)
    if mode == "congress":
        query = query.where(union_sq.c.kind == "congress")
    elif mode == "insider":
        query = query.where(union_sq.c.kind == "insider")

    t = func.lower(func.trim(func.coalesce(union_sq.c.trade_type, "")))
    if side == "buy":
        query = query.where(t.in_(["purchase", "buy", "p-purchase"]))
    elif side == "sell":
        query = query.where(t.in_(["sale", "sell", "s-sale"]))
    elif side == "buy_or_sell":
        query = query.where(t.in_(["purchase", "buy", "p-purchase", "sale", "sell", "s-sale"]))
    elif side == "award":
        query = query.where(t.like("a-%") | t.like("%award%"))
    elif side == "inkind":
        query = query.where(t.like("f-%") | t.like("%inkind%"))
    elif side == "exempt":
        query = query.where(t.like("m-%") | t.like("%exempt%"))

    if sort == "recent":
        query = query.order_by(union_sq.c.ts.desc(), union_sq.c.unusual_multiple.desc())
    elif sort == "amount":
        query = query.order_by(
            union_sq.c.amount_max.desc(),
            union_sq.c.unusual_multiple.desc(),
            union_sq.c.ts.desc(),
        )
    elif sort == "multiple":
        query = query.order_by(union_sq.c.unusual_multiple.desc(), union_sq.c.ts.desc())
    else:  # sort == "smart"
        # Preorder by strongest candidates, then compute smart_score in Python and resort
        query = query.order_by(
            union_sq.c.unusual_multiple.desc(),
            union_sq.c.amount_max.desc(),
            union_sq.c.ts.desc(),
        )

    fetch_limit = min(MAX_LIMIT, max(limit + offset, limit * 3, 100))
    rows = db.execute(query.limit(fetch_limit)).all()
    confirmation_metrics_by_symbol = get_confirmation_metrics_for_symbols(
        db,
        [row.symbol for row in rows if row.symbol],
    )

    items: list[UnifiedSignalOut] = []
    for row in rows:
        who = row.who
        position = None
        reporting_cik = None
        if row.kind == "insider":
            who = _insider_reporting_name(row.payload_json) or who
            position = _insider_position(row.payload_json)
            reporting_cik = _insider_reporting_cik(row.payload_json)

        smart_score, smart_band = calculate_smart_score(
            unusual_multiple=row.unusual_multiple,
            amount_max=row.amount_max,
            ts=row.ts,
        )

        if min_smart_score is not None and smart_score < min_smart_score:
            continue

        items.append(
            UnifiedSignalOut(
                kind=row.kind,
                event_id=row.event_id,
                ts=row.ts,
                symbol=row.symbol,
                who=who,
                position=position,
                reporting_cik=reporting_cik,
                reportingCik=reporting_cik,
                member_bioguide_id=row.member_bioguide_id,
                party=row.party,
                chamber=row.chamber,
                trade_type=row.trade_type,
                amount_min=row.amount_min,
                amount_max=row.amount_max,
                baseline_median_amount_max=row.baseline_median_amount_max,
                baseline_count=row.baseline_count,
                unusual_multiple=row.unusual_multiple,
                smart_score=smart_score,
                smart_band=smart_band,
                source=row.source,
                confirmation_30d=(
                    confirmation_metrics_by_symbol[row.symbol].as_dict()
                    if row.symbol in confirmation_metrics_by_symbol
                    else None
                ),
            )
        )

    if sort == "recent":
        items.sort(key=lambda item: (item.ts, item.unusual_multiple), reverse=True)
    elif sort == "amount":
        items.sort(
            key=lambda item: (item.amount_max if item.amount_max is not None else -1, item.unusual_multiple, item.ts),
            reverse=True,
        )
    elif sort == "smart":
        items.sort(key=lambda item: (item.smart_score, item.ts), reverse=True)
    else:
        items.sort(key=lambda item: (item.unusual_multiple, item.ts), reverse=True)

    return items[offset : offset + limit]

def _query_unusual_signals(
    *,
    db: Session,
    recent_days: int,
    baseline_days: int,
    min_baseline_count: int,
    multiple: float,
    min_amount: float,
    limit: int,
    offset: int = 0,
    sort: str = "multiple",
) -> tuple[list[UnusualSignalOut], dict[str, int], int]:
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

    base = (
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
    )

    total_hits = db.execute(select(func.count()).select_from(base.subquery())).scalar_one()

    if sort == "recent":
        ordered = base.order_by(Event.ts.desc(), unusual_multiple.desc())
    elif sort == "amount":
        ordered = base.order_by(Event.amount_max.desc(), unusual_multiple.desc(), Event.ts.desc())
    else:  # "multiple"
        ordered = base.order_by(unusual_multiple.desc(), Event.ts.desc())

    query = ordered.offset(offset).limit(limit)

    rows = db.execute(query).all()
    items = []
    for row in rows:
        smart_score, smart_band = calculate_smart_score(
            unusual_multiple=row.unusual_multiple,
            amount_max=row.amount_max,
            ts=row.ts,
        )
        items.append(
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
                smart_score=smart_score,
                smart_band=smart_band,
                source=row.source,
            )
        )
    return items, {
        "baseline_events_count": baseline_events_count,
        "median_rows_count": median_rows_count,
        "recent_events_count": recent_events_count,
        "symbols_passing_min_baseline_count": symbols_passing_min_baseline_count,
        "final_hits_count": len(items),
    }, total_hits


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
    offset: int = Query(0, ge=0),
    sort: str = Query("multiple", pattern="^(multiple|recent|amount|smart)$"),
    min_smart_score: int | None = Query(None, ge=0, le=100),
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

    items, counts, total_hits = _query_unusual_signals(
        db=db,
        recent_days=effective_recent_days,
        baseline_days=effective_baseline_days,
        min_baseline_count=effective_min_baseline_count,
        multiple=effective_multiple,
        min_amount=effective_min_amount,
        limit=effective_limit,
        offset=offset,
        sort=sort,
    )

    if sort == "smart":
        items.sort(key=lambda item: item.smart_score, reverse=True)

    if min_smart_score is not None:
        items = [item for item in items if item.smart_score >= min_smart_score]

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
            total_hits=total_hits,
            sort=sort,
            offset=offset,
            effective_params={
                "recent_days": effective_recent_days,
                "baseline_days": effective_baseline_days,
                "min_baseline_count": effective_min_baseline_count,
                "multiple": effective_multiple,
                "min_amount": effective_min_amount,
                "limit": effective_limit,
                "preset": (preset_input or PRESET_DEFAULT) if mode == "preset" else "custom",
                "adaptive_baseline": adaptive_baseline,
                "total_hits": total_hits,
                "offset": offset,
                "sort": sort,
                "min_smart_score": min_smart_score,
            },
            adaptive_applied=adaptive_applied,
            **counts,
        ),
    )


@router.get("/signals/all", response_model=list[UnifiedSignalOut])
def list_all_signals(
    db: Session = Depends(get_db),
    mode: str = Query("all", pattern="^(all|congress|insider)$"),
    preset: str | None = Query(None, pattern="^(discovery|balanced|strict)$"),
    sort: str = Query("smart", pattern="^(multiple|recent|amount|smart)$"),
    limit: int = Query(100, ge=1, le=MAX_LIMIT),
    offset: int = Query(0, ge=0),
    baseline_days: int = Query(365, ge=1),
    congress_recent_days: int | None = Query(None, ge=1),
    insider_recent_days: int | None = Query(None, ge=1),
    congress_multiple: float | None = Query(None, ge=1.0),
    insider_multiple: float | None = Query(None, ge=1.0),
    congress_min_amount: float | None = Query(None, ge=0),
    insider_min_amount: float | None = Query(None, ge=0),
    congress_min_baseline_count: int | None = Query(None, ge=1),
    insider_min_baseline_count: int | None = Query(None, ge=1),
    min_smart_score: int | None = Query(None, ge=0, le=100),
    side: str = Query("all", pattern="^(all|buy|sell|buy_or_sell|award|inkind|exempt)$"),
    symbol: str | None = Query(None),
):
    symbol_value = symbol.strip().upper() if isinstance(symbol, str) and symbol.strip() else None
    base_preset = preset or PRESET_DEFAULT
    preset_values = PRESETS[base_preset]

    effective_congress_recent_days = (
        congress_recent_days
        if congress_recent_days is not None
        else preset_values["recent_days"]
    )
    effective_congress_multiple = (
        congress_multiple if congress_multiple is not None else preset_values["multiple"]
    )
    effective_congress_min_amount = (
        congress_min_amount if congress_min_amount is not None else preset_values["min_amount"]
    )
    effective_congress_min_baseline_count = (
        congress_min_baseline_count
        if congress_min_baseline_count is not None
        else preset_values["min_baseline_count"]
    )

    effective_insider_recent_days = (
        insider_recent_days
        if insider_recent_days is not None
        else INSIDER_DEFAULTS["recent_days"]
    )
    effective_insider_multiple = (
        insider_multiple if insider_multiple is not None else INSIDER_DEFAULTS["multiple"]
    )
    effective_insider_min_amount = (
        insider_min_amount if insider_min_amount is not None else INSIDER_DEFAULTS["min_amount"]
    )
    effective_insider_min_baseline_count = (
        insider_min_baseline_count
        if insider_min_baseline_count is not None
        else INSIDER_DEFAULTS["min_baseline_count"]
    )

    return _query_unified_signals(
        db=db,
        mode=mode,
        sort=sort,
        limit=limit,
        offset=offset,
        baseline_days=baseline_days,
        congress_recent_days=effective_congress_recent_days,
        insider_recent_days=effective_insider_recent_days,
        congress_min_baseline_count=effective_congress_min_baseline_count,
        insider_min_baseline_count=effective_insider_min_baseline_count,
        congress_multiple=effective_congress_multiple,
        insider_multiple=effective_insider_multiple,
        congress_min_amount=effective_congress_min_amount,
        insider_min_amount=effective_insider_min_amount,
        min_smart_score=min_smart_score,
        side=side,
        symbol=symbol_value,
    )


@router.get("/signals/insiders", response_model=list[InsiderSignalOut])
def list_insider_signals(
    db: Session = Depends(get_db),
    baseline_days: int = Query(365, ge=1),
    recent_days: int = Query(60, ge=1),
    multiple: float = Query(1.5, ge=1.0),
    min_amount: float = Query(10000, ge=0),
    min_baseline_count: int = Query(3, ge=1),
    limit: int = Query(100, ge=1, le=MAX_LIMIT),
    offset: int = Query(0, ge=0),
    sort: str = Query("multiple", pattern="^(multiple|recent|amount|smart)$"),
    min_smart_score: int | None = Query(None, ge=0, le=100),
):
    items = _query_insider_signals(
        db=db,
        recent_days=recent_days,
        baseline_days=baseline_days,
        min_baseline_count=min_baseline_count,
        multiple=multiple,
        min_amount=min_amount,
        limit=limit,
        offset=offset,
        sort=sort,
    )

    if sort == "smart":
        items.sort(key=lambda item: item.smart_score, reverse=True)

    if min_smart_score is not None:
        items = [item for item in items if item.smart_score >= min_smart_score]

    return items
