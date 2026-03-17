from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.models import Event, TradeOutcome
from app.services.member_performance import compute_congress_trade_outcomes


def _parse_iso_date(value: object) -> date | None:
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return datetime.strptime(value[:10], "%Y-%m-%d").date()
        except ValueError:
            return None
    return None


_CONGRESS_RETRYABLE_STATUSES = {
    "no_current_price",
    "no_benchmark_current",
    "provider_402",
    "provider_429",
    "provider_unavailable",
}


def ensure_member_congress_trade_outcomes(
    db: Session,
    member_ids: list[str],
    lookback_days: int,
    benchmark_symbol: str = "^GSPC",
    max_events: int = 500,
) -> dict[str, int]:
    """Backfill/recompute recent congress trade outcomes for member analytics freshness.

    This targets only congress events for the requested member IDs and is safe to call
    right before analytics reads. It persists both ok and non-ok scoring statuses so
    API counts can distinguish "not yet scoreable" from "missing outcome rows".
    """

    unique_member_ids = [value for value in sorted(set(member_ids)) if value]
    if not unique_member_ids:
        return {"scanned_events": 0, "computed": 0, "inserted": 0, "updated": 0}

    cutoff_dt = datetime.utcnow() - timedelta(days=lookback_days)
    sort_ts = func.coalesce(Event.event_date, Event.ts)
    candidate_events = db.execute(
        select(Event)
        .where(Event.event_type == "congress_trade")
        .where(Event.member_bioguide_id.in_(unique_member_ids))
        .where(sort_ts >= cutoff_dt)
        .order_by(sort_ts.desc(), Event.id.desc())
        .limit(max_events)
    ).scalars().all()

    if not candidate_events:
        return {"scanned_events": 0, "computed": 0, "inserted": 0, "updated": 0}

    event_ids = [event.id for event in candidate_events]
    existing_rows = db.execute(select(TradeOutcome).where(TradeOutcome.event_id.in_(event_ids))).scalars().all()
    existing_by_event_id = {row.event_id: row for row in existing_rows}

    events_to_compute: list[Event] = []
    for event in candidate_events:
        existing = existing_by_event_id.get(event.id)
        if existing is None:
            events_to_compute.append(event)
            continue
        if existing.scoring_status in _CONGRESS_RETRYABLE_STATUSES:
            events_to_compute.append(event)

    if not events_to_compute:
        return {"scanned_events": len(candidate_events), "computed": 0, "inserted": 0, "updated": 0}

    outcomes = compute_congress_trade_outcomes(
        db=db,
        events=events_to_compute,
        benchmark_symbol=benchmark_symbol,
    )
    outcome_by_event_id = {row["event_id"]: row for row in outcomes}

    now = datetime.now(timezone.utc)
    inserted = 0
    updated = 0
    for event in events_to_compute:
        payload = outcome_by_event_id.get(event.id)
        if not payload:
            continue
        status = payload.get("scoring_status") or "unknown"
        target = existing_by_event_id.get(event.id)
        if target is None:
            target = TradeOutcome(event_id=event.id)
            db.add(target)
            inserted += 1
        else:
            updated += 1

        target.member_id = payload.get("member_id")
        target.member_name = payload.get("member_name")
        target.symbol = payload.get("symbol")
        target.trade_type = payload.get("trade_type")
        target.source = payload.get("source")
        target.trade_date = _parse_iso_date(payload.get("trade_date"))
        target.entry_price = payload.get("entry_price")
        target.entry_price_date = _parse_iso_date(payload.get("entry_price_date"))
        target.current_price = payload.get("current_price")
        target.current_price_date = _parse_iso_date(payload.get("current_price_date"))
        target.benchmark_symbol = payload.get("benchmark_symbol") or benchmark_symbol
        target.benchmark_entry_price = payload.get("benchmark_entry_price")
        target.benchmark_current_price = payload.get("benchmark_current_price")
        target.return_pct = payload.get("return_pct")
        target.benchmark_return_pct = payload.get("benchmark_return_pct")
        target.alpha_pct = payload.get("alpha_pct")
        target.holding_days = payload.get("holding_days")
        target.amount_min = payload.get("amount_min")
        target.amount_max = payload.get("amount_max")
        target.scoring_status = status
        target.scoring_error = payload.get("scoring_error")
        target.methodology_version = payload.get("methodology_version") or "congress_v1"
        target.computed_at = now

    db.commit()
    return {
        "scanned_events": len(candidate_events),
        "computed": len(events_to_compute),
        "inserted": inserted,
        "updated": updated,
    }


def get_member_trade_outcomes(
    db: Session,
    member_id: str,
    lookback_days: int,
    benchmark_symbol: str = "^GSPC",
    member_ids: list[str] | None = None,
) -> list[TradeOutcome]:
    cutoff_dt = datetime.utcnow() - timedelta(days=lookback_days)
    candidate_member_ids = [member_id]
    if member_ids:
        candidate_member_ids = [value for value in member_ids if value]
        if not candidate_member_ids:
            candidate_member_ids = [member_id]

    return db.execute(
        select(TradeOutcome)
        .where(TradeOutcome.member_id.in_(candidate_member_ids))
        .where(TradeOutcome.benchmark_symbol == benchmark_symbol)
        .where(TradeOutcome.scoring_status == "ok")
        .where(TradeOutcome.trade_date.is_not(None))
        .where(TradeOutcome.trade_date >= cutoff_dt.date())
        .order_by(TradeOutcome.trade_date.asc(), TradeOutcome.event_id.asc())
    ).scalars().all()


def summarize_trade_outcome_statuses(db: Session) -> dict[str, int]:
    rows = db.execute(
        select(TradeOutcome.scoring_status, func.count())
        .group_by(TradeOutcome.scoring_status)
    ).all()
    return {status: int(count) for status, count in rows if status}


def count_member_trade_outcomes(
    db: Session,
    member_id: str,
    lookback_days: int,
    benchmark_symbol: str = "^GSPC",
    member_ids: list[str] | None = None,
) -> int:
    cutoff_dt = datetime.utcnow() - timedelta(days=lookback_days)
    candidate_member_ids = [member_id]
    if member_ids:
        candidate_member_ids = [value for value in member_ids if value]
        if not candidate_member_ids:
            candidate_member_ids = [member_id]

    return int(
        db.execute(
            select(func.count(TradeOutcome.id))
            .where(TradeOutcome.member_id.in_(candidate_member_ids))
            .where(TradeOutcome.benchmark_symbol == benchmark_symbol)
            .where(TradeOutcome.trade_date.is_not(None))
            .where(TradeOutcome.trade_date >= cutoff_dt.date())
        ).scalar_one()
        or 0
    )
