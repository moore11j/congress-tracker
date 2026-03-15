from __future__ import annotations

import argparse
import json
import logging
from collections import Counter
from datetime import datetime, timedelta, timezone

from sqlalchemy import func, select

from app.db import Base, SessionLocal, engine, ensure_event_columns
from app.models import Event, TradeOutcome
from app.services.member_performance import (
    INSIDER_METHODOLOGY_VERSION,
    METHODOLOGY_VERSION,
    compute_congress_trade_outcomes,
    compute_insider_trade_outcomes,
)

logger = logging.getLogger(__name__)
METHODOLOGY_BY_EVENT_TYPE = {
    "congress_trade": METHODOLOGY_VERSION,
    "insider_trade": INSIDER_METHODOLOGY_VERSION,
}


def _parse_date(value: str | None):
    if not value:
        return None
    try:
        return datetime.strptime(value[:10], "%Y-%m-%d").date()
    except ValueError:
        return None




def _normalize_cik(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = "".join(ch for ch in str(value).strip() if ch.isdigit())
    if not cleaned:
        return None
    return cleaned.zfill(10)


def _event_reporting_cik(event: Event) -> str | None:
    payload_raw = event.payload_json
    payload = payload_raw if isinstance(payload_raw, dict) else None
    if payload is None and isinstance(payload_raw, str) and payload_raw:
        try:
            parsed = json.loads(payload_raw)
            payload = parsed if isinstance(parsed, dict) else None
        except Exception:
            payload = None
    payload = payload or {}
    raw = payload.get("raw") if isinstance(payload.get("raw"), dict) else {}
    return _normalize_cik(
        payload.get("reporting_cik")
        or payload.get("reportingCik")
        or raw.get("reportingCik")
        or raw.get("reportingCIK")
        or raw.get("rptOwnerCik")
    )

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Compute and persist trade outcomes.")
    parser.add_argument("--replace", action="store_true", help="Recompute and update outcomes even when event_id already exists.")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of events scanned.")
    parser.add_argument("--member-id", type=str, default=None, help="Only compute outcomes for one member_bioguide_id/reporting_cik.")
    parser.add_argument("--event-type", type=str, default="all", help="Event type to score: congress_trade, insider_trade, or all.")
    parser.add_argument("--benchmark", type=str, default="^GSPC", help="Benchmark symbol (default: ^GSPC).")
    parser.add_argument("--lookback-days", type=int, default=None, help="Only include events from the last N days.")
    parser.add_argument("--trade-date-after", type=str, default=None, help="Only include events with event_date/ts >= YYYY-MM-DD.")
    parser.add_argument("--only-missing", action="store_true", help="Only process events without a trade_outcomes row.")
    parser.add_argument("--retry-failed-status", type=str, default=None, help="Recompute only existing outcomes with this scoring_status.")
    parser.add_argument("--retry-failed-statuses", type=str, default=None, help="Comma-separated scoring_status values to recompute (e.g. no_current_price,provider_402,provider_429).")
    parser.add_argument("--log-level", type=str, default="INFO", help="Python log level.")
    return parser


def run_compute(
    *,
    replace: bool,
    limit: int | None,
    member_id: str | None,
    event_type: str,
    benchmark_symbol: str,
    lookback_days: int | None,
    trade_date_after: str | None,
    only_missing: bool,
    retry_failed_status: str | None,
    retry_failed_statuses: str | None,
) -> dict:
    Base.metadata.create_all(bind=engine)
    ensure_event_columns()

    requested_event_type = (event_type or "all").strip().lower()
    if requested_event_type not in {"all", "congress_trade", "insider_trade"}:
        raise ValueError("event_type must be one of: all, congress_trade, insider_trade")
    event_types = [requested_event_type] if requested_event_type != "all" else ["congress_trade", "insider_trade"]

    with SessionLocal() as db:
        sort_ts = func.coalesce(Event.event_date, Event.ts)
        query = select(Event).where(Event.event_type.in_(event_types)).order_by(sort_ts.desc(), Event.id.desc())

        normalized_member_id = _normalize_cik(member_id) if member_id else None
        if member_id and requested_event_type != "insider_trade":
            query = query.where(Event.member_bioguide_id == member_id)
        if lookback_days is not None and lookback_days > 0:
            query = query.where(sort_ts >= (datetime.now(timezone.utc) - timedelta(days=lookback_days)))
        if trade_date_after:
            parsed = _parse_date(trade_date_after)
            if parsed is not None:
                query = query.where(sort_ts >= datetime.combine(parsed, datetime.min.time()))
        if limit is not None and limit > 0:
            query = query.limit(limit)

        events = db.execute(query).scalars().all()
        scanned = len(events)
        insider_selected_events = [event for event in events if event.event_type == "insider_trade"]
        insider_skip_reasons: Counter[str] = Counter()
        eligible_events = [event for event in events if event.event_type in {"congress_trade", "insider_trade"}]
        if member_id and requested_event_type == "insider_trade":
            before = {event.id for event in eligible_events if event.event_type == "insider_trade"}
            eligible_events = [
                event
                for event in eligible_events
                if _event_reporting_cik(event) == normalized_member_id
            ]
            after = {event.id for event in eligible_events if event.event_type == "insider_trade"}
            insider_skip_reasons["filtered_member_id"] += len(before - after)

        candidate_event_ids = [event.id for event in eligible_events]
        existing_rows = (
            db.execute(select(TradeOutcome).where(TradeOutcome.event_id.in_(candidate_event_ids))).scalars().all()
            if candidate_event_ids
            else []
        )
        existing_by_event_id = {row.event_id: row for row in existing_rows}

        if only_missing:
            before = {event.id for event in eligible_events if event.event_type == "insider_trade"}
            eligible_events = [event for event in eligible_events if event.id not in existing_by_event_id]
            after = {event.id for event in eligible_events if event.event_type == "insider_trade"}
            insider_skip_reasons["filtered_only_missing_existing_outcome"] += len(before - after)

        retry_status_set = {s.strip() for s in (retry_failed_statuses or "").split(",") if s.strip()}
        if retry_failed_status:
            retry_status_set.add(retry_failed_status.strip())

        if retry_status_set:
            before = {event.id for event in eligible_events if event.event_type == "insider_trade"}
            eligible_events = [
                event
                for event in eligible_events
                if existing_by_event_id.get(event.id) is not None
                and existing_by_event_id[event.id].scoring_status in retry_status_set
            ]
            after = {event.id for event in eligible_events if event.event_type == "insider_trade"}
            insider_skip_reasons["filtered_retry_status_mismatch"] += len(before - after)

        outcomes: list[dict] = []
        congress_events = [event for event in eligible_events if event.event_type == "congress_trade"]
        insider_events = [event for event in eligible_events if event.event_type == "insider_trade"]
        insider_entering_scoring = len(insider_events)
        insider_outcomes: list[dict] = []

        if congress_events:
            outcomes.extend(
                compute_congress_trade_outcomes(
                    db=db,
                    events=congress_events,
                    benchmark_symbol=(benchmark_symbol or "^GSPC").strip() or "^GSPC",
                )
            )
        if insider_events:
            insider_outcomes = compute_insider_trade_outcomes(
                db=db,
                events=insider_events,
                benchmark_symbol=(benchmark_symbol or "^GSPC").strip() or "^GSPC",
            )
            outcomes.extend(insider_outcomes)

        outcome_by_event_id = {outcome["event_id"]: outcome for outcome in outcomes}

        inserted = 0
        updated = 0
        skipped = 0
        status_counts: Counter[str] = Counter()
        insider_inserted = 0
        insider_methodology_versions: Counter[str] = Counter()

        now = datetime.now(timezone.utc)
        for event in eligible_events:
            outcome = outcome_by_event_id.get(event.id)
            if outcome is None:
                skipped += 1
                status_counts["missing_outcome"] += 1
                if event.event_type == "insider_trade":
                    insider_skip_reasons["missing_outcome"] += 1
                continue

            status = outcome.get("scoring_status") or "unknown"
            status_counts[status] += 1
            existing = existing_by_event_id.get(event.id)
            if existing is not None and not replace and not retry_status_set:
                skipped += 1
                if event.event_type == "insider_trade":
                    insider_skip_reasons["existing_row_no_replace"] += 1
                continue

            target = existing or TradeOutcome(event_id=event.id)
            target.member_id = outcome.get("member_id")
            target.member_name = outcome.get("member_name")
            target.symbol = outcome.get("symbol")
            target.trade_type = outcome.get("trade_type")
            target.source = outcome.get("source")
            target.trade_date = _parse_date(outcome.get("trade_date"))
            target.entry_price = outcome.get("entry_price")
            target.entry_price_date = _parse_date(outcome.get("entry_price_date"))
            target.current_price = outcome.get("current_price")
            target.current_price_date = _parse_date(outcome.get("current_price_date"))
            target.benchmark_symbol = outcome.get("benchmark_symbol") or benchmark_symbol
            target.benchmark_entry_price = outcome.get("benchmark_entry_price")
            target.benchmark_current_price = outcome.get("benchmark_current_price")
            target.return_pct = outcome.get("return_pct")
            target.benchmark_return_pct = outcome.get("benchmark_return_pct")
            target.alpha_pct = outcome.get("alpha_pct")
            target.holding_days = outcome.get("holding_days")
            target.amount_min = outcome.get("amount_min")
            target.amount_max = outcome.get("amount_max")
            target.scoring_status = status
            target.scoring_error = outcome.get("scoring_error")
            target.methodology_version = outcome.get("methodology_version") or METHODOLOGY_BY_EVENT_TYPE.get(event.event_type, METHODOLOGY_VERSION)
            target.computed_at = now

            if existing is None:
                db.add(target)
                inserted += 1
                if event.event_type == "insider_trade":
                    insider_inserted += 1
                    insider_methodology_versions[target.methodology_version or "<empty>"] += 1
            else:
                updated += 1

        commit_reached = False
        db.commit()
        commit_reached = True

        insider_debug_report = {
            "selected_insider_trade_events": len(insider_selected_events),
            "entering_insider_scoring": insider_entering_scoring,
            "insider_skip_reasons": dict(insider_skip_reasons),
            "scored_insider_outcomes_built": len(insider_outcomes),
            "inserted_insider_outcomes": insider_inserted,
            "insider_insert_methodology_versions": dict(insider_methodology_versions),
            "commit_reached": commit_reached,
        }
        logger.info("insider scoring debug=%s", insider_debug_report)

        report = {
            "scanned": scanned,
            "eligible": len(eligible_events),
            "inserted": inserted,
            "updated": updated,
            "skipped": skipped,
            "status_counts": dict(status_counts),
            "benchmark_symbol": benchmark_symbol,
            "event_type": requested_event_type,
            "replace": replace,
            "limit": limit,
            "member_id": member_id,
            "lookback_days": lookback_days,
            "trade_date_after": trade_date_after,
            "only_missing": only_missing,
            "retry_failed_status": retry_failed_status,
            "retry_failed_statuses": sorted(retry_status_set),
            "insider_debug": insider_debug_report,
        }
        logger.info("compute_trade_outcomes report=%s", report)
        return report


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO))
    report = run_compute(
        replace=args.replace,
        limit=args.limit,
        member_id=args.member_id,
        event_type=args.event_type,
        benchmark_symbol=args.benchmark,
        lookback_days=args.lookback_days,
        trade_date_after=args.trade_date_after,
        only_missing=args.only_missing,
        retry_failed_status=args.retry_failed_status,
        retry_failed_statuses=args.retry_failed_statuses,
    )
    print(report)


if __name__ == "__main__":
    main()
