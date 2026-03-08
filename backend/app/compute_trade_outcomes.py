from __future__ import annotations

import argparse
import logging
from collections import Counter
from datetime import datetime

from sqlalchemy import func, select

from app.db import Base, SessionLocal, engine, ensure_event_columns
from app.models import Event, TradeOutcome
from app.services.member_performance import METHODOLOGY_VERSION, compute_congress_trade_outcomes

logger = logging.getLogger(__name__)


def _parse_date(value: str | None):
    if not value:
        return None
    try:
        return datetime.strptime(value[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Compute and persist congress trade outcomes.")
    parser.add_argument("--replace", action="store_true", help="Recompute and update outcomes even when event_id already exists.")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of congress events scanned.")
    parser.add_argument("--member-id", type=str, default=None, help="Only compute outcomes for one member_bioguide_id.")
    parser.add_argument("--benchmark", type=str, default="^GSPC", help="Benchmark symbol (default: ^GSPC).")
    parser.add_argument("--log-level", type=str, default="INFO", help="Python log level.")
    return parser


def run_compute(
    *,
    replace: bool,
    limit: int | None,
    member_id: str | None,
    benchmark_symbol: str,
) -> dict:
    Base.metadata.create_all(bind=engine)
    ensure_event_columns()

    with SessionLocal() as db:
        sort_ts = func.coalesce(Event.event_date, Event.ts)
        query = (
            select(Event)
            .where(Event.event_type == "congress_trade")
            .order_by(sort_ts.desc(), Event.id.desc())
        )
        if member_id:
            query = query.where(Event.member_bioguide_id == member_id)
        if limit is not None and limit > 0:
            query = query.limit(limit)

        events = db.execute(query).scalars().all()
        scanned = len(events)
        eligible_events = [event for event in events if (event.member_bioguide_id or "").strip()]

        outcomes = compute_congress_trade_outcomes(
            db=db,
            events=eligible_events,
            benchmark_symbol=(benchmark_symbol or "^GSPC").strip() or "^GSPC",
        )

        outcome_by_event_id = {outcome["event_id"]: outcome for outcome in outcomes}
        existing_rows = db.execute(
            select(TradeOutcome).where(TradeOutcome.event_id.in_(list(outcome_by_event_id.keys())))
        ).scalars().all() if outcome_by_event_id else []
        existing_by_event_id = {row.event_id: row for row in existing_rows}

        inserted = 0
        updated = 0
        skipped = 0
        status_counts: Counter[str] = Counter()

        now = datetime.utcnow()
        for event in eligible_events:
            outcome = outcome_by_event_id.get(event.id)
            if outcome is None:
                skipped += 1
                status_counts["missing_outcome"] += 1
                continue

            status = outcome.get("scoring_status") or "unknown"
            status_counts[status] += 1
            existing = existing_by_event_id.get(event.id)
            if existing is not None and not replace:
                skipped += 1
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
            target.methodology_version = outcome.get("methodology_version") or METHODOLOGY_VERSION
            target.computed_at = now

            if existing is None:
                db.add(target)
                inserted += 1
            else:
                updated += 1

        db.commit()

        report = {
            "scanned": scanned,
            "eligible": len(eligible_events),
            "inserted": inserted,
            "updated": updated,
            "skipped": skipped,
            "status_counts": dict(status_counts),
            "benchmark_symbol": benchmark_symbol,
            "methodology_version": METHODOLOGY_VERSION,
            "replace": replace,
            "limit": limit,
            "member_id": member_id,
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
        benchmark_symbol=args.benchmark,
    )
    print(report)


if __name__ == "__main__":
    main()
