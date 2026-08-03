from __future__ import annotations

import argparse
import json
from datetime import date, datetime, timedelta, timezone
from typing import Any

from sqlalchemy import func, select, update
from sqlalchemy.orm import Session

from app.db import SessionLocal
from app.models import (
    Event,
    InstitutionalActivityEvent,
    InstitutionalFiling,
    InstitutionalHolder,
    InstitutionalPosition,
    InstitutionalPositionChange,
    InstitutionalSymbolSummary,
)
from app.services.institutional_activity import (
    INSTITUTIONAL_EVENT_SOURCE,
    generate_activity_events_for_symbol,
    materialize_feed_events_for_symbol,
    refresh_symbol_summary,
)


def _load_object(value: str | None) -> dict[str, Any]:
    if not value:
        return {}
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _parse_date(value: Any) -> date | None:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value.strip().replace("Z", "+00:00")).date()
    except ValueError:
        return None


def _quarter_end(year: int, quarter: int) -> date:
    month, day = {
        1: (3, 31),
        2: (6, 30),
        3: (9, 30),
        4: (12, 31),
    }[max(1, min(int(quarter), 4))]
    return date(int(year), month, day)


def _estimated_filing_date(year: int, quarter: int) -> date:
    return _quarter_end(year, quarter) + timedelta(days=45)


def _is_sparse_historical_dates_payload(filing: InstitutionalFiling) -> bool:
    raw = _load_object(filing.raw_metadata_json)
    if not raw:
        return False
    if raw.get("_walnut_filing_date_source") == "estimated_13f_deadline":
        return False
    if any(raw.get(key) for key in ("filingDate", "filing_date", "acceptedDate", "accepted_date")):
        return False
    raw_date = _parse_date(raw.get("date"))
    if raw_date is None:
        return False
    return filing.filing_date == raw_date


def _target_filings(db: Session, *, cik: str | None = None, limit: int | None = None) -> list[InstitutionalFiling]:
    statement = select(InstitutionalFiling).order_by(
        InstitutionalFiling.cik.asc(),
        InstitutionalFiling.report_year.asc(),
        InstitutionalFiling.report_quarter.asc(),
        InstitutionalFiling.id.asc(),
    )
    if cik:
        statement = statement.where(InstitutionalFiling.cik == cik)
    rows = db.execute(statement).scalars().all()
    targets = [row for row in rows if _is_sparse_historical_dates_payload(row)]
    return targets[: max(0, int(limit))] if limit is not None else targets


def _feed_source_filing_id(activity: InstitutionalActivityEvent) -> str:
    return f"institutional:{activity.id}:{activity.event_type}:{activity.report_year}q{activity.report_quarter}"


def run_repair(
    db: Session,
    *,
    apply: bool = False,
    cik: str | None = None,
    limit: int | None = None,
    refresh_summaries: bool = False,
    repair_aggregate_rows: bool = False,
) -> dict[str, Any]:
    filings = _target_filings(db, cik=cik, limit=limit)
    now = datetime.now(timezone.utc)
    rows: list[dict[str, Any]] = []
    affected_period_symbols: set[tuple[str, int, int]] = set()
    affected_ciks: set[str] = set()
    totals = {
        "filings": len(filings),
        "positions": 0,
        "position_changes": 0,
        "activity_events": 0,
        "feed_events": 0,
        "symbol_summaries": 0,
        "holders": 0,
        "summary_refreshes": 0,
        "activity_events_generated": 0,
        "feed_events_materialized": 0,
        "aggregate_activity_events": 0,
        "aggregate_feed_events": 0,
        "aggregate_symbol_summaries": 0,
    }

    for filing in filings:
        old_filing_date = filing.filing_date
        old_period_end = filing.report_period_end
        new_period_end = _quarter_end(filing.report_year, filing.report_quarter)
        new_filing_date = _estimated_filing_date(filing.report_year, filing.report_quarter)
        raw = _load_object(filing.raw_metadata_json)
        raw["_walnut_filing_date_source"] = "estimated_13f_deadline"
        raw["_walnut_report_period_source"] = "derived_from_report_year_quarter"
        raw["_walnut_original_filing_date"] = old_filing_date.isoformat() if old_filing_date else None
        raw["_walnut_original_report_period_end"] = old_period_end.isoformat() if old_period_end else None
        raw["_walnut_date_repair"] = "sparse_historical_13f_dates_v1"

        position_count = db.query(InstitutionalPosition).filter(InstitutionalPosition.filing_id == filing.id).count()
        change_query = db.query(InstitutionalPositionChange).filter(
            InstitutionalPositionChange.cik == filing.cik,
            InstitutionalPositionChange.report_year == filing.report_year,
            InstitutionalPositionChange.report_quarter == filing.report_quarter,
            InstitutionalPositionChange.filing_date == old_filing_date,
        )
        activity_query = db.query(InstitutionalActivityEvent).filter(
            InstitutionalActivityEvent.cik == filing.cik,
            InstitutionalActivityEvent.report_year == filing.report_year,
            InstitutionalActivityEvent.report_quarter == filing.report_quarter,
            InstitutionalActivityEvent.filing_date == old_filing_date,
        )
        changes = change_query.all()
        activities = activity_query.all()
        feed_ids = [_feed_source_filing_id(activity) for activity in activities if activity.id is not None]
        feed_count = 0
        if feed_ids:
            feed_count = (
                db.query(Event)
                .filter(Event.source_provider == INSTITUTIONAL_EVENT_SOURCE, Event.source_filing_id.in_(feed_ids))
                .count()
            )

        totals["positions"] += position_count
        totals["position_changes"] += len(changes)
        totals["activity_events"] += len(activities)
        totals["feed_events"] += feed_count
        affected_ciks.add(filing.cik)
        for change in changes:
            if change.normalized_symbol:
                affected_period_symbols.add((change.normalized_symbol, change.report_year, change.report_quarter))
        for activity in activities:
            if activity.normalized_symbol:
                affected_period_symbols.add((activity.normalized_symbol, activity.report_year, activity.report_quarter))

        if len(rows) < 25:
            rows.append(
                {
                    "filing_id": filing.id,
                    "cik": filing.cik,
                    "report_year": filing.report_year,
                    "report_quarter": filing.report_quarter,
                    "old_filing_date": old_filing_date.isoformat() if old_filing_date else None,
                    "new_filing_date": new_filing_date.isoformat(),
                    "old_report_period_end": old_period_end.isoformat() if old_period_end else None,
                    "new_report_period_end": new_period_end.isoformat(),
                    "positions": position_count,
                    "position_changes": len(changes),
                    "activity_events": len(activities),
                    "feed_events": feed_count,
                }
            )

        if not apply:
            continue

        filing.filing_date = new_filing_date
        filing.report_period_end = new_period_end
        filing.raw_metadata_json = json.dumps(raw, sort_keys=True, default=str)
        filing.updated_at = now
        db.execute(
            update(InstitutionalPosition)
            .where(InstitutionalPosition.filing_id == filing.id)
            .values(filing_date=new_filing_date, updated_at=now)
        )
        for change in changes:
            change.filing_date = new_filing_date
            change.updated_at = now
        for activity in activities:
            activity.filing_date = new_filing_date
            activity.updated_at = now
        if feed_ids:
            event_dt = datetime.combine(new_filing_date, datetime.min.time(), tzinfo=timezone.utc)
            db.query(Event).filter(
                Event.source_provider == INSTITUTIONAL_EVENT_SOURCE,
                Event.source_filing_id.in_(feed_ids),
            ).update({Event.ts: event_dt, Event.event_date: event_dt}, synchronize_session=False)

    if apply:
        for holder_cik in affected_ciks:
            latest = db.execute(
                select(func.max(InstitutionalFiling.filing_date)).where(InstitutionalFiling.cik == holder_cik)
            ).scalar_one_or_none()
            holder = db.get(InstitutionalHolder, holder_cik)
            if holder is not None:
                holder.latest_filing_date = latest
                holder.updated_at = now
                totals["holders"] += 1

        db.flush()
        if refresh_summaries:
            for symbol, year, quarter in sorted(affected_period_symbols):
                summary = refresh_symbol_summary(db, symbol, year, quarter)
                if summary is not None:
                    totals["symbol_summaries"] += 1
                    totals["activity_events_generated"] += generate_activity_events_for_symbol(db, summary)
                    db.flush()
                    totals["feed_events_materialized"] += materialize_feed_events_for_symbol(db, summary)
                    totals["summary_refreshes"] += 1

    if repair_aggregate_rows:
        aggregate_result = _repair_aggregate_rows(db, apply=apply, now=now)
        totals["aggregate_activity_events"] += aggregate_result["activity_events"]
        totals["aggregate_feed_events"] += aggregate_result["feed_events"]
        totals["aggregate_symbol_summaries"] += aggregate_result["symbol_summaries"]

    return {
        "mode": "apply" if apply else "dry_run",
        "refresh_summaries": refresh_summaries,
        "repair_aggregate_rows": repair_aggregate_rows,
        "target_count": len(filings),
        "affected_period_symbols": len(affected_period_symbols),
        "affected_ciks": len(affected_ciks),
        "totals": totals,
        "sample": rows,
    }


def _aggregate_rows_needing_repair(db: Session) -> tuple[list[InstitutionalActivityEvent], list[InstitutionalSymbolSummary]]:
    bad_date = date(2026, 3, 31)
    activities = (
        db.execute(
            select(InstitutionalActivityEvent).where(
                InstitutionalActivityEvent.cik.is_(None),
                InstitutionalActivityEvent.filing_date == bad_date,
            )
        )
        .scalars()
        .all()
    )
    summaries = (
        db.execute(
            select(InstitutionalSymbolSummary).where(
                InstitutionalSymbolSummary.latest_filing_date == bad_date,
            )
        )
        .scalars()
        .all()
    )
    return activities, summaries


def _repair_aggregate_rows(db: Session, *, apply: bool, now: datetime) -> dict[str, int]:
    activities, summaries = _aggregate_rows_needing_repair(db)
    feed_ids = [_feed_source_filing_id(activity) for activity in activities if activity.id is not None]
    feed_events = []
    if feed_ids:
        feed_events = (
            db.execute(
                select(Event).where(
                    Event.source_provider == INSTITUTIONAL_EVENT_SOURCE,
                    Event.source_filing_id.in_(feed_ids),
                )
            )
            .scalars()
            .all()
        )
    if apply:
        for activity in activities:
            activity.filing_date = _estimated_filing_date(activity.report_year, activity.report_quarter)
            activity.updated_at = now
        for summary in summaries:
            summary.latest_filing_date = _estimated_filing_date(summary.report_year, summary.report_quarter)
            summary.updated_at = now
        for event in feed_events:
            activity_id = _activity_id_from_feed_source_filing_id(event.source_filing_id)
            activity = next((row for row in activities if row.id == activity_id), None)
            if activity is None:
                continue
            event_dt = datetime.combine(_estimated_filing_date(activity.report_year, activity.report_quarter), datetime.min.time(), tzinfo=timezone.utc)
            event.ts = event_dt
            event.event_date = event_dt
    return {
        "activity_events": len(activities),
        "feed_events": len(feed_events),
        "symbol_summaries": len(summaries),
    }


def _activity_id_from_feed_source_filing_id(value: str | None) -> int | None:
    if not value:
        return None
    parts = str(value).split(":")
    if len(parts) < 2 or parts[0] != "institutional":
        return None
    try:
        return int(parts[1])
    except (TypeError, ValueError):
        return None


def main() -> None:
    parser = argparse.ArgumentParser(description="Repair sparse historical 13F date rows created from provider dates payloads.")
    parser.add_argument("--apply", action="store_true", help="Apply the repair. Without this flag the command is a dry run.")
    parser.add_argument("--cik", help="Optional holder CIK scope.")
    parser.add_argument("--limit", type=int, help="Optional maximum number of filing rows to repair.")
    parser.add_argument("--refresh-summaries", action="store_true", help="Also regenerate institutional activity/feed events for affected summaries.")
    parser.add_argument("--repair-aggregate-rows", action="store_true", help="Repair aggregate institutional activity and symbol summary rows with sparse historical dates.")
    args = parser.parse_args()

    db = SessionLocal()
    try:
        result = run_repair(
            db,
            apply=args.apply,
            cik=args.cik,
            limit=args.limit,
            refresh_summaries=args.refresh_summaries,
            repair_aggregate_rows=args.repair_aggregate_rows,
        )
        if args.apply:
            db.commit()
        else:
            db.rollback()
        print(json.dumps(result, indent=2, sort_keys=True, default=str))
    finally:
        db.close()


if __name__ == "__main__":
    main()
