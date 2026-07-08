from __future__ import annotations

import argparse
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.backfill_events_from_trades import insert_missing_congress_events_from_transactions
from app.db import SessionLocal
from app.ingest_house import DEFAULT_RECENT_PAGES as DEFAULT_HOUSE_RECENT_PAGES
from app.ingest_house import ingest_house
from app.ingest_senate import DEFAULT_RECENT_PAGES as DEFAULT_SENATE_RECENT_PAGES
from app.ingest_senate import ingest_senate
from app.models import AppSetting, Event, Filing, Member, Transaction
from app.services.congress_assets import CONGRESS_DISCLOSURE_EVENT_TYPES
from app.services.congress_outcome_coverage import repair_recent_congress_outcomes
from app.services.feed_cache_epoch import try_bump_feed_events_epoch

logger = logging.getLogger(__name__)

CONGRESS_RECENT_STATUS_KEY = "congress_ingest.recent.status"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run recent Congress disclosure ingest.")
    parser.add_argument("--days", type=int, default=int(os.getenv("CONGRESS_RECENT_DAYS", "7")))
    parser.add_argument("--pages", type=int, default=int(os.getenv("CONGRESS_RECENT_PAGES", "25")))
    parser.add_argument("--limit", type=int, default=int(os.getenv("CONGRESS_RECENT_LIMIT", "100")))
    parser.add_argument("--sleep-s", type=float, default=float(os.getenv("CONGRESS_RECENT_SLEEP_S", "0.1")))
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def _set_setting(db: Session, key: str, value: str) -> None:
    row = db.get(AppSetting, key)
    if row is None:
        row = AppSetting(key=key, value=value)
        db.add(row)
    else:
        row.value = value


def _current_freshness_snapshot(db: Session) -> dict[str, Any]:
    latest_by_source = {
        source: latest.isoformat() if latest else None
        for source, latest in db.execute(
            select(Filing.source, func.max(Filing.filing_date))
            .where(Filing.source.in_(("house_fmp", "senate_fmp")))
            .group_by(Filing.source)
        )
    }
    latest_by_chamber = {
        chamber: latest.isoformat() if latest else None
        for chamber, latest in db.execute(
            select(Member.chamber, func.max(Transaction.report_date))
            .join(Member, Member.id == Transaction.member_id)
            .group_by(Member.chamber)
        )
    }
    latest_event_ts = db.execute(
        select(func.max(Event.ts)).where(Event.event_type.in_(CONGRESS_DISCLOSURE_EVENT_TYPES))
    ).scalar_one_or_none()
    return {
        "latest_house_report_date": latest_by_chamber.get("house") or latest_by_source.get("house_fmp"),
        "latest_senate_report_date": latest_by_chamber.get("senate") or latest_by_source.get("senate_fmp"),
        "latest_congress_event_ts": latest_event_ts.isoformat() if latest_event_ts else None,
    }


def _int_metric(payload: dict[str, Any], key: str) -> int:
    value = payload.get(key)
    return value if isinstance(value, int) else 0


def _persist_recent_status(result: dict[str, Any]) -> None:
    db = SessionLocal()
    try:
        _set_setting(db, CONGRESS_RECENT_STATUS_KEY, json.dumps(result, sort_keys=True))
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def run_recent_congress_ingest(
    *,
    days: int = 7,
    pages: int = 25,
    limit: int = 100,
    sleep_s: float = 0.1,
    dry_run: bool = False,
) -> dict[str, Any]:
    started_at = datetime.now(timezone.utc)
    house_pages = pages or DEFAULT_HOUSE_RECENT_PAGES
    senate_pages = pages or DEFAULT_SENATE_RECENT_PAGES
    logger.info(
        "recent congress ingest starting days=%s house_pages=%s senate_pages=%s limit=%s dry_run=%s",
        days,
        house_pages,
        senate_pages,
        limit,
        dry_run,
    )

    house_result = ingest_house(
        pages=house_pages,
        limit=limit,
        sleep_s=sleep_s,
        dry_run=dry_run,
        recent_days=days,
    )
    senate_result = ingest_senate(
        pages=senate_pages,
        limit=limit,
        sleep_s=sleep_s,
        dry_run=dry_run,
        recent_days=days,
    )

    db = SessionLocal()
    try:
        events_inserted = insert_missing_congress_events_from_transactions(
            db,
            dry_run=dry_run,
            recent_days=days,
        )
        if dry_run:
            db.rollback()
        else:
            db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()

    since_report_date = datetime.now(timezone.utc).date() - timedelta(days=max(days, 0))
    if dry_run:
        outcome_coverage = {"skipped": "dry_run"}
        snapshot_db = SessionLocal()
        try:
            snapshot = _current_freshness_snapshot(snapshot_db)
            snapshot_db.rollback()
        finally:
            snapshot_db.close()
    else:
        repair_db = SessionLocal()
        try:
            outcome_coverage = repair_recent_congress_outcomes(
                repair_db,
                since_report_date=since_report_date,
                dry_run=False,
                benchmark_symbol=os.getenv("INGEST_SIGNALS_BENCHMARK", "^GSPC"),
            )
        finally:
            repair_db.close()

        outcome_inserted = _int_metric(outcome_coverage, "inserted")
        feed_cache_epoch = (
            try_bump_feed_events_epoch(reason="recent_congress_ingest")
            if events_inserted or outcome_inserted
            else {"status": "skipped", "reason": "no_feed_changes"}
        )

        snapshot_db = SessionLocal()
        try:
            snapshot = _current_freshness_snapshot(snapshot_db)
        finally:
            snapshot_db.close()

    finished_at = datetime.now(timezone.utc)
    result = {
        "status": "ok",
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "duration_ms": int((finished_at - started_at).total_seconds() * 1000),
        "days": days,
        "pages": pages,
        "limit": limit,
        "dry_run": dry_run,
        "house": house_result,
        "senate": senate_result,
        "events_inserted": events_inserted,
        "outcome_coverage": outcome_coverage,
        "feed_cache_epoch": feed_cache_epoch if not dry_run else {"status": "skipped", "reason": "dry_run"},
        "filings_scanned": _int_metric(house_result, "filings_scanned")
        + _int_metric(senate_result, "filings_scanned"),
        "transactions_inserted": _int_metric(house_result, "inserted")
        + _int_metric(senate_result, "inserted"),
        "transactions_skipped": _int_metric(house_result, "skipped")
        + _int_metric(senate_result, "skipped"),
        "skipped_old": _int_metric(house_result, "skipped_old")
        + _int_metric(senate_result, "skipped_old"),
        "non_equity_symbol_skipped": _int_metric(house_result, "non_equity_symbol_skipped")
        + _int_metric(senate_result, "non_equity_symbol_skipped"),
        **snapshot,
    }
    if not dry_run:
        _persist_recent_status(result)
    logger.info("recent congress ingest finished: %s", result)
    return result


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    args = _parse_args()
    print(
        json.dumps(
            run_recent_congress_ingest(
                days=args.days,
                pages=args.pages,
                limit=args.limit,
                sleep_s=args.sleep_s,
                dry_run=args.dry_run,
            ),
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
