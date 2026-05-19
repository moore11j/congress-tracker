from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
BACKEND = ROOT / "backend"
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

from app.backfill_events_from_trades import insert_missing_congress_events_from_transactions  # noqa: E402
from app.db import SessionLocal  # noqa: E402
from app.ingest_house import ingest_house  # noqa: E402
from app.ingest_senate import ingest_senate  # noqa: E402
from app.models import Event  # noqa: E402


logger = logging.getLogger(__name__)


def _sample_missing_events(limit: int = 10) -> list[dict]:
    db = SessionLocal()
    try:
        existing = insert_missing_congress_events_from_transactions(db, dry_run=True, limit=limit)
        db.rollback()
        return [{"kind": "persisted_transaction_missing_event", "would_insert": existing}]
    finally:
        db.close()


def run(
    *,
    apply: bool,
    pages: int,
    limit: int,
    sleep_s: float,
    skip_source_refresh: bool,
) -> dict:
    mode = "apply" if apply else "dry-run"
    result: dict[str, object] = {
        "mode": mode,
        "source_refresh": "skipped" if skip_source_refresh else "run",
        "house": None,
        "senate": None,
        "events_inserted": 0,
    }

    if not skip_source_refresh:
        result["house"] = ingest_house(pages=pages, limit=limit, sleep_s=sleep_s, dry_run=not apply)
        result["senate"] = ingest_senate(pages=pages, limit=limit, sleep_s=sleep_s, dry_run=not apply)

    db = SessionLocal()
    try:
        before = db.query(Event).filter(Event.event_type == "congress_trade").count()
        inserted = insert_missing_congress_events_from_transactions(db, dry_run=not apply)
        if apply:
            db.commit()
        else:
            db.rollback()
        after = db.query(Event).filter(Event.event_type == "congress_trade").count()
        result["events_inserted"] = inserted
        result["events_before"] = before
        result["events_after"] = after
    finally:
        db.close()

    if not apply:
        result["sample"] = _sample_missing_events()
        result["note"] = (
            "Dry-run source refresh estimates transaction rows that would be recovered from recent source pages. "
            "Event insertion counts only persisted transactions because dry-run does not write recovered transactions."
        )
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill missing congressional multi-trade events.")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true", help="Preview without writing.")
    mode.add_argument("--apply", action="store_true", help="Write recovered transactions and missing events.")
    parser.add_argument("--pages", type=int, default=3, help="Recent source pages to scan before event projection.")
    parser.add_argument("--limit", type=int, default=200, help="Rows per source page.")
    parser.add_argument("--sleep-s", type=float, default=0.25)
    parser.add_argument(
        "--skip-source-refresh",
        action="store_true",
        help="Only project missing events from transactions already persisted locally.",
    )
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO))
    result = run(
        apply=args.apply,
        pages=args.pages,
        limit=args.limit,
        sleep_s=args.sleep_s,
        skip_source_refresh=args.skip_source_refresh,
    )
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
