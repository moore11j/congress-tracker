from __future__ import annotations

import argparse
import json
import logging
from datetime import date
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.db import Base, SessionLocal, engine, ensure_event_columns, ensure_trade_outcomes_amount_bigint  # noqa: E402
from app.services.congress_outcome_coverage import repair_recent_congress_outcomes  # noqa: E402


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Repair recent missing Congress public-equity trade outcomes.")
    parser.add_argument("--since-report-date", required=True, help="Only inspect disclosures reported on/after YYYY-MM-DD.")
    parser.add_argument("--dry-run", action="store_true", help="Compute the proposed repair without inserting outcomes.")
    parser.add_argument("--apply", action="store_true", help="Insert missing outcome rows.")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--benchmark", default="SPY")
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    if bool(args.dry_run) == bool(args.apply):
        raise SystemExit("Specify exactly one of --dry-run or --apply.")

    logging.basicConfig(level=getattr(logging, str(args.log_level).upper(), logging.INFO))
    try:
        since_report_date = date.fromisoformat(args.since_report_date[:10])
    except ValueError as exc:
        raise SystemExit("--since-report-date must be YYYY-MM-DD.") from exc

    Base.metadata.create_all(bind=engine)
    ensure_event_columns()
    ensure_trade_outcomes_amount_bigint()
    with SessionLocal() as db:
        report = repair_recent_congress_outcomes(
            db,
            since_report_date=since_report_date,
            dry_run=args.dry_run,
            limit=args.limit,
            benchmark_symbol=args.benchmark,
        )
    print(json.dumps(report, default=str, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
