from __future__ import annotations

import argparse
import json

from app.db import SessionLocal
from app.services.email_intraday import (
    email_alert_sweep_lookback_minutes,
    run_intraday_alert_sweep,
    summarize_intraday_alert_results,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Walnut intraday high-priority email alert sweep.")
    parser.add_argument("--lookback-minutes", type=int, default=email_alert_sweep_lookback_minutes())
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--allow-outside-market-hours", action="store_true")
    args = parser.parse_args()

    with SessionLocal() as db:
        results = run_intraday_alert_sweep(
            db,
            lookback_minutes=args.lookback_minutes,
            limit=args.limit,
            dry_run=args.dry_run,
            market_hours_only=not args.allow_outside_market_hours,
        )
    print(
        json.dumps(
            {
                "dry_run": args.dry_run,
                "lookback_minutes": args.lookback_minutes,
                "limit": args.limit,
                "summary": summarize_intraday_alert_results(results),
                "items": results,
            },
            default=str,
        )
    )


if __name__ == "__main__":
    main()
