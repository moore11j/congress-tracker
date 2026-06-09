from __future__ import annotations

import argparse
import json

from app.db import SessionLocal
from app.services.billing_reminders import run_billing_expiry_reminders, summarize_billing_reminders


def main() -> None:
    parser = argparse.ArgumentParser(description="Send Walnut transactional billing expiry reminders.")
    parser.add_argument("--window", choices=["7d", "24h"], default=None)
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    with SessionLocal() as db:
        results = run_billing_expiry_reminders(
            db,
            window=args.window,
            limit=args.limit,
            dry_run=args.dry_run,
        )
    print(
        json.dumps(
            {
                "window": args.window or "all",
                "dry_run": args.dry_run,
                "summary": summarize_billing_reminders(results),
                "items": results,
            },
            default=str,
        )
    )


if __name__ == "__main__":
    main()
