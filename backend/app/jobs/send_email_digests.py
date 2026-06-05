from __future__ import annotations

import argparse
import json

from app.db import SessionLocal
from app.services.email_digests import run_digest_job, summarize_digest_results


def main() -> None:
    parser = argparse.ArgumentParser(description="Send Walnut email digests through the configured email provider.")
    parser.add_argument("--kind", choices=["monitoring", "watchlist_activity", "signals"], required=True)
    parser.add_argument("--lookback-days", type=int, default=1)
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    with SessionLocal() as db:
        results = run_digest_job(
            db,
            kind=args.kind,
            lookback_days=args.lookback_days,
            limit=args.limit,
            force=args.force,
            dry_run=args.dry_run,
        )
    print(
        json.dumps(
            {
                "kind": args.kind,
                "dry_run": args.dry_run,
                "force": args.force,
                "summary": summarize_digest_results(results),
                "items": results,
            },
            default=str,
        )
    )


if __name__ == "__main__":
    main()
