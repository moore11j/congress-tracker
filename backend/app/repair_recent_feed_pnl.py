from __future__ import annotations

import argparse
import json
import logging

from app.db import SessionLocal
from app.services.feed_cache_epoch import try_bump_feed_events_epoch
from app.services.feed_pnl_enrichment import refresh_recent_feed_pnl_now, repair_recent_feed_pnl


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Queue targeted PnL enrichment for recent feed trade events.")
    parser.add_argument("--days", type=int, default=3)
    parser.add_argument("--limit", type=int, default=200)
    parser.add_argument(
        "--symbols",
        default="",
        help="Optional comma-separated symbols to restrict the repair, e.g. BLND,CTSO,ACEL.",
    )
    parser.add_argument(
        "--process-now",
        action="store_true",
        help="Refresh current prices and recompute matching feed PnL rows immediately instead of only queuing jobs.",
    )
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    logging.basicConfig(level=getattr(logging, str(args.log_level).upper(), logging.INFO))
    symbols = [value.strip() for value in str(args.symbols or "").split(",") if value.strip()]
    db = SessionLocal()
    try:
        if args.process_now:
            report = refresh_recent_feed_pnl_now(db, days=args.days, limit=args.limit, symbols=symbols or None)
            if int(report.get("pnl_refreshed") or 0) > 0:
                report["feed_cache_epoch"] = try_bump_feed_events_epoch(reason="recent_feed_pnl_repair")
            else:
                report["feed_cache_epoch"] = {"status": "skipped", "reason": "no_pnl_writes"}
        else:
            report = repair_recent_feed_pnl(db, days=args.days, limit=args.limit, symbols=symbols or None)
    finally:
        db.close()
    print(json.dumps(report, sort_keys=True))


if __name__ == "__main__":
    main()
