from __future__ import annotations

import argparse
import json
import logging

from app.db import SessionLocal
from app.services.feed_pnl_enrichment import repair_recent_feed_pnl


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Queue targeted PnL enrichment for recent feed trade events.")
    parser.add_argument("--days", type=int, default=3)
    parser.add_argument("--limit", type=int, default=200)
    parser.add_argument(
        "--symbols",
        default="",
        help="Optional comma-separated symbols to restrict the repair, e.g. BLND,CTSO,ACEL.",
    )
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    logging.basicConfig(level=getattr(logging, str(args.log_level).upper(), logging.INFO))
    symbols = [value.strip() for value in str(args.symbols or "").split(",") if value.strip()]
    db = SessionLocal()
    try:
        report = repair_recent_feed_pnl(db, days=args.days, limit=args.limit, symbols=symbols or None)
    finally:
        db.close()
    print(json.dumps(report, sort_keys=True))


if __name__ == "__main__":
    main()
