from __future__ import annotations

import argparse
import json
import logging

from sqlalchemy import select

from app.db import SessionLocal
from app.insider_market_trade import classify_insider_market_trade
from app.models import Event

logger = logging.getLogger(__name__)


def purify_insider_market_trades(*, apply: bool = False, batch_size: int = 500) -> dict[str, int | bool]:
    db = SessionLocal()
    scanned = 0
    updated = 0
    flagged_non_market = 0
    unchanged = 0
    batch_updates = 0

    try:
        events = db.execute(select(Event).where(Event.event_type == "insider_trade")).scalars()

        for event in events:
            scanned += 1

            payload: dict | None = None
            try:
                parsed = json.loads(event.payload_json) if event.payload_json else {}
                payload = parsed if isinstance(parsed, dict) else {}
            except Exception:
                payload = {}

            raw_payload = payload.get("raw") if isinstance(payload.get("raw"), dict) else payload
            raw_type = event.transaction_type or event.trade_type
            canonical, is_market = classify_insider_market_trade(raw_type, raw_payload)

            changed = False
            if is_market:
                if event.trade_type != canonical and canonical is not None:
                    event.trade_type = canonical
                    changed = True
            else:
                flagged_non_market += 1

            if isinstance(payload, dict):
                if payload.get("is_market_trade") != is_market:
                    payload["is_market_trade"] = is_market
                    changed = True
                if payload.get("trade_type_canonical") != canonical:
                    payload["trade_type_canonical"] = canonical
                    changed = True
                if changed:
                    event.payload_json = json.dumps(payload, sort_keys=True)

            if changed:
                updated += 1
                batch_updates += 1
            else:
                unchanged += 1

            if apply and batch_updates >= batch_size:
                db.commit()
                batch_updates = 0

        if apply:
            db.commit()
        else:
            db.rollback()

        result = {
            "apply": apply,
            "scanned": scanned,
            "updated": updated,
            "flagged_non_market": flagged_non_market,
            "unchanged": unchanged,
        }
        logger.info("Purify insider market trades completed: %s", result)
        return result
    finally:
        db.close()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Canonicalize insider trade events to market buy/sell types.")
    parser.add_argument("--apply", action="store_true", help="Apply updates. Without this flag the run is dry-run.")
    parser.add_argument("--batch-size", type=int, default=500, help="Commit after this many updates.")
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    logging.basicConfig(level=getattr(logging, str(args.log_level).upper(), logging.INFO))
    purify_insider_market_trades(apply=args.apply, batch_size=args.batch_size)


if __name__ == "__main__":
    main()
