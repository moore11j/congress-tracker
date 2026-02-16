from __future__ import annotations

import argparse
import json
import logging
from typing import Any

from sqlalchemy import select

from app.db import SessionLocal
from app.insider_market_trade import canonicalize_market_trade_type
from app.models import Event

logger = logging.getLogger(__name__)


def _extract_raw_trade_type(event: Event, payload: dict[str, Any]) -> str | None:
    current = (event.trade_type or "").strip()
    if current:
        return current

    tx_type = payload.get("transactionType")
    if isinstance(tx_type, str) and tx_type.strip():
        return tx_type.strip()

    raw = payload.get("raw")
    if isinstance(raw, dict):
        raw_tx = raw.get("transactionType")
        if isinstance(raw_tx, str) and raw_tx.strip():
            return raw_tx.strip()

    return None


def backfill_insider_market_trade_types(*, apply: bool = False, batch_size: int = 500) -> dict[str, int | bool]:
    db = SessionLocal()
    scanned = 0
    updated_to_sale_purchase = 0
    unchanged = 0
    non_market_left_raw = 0
    batch_updates = 0

    try:
        events = db.execute(select(Event).where(Event.event_type == "insider_trade")).scalars()

        for event in events:
            scanned += 1
            try:
                payload_obj = json.loads(event.payload_json) if event.payload_json else {}
                payload = payload_obj if isinstance(payload_obj, dict) else {}
            except Exception:
                payload = {}

            raw_trade_type = _extract_raw_trade_type(event, payload)
            canonical = canonicalize_market_trade_type(raw_trade_type)

            if canonical:
                if event.trade_type != canonical:
                    event.trade_type = canonical
                    updated_to_sale_purchase += 1
                    batch_updates += 1
                else:
                    unchanged += 1
            else:
                non_market_left_raw += 1
                unchanged += 1

            if apply and batch_updates >= batch_size:
                db.commit()
                batch_updates = 0

        if apply:
            db.commit()
        else:
            db.rollback()

        result: dict[str, int | bool] = {
            "apply": apply,
            "scanned": scanned,
            "updated_to_sale_purchase": updated_to_sale_purchase,
            "unchanged": unchanged,
            "non_market_left_raw": non_market_left_raw,
        }
        logger.info("Backfill insider market trade types completed: %s", result)
        return result
    finally:
        db.close()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill insider Event.trade_type to canonical market values: sale/purchase."
    )
    parser.add_argument("--apply", action="store_true", help="Apply updates. Without this flag the run is dry-run.")
    parser.add_argument("--batch-size", type=int, default=500, help="Commit after this many updates.")
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    logging.basicConfig(level=getattr(logging, str(args.log_level).upper(), logging.INFO))
    backfill_insider_market_trade_types(apply=args.apply, batch_size=args.batch_size)


if __name__ == "__main__":
    main()
