from __future__ import annotations

import json
import os

from app.db import SessionLocal
from app.services.strategy_subscriptions import queue_recent_strategy_event_deliveries


def _enabled() -> bool:
    return os.getenv("STRATEGY_EVENT_DELIVERY_QUEUE_ENABLED", "false").strip().lower() in {"1", "true", "yes", "on"}


def main() -> None:
    if not _enabled():
        print(json.dumps({"status": "disabled", "reason": "strategy_event_delivery_queue_disabled"}, sort_keys=True))
        return
    with SessionLocal() as db:
        result = queue_recent_strategy_event_deliveries(
            db,
            limit=int(os.getenv("STRATEGY_EVENT_DELIVERY_QUEUE_LIMIT", "100") or 100),
            lookback_hours=int(os.getenv("STRATEGY_EVENT_DELIVERY_QUEUE_LOOKBACK_HOURS", "48") or 48),
        )
    print(json.dumps({"status": "ok", **result}, sort_keys=True, default=str))


if __name__ == "__main__":
    main()
