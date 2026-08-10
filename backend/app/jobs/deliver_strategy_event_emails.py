from __future__ import annotations

import json
import os

from app.db import SessionLocal
from app.services.strategy_subscriptions import process_pending_strategy_event_deliveries


def main() -> None:
    with SessionLocal() as db:
        result = process_pending_strategy_event_deliveries(
            db,
            limit=int(os.getenv("STRATEGY_EMAIL_DELIVERY_LIMIT", "50") or 50),
        )
    print(json.dumps(result, sort_keys=True, default=str))


if __name__ == "__main__":
    main()
