from __future__ import annotations

import json
import logging
import sys

from app.db import SessionLocal
from app.services.strategy_scheduler import run_active_strategy_evaluations


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    with SessionLocal() as db:
        result = run_active_strategy_evaluations(db)
    print(json.dumps(result, sort_keys=True, default=str))
    sys.exit(1 if result.get("status") == "partial" else 0)


if __name__ == "__main__":
    main()
