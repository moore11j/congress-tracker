from __future__ import annotations

import argparse
import json
import logging

from app.db import SessionLocal, engine, ensure_search_and_insights_schema
from app.services.fred_macro_cache import refresh_fred_macro_cache

logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description="Refresh cached FRED macro observations.")
    parser.add_argument("--force", action="store_true", help="Refresh even when the FRED cache is still fresh.")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    ensure_search_and_insights_schema(engine)
    db = SessionLocal()
    try:
        payload = refresh_fred_macro_cache(db, force=args.force)
        print(
            json.dumps(
                {
                    "status": payload.get("status"),
                    "refreshed_series": payload.get("refreshed_series"),
                    "failed_series": payload.get("failed_series"),
                    "last_refresh_at": payload.get("last_refresh_at"),
                    "missing_series": payload.get("missing_series"),
                },
                indent=2,
            )
        )
    finally:
        db.close()


if __name__ == "__main__":
    main()
