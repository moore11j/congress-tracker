from __future__ import annotations

import logging

from app.db import SessionLocal, ensure_macro_positioning_schema
from app.services.macro_positioning import refresh_macro_positioning_cache

logger = logging.getLogger(__name__)


def run_macro_positioning_weekly_refresh() -> dict:
    ensure_macro_positioning_schema()
    db = SessionLocal()
    try:
        result = refresh_macro_positioning_cache(db)
        logger.info("macro_positioning_weekly_refresh status=%s refreshed=%s skipped=%s", result.get("status"), result.get("refreshed"), result.get("skipped"))
        return result
    finally:
        db.close()


if __name__ == "__main__":
    print(run_macro_positioning_weekly_refresh())
