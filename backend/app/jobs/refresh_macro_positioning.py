from __future__ import annotations

import logging

from app.db import SessionLocal, ensure_macro_positioning_schema
from app.services.macro_positioning import ingest_macro_positioning_assets, refresh_macro_positioning_cache

logger = logging.getLogger(__name__)


def run_macro_positioning_weekly_refresh() -> dict:
    ensure_macro_positioning_schema()
    db = SessionLocal()
    try:
        ingest_result = ingest_macro_positioning_assets(db)
        cache_result = refresh_macro_positioning_cache(db)
        result = {"status": ingest_result.get("status"), "assets": ingest_result, "cache": cache_result}
        logger.info(
            "macro_positioning_weekly_refresh status=%s assets=%s cache_refreshed=%s cache_skipped=%s",
            result.get("status"),
            ingest_result.get("refreshed"),
            cache_result.get("refreshed"),
            cache_result.get("skipped"),
        )
        return result
    finally:
        db.close()


if __name__ == "__main__":
    print(run_macro_positioning_weekly_refresh())
