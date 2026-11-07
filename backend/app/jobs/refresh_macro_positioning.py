from __future__ import annotations

import logging

from app.db import SessionLocal, ensure_macro_positioning_schema
from app.services.macro_positioning import ingest_macro_positioning_assets, refresh_macro_positioning_cache, refresh_macro_positioning_feed_events, get_insights_macro_positioning

logger = logging.getLogger(__name__)


def run_macro_positioning_weekly_refresh() -> dict:
    ensure_macro_positioning_schema()
    db = SessionLocal()
    try:
        ingest_result = ingest_macro_positioning_assets(db)
        feed_result = (
            refresh_macro_positioning_feed_events(db)
            if ingest_result.get("status") == "ok"
            else {"status": "skipped", "reason": "asset_ingest_incomplete"}
        )
        cache_result = refresh_macro_positioning_cache(db)
        # At most one summary request, after durable source/feed/ticker data.
        commentary_failed = cache_result.get('commentary_attempted', 0) > cache_result.get('commentary_enriched', 0)
        summary = get_insights_macro_positioning(db, prepare_summary=ingest_result.get('status') == 'ok' and not commentary_failed)
        result = {"status": ingest_result.get("status"), "assets": ingest_result, "cache": cache_result, "feed": feed_result}
        result['summary_source'] = summary.get('summary_source')
        logger.info(
            "macro_positioning_weekly_refresh status=%s assets=%s cache_refreshed=%s cache_skipped=%s feed_generated=%s",
            result.get("status"),
            ingest_result.get("refreshed"),
            cache_result.get("refreshed"),
            cache_result.get("skipped"),
            feed_result.get("generated"),
        )
        return result
    finally:
        db.close()


if __name__ == "__main__":
    print(run_macro_positioning_weekly_refresh())
