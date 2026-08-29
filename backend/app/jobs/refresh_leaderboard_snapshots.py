from __future__ import annotations

import logging

from app.db import Base, SessionLocal, engine, ensure_leaderboard_snapshot_schema
from app.models import LeaderboardSnapshot
from app.services.leaderboard_snapshots import refresh_performance_leaderboard_snapshots
from app.services.top_stocks import refresh_top_stocks_leaderboard

logger = logging.getLogger(__name__)


def run() -> dict:
    """Refresh daily leaderboard caches. Future leaderboard builders belong here."""
    Base.metadata.create_all(bind=engine, tables=[LeaderboardSnapshot.__table__])
    ensure_leaderboard_snapshot_schema(engine)
    db = SessionLocal()
    try:
        payload = refresh_top_stocks_leaderboard(db)
        performance = refresh_performance_leaderboard_snapshots(db)
        result = {
            "top_stocks": {"returned": payload["returned"], "generated_at": payload["generated_at"]},
            **{key: {"returned": len(value.get("items") or []), "generated_at": value.get("generated_at")} for key, value in performance.items()},
        }
        logger.info("leaderboard_snapshot_refresh_complete result=%s", result)
        return result
    finally:
        db.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print(run())
