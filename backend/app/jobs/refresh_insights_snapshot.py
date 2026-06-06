from __future__ import annotations

import argparse
import json
import logging

from app.db import SessionLocal, engine, ensure_search_and_insights_schema
from app.services.insights_snapshots import INSIGHTS_SNAPSHOT_KIND, refresh_insights_snapshot

logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description="Refresh cached Insights market snapshots.")
    parser.add_argument("--kind", default="all", choices=("all", INSIGHTS_SNAPSHOT_KIND))
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    ensure_search_and_insights_schema(engine)
    db = SessionLocal()
    try:
        payload = refresh_insights_snapshot(db, kind=args.kind)
        print(json.dumps({"status": payload.get("status"), "as_of": payload.get("as_of"), "stale": payload.get("stale")}, indent=2))
    finally:
        db.close()


if __name__ == "__main__":
    main()
