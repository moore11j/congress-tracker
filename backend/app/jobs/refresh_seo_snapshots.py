from __future__ import annotations

import argparse
import logging

from app.db import SessionLocal
from app.services.seo_snapshots import refresh_seo_snapshot

logger = logging.getLogger(__name__)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Refresh precomputed SEO snapshots from persisted Walnut data.")
    parser.add_argument("--entity-type", choices=["ticker", "member", "insider"], required=True)
    parser.add_argument("--entity-key", action="append", required=True, help="Symbol, member slug/Bioguide ID, or insider reporting CIK. Repeatable.")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    refreshed = 0
    failed = 0
    with SessionLocal() as db:
        for entity_key in args.entity_key:
            try:
                refresh_seo_snapshot(db, args.entity_type, entity_key)
                db.commit()
                refreshed += 1
            except Exception:
                db.rollback()
                failed += 1
                logger.exception("seo_snapshot_refresh_failed entity_type=%s entity_key=%s", args.entity_type, entity_key)
    print({"status": "ok" if failed == 0 else "partial", "refreshed": refreshed, "failed": failed})
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
