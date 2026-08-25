from __future__ import annotations

import argparse
import logging

from app.db import SessionLocal
from app.services.seo_snapshots import list_seo_snapshot_batch_candidates, refresh_seo_snapshot

logger = logging.getLogger(__name__)
ENTITY_TYPES = ("ticker", "member", "insider")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Refresh precomputed SEO snapshots from persisted Walnut data.")
    parser.add_argument("--entity-type", choices=[*ENTITY_TYPES, "all"], required=True)
    parser.add_argument("--entity-key", action="append", default=[], help="Symbol, member slug/Bioguide ID, or insider reporting CIK. Repeatable.")
    parser.add_argument("--batch", action="store_true", help="Select stored-data candidates automatically for the requested entity type.")
    parser.add_argument("--limit", type=int, default=25, help="Maximum candidates per entity type in batch mode. Capped by the service.")
    parser.add_argument("--include-existing", action="store_true", help="Allow batch mode to refresh snapshots that already exist.")
    parser.add_argument("--dry-run", action="store_true", help="Print selected candidates without writing snapshots.")
    return parser.parse_args()


def _requested_entity_types(entity_type: str) -> tuple[str, ...]:
    return ENTITY_TYPES if entity_type == "all" else (entity_type,)


def _selected_entity_keys(db, args: argparse.Namespace, entity_type: str) -> list[str]:
    if args.batch:
        return list_seo_snapshot_batch_candidates(
            db,
            entity_type,
            limit=args.limit,
            include_existing=args.include_existing,
        )
    if args.entity_type == "all":
        raise ValueError("--entity-type all requires --batch")
    return list(args.entity_key)


def main() -> int:
    args = _parse_args()
    refreshed = 0
    failed = 0
    selected: dict[str, list[str]] = {}
    if not args.batch and not args.entity_key:
        raise SystemExit("--entity-key is required unless --batch is set")
    with SessionLocal() as db:
        for entity_type in _requested_entity_types(args.entity_type):
            entity_keys = _selected_entity_keys(db, args, entity_type)
            selected[entity_type] = entity_keys
            if args.dry_run:
                continue
            for entity_key in entity_keys:
                try:
                    refresh_seo_snapshot(db, entity_type, entity_key)
                    db.commit()
                    refreshed += 1
                except Exception:
                    db.rollback()
                    failed += 1
                    logger.exception("seo_snapshot_refresh_failed entity_type=%s entity_key=%s", entity_type, entity_key)
    print({
        "status": "dry_run" if args.dry_run else "ok" if failed == 0 else "partial",
        "selected": selected,
        "refreshed": refreshed,
        "failed": failed,
    })
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
