from __future__ import annotations

import argparse
import logging
import sys

from app.db import Base, SessionLocal, engine, ensure_index_membership_metadata_schema
from app.models import IndexMembership
from app.services.index_memberships import (
    INDEX_UNIVERSES,
    refresh_all_index_memberships_from_provider,
    refresh_index_memberships_from_provider,
)

logger = logging.getLogger(__name__)


def run(*, index: str | None = None, all_indexes: bool = False, source: str | None = None, dry_run: bool = False) -> list[dict]:
    Base.metadata.create_all(bind=engine, tables=[IndexMembership.__table__])
    ensure_index_membership_metadata_schema(engine)
    db = SessionLocal()
    try:
        if all_indexes or not index:
            results = refresh_all_index_memberships_from_provider(db, source=source, dry_run=dry_run)
        else:
            results = [refresh_index_memberships_from_provider(db, index, source=source, dry_run=dry_run)]
        payload = [result.__dict__ for result in results]
        logger.info("index_membership_refresh_job_complete dry_run=%s source=%s results=%s", dry_run, source, payload)
        return payload
    finally:
        db.close()


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Refresh Walnut index membership reference data.")
    parser.add_argument("--index", choices=sorted(INDEX_UNIVERSES), help="Refresh one index universe.")
    parser.add_argument("--all", action="store_true", help="Refresh all configured index universes.")
    parser.add_argument("--source", default=None, help="Provider source override, for example wikipedia or fmp.")
    parser.add_argument("--dry-run", action="store_true", help="Download, parse, and validate without activating memberships.")
    args = parser.parse_args(argv)
    if args.index and args.all:
        parser.error("Pass --index or --all, not both.")
    if not args.index and not args.all:
        args.all = True
    return args


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    results = run(index=args.index, all_indexes=args.all, source=args.source, dry_run=args.dry_run)
    failed_statuses = {"failed", "rejected", "restricted"}
    for result in results:
        print(result)
    return 1 if any(result.get("status") in failed_statuses for result in results) else 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    raise SystemExit(main())
