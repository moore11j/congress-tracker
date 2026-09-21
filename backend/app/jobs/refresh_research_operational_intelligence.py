"""Bounded source ingestion and one-time operational evidence extraction."""
from __future__ import annotations

import argparse
import json
from contextlib import contextmanager
from sqlalchemy import text

from app.db import SessionLocal, engine, ensure_research_claim_matching_schema, ensure_research_evidence_schema
from app.models import Security
from app.services.operational_intelligence import candidate_securities, refresh_operational_intelligence


@contextmanager
def operational_refresh_lock(bind=engine):
    """Keep one dedicated lock connection outside the two-slot cron work pool."""
    if bind.dialect.name != 'postgresql':
        yield True
        return
    with bind.connect() as guard:
        if not guard.scalar(text('SELECT pg_try_advisory_lock(84193639)')):
            yield False
            return
        # Detached connections close physically on context exit. This keeps the
        # lock across source commits without starving provider/AI audit writes.
        guard.detach()
        try:
            yield True
        finally:
            guard.execute(text('SELECT pg_advisory_unlock(84193639)'))


def main() -> None:
    parser = argparse.ArgumentParser(description="Refresh global operational Research Evidence from news, releases, and optionally transcripts.")
    parser.add_argument("--security-id", type=int)
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    # A dry run is deliberately a read-only scheduling preview. Do not run
    # additive schema work, fetch providers, or invoke extraction—the latter
    # owns commits to make each durable source event available to claim matching.
    if args.dry_run:
        with SessionLocal() as db:
            candidates = [db.get(Security, args.security_id)] if args.security_id else candidate_securities(db, limit=args.limit)
            securities = [security for security in candidates if security is not None]
            result = {
                "status": "dry_run",
                "dry_run": True,
                "committed": False,
                "securities": len(securities),
                "security_ids": [security.id for security in securities],
                "symbols": [security.symbol for security in securities],
                "documents": 0,
                "events": 0,
                "matches": 0,
                "skipped": 0,
            }
        print(json.dumps(result, sort_keys=True))
        return
    ensure_research_evidence_schema(engine)
    ensure_research_claim_matching_schema(engine)
    with operational_refresh_lock() as acquired:
        if not acquired:
            print(json.dumps({"status": "busy", "committed": False}))
            return
        with SessionLocal() as db:
            result = refresh_operational_intelligence(db, security_id=args.security_id, limit=args.limit)
            db.commit(); result = {**result, "dry_run": False, "committed": True}
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
