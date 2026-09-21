"""Bounded source ingestion and one-time operational evidence extraction."""
from __future__ import annotations

import argparse
import json
from sqlalchemy import text

from app.db import SessionLocal, engine, ensure_research_claim_matching_schema, ensure_research_evidence_schema
from app.models import Security
from app.services.operational_intelligence import candidate_securities, refresh_operational_intelligence


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
    # A dedicated session-level lock survives source commits and prevents two
    # schedulers from multiplying per-run spend. Unlock before returning to pool.
    with engine.connect() as guard:
        postgres = guard.dialect.name == "postgresql"
        if postgres and not guard.scalar(text("SELECT pg_try_advisory_lock(84193639)")):
            print(json.dumps({"status": "busy", "committed": False}))
            return
        try:
            with SessionLocal() as db:
                result = refresh_operational_intelligence(db, security_id=args.security_id, limit=args.limit)
                db.commit(); result = {**result, "dry_run": False, "committed": True}
        finally:
            if postgres:
                guard.execute(text("SELECT pg_advisory_unlock(84193639)"))
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
