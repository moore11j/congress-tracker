"""Run bounded Phase 2 deterministic evidence processing outside request/page paths."""
from __future__ import annotations

import argparse
import json

from app.db import SessionLocal, engine, ensure_research_evidence_schema
from app.services.research_evidence import run_deterministic_adapters


def main() -> None:
    parser = argparse.ArgumentParser(description="Create idempotent Research Evidence events from retained structured source rows.")
    parser.add_argument("--security-id", type=int, help="Optional permanent security id for a controlled verification run.")
    parser.add_argument("--limit", type=int, default=200, help="Maximum source rows per adapter (1-1000).")
    parser.add_argument("--dry-run", action="store_true", help="Execute and roll back writes for verification.")
    args = parser.parse_args()

    ensure_research_evidence_schema(engine)
    with SessionLocal() as db:
        result = run_deterministic_adapters(db, security_id=args.security_id, limit=args.limit)
        if args.dry_run:
            db.rollback()
            result = {**result, "dry_run": True, "committed": False}
        else:
            db.commit()
            result = {**result, "dry_run": False, "committed": True}
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
