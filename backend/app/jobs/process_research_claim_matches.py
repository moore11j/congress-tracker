"""Run bounded Phase 3 private claim matching outside user-facing request paths."""
from __future__ import annotations

import argparse
import json

from app.db import SessionLocal, engine, ensure_research_claim_matching_schema, ensure_research_evidence_schema
from app.services.research_claim_matching import run_claim_matching


def main() -> None:
    parser = argparse.ArgumentParser(description="Match bounded global Evidence Events to active private Research Memory claims.")
    parser.add_argument("--evidence-event-id", help="Optional one-event verification run.")
    parser.add_argument("--security-id", type=int, help="Optional permanent security id scope.")
    parser.add_argument("--limit", type=int, default=100, help="Maximum events to process (1-500).")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    ensure_research_evidence_schema(engine)
    ensure_research_claim_matching_schema(engine)
    with SessionLocal() as db:
        result = run_claim_matching(db, evidence_event_id=args.evidence_event_id, security_id=args.security_id, limit=args.limit)
        if args.dry_run:
            db.rollback(); result = {**result, "dry_run": True, "committed": False}
        else:
            db.commit(); result = {**result, "dry_run": False, "committed": True}
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
