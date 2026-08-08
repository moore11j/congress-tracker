from __future__ import annotations

import argparse
import json
import logging

from app.db import SessionLocal, engine, ensure_search_entities_schema
from app.services.universal_search import rebuild_search_entities, search_coverage_audit, smoke_search_queries


DEFAULT_SMOKE_QUERIES = [
    "NVDA",
    "NVIDIA",
    "Apple",
    "AAPL",
    "Nancy Pelosi",
    "Pelosi",
    "Tim Cook",
    "Timothy Cook",
    "Timothy D. Cook",
    "Jensen Huang",
    "Jen-Hsun Huang",
    "DoD",
    "Department of Defense",
    "NVIDA",
    "Tim Cok",
    "Jenson Huang",
]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Rebuild and audit Walnut universal search entities.")
    parser.add_argument("--audit-only", action="store_true", help="Print coverage without rebuilding.")
    parser.add_argument("--smoke", action="store_true", help="Run named smoke queries after rebuild/audit.")
    parser.add_argument("--query", action="append", dest="queries", help="Smoke query to run. Repeat for multiple queries.")
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    logging.basicConfig(level=getattr(logging, str(args.log_level).upper(), logging.INFO))
    ensure_search_entities_schema(engine)
    db = SessionLocal()
    try:
        payload: dict[str, object] = {}
        if not args.audit_only:
            stats = rebuild_search_entities(db)
            db.commit()
            payload["rebuild"] = {
                "indexed_by_type": stats.indexed_by_type,
                "total_indexed": stats.total_indexed,
            }
        payload["coverage"] = search_coverage_audit(db)
        if args.smoke or args.queries:
            payload["smoke"] = smoke_search_queries(db, args.queries or DEFAULT_SMOKE_QUERIES)
        print(json.dumps(payload, indent=2, default=str))
    finally:
        db.close()


if __name__ == "__main__":
    main()
