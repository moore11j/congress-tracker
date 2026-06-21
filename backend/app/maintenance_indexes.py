from __future__ import annotations

import argparse
import logging

from app.db import OPTIONAL_PERFORMANCE_INDEXES, engine, ensure_optional_performance_indexes


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create optional production performance indexes outside web startup."
    )
    parser.add_argument(
        "--index",
        action="append",
        dest="indexes",
        choices=[spec.name for spec in OPTIONAL_PERFORMANCE_INDEXES],
        help="Optional index name to create. Repeat to create multiple. Defaults to all optional indexes.",
    )
    parser.add_argument(
        "--no-concurrent",
        action="store_true",
        help="Disable Postgres CREATE INDEX CONCURRENTLY. Do not use on hot production tables.",
    )
    parser.add_argument("--lock-timeout", default="2s")
    parser.add_argument("--statement-timeout", default="30s")
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    logging.basicConfig(level=getattr(logging, str(args.log_level).upper(), logging.INFO))
    result = ensure_optional_performance_indexes(
        engine,
        concurrent=not args.no_concurrent,
        index_names=set(args.indexes) if args.indexes else None,
        lock_timeout=args.lock_timeout,
        statement_timeout=args.statement_timeout,
    )
    logging.getLogger(__name__).info("optional_index_maintenance_complete result=%s", result)


if __name__ == "__main__":
    main()
