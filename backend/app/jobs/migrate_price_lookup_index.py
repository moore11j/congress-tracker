"""One bounded, explicitly queued price lookup index build; no row changes."""
from __future__ import annotations

import argparse
import json
import os
import time

from sqlalchemy import text
from app.db import engine

INDEX_NAME = "ix_price_cache_upper_symbol_date"
DEFINITION = "CREATE INDEX ix_price_cache_upper_symbol_date ON public.price_cache USING btree (upper(symbol), date)"
DDL = "CREATE INDEX CONCURRENTLY ix_price_cache_upper_symbol_date ON public.price_cache ((upper(symbol)), date)"


def inspect_index(conn):
    row = conn.execute(text("""SELECT i.indisvalid AS valid, i.indisready AS ready,
        pg_get_indexdef(i.indexrelid) AS definition, pg_relation_size(i.indexrelid) AS bytes
        FROM pg_index i WHERE i.indexrelid=to_regclass(:name)"""),
        {"name": "public." + INDEX_NAME}).mappings().one_or_none()
    return dict(row) if row else None


def verify_index(row):
    if not row or not row["valid"] or not row["ready"] or row["definition"] != DEFINITION:
        raise ValueError("Price lookup index is absent, invalid or has a different definition; inspect before retrying")
    return row


def create_index(conn):
    existing = inspect_index(conn)
    if existing is not None:
        return {"created": False, "index": verify_index(existing)}
    conn.execute(text("SET statement_timeout='90s'"))
    conn.execute(text("SET lock_timeout='2s'"))
    conn.execute(text("SET maintenance_work_mem='32MB'"))
    conn.execute(text("SET max_parallel_maintenance_workers=0"))
    conn.exec_driver_sql(DDL)
    return {"created": True, "index": verify_index(inspect_index(conn))}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    if engine.dialect.name != "postgresql":
        raise ValueError("This bounded concurrent migration requires PostgreSQL")
    if args.apply and (os.getenv("FLY_PROCESS_GROUP") != "cron" or not os.getenv("WALNUT_JOB_TOKEN")):
        raise ValueError("Apply only through the serialized scheduled-jobs data lane")
    started = time.monotonic()
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        # Do not return maintenance session settings to the API connection pool.
        conn.detach()
        result = create_index(conn) if args.apply else {"created": False, "index": inspect_index(conn)}
    print(json.dumps({**result, "apply_requested": args.apply, "seconds": round(time.monotonic()-started, 3),
                      "canonical_row_writes": 0, "provider_requests": 0, "emails": 0}))


if __name__ == "__main__":
    main()
