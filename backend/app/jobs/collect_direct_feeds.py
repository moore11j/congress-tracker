"""Opt-in primary-source shadow collector and readiness report."""
from __future__ import annotations

import argparse
import json
import os
from contextlib import contextmanager
from datetime import date, timedelta
from pathlib import Path
from urllib.parse import urlsplit

from sqlalchemy import text

from app.background_job_guard import check_background_job_guard
from app.clients.direct_sources import DirectSourceClient
from app.clients.senate_efd import SenateEfdClient
from app.db import SessionLocal, engine
from app.services.direct_feed_collection import SOURCES, collect_direct_feeds
from app.services.direct_feed_store import dumps, ensure_direct_feed_schema, readiness_report


@contextmanager
def collector_lock(bind=engine):
    if bind.dialect.name != "postgresql":
        # Local SQLite is restricted to one operator; BEGIN IMMEDIATE is not
        # held across network requests. Database uniqueness still protects IDs.
        yield True
        return
    with bind.connect() as guard:
        if not guard.scalar(text("SELECT pg_try_advisory_lock(84193647)")):
            yield False
            return
        guard.detach()
        try:
            yield True
        finally:
            guard.execute(text("SELECT pg_advisory_unlock(84193647)"))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", action="store_true", help="Read staging coverage without requesting providers")
    parser.add_argument("--sources", nargs="+", choices=SOURCES, default=list(SOURCES))
    parser.add_argument("--start", type=date.fromisoformat, default=date.today() - timedelta(days=7))
    parser.add_argument("--end", type=date.fromisoformat, default=date.today() - timedelta(days=1))
    parser.add_argument("--symbols", nargs="*", default=os.getenv("DIRECT_FEEDS_SYMBOLS", "AAPL,MSFT,NVDA").split(","))
    parser.add_argument("--limit", type=int, default=25, help="Maximum documents per source per run")
    parser.add_argument("--recheck-hours", type=int, default=24)
    parser.add_argument("--retry-failed", action="store_true", help="Retry failed documents now, after an operator has resolved their cause")
    parser.add_argument("--issuer-registry", type=Path, default=Path(__file__).resolve().parents[2] / "config" / "direct_issuer_sources.json")
    parser.add_argument("--senate-reports", type=Path, help="Reviewed official PTR URL/filing metadata JSON list")
    args = parser.parse_args()
    if args.report:
        # Report never does DDL or initializes an empty database as healthy.
        with SessionLocal() as db:
            print(dumps(readiness_report(db)))
        return
    mode = os.getenv("DIRECT_FEEDS_MODE", "off").strip().lower()
    if mode == "off":
        print(dumps({"status": "disabled", "fmp_disabled": False}))
        return
    if mode != "shadow":
        raise SystemExit("DIRECT_FEEDS_MODE supports off/shadow only. Public cutover is not yet validated.")
    guard = check_background_job_guard("direct-feed-collection")
    if not guard.proceed:
        print(dumps(guard.to_dict()))
        return
    registry = json.loads(args.issuer_registry.read_text(encoding="utf-8")) if "issuer_earnings" in args.sources else []
    senate = json.loads(args.senate_reports.read_text(encoding="utf-8")) if args.senate_reports else []
    senate_client = SenateEfdClient(accept_notice=os.getenv('SENATE_PUBLIC_NOTICE_ACKNOWLEDGED', 'false').lower() == 'true') if 'senate_ptr' in args.sources else None
    # Host permission comes from the reviewed company registry, not a fetched
    # page or a redirect. Filings cannot add arbitrary hosts to the allowlist.
    hosts = {urlsplit(company["investor_website"]).hostname for company in registry}
    with collector_lock() as acquired:
        if not acquired:
            print(dumps({"status": "busy"}))
            return
        ensure_direct_feed_schema(engine)
        with SessionLocal() as db:
            result = collect_direct_feeds(db, DirectSourceClient(issuer_hosts=hosts), sources=args.sources,
                                         start=args.start, end=args.end, symbols=args.symbols, limit=args.limit,
                                         recheck_hours=args.recheck_hours, retry_failed=args.retry_failed, issuer_registry=registry, senate_reports=senate,
                                         senate_client=senate_client)
            print(dumps(result))
    if result["errors"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
