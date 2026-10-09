"""Explicit bounded prior-quarter staging. Canonical publication is separate."""
import argparse
import os
from datetime import date

from app.background_job_guard import check_background_job_guard
from app.clients.direct_sources import DirectSourceClient
from app.db import SessionLocal
from app.jobs.collect_direct_feeds import collector_lock
from app.services.direct_13f_priors import collect_13f_priors, pending_prior_documents
from app.services.direct_feed_store import dumps


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    selection = parser.add_mutually_exclusive_group(required=True)
    selection.add_argument('--document-ids', nargs='+', type=int)
    selection.add_argument('--since', type=date.fromisoformat, help='Automatically select recent staged filings from this boundary')
    parser.add_argument('--limit', type=int, default=20)
    parser.add_argument('--recheck-hours', type=int, default=24)
    parser.add_argument('--lookback-days', type=int, default=93)
    parser.add_argument('--preview', action='store_true', help='Read automatic selection without network or writes')
    args = parser.parse_args()
    if os.getenv('DIRECT_FEEDS_MODE', 'off').lower() != 'shadow':
        print(dumps({'status': 'disabled'})); return
    if args.preview and not args.since:
        parser.error('--preview requires --since')
    guard = check_background_job_guard('direct-13f-prior-collection')
    if not guard.proceed:
        print(dumps(guard.to_dict())); return
    with collector_lock(recover_orphaned=not args.preview) as acquired:
        if not acquired:
            print(dumps({'status': 'busy'})); return
        with SessionLocal() as db:
            picked = (pending_prior_documents(db, since=args.since, limit=args.limit,
                      recheck_hours=args.recheck_hours, lookback_days=args.lookback_days) if args.since else None)
            ids = picked['document_ids'] if picked is not None else args.document_ids
            if args.preview or not ids:
                print(dumps({'status': 'preview' if args.preview else 'idle', 'selection': picked, 'public_writes': 0})); return
            result = collect_13f_priors(db, DirectSourceClient(), document_ids=ids)
            if picked is not None:
                result['selection'] = picked
    print(dumps(result))
    if result['status'] == 'partial':
        raise SystemExit(1)


if __name__ == '__main__':
    main()
