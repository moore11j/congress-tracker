"""Explicit bounded prior-quarter staging. Canonical publication is separate."""
import argparse
import os

from app.background_job_guard import check_background_job_guard
from app.clients.direct_sources import DirectSourceClient
from app.db import SessionLocal
from app.jobs.collect_direct_feeds import collector_lock
from app.services.direct_13f_priors import collect_13f_priors
from app.services.direct_feed_store import dumps


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--document-ids', nargs='+', type=int, required=True)
    args = parser.parse_args()
    if os.getenv('DIRECT_FEEDS_MODE', 'off').lower() != 'shadow':
        print(dumps({'status': 'disabled'})); return
    guard = check_background_job_guard('direct-13f-prior-collection')
    if not guard.proceed:
        print(dumps(guard.to_dict())); return
    with collector_lock() as acquired:
        if not acquired:
            print(dumps({'status': 'busy'})); return
        with SessionLocal() as db:
            result = collect_13f_priors(db, DirectSourceClient(), document_ids=args.document_ids)
    print(dumps(result))
    if result['status'] == 'partial':
        raise SystemExit(1)


if __name__ == '__main__':
    main()
