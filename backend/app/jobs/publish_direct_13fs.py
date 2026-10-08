"""Opt-in SEC institutional publisher. Does not fetch data or deliver emails."""
import argparse
import os

from app.background_job_guard import check_background_job_guard
from app.db import SessionLocal
from app.services.direct_feed_store import dumps
from app.services.direct_13f_batch import load_identifier_manifest, publish_13f_batch
from app.services.feed_source_control import FeedSourceMismatch, FeedWriterBusy


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--limit', type=int, default=100)
    parser.add_argument('--identifier-manifest')
    parser.add_argument('--retry-waiting', action='store_true')
    args = parser.parse_args()
    if os.getenv('DIRECT_13F_PUBLICATION_ENABLED', 'false').strip().lower() != 'true':
        print(dumps({'status': 'disabled'}))
        return
    if not args.identifier_manifest:
        parser.error('Enabled publication requires a reviewed identifier-manifest')
    guard = check_background_job_guard('direct-13f-publication')
    if not guard.proceed:
        print(dumps(guard.to_dict()))
        return
    try:
        with SessionLocal() as db:
            result = publish_13f_batch(db, identifier_documents=load_identifier_manifest(args.identifier_manifest),
                limit=args.limit, retry_waiting=args.retry_waiting)
    except (FeedSourceMismatch, FeedWriterBusy) as exc:
        print(dumps({'status': 'skipped', 'reason': type(exc).__name__}))
        return
    print(dumps(result))
    if result['status'] == 'partial':
        raise SystemExit(1)


if __name__ == '__main__':
    main()
