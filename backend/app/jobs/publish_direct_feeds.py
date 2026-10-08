"""Explicitly enabled canonical Form 4 publisher, with no email delivery."""
import argparse
import os

from app.background_job_guard import check_background_job_guard
from app.db import SessionLocal
from app.services.direct_feed_store import dumps
from app.services.direct_feed_worker import publish_batch
from app.services.feed_source_control import FeedSourceMismatch, FeedWriterBusy


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--limit', type=int, default=100)
    parser.add_argument('--retry-held', action='store_true')
    args = parser.parse_args()
    if os.getenv('DIRECT_FEED_PUBLICATION_ENABLED', 'false').strip().lower() != 'true':
        print(dumps({'status': 'disabled'}))
        return
    guard = check_background_job_guard('direct-feed-publication')
    if not guard.proceed:
        print(dumps(guard.to_dict()))
        return
    try:
        with SessionLocal() as db:
            result = publish_batch(db, limit=args.limit, retry_held=args.retry_held)
    except (FeedSourceMismatch, FeedWriterBusy) as exc:
        print(dumps({'status': 'skipped', 'reason': type(exc).__name__}))
        return
    print(dumps(result))
    if result['status'] == 'partial':
        raise SystemExit(1)


if __name__ == '__main__':
    main()
