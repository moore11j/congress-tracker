"""Explicitly enabled official Congress publication. No email delivery."""
import argparse
import hashlib
import json
import os
from pathlib import Path

from app.background_job_guard import check_background_job_guard
from app.db import SessionLocal
from app.services.direct_feed_store import dumps
from app.services.direct_congress_worker import publish_batch
from app.services.feed_source_control import FeedSourceMismatch, FeedWriterBusy


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--feed', required=True, choices=('house_ptr', 'senate_ptr'))
    parser.add_argument('--directory', required=True, type=Path)
    parser.add_argument('--directory-sha256', required=True)
    parser.add_argument('--limit', type=int, default=100)
    parser.add_argument('--retry-held', action='store_true')
    args = parser.parse_args()
    if os.getenv('DIRECT_CONGRESS_PUBLICATION_ENABLED', 'false').strip().lower() != 'true':
        print(dumps({'status': 'disabled'})); return
    guard = check_background_job_guard('direct-congress-publication')
    if not guard.proceed:
        print(dumps(guard.to_dict())); return
    raw = args.directory.read_bytes()
    if len(raw) > 10_000_000 or hashlib.sha256(raw).hexdigest() != args.directory_sha256:
        raise ValueError('Verified Congress directory checksum differs')
    directory = json.loads(raw)
    if not isinstance(directory, list) or not 1 <= len(directory) <= 10000:
        raise ValueError('Congress directory is not a bounded person list')
    try:
        with SessionLocal() as db:
            result = publish_batch(db, feed=args.feed, directory=directory, limit=args.limit, retry_held=args.retry_held)
    except (FeedSourceMismatch, FeedWriterBusy) as exc:
        print(dumps({'status': 'skipped', 'reason': type(exc).__name__})); return
    print(dumps(result))
    if result['status'] == 'partial':
        raise SystemExit(1)


if __name__ == '__main__':
    main()
