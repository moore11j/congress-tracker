"""Inspect one staged Congress duplicate repair; application is explicit and disabled by default."""
import argparse
import hashlib
import json
import os
from pathlib import Path

from sqlalchemy import select, text
from sqlalchemy.orm import Session

from app.db import engine, SessionLocal
from app.services.direct_feed_store import DirectFeedDocument, DirectFeedRevision, dumps
from app.services.feed_source_control import FeedSourceControl
from app.services.direct_congress_repair import inspect_duplicate_repair, apply_reviewed_duplicate_repair


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--document-id', type=int, required=True)
    parser.add_argument('--source-sha256', required=True)
    parser.add_argument('--directory', type=Path, required=True)
    parser.add_argument('--directory-sha256', required=True)
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--before-sha256')
    parser.add_argument('--jobs-sha256')
    parser.add_argument('--generation', type=int)
    args = parser.parse_args()
    if args.apply and os.getenv('DIRECT_CONGRESS_REPAIR_ENABLED', 'false').lower() != 'true':
        print(dumps({'status': 'disabled'}))
        return
    if engine.dialect.name != 'postgresql':
        raise ValueError('Production inspection/application requires PostgreSQL')
    if args.apply and (not args.before_sha256 or not args.jobs_sha256 or args.generation is None):
        raise ValueError('Application requires both inspected hashes and paused generation')
    raw_directory = args.directory.read_bytes()
    if len(raw_directory) > 10_000_000 or hashlib.sha256(raw_directory).hexdigest() != args.directory_sha256:
        raise ValueError('Directory checksum differs')
    directory = json.loads(raw_directory)
    if not isinstance(directory, list) or not 1 <= len(directory) <= 10000:
        raise ValueError('Directory must be a bounded person list')
    with engine.connect() as conn, conn.begin():
        conn.execute(text('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY'))
        conn.execute(text("SET LOCAL statement_timeout = '20s'"))
        with Session(bind=conn) as db:
            staged = db.get(DirectFeedDocument, args.document_id)
            if (staged is None or staged.feed not in {'house_ptr', 'senate_ptr'}
                    or staged.status not in {'parsed', 'quarantined'}
                    or staged.content_hash != args.source_sha256):
                raise ValueError('Staged source identity/state differs')
            revision = db.scalar(select(DirectFeedRevision).where(
                DirectFeedRevision.document_id == staged.id,
                DirectFeedRevision.content_hash == args.source_sha256))
            if (revision is None or not revision.source_bytes
                    or hashlib.sha256(revision.source_bytes).hexdigest() != args.source_sha256):
                raise ValueError('Captured source bytes unavailable or changed')
            document = {'document_id': staged.id, 'feed': staged.feed,
                'content_hash': staged.content_hash, 'metadata': json.loads(staged.metadata_json),
                'raw': revision.source_bytes}
            control = db.get(FeedSourceControl, staged.feed)
            source_state = {'provider': control.provider if control else 'fmp',
                            'generation': control.generation if control else 0}
            plan = inspect_duplicate_repair(db, document, directory)
    if not args.apply:
        print(dumps({'document_id': args.document_id, 'source_control': source_state,
                     'transaction_read_only': True, 'plan': plan}))
        return
    with SessionLocal() as db:
        result = apply_reviewed_duplicate_repair(db, document, directory,
            expected_before_hash=args.before_sha256, expected_jobs_hash=args.jobs_sha256,
            expected_generation=args.generation)
    print(dumps(result))
    if result['status'] not in {'applied', 'existing'}:
        raise SystemExit(1)


if __name__ == '__main__':
    main()
