"""Opt-in SEC institutional publisher. Does not fetch data or deliver emails."""
import argparse
import os
import json


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--limit', type=int, default=100)
    parser.add_argument('--identifier-manifest')
    parser.add_argument('--staged-identifiers', action='store_true', help='Load the reviewed manifest from shared staging')
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument('--retry-waiting', action='store_true')
    modes.add_argument('--include-waiting', action='store_true', help='Share one bounded batch between new filings and rotating retries')
    parser.add_argument('--staged-references', action='store_true', help='Use verified, dated saved reference identities')
    args = parser.parse_args()
    if not 1 <= args.limit <= 200 or (args.include_waiting and args.limit < 2):
        parser.error('Limit must be 1 to 200, or 2 to 200 with include-waiting')
    if os.getenv('DIRECT_13F_PUBLICATION_ENABLED', 'false').strip().lower() != 'true':
        print(json.dumps({'status': 'disabled'}))
        return
    if not args.identifier_manifest:
        parser.error('Enabled publication requires a reviewed identifier-manifest')
    from app.background_job_guard import check_background_job_guard
    from app.db import SessionLocal
    from app.services.direct_feed_store import dumps
    from app.services.direct_13f_batch import load_identifier_manifest, publish_13f_batch
    from app.services.direct_13f_identifiers import load_staged_identifiers
    from app.services.feed_source_control import FeedSourceMismatch, FeedWriterBusy

    guard = check_background_job_guard('direct-13f-publication')
    if not guard.proceed:
        print(dumps(guard.to_dict()))
        return
    try:
        with SessionLocal() as db:
            identifiers = (load_staged_identifiers(db, args.identifier_manifest) if args.staged_identifiers
                           else load_identifier_manifest(args.identifier_manifest))
            result = publish_13f_batch(db, identifier_documents=identifiers,
                limit=args.limit, retry_waiting=args.retry_waiting, use_staged_references=args.staged_references,
                include_waiting=args.include_waiting)
    except (FeedSourceMismatch, FeedWriterBusy) as exc:
        print(dumps({'status': 'skipped', 'reason': type(exc).__name__}))
        return
    print(dumps(result))
    if result['status'] == 'partial':
        raise SystemExit(1)


if __name__ == '__main__':
    main()
