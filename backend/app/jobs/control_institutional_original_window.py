"""Explicit operator control, no recurring schedule or automatic source hold."""
import argparse
import json
import os


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('open', 'close'))
    parser.add_argument('--window-id', required=True)
    parser.add_argument('--package-sha256', required=True)
    parser.add_argument('--approved-at', type=float)
    parser.add_argument('--expires-at', type=float)
    parser.add_argument('--approve-source-collection-hold', action='store_true')
    args = parser.parse_args(argv)
    from app.jobs.capture_direct_13f_evidence import package_hash
    from app.jobs.collect_direct_feeds import collector_lock
    from app.services.institutional_original_window import create_window, close_window, SCOPE
    if (not args.approve_source_collection_hold or args.package_sha256 != package_hash() or
            os.getenv('COMBINED_SCHEDULER_ENABLED', '').lower() != 'true' or
            os.getenv('FLY_MACHINE_ID') != os.getenv('COMBINED_SCHEDULER_MACHINE_ID') or
            not os.getenv('FLY_MACHINE_ID') or
            os.getenv('DIRECT_13F_PUBLICATION_ENABLED', 'false').lower() != 'false'):
        raise ValueError('exact_source_hold_approval_required')
    with collector_lock(recover_orphaned=False) as acquired:
        if not acquired:
            print(json.dumps({'status': 'busy', 'changed': False})); return
        if args.action == 'open':
            create_window(dict(version=1, window_id=args.window_id, package_sha256=args.package_sha256,
                approved_at=args.approved_at, expires_at=args.expires_at, scope=SCOPE),
                expected_package=args.package_sha256)
        else:
            close_window(args.window_id)
    print(json.dumps({'status': 'opened' if args.action == 'open' else 'closed',
                     'window_id': args.window_id, 'production_database_writes': 0}))


if __name__ == '__main__':
    main()
