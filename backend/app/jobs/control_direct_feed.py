"""Inspect/migrate direct-feed controls or explicitly select a validated source.

Selection requires all workers to have the shared writer guard deployed first.
This command cannot re-enable FMP after direct publication; pause for rollback.
"""
import argparse
from datetime import date

from sqlalchemy import select

from app.db import SessionLocal, engine
from app.services.direct_feed_store import dumps, ensure_direct_feed_schema
from app.services.feed_source_control import FeedSourceControl, select_feed_source


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('status', 'migrate', 'select'), default='status', nargs='?')
    parser.add_argument('--provider', choices=('sec_edgar', 'official_house', 'official_senate', 'paused'))
    parser.add_argument('--feed', choices=('sec_form4', 'sec_13f', 'house_ptr', 'senate_ptr'), default='sec_form4')
    parser.add_argument('--publish-since', type=date.fromisoformat)
    parser.add_argument('--expected-generation', type=int)
    parser.add_argument('--reason', help='Reference verified worker release and readiness evidence')
    args = parser.parse_args()
    if args.action == 'migrate':
        ensure_direct_feed_schema(engine)
        print(dumps({'status': 'schema_ready', 'source_changed': False}))
        return
    if args.action == 'select' and (not args.provider or args.publish_since is None or
                                   args.expected_generation is None or not args.reason):
        parser.error('select requires provider, publish-since, expected-generation and reason')
    with SessionLocal() as db:
        if args.action == 'select':
            select_feed_source(db, feed=args.feed, provider=args.provider, publish_since=args.publish_since,
                               expected_generation=args.expected_generation, reason=args.reason)
            db.commit()
        rows = db.scalars(select(FeedSourceControl)).all()
        print(dumps({'controls': [{c.name: getattr(row, c.name) for c in row.__table__.columns} for row in rows],
                     'absent_feed_default': 'fmp', 'publication_enabled_separately': True}))


if __name__ == '__main__':
    main()
