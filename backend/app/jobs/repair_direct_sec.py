"""Inspect or apply one reviewed existing Form 4 correction. Never sends email."""
import argparse

from sqlalchemy import text
from app.db import SessionLocal
from app.services.direct_feed_store import dumps
from app.services.direct_sec_repair import inspect_staged_repair, apply_reviewed_staged_repair


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--document-id', required=True, type=int)
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--plan-sha256')
    parser.add_argument('--generation', type=int)
    args = parser.parse_args()
    if args.apply and (not args.plan_sha256 or args.generation is None):
        parser.error('--apply requires --plan-sha256 and --generation')
    with SessionLocal() as db:
        if args.apply:
            result = apply_reviewed_staged_repair(db, args.document_id,
                expected_plan_hash=args.plan_sha256, expected_generation=args.generation)
        else:
            if db.get_bind().dialect.name == 'postgresql':
                db.execute(text('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY'))
                db.execute(text("SET LOCAL statement_timeout = '20s'"))
            result = inspect_staged_repair(db, args.document_id)
            db.rollback()
    print(dumps(result))


if __name__ == '__main__':
    main()
