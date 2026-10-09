"""Run the production repair planner against a saved public baseline in memory."""
import argparse
import base64
from collections import Counter
import hashlib
import json
import os
from pathlib import Path
from unittest.mock import patch


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--baseline', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2] / 'artifacts' / 'direct-feeds'
    assert args.output.resolve().is_relative_to(root.resolve())
    os.environ['DATABASE_URL'] = 'sqlite:///:memory:'
    from sqlalchemy import select
    from app.db import Base, engine, SessionLocal
    from app.models import Event, InsiderTransactionNormalized, InsiderTransaction
    from app.services.direct_feed_collection import parse_document
    from app.services.direct_feed_store import discover, record_document
    from app.services.direct_feed_rehearsal import digest, snapshot
    from app.services.direct_sec_repair import inspect_staged_repair, rehearse_staged_repair, SecRepairReceipt
    from scripts.compare_direct_sec_baseline import load_baseline
    assert engine.dialect.name == 'sqlite' and engine.url.database == ':memory:'
    Base.metadata.create_all(engine)
    raw_baseline = args.baseline.read_bytes()
    baseline = json.loads(raw_baseline)
    assert baseline['transaction_read_only']
    def forbidden(*args, **kwargs):
        raise AssertionError('Offline repair attempted network or email')
    results = []
    with patch('requests.sessions.Session.request', forbidden), patch('httpx.Client.send', forbidden), \
         patch('socket.create_connection', forbidden), SessionLocal() as db:
        load_baseline(db, baseline)
        def state():
            return digest({model.__tablename__: [snapshot(row) for row in db.scalars(select(model).order_by(model.id))]
                for model in (Event, InsiderTransactionNormalized, InsiderTransaction)})
        for item in baseline['documents']:
            raw = base64.b64decode(item['raw_base64'], validate=True)
            assert hashlib.sha256(raw).hexdigest() == item['content_hash']
            source, parsed, reasons = parse_document('sec_form4', raw, item['metadata'])
            staged = discover(db, 'sec_form4', item['metadata'])
            record_document(db, staged, raw, source, parsed, reasons=reasons)
            db.commit()
            review = inspect_staged_repair(db, staged.id)
            result = dict(review, source_document_id=item['id'])
            if review['status'] == 'planned':
                result['apply'] = rehearse_staged_repair(db, staged.id, expected_plan_hash=review['plan_sha256'])
                db.commit()
            results.append(result)
        after = state()
        for receipt in db.scalars(select(SecRepairReceipt)):
            assert rehearse_staged_repair(db, receipt.document_id, expected_plan_hash=receipt.plan_hash)['updated'] == 0
        assert after == state()
    totals = Counter()
    for item in results:
        if 'apply' in item:
            totals.update(item['updates'])
    report = dict(baseline_sha256=hashlib.sha256(raw_baseline).hexdigest(),
        statuses=dict(Counter(item['status'] for item in results)), updates=dict(totals),
        repeat_state_sha256=after, results=results, production_writes=0, emails=0)
    args.output.write_text(json.dumps(report, indent=2), encoding='utf-8')
    print(json.dumps({k: v for k, v in report.items() if k != 'results'}))


if __name__ == '__main__':
    main()
