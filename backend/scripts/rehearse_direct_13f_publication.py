"""Replay saved 13Fs into an in-memory canonical database; no network or mail."""
import argparse
from collections import Counter
from datetime import date
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
from unittest.mock import patch


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--baseline', type=Path, required=True)
    parser.add_argument('--staging', type=Path, nargs='+', required=True)
    parser.add_argument('--publish-since', type=date.fromisoformat, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2] / 'artifacts' / 'direct-feeds'
    if not args.output.resolve().is_relative_to(root.resolve()):
        parser.error('Output must stay inside artifacts/direct-feeds')
    os.environ['DATABASE_URL'] = 'sqlite:///:memory:'
    from sqlalchemy import select, func
    from app.db import Base, engine, SessionLocal
    from app.models import (InstitutionalFiling, InstitutionalPosition, InstitutionalPositionChange,
                            InstitutionalHolder, InstitutionalSymbolSummary, InstitutionalActivityEvent,
                            Event, MonitoringAlert, EmailDelivery)
    from app.services.direct_13f_publication import rehearse_new_13f
    from app.clients.direct_sources import DirectSourceError
    from app.services.direct_feed_store import dumps
    from compare_direct_sec_baseline import load_baseline
    assert engine.dialect.name == 'sqlite' and engine.url.database == ':memory:'
    Base.metadata.create_all(engine)
    baseline_bytes = args.baseline.read_bytes()
    baseline = json.loads(baseline_bytes)
    assert baseline['transaction_read_only'] is True
    documents = {}
    for path in args.staging:
        sources = {}
        for source in path.parent.glob('*.source'):
            raw = source.read_bytes()
            assert hashlib.sha256(raw).hexdigest() == source.stem
            match = re.search(rb'ACCESSION NUMBER:\s*(\d{10}-\d{2}-\d{6})', raw)
            if match:
                sources[match[1].decode()] = (source.stem, raw)
        with sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True) as saved:
            for metadata_json, stored_hash in saved.execute(
                    "SELECT metadata_json, content_hash FROM direct_feed_documents WHERE feed='sec_13f'"):
                metadata = json.loads(metadata_json)
                digest, raw = sources[metadata['key']]
                assert not stored_hash or digest == stored_hash
                previous = documents.get(metadata['key'])
                assert previous is None or previous['content_hash'] == digest
                documents[metadata['key']] = {'feed': 'sec_13f', 'metadata': metadata, 'raw': raw, 'content_hash': digest}
    ordered = sorted(documents.values(), key=lambda d: (d['metadata']['filing_date'], d['metadata']['key']))
    models = (InstitutionalFiling, InstitutionalPosition, InstitutionalPositionChange, InstitutionalHolder,
              InstitutionalSymbolSummary, InstitutionalActivityEvent, Event, MonitoringAlert, EmailDelivery)
    with patch('socket.socket.connect', side_effect=AssertionError('Offline rehearsal attempted network')), \
            patch('app.services.email_digests._send_digest', side_effect=AssertionError('No-send rehearsal')), \
            SessionLocal() as db:
        load_baseline(db, baseline)
        def state():
            rows = {model.__tablename__: [
                {c.name: getattr(row, c.name) for c in model.__table__.columns}
                for row in db.scalars(select(model).order_by(*model.__table__.primary_key.columns))] for model in models}
            return hashlib.sha256(dumps(rows).encode()).hexdigest()
        def run():
            results = []
            for document in ordered:
                try:
                    result = rehearse_new_13f(db, document, publish_since=args.publish_since)
                    db.commit()
                except (ValueError, DirectSourceError) as exc:
                    db.rollback()
                    result = {'status': 'rejected', 'reason': str(exc), 'inserted_filings': 0,
                              'inserted_positions': 0, 'feed_events': 0}
                results.append({'accession': document['metadata']['key'], **result})
            return results
        before = state()
        first = run()
        after = state()
        repeat = run()
        assert state() == after, 'Repeat changed canonical state'
        assert sum(r['inserted_filings'] + r['inserted_positions'] + r['feed_events'] for r in repeat) == 0
        assert db.scalar(select(func.count()).select_from(EmailDelivery)) == 0
        report = {'baseline_captured_at': baseline['captured_at'],
            'baseline_sha256': hashlib.sha256(baseline_bytes).hexdigest(),
            'publish_since': args.publish_since.isoformat(), 'documents': len(ordered),
            'first_statuses': dict(Counter(r['status'] for r in first)),
            'derived_states': dict(Counter(r.get('derived_state', r['status']) for r in first)),
            'first_totals': {key: sum(r.get(key, 0) for r in first) for key in
                ('inserted_filings', 'inserted_positions', 'changes', 'summaries', 'activity_events', 'feed_events')},
            'before_sha256': before, 'after_sha256': after, 'repeat_state_identical': True,
            'repeat_new_records': 0, 'email_deliveries': 0, 'production_writes': 0,
            'cutover_ready': False, 'publications': first,
            'limitations': 'Saved source corpus and public-record baseline only; no live collection, '
                'production repair, full-universe ranking refresh or delivery validation.'}
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(dumps(report), encoding='utf-8')
        print(dumps({k: v for k, v in report.items() if k != 'publications'}))


if __name__ == '__main__':
    main()
