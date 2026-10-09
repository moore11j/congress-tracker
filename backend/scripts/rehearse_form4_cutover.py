"""Reparse a fresh public Form 4 baseline and rehearse publication in memory."""
import argparse
import base64
from collections import Counter
from datetime import date, datetime
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
    from sqlalchemy import Date, DateTime, select, func
    from app.db import Base, engine, SessionLocal
    from app.models import Event, SecForm4Filing, MonitoringAlert, UserAccount, Watchlist, EmailDelivery
    from app.services.direct_feed_store import discover, record_document, dumps, reconcile_insider
    from app.services.direct_feed_collection import parse_document
    from app.services.direct_feed_rehearsal import plan_corrections, include_event_corrections, apply_rehearsal
    from app.services.direct_feed_worker import publish_batch, publish_document
    from app.services.feed_source_control import select_feed_source
    from app.services.monitoring_alerts import _ensure_alert_for_event
    from scripts.compare_direct_sec_baseline import load_baseline
    assert engine.dialect.name == 'sqlite' and engine.url.database == ':memory:'
    Base.metadata.create_all(engine)
    baseline_bytes = args.baseline.read_bytes()
    baseline = json.loads(baseline_bytes)
    assert baseline['transaction_read_only']
    documents = [{**item, 'feed':'sec_form4', 'raw':base64.b64decode(item['raw_base64'])}
                 for item in baseline['documents']]
    def forbidden(*a, **k):
        raise AssertionError('Rehearsal attempted external request or mail')
    with patch('requests.sessions.Session.request', forbidden), patch('httpx.Client.send', forbidden), \
         patch('socket.create_connection', forbidden), patch('app.services.email_digests._send_digest', forbidden), SessionLocal() as db:
        load_baseline(db, baseline)
        for source in baseline['form4_filings']:
            record = {}
            for column in SecForm4Filing.__table__.columns:
                value = source[column.name]
                if value is not None:
                    if isinstance(column.type, DateTime): value = datetime.fromisoformat(value)
                    elif isinstance(column.type, Date): value = date.fromisoformat(value)
                record[column.name] = value
            db.add(SecForm4Filing(**record))
        db.commit()
        correction = include_event_corrections(db, plan_corrections(db, documents))
        repaired = apply_rehearsal(db, correction)
        db.commit()
        assert apply_rehearsal(db, correction)['updated'] == 0
        parsing, reconciliation, held_sources = Counter(), Counter(), []
        for doc in documents:
            source, parsed, reasons = parse_document(doc['feed'], doc['raw'], doc['metadata'])
            matched = reconcile_insider(db, parsed)
            for key in ('matched','unmatched','ambiguous','existing_only_ids'):
                reconciliation[key] += len(matched[key])
            reasons = list(reasons)
            if matched['ambiguous'] or matched['existing_only_ids']:
                reasons.append('Existing records differ from source; canonical reconciliation required')
            row = discover(db, doc['feed'], doc['metadata'])
            record_document(db, row, doc['raw'], source, parsed, reasons=reasons)
            parsing[row.status] += 1
            if reasons: held_sources.append(dict(source_id=doc['id'], accession=doc['metadata']['key'], reasons=reasons))
        db.commit()
        select_feed_source(db, feed='sec_form4', provider='sec_edgar', publish_since=date.fromisoformat(baseline['filing_date']),
                           expected_generation=0, reason='Fresh public baseline rehearsal in memory only')
        db.commit()
        before_ids = set(db.scalars(select(Event.id)))
        batches = [publish_batch(db, limit=200) for _ in range(4)]
        assert batches[-1]['processed'] == 0
        results = [row for batch in batches for row in batch['results']]
        new_events = list(db.scalars(select(Event).where(Event.id.not_in(before_ids))))
        user = UserAccount(email='form4-cutover@example.test', entitlement_tier='premium')
        db.add(user); db.flush()
        watchlist = Watchlist(name='Synthetic SEC cutover', owner_user_id=user.id)
        db.add(watchlist); db.flush()
        for event in new_events:
            assert _ensure_alert_for_event(db, user_id=user.id, watchlist=watchlist, event=event)
            payload = json.loads(event.payload_json)
            assert payload['source_availability']['date'] == event.ts.date().isoformat()
            assert event.ts.date() >= event.event_date.date()
        db.commit()
        def fingerprint():
            digest = hashlib.sha256()
            for table in Base.metadata.sorted_tables:
                rows = [dict(row._mapping) for row in db.execute(select(table))]
                digest.update(dumps([table.name, sorted(rows, key=dumps)]).encode())
            return digest.hexdigest()
        before = fingerprint()
        for row in results:
            if row['status'] in {'published','existing'}:
                repeat = publish_document(db, row['document_id'])
                assert repeat['status'] == 'existing' and repeat['inserted_events'] == 0
        for event in new_events:
            assert not _ensure_alert_for_event(db, user_id=user.id, watchlist=watchlist, event=event)
        db.commit()
        assert fingerprint() == before
        assert db.scalar(select(func.count()).select_from(EmailDelivery)) == 0
        report = dict(baseline_captured_at=baseline['captured_at'], baseline_sha256=hashlib.sha256(baseline_bytes).hexdigest(),
            sources=len(documents), baseline_counts={key:len(baseline[key]) for key in ['insiders','raw_insiders','events','form4_filings']},
            canonical_corrections_local_only=repaired, correction_fields=dict(Counter(field for op in correction['operations'] for field in op['changes'])),
            correction_counts=correction['counts'], correction_holds=correction['held'],
            staged_after_rehearsed_corrections=dict(parsing), reconciliation=dict(reconciliation), held_sources=held_sources,
            statuses=dict(Counter(row['status'] for row in results)), results=results, new_events=len(new_events),
            synthetic_alerts=db.scalar(select(func.count()).select_from(MonitoringAlert)),
            identical_full_state_after_repeat=True, state_sha256=before,
            production_writes=0, email_deliveries=0, cutover_ready=False)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(dumps(report), encoding='utf-8')
        args.output.with_suffix('.plan.json').write_text(dumps(correction), encoding='utf-8')
        print(dumps({key:value for key,value in report.items() if key not in {'results','held_sources','correction_holds'}}))


if __name__ == '__main__':
    main()
