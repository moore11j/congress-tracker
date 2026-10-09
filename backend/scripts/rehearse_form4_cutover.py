"""Reparse a fresh public Form 4 baseline and rehearse publication in memory."""
import argparse
import base64
from collections import Counter
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
from unittest.mock import patch


def preview_alerts(db, user, watchlist, events):
    from app.services import email_digests as digests
    start = min(event.ts for event in events).replace(tzinfo=timezone.utc) - timedelta(seconds=1)
    end = max(event.ts for event in events).replace(tzinfo=timezone.utc) + timedelta(seconds=1)
    class FrozenDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return end.astimezone(tz) if tz else end.replace(tzinfo=None)
    with patch.object(digests, 'datetime', FrozenDatetime), patch.object(digests,
            '_upcoming_calendar_events_for_digest', return_value=([], 'Calendar outside SEC publication rehearsal')):
        activity = digests.build_watchlist_activity_digest(db, user, watchlist, start)
        monitoring = digests.build_monitoring_digest(db, user, watchlist, start, window_end=end)
        daily = digests.build_signal_alert_digest(db, user, start, window_end=end)
    assert all(item['date'].startswith('Traded ') and '; filed ' in item['date'] for item in activity.items)
    assert 'Traded ' in daily.context['insider_trades_text']
    assert 'filed ' in monitoring.context['items_text']
    return dict(activity=activity.items_count, monitoring=monitoring.items_count, daily=daily.items_count,
        rendered_state=hashlib.sha256(json.dumps(dict(activity=activity.items,
            monitoring=monitoring.context['items_text'], daily=daily.context['insider_trades_text']),sort_keys=True).encode()).hexdigest())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--baseline', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--complete-filings-only', action='store_true', help='Use the guarded production repair policy')
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2] / 'artifacts' / 'direct-feeds'
    assert args.output.resolve().is_relative_to(root.resolve())
    os.environ['DATABASE_URL'] = 'sqlite:///:memory:'
    from sqlalchemy import Date, DateTime, select, func
    from app.db import Base, engine, SessionLocal
    from app.models import (Event, SecForm4Filing, MonitoringAlert, UserAccount, Watchlist, EmailDelivery,
                            Security, WatchlistItem, NotificationSubscription)
    from app.services.direct_feed_store import discover, record_document, dumps, reconcile_insider
    from app.services.direct_feed_collection import parse_document
    from app.services.direct_feed_rehearsal import plan_corrections, include_event_corrections, apply_rehearsal
    from app.services.direct_sec_repair import inspect_staged_repair, rehearse_staged_repair, SecRepairReceipt
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
        if args.complete_filings_only:
            correction = dict(operations=[], counts={}, held=[])
            repaired = dict(updated=0, inserted=0, deleted=0, production_writes=0)
            for doc in documents:
                source, parsed, reasons = parse_document(doc['feed'], doc['raw'], doc['metadata'])
                staged = discover(db, doc['feed'], doc['metadata'])
                record_document(db, staged, doc['raw'], source, parsed, reasons=reasons)
                db.commit()
                reviewed = inspect_staged_repair(db, staged.id)
                if reviewed['status'] == 'planned':
                    applied = rehearse_staged_repair(db, staged.id, expected_plan_hash=reviewed['plan_sha256'])
                    db.commit()
                    repaired['updated'] += applied['updated']
                    receipt = db.get(SecRepairReceipt, staged.id)
                    correction['operations'].extend(json.loads(receipt.plan_json)['operations'])
                    assert rehearse_staged_repair(db, staged.id, expected_plan_hash=reviewed['plan_sha256'])['updated'] == 0
                elif reviewed['status'] == 'held':
                    correction['held'].append(dict(source_document_id=doc['id'], reasons=reviewed['held']))
            correction['counts'] = dict(Counter(op['table'] for op in correction['operations']))
        else:
            correction = include_event_corrections(db, plan_corrections(db, documents))
            repaired = apply_rehearsal(db, correction)
            db.commit()
            assert apply_rehearsal(db, correction)['updated'] == 0
        parsing, reconciliation, held_sources, source_ids = Counter(), Counter(), [], {}
        for doc in documents:
            source, parsed, reasons = parse_document(doc['feed'], doc['raw'], doc['metadata'])
            matched = reconcile_insider(db, parsed)
            for key in ('matched','unmatched','ambiguous','existing_only_ids'):
                reconciliation[key] += len(matched[key])
            reasons = list(reasons)
            if matched['ambiguous'] or matched['existing_only_ids']:
                reasons.append('Existing records differ from source; canonical reconciliation required')
            row = discover(db, doc['feed'], doc['metadata'])
            source_ids[row.id] = doc['id']
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
        results = [{**row, 'source_document_id':source_ids[row['document_id']]}
                   for batch in batches for row in batch['results']]
        new_events = list(db.scalars(select(Event).where(Event.id.not_in(before_ids))))
        user = UserAccount(email='form4-cutover@example.test', entitlement_tier='premium', watchlist_activity_notifications=True)
        db.add(user); db.flush()
        watchlist = Watchlist(name='Synthetic SEC cutover', owner_user_id=user.id)
        db.add(watchlist); db.flush()
        for symbol in sorted({event.symbol for event in new_events}):
            security = Security(symbol=symbol, name=symbol, asset_class='stock')
            db.add(security); db.flush()
            db.add(WatchlistItem(watchlist_id=watchlist.id, security_id=security.id))
        db.add(NotificationSubscription(email=user.email, source_type='watchlist', source_id=str(watchlist.id),
            source_name=watchlist.name, frequency='daily', only_if_new=True, active=True,
            source_payload_json='{}', alert_triggers_json='["insider_activity"]'))
        db.commit()
        for event in new_events:
            assert _ensure_alert_for_event(db, user_id=user.id, watchlist=watchlist, event=event)
            payload = json.loads(event.payload_json)
            assert payload['source_availability']['date'] == event.ts.date().isoformat()
            assert event.ts.date() >= event.event_date.date()
        db.commit()
        for alert in db.scalars(select(MonitoringAlert)):
            payload = json.loads(db.get(Event,alert.event_id).payload_json)
            assert str(payload['transaction_date']) in alert.body and str(payload['filing_date']) in alert.body
        previews = preview_alerts(db,user,watchlist,new_events) if new_events else None
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
        if new_events:
            assert preview_alerts(db,user,watchlist,new_events) == previews
        db.commit()
        assert fingerprint() == before
        assert db.scalar(select(func.count()).select_from(EmailDelivery)) == 0
        report = dict(baseline_captured_at=baseline['captured_at'], baseline_sha256=hashlib.sha256(baseline_bytes).hexdigest(),
            sources=len(documents), baseline_counts={key:len(baseline[key]) for key in ['insiders','raw_insiders','events','form4_filings']},
            complete_filings_only=args.complete_filings_only,
            canonical_corrections_local_only=repaired, correction_fields=dict(Counter(field for op in correction['operations'] for field in op['changes'])),
            correction_counts=correction['counts'], correction_holds=correction['held'],
            staged_after_rehearsed_corrections=dict(parsing), reconciliation=dict(reconciliation), held_sources=held_sources,
            statuses=dict(Counter(row['status'] for row in results)), results=results, new_events=len(new_events),
            synthetic_alerts=db.scalar(select(func.count()).select_from(MonitoringAlert)),
            actual_no_send_builders=previews,
            identical_full_state_after_repeat=True, state_sha256=before,
            production_writes=0, email_deliveries=0, cutover_ready=False)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(dumps(report), encoding='utf-8')
        args.output.with_suffix('.plan.json').write_text(dumps(correction), encoding='utf-8')
        print(dumps({key:value for key,value in report.items() if key not in {'results','held_sources','correction_holds'}}))


if __name__ == '__main__':
    main()
