"""Reviewed filing-date correction without backdating event availability.

Only complete, uniquely matched stock populations with one consistent legacy
report/arrival day qualify. No trade, event, outcome or delivery is inserted.
"""
import copy
from datetime import date, datetime, time, timezone
import hashlib
import json

from sqlalchemy import select, text

from app.models import Event, Filing, Transaction
from app.services.direct_feed_collection import parse_document
from app.services.direct_feed_store import dumps, DirectFeedDocument
from app.services.direct_congress_reconciliation import reconcile_direct_congress, resolve_direct_member
from app.services.direct_congress_repair import (
    CongressRepairArchive, CongressRepairReceipt, CongressRowBinding,
    filing_state, _digest, _record, _event_matches, _evidence_hash, _lock_repair_tables,
)


def _canonical_state(state):
    return {k: v for k, v in state.items() if k != 'outcomes'}


def plan_date_correction(parsed, member, state):
    held = lambda reason: {'status': 'held', 'reason': reason}
    rows, metadata = parsed['transactions'], parsed['metadata']
    if len(state['filings']) != 1 or not rows or any(r['asset_type_normalized'] != 'stock' for r in rows):
        return held('Date correction requires one complete ordinary-stock filing')
    filing = state['filings'][0]
    old_day, official_day = str(filing['filing_date'])[:10], metadata['filing_date']
    if official_day >= old_day:
        return held('Correction requires an earlier verified official filing date')
    if any(str(t['report_date'])[:10] != old_day for t in state['transactions']):
        return held('Legacy report-date population is inconsistent')
    proposed = copy.deepcopy(state)
    proposed['filings'][0]['filing_date'] = official_day
    for row in proposed['transactions']:
        row['report_date'] = official_day
    reconciliation = reconcile_direct_congress(parsed, member,
        **{k: proposed[k] for k in ('filings','transactions','events','members','securities')})
    if reconciliation['status'] != 'existing':
        return held('Canonical population has conflicts beyond disclosure dates')
    source = {r['source_line_ref']: r for r in rows}
    events = {r['id']: r for r in state['events']}
    bindings = []
    for match in reconciliation['matched']:
        if len(match['event_ids']) != 1:
            return held('Date correction requires exactly one event for each source row')
        event = events[match['event_ids'][0]]
        payload = json.loads(event['payload_json'])
        if (not _event_matches(event, source[match['source_line_ref']], member,
                match['transaction_id'], filing['id'], correct_disclosure=False)
                or any(str(event.get(k) or '')[:10] != old_day for k in ('ts','event_date','created_at'))
                or payload.get('filing_date') != old_day or payload.get('report_date') != old_day
                or 'source_availability' in payload or 'source_date_correction' in payload):
            return held('Event economics, original availability or prior correction conflicts')
        bindings.append(dict(source_line_ref=match['source_line_ref'],
            normalized_hash=source[match['source_line_ref']]['normalized_hash'],
            transaction_id=match['transaction_id'], event_id=event['id']))
    if len(bindings) != len(rows) or len(events) != len(rows):
        return held('Date correction source/event population differs')
    return dict(status='planned', filing_id=filing['id'], official_date=official_day,
        previous_date=old_day, bindings=bindings, before_hash=_digest(state))


def _correct_dates(db, document, directory, *, expected_before_hash=None, inspect_only=False):
    if db.new or db.dirty or db.deleted:
        raise ValueError('Date correction has unrelated pending changes')
    if document['feed'] not in {'house_ptr','senate_ptr'} or hashlib.sha256(document['raw']).hexdigest() != document['content_hash']:
        raise ValueError('Date correction source identity or checksum differs')
    _, parsed, reasons = parse_document(document['feed'], document['raw'], document['metadata'])
    if reasons:
        return {'status':'held','reason':'; '.join(reasons)}
    resolved = resolve_direct_member(document['metadata'], document['feed'].split('_')[0], directory)
    if resolved['status'] != 'resolved':
        return resolved
    url = document['metadata']['url']
    state = filing_state(db, url)
    receipt = db.get(CongressRepairReceipt, url)
    if receipt:
        report = json.loads(receipt.report_json)
        if (report.get('repair_kind') != 'official_date_preserve_availability'
                or receipt.source_hash != document['content_hash']
                or receipt.after_hash != _digest(_canonical_state(state))
                or report['evidence_hash'] != _evidence_hash(db, url)):
            raise ValueError('Prior date correction or canonical evidence changed')
        return {'status':'existing','updated_events':0,'inserted_events':0,'emails':0}
    if db.scalar(select(CongressRowBinding.id).where(CongressRowBinding.source_url == url).limit(1)):
        return {'status':'held','reason':'Prior source bindings require separate reconciliation'}
    plan = plan_date_correction(parsed, resolved['member'], state)
    if plan['status'] != 'planned' or inspect_only:
        return plan
    if plan['before_hash'] != expected_before_hash:
        raise ValueError('Date correction population changed since review')
    with db.begin_nested():
        def archive(model, identifier, kind):
            row = db.get(model, identifier)
            db.add(CongressRepairArchive(entity_type='date_'+kind, original_id=identifier,
                canonical_id=identifier, source_hash=document['content_hash'], source_url=url,
                record_json=dumps(_record(row))))
            return row
        archive(Filing, plan['filing_id'], 'filing').filing_date = date.fromisoformat(plan['official_date'])
        for binding in plan['bindings']:
            tx = archive(Transaction, binding['transaction_id'], 'transaction')
            event = archive(Event, binding['event_id'], 'event')
            tx.report_date = date.fromisoformat(plan['official_date'])
            event.event_date = datetime.combine(tx.report_date, time.min, tzinfo=timezone.utc)
            payload = json.loads(event.payload_json)
            payload.update(filing_date=plan['official_date'], report_date=plan['official_date'])
            payload['source_availability'] = dict(date=plan['previous_date'],
                basis='retained_legacy_report_date', observed_at=event.created_at.isoformat())
            payload['source_date_correction'] = dict(source_sha256=document['content_hash'],
                source_url=url, previous_filing_date=plan['previous_date'],
                official_filing_date=plan['official_date'])
            event.payload_json = dumps(payload)
            db.add(CongressRowBinding(source_url=url, source_hash=document['content_hash'], **binding))
        db.flush()
        # Hash the persisted timestamp representation, including SQLite's
        # timezone normalization, so commit/reload does not change the receipt.
        for binding in plan['bindings']:
            db.refresh(db.get(Event, binding['event_id']))
        after = filing_state(db, url)
        if _digest(state['outcomes']) != _digest(after['outcomes']):
            raise ValueError('Date correction changed recorded outcomes')
        exact = reconcile_direct_congress(parsed, resolved['member'],
            **{k: after[k] for k in ('filings','transactions','events','members','securities')})
        if exact['status'] != 'existing':
            raise ValueError('Date correction did not produce exact source reconciliation')
        db.add(CongressRepairReceipt(source_url=url, source_hash=document['content_hash'],
            before_hash=expected_before_hash, after_hash=_digest(_canonical_state(after)),
            report_json=dumps({**plan,'repair_kind':'official_date_preserve_availability',
                'evidence_hash':_evidence_hash(db,url)})))
        db.flush()
    return dict(status='rehearsed', updated_events=len(plan['bindings']), inserted_events=0,
        official_date=plan['official_date'], availability_date=plan['previous_date'], emails=0)


def inspect_date_correction(db, document, directory):
    return _correct_dates(db, document, directory, inspect_only=True)


def rehearse_date_correction(db, document, directory, *, expected_before_hash):
    bind = db.get_bind()
    if bind.dialect.name != 'sqlite' or bind.url.database != ':memory:':
        raise ValueError('Date rehearsal requires in-memory SQLite')
    return _correct_dates(db, document, directory, expected_before_hash=expected_before_hash)


def apply_reviewed_date_correction(db, document, directory, *, expected_before_hash, expected_generation):
    from app.services.feed_source_control import FeedSourceControl, lock_feed
    from app.services.feed_cache_epoch import bump_feed_events_epoch
    if db.get_bind().dialect.name != 'postgresql' or db.in_transaction() or db.new or db.dirty or db.deleted:
        raise ValueError('Date correction requires a fresh PostgreSQL session')
    if not expected_before_hash or document['feed'] not in {'house_ptr','senate_ptr'}:
        raise ValueError('Date correction requires reviewed source and population')
    with db.begin():
        db.execute(text("SET LOCAL lock_timeout = '100ms'"))
        db.execute(text("SET LOCAL statement_timeout = '15s'"))
        lock_feed(db, document['feed'])
        control = db.get(FeedSourceControl, document['feed'], populate_existing=True)
        if control is None or control.provider != 'paused' or control.generation != expected_generation:
            raise ValueError('Date correction requires the reviewed paused source generation')
        _lock_repair_tables(db)
        staged = db.get(DirectFeedDocument, document['document_id'], populate_existing=True)
        if (staged is None or staged.feed != document['feed'] or staged.status not in {'parsed','quarantined'}
                or staged.content_hash != document['content_hash']
                or dumps(json.loads(staged.metadata_json)) != dumps(document['metadata'])):
            raise ValueError('Date correction staged source changed')
        result = _correct_dates(db, document, directory, expected_before_hash=expected_before_hash)
        if result['status'] == 'rehearsed':
            _, parsed, reasons = parse_document(document['feed'], document['raw'], document['metadata'])
            member = resolve_direct_member(document['metadata'], document['feed'].split('_')[0], directory)['member']
            state = filing_state(db, document['metadata']['url'])
            reconciliation = reconcile_direct_congress(parsed, member,
                **{k:state[k] for k in ('filings','transactions','events','members','securities')})
            if reasons or reconciliation['status'] != 'existing':
                raise ValueError('Date correction failed final source reconciliation')
            staged.status, staged.error = 'parsed', None
            staged.parsed_json, staged.reconciliation_json = dumps(parsed), dumps(reconciliation)
            bump_feed_events_epoch(reason='official_congress_date_correction', db=db)
            result = {**result,'status':'applied'}
    return result
