"""Source-bound duplicate repair rehearsal. Production application is disabled.

Only a complete, uniquely identified stock filing with verified surviving
trade/event pairs is eligible. Archives preserve every removed public record.
"""
from collections import defaultdict
from datetime import datetime, timezone
import hashlib
import json

from sqlalchemy import Text, UniqueConstraint, select, func, or_, cast
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.db import Base
from app.models import (Event, Filing, Transaction, Security, Member, TradeOutcome,
    MonitoringAlert, ResearchEvidenceEvent, DataEnrichmentJob)
from app.services.direct_feed_collection import parse_document
from app.services.direct_feed_store import dumps
from app.services.direct_congress_reconciliation import document_identity, resolve_direct_member, _key


class CongressRepairArchive(Base):
    __tablename__ = 'congress_repair_archives'
    __table_args__ = (UniqueConstraint('entity_type', 'original_id', name='uq_congress_repair_archive'),)
    id: Mapped[int] = mapped_column(primary_key=True)
    entity_type: Mapped[str]
    original_id: Mapped[int]
    canonical_id: Mapped[int | None]
    source_hash: Mapped[str]
    source_url: Mapped[str] = mapped_column(Text)
    record_json: Mapped[str] = mapped_column(Text)


class CongressRowBinding(Base):
    __tablename__ = 'congress_row_bindings'
    __table_args__ = (UniqueConstraint('source_url', 'source_line_ref', name='uq_congress_source_row'),
                     UniqueConstraint('transaction_id', name='uq_congress_bound_transaction'),
                     UniqueConstraint('event_id', name='uq_congress_bound_event'))
    id: Mapped[int] = mapped_column(primary_key=True)
    source_url: Mapped[str] = mapped_column(Text)
    source_line_ref: Mapped[str]
    source_hash: Mapped[str]
    normalized_hash: Mapped[str]
    transaction_id: Mapped[int]
    event_id: Mapped[int | None]


class CongressRepairReceipt(Base):
    __tablename__ = 'congress_repair_receipts'
    source_url: Mapped[str] = mapped_column(Text, primary_key=True)
    source_hash: Mapped[str]
    before_hash: Mapped[str]
    after_hash: Mapped[str]
    report_json: Mapped[str] = mapped_column(Text)


def _digest(value):
    return hashlib.sha256(dumps(value).encode()).hexdigest()


def _record(row):
    return {column.name: getattr(row, column.name) for column in row.__table__.columns}


def _evidence_hash(db, url):
    archives = db.scalars(select(CongressRepairArchive).where(CongressRepairArchive.source_url == url)
        .order_by(CongressRepairArchive.entity_type, CongressRepairArchive.original_id)).all()
    bindings = db.scalars(select(CongressRowBinding).where(CongressRowBinding.source_url == url)
        .order_by(CongressRowBinding.source_line_ref)).all()
    retired_jobs = list(db.scalars(select(DataEnrichmentJob).where(DataEnrichmentJob.id.in_(
        [row.original_id for row in archives if row.entity_type == 'enrichment_job'])).order_by(DataEnrichmentJob.id)))
    evidence = {'archives': [_record(row) for row in archives], 'bindings': [_record(row) for row in bindings]}
    if any(row.entity_type == 'enrichment_job' for row in archives):
        evidence['retired_jobs'] = [_record(row) for row in retired_jobs]
    return _digest(evidence)


def linked_jobs(db, event_ids):
    return list(db.scalars(select(DataEnrichmentJob).where(
        DataEnrichmentJob.window_key.in_([f'event:{identifier}' for identifier in event_ids]))
        .order_by(DataEnrichmentJob.id).with_for_update().execution_options(populate_existing=True)))


def _verified_queued_jobs(jobs, events):
    from app.services.data_enrichment_queue import build_dedupe_key
    queued = []
    for job in jobs:
        if job.status not in {'queued', 'done', 'skipped'}:
            return None
        if job.status != 'queued':
            continue
        try:
            payload = json.loads(job.payload_json or '{}')
            event = events[payload['event_id']]
            source = json.loads(event['payload_json'])
        except (KeyError, TypeError, ValueError):
            return None
        if (job.job_type != 'pnl_refresh' or not isinstance(payload, dict)
                or set(payload) != {'event_id', 'event_type', 'symbol', 'trade_date'}
                or payload['event_type'] != 'congress_trade' or event['event_type'] != 'congress_trade'
                or job.window_key != f"event:{payload['event_id']}"
                or job.symbol != event['symbol'] or payload['symbol'] != event['symbol']
                or job.date_key != payload['trade_date'] or job.date_key != source.get('trade_date')
                or job.dedupe_key != build_dedupe_key(job_type=job.job_type, symbol=job.symbol, date_key=job.date_key, window_key=job.window_key)):
            return None
        queued.append(job)
    return queued


def _source_key(row):
    return _key(day=row['transaction_date'], owner=row['owner_normalized'], action=row['transaction_type_normalized'],
        lower=row['amount_low'], upper=row['amount_high'], symbol=row['ticker_normalized'], description=None)


def _trade_key(row, security):
    return _key(day=row['trade_date'], owner=row['owner_type'], action=row['transaction_type'],
        lower=row['amount_range_min'], upper=row['amount_range_max'], symbol=security.get('symbol'), description=None)


def _event_matches(event, source, member, transaction_id, filing_id, *, correct_disclosure):
    payload = json.loads(event['payload_json'])
    if (payload.get('transaction_id') != transaction_id or payload.get('filing_id') != filing_id
            or document_identity(payload.get('document_url')) != document_identity(source['document_url'])
            or (event.get('source_document_url') and document_identity(event['source_document_url']) != document_identity(source['document_url']))
            or (payload.get('member') or {}).get('bioguide_id') != member['bioguide_id']
            or (payload.get('member') or {}).get('chamber') != member['chamber']):
        return False
    # Inspect public event columns as well as the linked trade, never just IDs.
    if (event.get('event_type') != 'congress_trade' or event.get('member_bioguide_id') != member['bioguide_id']
            or event.get('chamber') != member['chamber']):
        return False
    key = _key(day=payload.get('transaction_date') or payload.get('trade_date'), owner=payload.get('owner_type'),
        action=event.get('trade_type') or event.get('transaction_type'), lower=event.get('amount_min'),
        upper=event.get('amount_max'), symbol=event.get('symbol'), description=None)
    if key != _source_key(source):
        return False
    if correct_disclosure:
        day = str(source['disclosure_date'])[:10]
        return (str(event.get('ts'))[:10] == day and str(event.get('event_date'))[:10] == day
                and str(payload.get('report_date') or payload.get('filing_date'))[:10] == day)
    return True


def plan_duplicate_repair(parsed, member, state):
    """Pure planner. Refuses separate identical lots and incomplete populations."""
    held = lambda reason: {'status': 'held', 'reason': reason}
    rows, metadata = parsed['transactions'], parsed['metadata']
    if (not rows or any(row['asset_type_normalized'] != 'stock' or not row['ticker_normalized']
            or row['owner_normalized'] not in {'self', 'spouse', 'joint', 'dependent'}
            or row['transaction_type_normalized'] not in {'purchase', 'sale'} for row in rows)):
        return held('Repair requires complete identified ordinary-stock transactions')
    if len({_source_key(row) for row in rows}) != len(rows):
        return held('Separate identical source lots require explicit account/row reconciliation')
    if len(state['filings']) != 1:
        return held('Repair requires exactly one legacy filing')
    filing = state['filings'][0]
    members = {row['id']: row for row in state['members']}
    securities = {row['id']: row for row in state['securities']}
    if (document_identity(filing['document_url']) != document_identity(metadata['url'])
            or members.get(filing['member_id'], {}).get('bioguide_id') != member['bioguide_id']
            or str(filing['filing_date'])[:10] != metadata['filing_date']):
        return held('Filing identity/date itself requires separate repair')
    groups = defaultdict(list)
    unclassified = []
    for row in state['transactions']:
        if row['filing_id'] != filing['id'] or row['member_id'] != filing['member_id']:
            return held('Legacy transaction filing/member differs')
        security = securities.get(row['security_id'], {})
        if not security:
            if row['security_id'] is not None or row['description']:
                return held('Unclassified legacy transaction has unverified security evidence')
            unclassified.append(row)
            continue
        if security.get('asset_class', '').lower() not in {'stock', 'stocks', 'equity'}:
            return held('Legacy instrument classification requires separate repair')
        groups[_trade_key(row, security)].append(row)
    if set(groups) != {_source_key(row) for row in rows}:
        return held('Legacy/source economic population differs')
    events = defaultdict(list)
    transaction_ids = {row['id'] for row in state['transactions']}
    for event in state['events']:
        payload = json.loads(event['payload_json'])
        tx_id = payload.get('transaction_id')
        if tx_id not in transaction_ids:
            return held('Source has an orphan public event')
        events[tx_id].append(event)
    bindings, retire_transactions, retire_events = [], [], []
    for row in rows:
        candidates = groups[_source_key(row)]
        survivors = []
        for candidate in candidates:
            linked = events[candidate['id']]
            if any(not _event_matches(e, row, member, candidate['id'], filing['id'], correct_disclosure=False) for e in linked):
                return held('Legacy public event economic/evidence fields conflict')
            if str(candidate['report_date'])[:10] == metadata['filing_date']:
                survivors.extend((e['id'], candidate['id']) for e in linked
                    if _event_matches(e, row, member, candidate['id'], filing['id'], correct_disclosure=True))
        if not survivors:
            return held('No already-correct public event/transaction pair to preserve')
        event_id, transaction_id = min(survivors)
        bindings.append({'source_line_ref': row['source_line_ref'], 'normalized_hash': row['normalized_hash'],
                         'transaction_id': transaction_id, 'event_id': event_id})
        for candidate in candidates:
            if candidate['id'] != transaction_id:
                retire_transactions.append({'id': candidate['id'], 'canonical_id': transaction_id})
            retire_events.extend({'id': e['id'], 'canonical_id': event_id} for e in events[candidate['id']] if e['id'] != event_id)
    # Source has a full surviving population. Security-less residual rows can
    # be withdrawn without guessing which ticker they were supposed to carry.
    for row in unclassified:
        key = _trade_key(row, {})
        if events[row['id']] or not any(key[:5] == _source_key(source)[:5] for source in rows):
            return held('Security-less residual has public references or unsupported economics')
        retire_transactions.append({'id': row['id'], 'canonical_id': None})
    return {'status': 'planned', 'bindings': sorted(bindings, key=lambda row: row['event_id']),
        'retire_transactions': sorted(retire_transactions, key=lambda row: row['id']),
        'retire_events': sorted(retire_events, key=lambda row: row['id']), 'before_hash': _digest(state)}


def filing_state(db, url):
    identity = document_identity(url)
    if identity is None:
        raise ValueError('Unsupported source URL')
    candidates = list(db.scalars(select(Filing).where(Filing.document_url.contains(identity[-1])).limit(51)))
    if len(candidates) > 50:
        raise ValueError('Filing candidates exceed bounded repair scope')
    filings = [row for row in candidates if document_identity(row.document_url) == identity]
    tx = list(db.scalars(select(Transaction).where(Transaction.filing_id.in_([f.id for f in filings])).order_by(Transaction.id)))
    if len(tx) > 1000:
        raise ValueError('Filing transaction population exceeds repair scope')
    members = list(db.scalars(select(Member).where(Member.id.in_({r.member_id for r in tx} | {f.member_id for f in filings})).order_by(Member.id)))
    tx_ids = {r.id for r in tx}
    transaction_reference = (func.json_extract(Event.payload_json, '$.transaction_id').in_(tx_ids)
        if db.get_bind().dialect.name == 'sqlite' else cast(Event.payload_json, JSONB)['transaction_id'].astext.in_([str(i) for i in tx_ids]))
    events = list(db.scalars(select(Event).where(or_(
        Event.member_bioguide_id.in_([m.bioguide_id for m in members]), transaction_reference,
        Event.source_document_url.contains(identity[-1]), Event.payload_json.contains(identity[-1])))
        .order_by(Event.id).limit(10001)))
    if len(events) > 10000:
        raise ValueError('Member event population exceeds repair scope')
    selected_events = []
    for event in events:
        payload = json.loads(event.payload_json or '{}')
        if payload.get('transaction_id') in tx_ids or document_identity(event.source_document_url or payload.get('document_url')) == identity:
            selected_events.append(event)
    securities = list(db.scalars(select(Security).where(Security.id.in_([r.security_id for r in tx if r.security_id])).order_by(Security.id)))
    outcomes = list(db.scalars(select(TradeOutcome).where(TradeOutcome.event_id.in_([e.id for e in selected_events])).order_by(TradeOutcome.id)))
    return {key: [_record(row) for row in values] for key, values in dict(filings=filings,
        transactions=tx, members=members, securities=securities, events=selected_events, outcomes=outcomes).items()}


def rehearse_duplicate_repair(db, document, directory, *, expected_before_hash, expected_jobs_hash=None):
    """Apply only in isolated memory; preserves survivors and archives removals."""
    bind = db.get_bind()
    if bind.dialect.name != 'sqlite' or bind.url.database != ':memory:':
        raise ValueError('Congress repair currently requires in-memory SQLite')
    if db.new or db.dirty or db.deleted:
        raise ValueError('Repair session has unrelated pending changes')
    if hashlib.sha256(document['raw']).hexdigest() != document['content_hash']:
        raise ValueError('Source checksum differs')
    _, parsed, reasons = parse_document(document['feed'], document['raw'], document['metadata'])
    if reasons:
        return {'status': 'held', 'reason': '; '.join(reasons)}
    metadata = document['metadata']
    resolved = resolve_direct_member(metadata, document['feed'].split('_')[0], directory)
    if resolved['status'] != 'resolved':
        return resolved
    state = filing_state(db, metadata['url'])
    receipt = db.get(CongressRepairReceipt, metadata['url'])
    if receipt:
        recorded = json.loads(receipt.report_json)
        if (receipt.source_hash != document['content_hash'] or receipt.after_hash != _digest(state)
                or recorded['evidence_hash'] != _evidence_hash(db, metadata['url'])):
            raise ValueError('Previously repaired source or canonical state changed')
        return {'status': 'existing', 'inserted_events': 0, 'emails': 0}
    if _digest(state) != expected_before_hash:
        raise ValueError('Canonical population changed since reviewed repair plan')
    plan = plan_duplicate_repair(parsed, resolved['member'], state)
    if plan['status'] != 'planned':
        return plan
    retired_ids = [row['id'] for row in plan['retire_events']]
    # Saved portfolio history reads archived dates without modifying its rows.
    # Customer alert/research references still hold until separately handled.
    for column in [MonitoringAlert.event_id]:
        if db.scalar(select(func.count()).where(column.in_(retired_ids))):
            return {'status': 'held', 'reason': 'Retired event has saved alert references'}
    if db.scalar(select(func.count()).where(ResearchEvidenceEvent.related_event_id.in_(
            [str(i) for i in retired_ids] + [f'event:{i}' for i in retired_ids]))):
        return {'status': 'held', 'reason': 'Retired event is referenced by research evidence'}
    jobs = linked_jobs(db, retired_ids)
    queued = _verified_queued_jobs(jobs, {row['id']: row for row in state['events']})
    if queued is None:
        return {'status': 'held', 'reason': 'Retired event has running or unverified enrichment jobs'}
    if queued and expected_jobs_hash is None:
        return {'status': 'held', 'reason': 'Queued job retirement requires its reviewed population hash'}
    if expected_jobs_hash is not None and _digest([_record(job) for job in jobs]) != expected_jobs_hash:
        raise ValueError('Enrichment population changed since reviewed repair plan')
    with db.begin_nested():
        for job in queued:
            db.add(CongressRepairArchive(entity_type='enrichment_job', original_id=job.id,
                canonical_id=None, source_hash=document['content_hash'], source_url=metadata['url'], record_json=dumps(_record(job))))
            job.status = 'skipped'
            job.reason = 'source_event_withdrawn_duplicate'
            job.updated_at = datetime.now(timezone.utc)
        def archive(model, entity, item):
            row = db.get(model, item['id'])
            if row is None:
                raise ValueError('Repair row disappeared')
            db.add(CongressRepairArchive(entity_type=entity, original_id=row.id,
                canonical_id=item['canonical_id'], source_hash=document['content_hash'], source_url=metadata['url'], record_json=dumps(_record(row))))
            db.delete(row)
        for item in plan['retire_events']:
            for outcome in db.scalars(select(TradeOutcome).where(TradeOutcome.event_id == item['id'])):
                archive(TradeOutcome, 'trade_outcome', {'id': outcome.id, 'canonical_id': None})
            archive(Event, 'event', item)
        for item in plan['retire_transactions']:
            archive(Transaction, 'transaction', item)
        for row in plan['bindings']:
            db.add(CongressRowBinding(source_url=metadata['url'], source_hash=document['content_hash'], **row))
        db.flush()
        for job in queued:
            # Bind the persisted timestamp representation, including SQLite's
            # timezone normalization, so a commit/reload repeats identically.
            db.refresh(job)
        after_hash = _digest(filing_state(db, metadata['url']))
        db.add(CongressRepairReceipt(source_url=metadata['url'], source_hash=document['content_hash'],
            before_hash=expected_before_hash, after_hash=after_hash,
            report_json=dumps({**plan, 'retired_job_ids': [j.id for j in queued],
                'jobs_before_hash': expected_jobs_hash, 'evidence_hash': _evidence_hash(db, metadata['url'])})))
        db.flush()
    result = {'status': 'rehearsed', 'preserved_events': len(plan['bindings']), 'withdrawn_events': len(plan['retire_events']),
        'withdrawn_transactions': len(plan['retire_transactions']), 'inserted_events': 0, 'emails': 0}
    if queued:
        result['retired_jobs'] = len(queued)
    return result
