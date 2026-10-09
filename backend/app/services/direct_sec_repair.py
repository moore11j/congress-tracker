"""Reviewed, atomic Form 4 corrections. No source switch, inserts or email.

Review hashes are generated in the target database, not from SQLite exports.
The audit record contains private alert snapshots and must stay server-side.
"""
import json
from collections import Counter
from datetime import datetime, timezone

from sqlalchemy import DateTime, Text, select, text
from sqlalchemy.orm import Mapped, mapped_column

from app.db import Base
from app.models import Event, InsiderTransaction, InsiderTransactionNormalized, MonitoringAlert, DataEnrichmentJob
from app.services.direct_feed_collection import parse_document
from app.services.direct_feed_rehearsal import (
    MODELS, FIELDS, digest, snapshot, plan_corrections,
    include_event_corrections, include_monitoring_corrections,
)
from app.services.direct_feed_store import DirectFeedDocument, DirectFeedRevision, dumps, reconcile_insider
from app.services.direct_feed_worker import DirectFeedPublication, _source_url


class SecRepairReceipt(Base):
    __tablename__ = 'sec_repair_receipts'
    document_id: Mapped[int] = mapped_column(primary_key=True)
    source_hash: Mapped[str]
    metadata_hash: Mapped[str]
    plan_hash: Mapped[str]
    # Both original and corrected snapshots, including alert copies. Never print.
    plan_json: Mapped[str] = mapped_column(Text)
    applied_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))


def _source(db, document_id):
    import hashlib
    staged = db.get(DirectFeedDocument, document_id, populate_existing=True)
    if staged is None or staged.feed != 'sec_form4' or staged.status not in {'parsed', 'quarantined'}:
        raise ValueError('No verified staged Form 4 source')
    metadata = json.loads(staged.metadata_json)
    _source_url(metadata)
    if staged.source_key != metadata['key'] or staged.source_url != metadata['url']:
        raise ValueError('Staged source identity changed')
    revision = db.scalar(select(DirectFeedRevision).where(
        DirectFeedRevision.document_id == document_id, DirectFeedRevision.content_hash == staged.content_hash))
    if revision is None or not revision.source_bytes or hashlib.sha256(revision.source_bytes).hexdigest() != staged.content_hash:
        raise ValueError('Original source bytes missing or changed')
    return staged, dict(feed='sec_form4', metadata=metadata, raw=revision.source_bytes, content_hash=staged.content_hash)


def _summary(plan, status='planned'):
    return dict(status=status, document_id=plan['document_id'], plan_sha256=plan['sha256'],
        updates=dict(Counter(op['table'] for op in plan['operations'])),
        held=plan['held'], inserted_events=0, deleted_events=0, emails=0)


def plan_staged_repair(db, document_id):
    """Internal plan; callers must only emit its compact, nonprivate summary."""
    if db.new or db.dirty or db.deleted:
        raise ValueError('Repair planning has unrelated pending changes')
    staged, source = _source(db, document_id)
    if db.scalar(select(DirectFeedPublication.id).where(DirectFeedPublication.document_id == document_id)):
        raise ValueError('Prior publication requires separate receipt reconciliation')
    plan = plan_corrections(db, [source])
    symbols = {op['after']['ticker_normalized'] for op in plan['operations']}
    if symbols:
        plan = include_event_corrections(db, plan, symbols=symbols, parser_version='direct_sec_verified_repair_v1')
        plan = include_monitoring_corrections(db, plan)
    event_ids = [op['id'] for op in plan['operations'] if op['table'] == 'events']
    jobs = list(db.scalars(select(DataEnrichmentJob).where(
        DataEnrichmentJob.window_key.in_([f'event:{i}' for i in event_ids])).order_by(DataEnrichmentJob.id)))
    if any(row.status not in {'queued', 'done', 'skipped'} for row in jobs):
        plan['held'].append({'reason': 'Affected event has running or unverified enrichment work'})
    # Archive only associated raw records; bind the complete candidate populations
    # by hash so each filing receipt does not duplicate years of unrelated events.
    raw_ids = {json.loads(op['after']['payload_json'])['sec_verification']['raw_provider_id']
        for op in plan['operations'] if op['table'] == 'events'}
    raw_evidence = [snapshot(row) for row in db.scalars(select(InsiderTransaction).where(
        InsiderTransaction.id.in_(raw_ids)).order_by(InsiderTransaction.id))]
    plan['guards'] = [{k: v for k, v in guard.items() if k != 'rows'} |
        {'count': len(guard['rows']), 'sha256': digest(guard['rows'])} for guard in plan['guards']]
    plan.pop('sha256')
    plan.update(version=2, document_id=document_id, source_hash=staged.content_hash,
        metadata_hash=digest(source['metadata']), jobs=[snapshot(row) for row in jobs], raw_evidence=raw_evidence,
        scope='reviewed existing Form 4 economics and alert copies; preserve IDs, dates and saved history')
    return {**plan, 'sha256': digest(plan)}


def inspect_staged_repair(db, document_id):
    receipt = db.get(SecRepairReceipt, document_id)
    if receipt:
        return _verify_receipt(db, receipt)
    plan = plan_staged_repair(db, document_id)
    return _summary(plan, 'held' if plan['held'] else 'planned' if plan['operations'] else 'unchanged')


def _verify_receipt(db, receipt):
    staged, source = _source(db, receipt.document_id)
    plan = json.loads(receipt.plan_json)
    if (receipt.source_hash != staged.content_hash or receipt.metadata_hash != digest(source['metadata'])
            or receipt.plan_hash != plan['sha256']
            or plan['sha256'] != digest({k: v for k, v in plan.items() if k != 'sha256'})):
        raise ValueError('Repair source or archived plan changed')
    for op in plan['operations']:
        row = db.get(MODELS[op['table']], op['id'], populate_existing=True)
        if row is None or snapshot(row) != op['after']:
            raise ValueError('Previously repaired row changed')
    for before in plan['raw_evidence']:
        row = db.get(InsiderTransaction, before['id'], populate_existing=True)
        if row is None or snapshot(row) != before:
            raise ValueError('Previously retained raw evidence changed')
    _, parsed, reasons = parse_document('sec_form4', source['raw'], source['metadata'])
    reconciled = reconcile_insider(db, parsed)
    if reasons or any(reconciled[key] for key in ('unmatched', 'ambiguous', 'existing_only_ids')):
        raise ValueError('Previously repaired filing population changed')
    return dict(status='existing', document_id=receipt.document_id, updated=0, emails=0)


def _apply(db, document_id, expected_plan_hash):
    receipt = db.get(SecRepairReceipt, document_id)
    if receipt:
        if receipt.plan_hash != expected_plan_hash:
            raise ValueError('Reviewed repair hash differs from receipt')
        return _verify_receipt(db, receipt)
    plan = plan_staged_repair(db, document_id)
    if not expected_plan_hash or plan['sha256'] != expected_plan_hash:
        raise ValueError('Repair population changed since review')
    if plan['held'] or not plan['operations']:
        raise ValueError('Repair requires a complete, unambiguous filing with changes')
    # Never apply a normalized correction without the corresponding existing event.
    if Counter(op['table'] for op in plan['operations'])['events'] != Counter(
            op['table'] for op in plan['operations'])['insider_transactions_normalized']:
        raise ValueError('Incomplete normalized-to-event repair coverage')
    with db.begin_nested():
        for op in plan['operations']:
            if op['table'] not in {'events', 'insider_transactions_normalized', 'monitoring_alerts'} or not set(op['changes']) <= FIELDS[op['table']]:
                raise ValueError('Unsupported repair fields')
            row = db.get(MODELS[op['table']], op['id'])
            if row is None or snapshot(row) != op['before']:
                raise ValueError('Repair row changed during planning')
            for field, value in op['changes'].items():
                setattr(row, field, value)
        db.flush()
        staged, source = _source(db, document_id)
        _, parsed, reasons = parse_document('sec_form4', source['raw'], source['metadata'])
        reconciled = reconcile_insider(db, parsed)
        if reasons or any(reconciled[key] for key in ('unmatched', 'ambiguous', 'existing_only_ids')):
            raise ValueError('Repair failed complete source reconciliation')
        staged.status, staged.error = 'parsed', None
        staged.parsed_json, staged.reconciliation_json = dumps(parsed), dumps(reconciled)
        db.add(SecRepairReceipt(document_id=document_id, source_hash=plan['source_hash'],
            metadata_hash=plan['metadata_hash'], plan_hash=plan['sha256'], plan_json=dumps(plan)))
        db.flush()
    return {**_summary(plan, 'applied'), 'updated': len(plan['operations'])}


def rehearse_staged_repair(db, document_id, *, expected_plan_hash):
    bind = db.get_bind()
    if bind.dialect.name != 'sqlite' or bind.url.database != ':memory:':
        raise ValueError('Repair rehearsal requires in-memory SQLite')
    return _apply(db, document_id, expected_plan_hash)


def apply_reviewed_staged_repair(db, document_id, *, expected_plan_hash, expected_generation):
    from app.services.feed_source_control import FeedSourceControl, lock_feed
    from app.services.feed_cache_epoch import bump_feed_events_epoch
    if db.get_bind().dialect.name != 'postgresql' or db.in_transaction() or db.new or db.dirty or db.deleted:
        raise ValueError('Repair requires a fresh PostgreSQL session')
    with db.begin():
        db.execute(text("SET LOCAL lock_timeout = '100ms'"))
        db.execute(text("SET LOCAL statement_timeout = '20s'"))
        lock_feed(db, 'sec_form4')
        control = db.get(FeedSourceControl, 'sec_form4', populate_existing=True)
        if control is None or control.provider != 'paused' or control.generation != expected_generation:
            raise ValueError('Repair requires the reviewed paused source generation')
        models = (InsiderTransactionNormalized, InsiderTransaction, Event, MonitoringAlert,
            DataEnrichmentJob, DirectFeedDocument, DirectFeedRevision, DirectFeedPublication, SecRepairReceipt)
        preparer = db.get_bind().dialect.identifier_preparer
        tables = ', '.join(preparer.format_table(model.__table__) for model in models)
        db.execute(text(f'LOCK TABLE {tables} IN SHARE ROW EXCLUSIVE MODE NOWAIT'))
        result = _apply(db, document_id, expected_plan_hash)
        if result['status'] == 'applied':
            bump_feed_events_epoch(reason='verified_sec_form4_correction', db=db)
    return result
