"""Atomic publication of captured Form 4 documents. Does not deliver email."""
from datetime import datetime, timezone
import hashlib
import json
import re
from zoneinfo import ZoneInfo

from sqlalchemy import DateTime, Text, UniqueConstraint, select
from sqlalchemy.orm import Mapped, mapped_column

from app.db import Base
from app.models import Event, InsiderTransactionNormalized, SecForm4Filing
from app.services.direct_feed_store import DirectFeedDocument, DirectFeedRevision, dumps
from app.services.direct_feed_publication import _project_new_form4
from app.services.feed_source_control import require_selected_source, writer_transaction
from app.services.feed_pnl_enrichment import enqueue_feed_pnl_enrichment_for_event
from app.services.feed_cache_epoch import bump_feed_events_epoch


class DirectFeedPublication(Base):
    __tablename__ = 'direct_feed_publications'
    __table_args__ = (UniqueConstraint('document_id', name='uq_direct_feed_publication'),)
    id: Mapped[int] = mapped_column(primary_key=True)
    document_id: Mapped[int] = mapped_column(index=True)
    source_hash: Mapped[str]
    metadata_hash: Mapped[str]
    source_generation: Mapped[int]
    publish_since: Mapped[str]
    status: Mapped[str] = mapped_column(index=True)
    report_json: Mapped[str] = mapped_column(Text)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


def _source_url(metadata):
    cik = str(metadata['cik']).lstrip('0')
    accession = metadata['key']
    if not re.fullmatch(r'\d{10}-\d{2}-\d{6}', accession) or not cik.isdigit():
        raise ValueError('Invalid SEC filing identity')
    allowed = {
        f'https://www.sec.gov/Archives/edgar/data/{cik}/{accession}.txt',
        f'https://www.sec.gov/Archives/edgar/data/{cik}/{accession.replace("-", "")}/{accession}.txt',
    }
    if metadata['url'] not in allowed:
        raise ValueError('Publication source is not the matching official SEC submission')


def _canonical_hash(db, accession, event_ids):
    filing = db.scalar(select(SecForm4Filing).where(SecForm4Filing.accession_number == accession))
    transactions = list(db.scalars(select(InsiderTransactionNormalized).where(
        InsiderTransactionNormalized.accession_number == accession).order_by(InsiderTransactionNormalized.id)))
    events = list(db.scalars(select(Event).where(Event.id.in_(event_ids)).order_by(Event.id)))
    if filing is None or not transactions or len(events) != len(event_ids):
        return None
    # Exclude mutable derived scores and enrichment timestamps, but bind all
    # canonical transaction/evidence fields, public payloads and identities.
    fields = ('id', 'ts', 'event_date', 'source_provider', 'source_filing_id', 'source_document_url',
              'symbol', 'trade_type', 'transaction_type', 'amount_min', 'amount_max', 'payload_json')
    state = {'filing': {key: getattr(filing, key) for key in ('id', 'accession_number', 'document_hash', 'source_url')},
        'transactions': [{c.name: getattr(row, c.name) for c in row.__table__.columns
                          if c.name not in {'created_at', 'updated_at'}} for row in transactions],
        'events': [{key: getattr(row, key) for key in fields} for row in events]}
    return hashlib.sha256(dumps(state).encode()).hexdigest()


def _existing_transaction_hash(db, accession):
    """Bind an adopted legacy population without inventing event associations."""
    transactions = list(db.scalars(select(InsiderTransactionNormalized).where(
        InsiderTransactionNormalized.accession_number == accession).order_by(InsiderTransactionNormalized.id)))
    if not transactions:
        return None
    return hashlib.sha256(dumps([{c.name: getattr(row, c.name) for c in row.__table__.columns
        if c.name not in {'created_at', 'updated_at'}} for row in transactions]).encode()).hexdigest()


def publish_document(db, document_id):
    """Commit receipt, canonical rows and enrichment jobs together, or none."""
    with writer_transaction(db, 'sec_form4', 'sec_edgar') as control:
        document = db.scalar(select(DirectFeedDocument).where(
            DirectFeedDocument.id == document_id).with_for_update())
        if document is None or document.feed != 'sec_form4':
            raise ValueError('No supported staged document')
        metadata = json.loads(document.metadata_json)
        _source_url(metadata)
        if metadata['key'] != document.source_key or metadata['url'] != document.source_url:
            raise ValueError('Staged source identity changed')
        metadata_hash = hashlib.sha256(dumps(metadata).encode()).hexdigest()
        receipt = db.scalar(select(DirectFeedPublication).where(DirectFeedPublication.document_id == document.id))
        if receipt and (receipt.source_hash != document.content_hash or receipt.metadata_hash != metadata_hash
                        or receipt.publish_since != control.publish_since.isoformat()):
            return {'status': 'held', 'reason': 'Source changed after a publication attempt; reconcile explicitly',
                    'inserted_events': 0, 'inserted_transactions': 0}
        revision = db.scalar(select(DirectFeedRevision).where(
            DirectFeedRevision.document_id == document.id, DirectFeedRevision.content_hash == document.content_hash))
        if revision is not None and revision.source_bytes is not None and hashlib.sha256(revision.source_bytes).hexdigest() != document.content_hash:
            raise ValueError('Captured source checksum changed')
        if receipt and receipt.status in {'published', 'existing'}:
            if document.status != 'parsed' or revision is None or revision.source_bytes is None:
                return {'status': 'held', 'reason': 'Previously published source is no longer verified in staging',
                        'inserted_events': 0, 'inserted_transactions': 0}
            prior = json.loads(receipt.report_json)
            if receipt.status == 'published' and prior.get('canonical_sha256') != _canonical_hash(db, metadata['key'], prior['event_ids']):
                return {'status': 'held', 'reason': 'Published canonical state changed; reconcile explicitly',
                        'inserted_events': 0, 'inserted_transactions': 0}
            if receipt.status == 'existing' and (not prior.get('existing_transactions_sha256') or
                    prior['existing_transactions_sha256'] != _existing_transaction_hash(db, metadata['key'])):
                return {'status': 'held', 'reason': 'Adopted canonical population changed; reconcile explicitly',
                        'inserted_events': 0, 'inserted_transactions': 0}
            return {'status': 'existing', 'receipt_id': receipt.id, 'inserted_events': 0, 'inserted_transactions': 0}
        result = None
        if document.status != 'parsed':
            result = {'status': 'held', 'reason': 'Staged document is not parsed and reconciled'}
        elif revision is None or revision.source_bytes is None:
            result = {'status': 'held', 'reason': 'Original transport bytes are missing; re-collect source'}
        elif metadata['filing_date'] >= datetime.now(timezone.utc).astimezone(ZoneInfo('America/New_York')).date().isoformat():
            result = {'status': 'held', 'reason': 'Publish completed filing days only'}
        else:
            result = _project_new_form4(db, {'feed': document.feed, 'metadata': metadata,
                'raw': revision.source_bytes, 'content_hash': document.content_hash}, publish_since=control.publish_since)
        event_ids = []
        if result['status'] == 'published':
            events = list(db.scalars(select(Event).where(Event.source_provider == 'sec_edgar',
                Event.event_type == 'insider_trade', Event.source_filing_id.in_(result['normalized_hashes']))))
            if len(events) != result['inserted_events']:
                raise ValueError('Published event identity count mismatch')
            for event in events:
                enqueue_feed_pnl_enrichment_for_event(db, event, source='direct_sec_form4',
                    reason='event_insert', use_current_session=True)
                event_ids.append(event.id)
            bump_feed_events_epoch(reason='direct_sec_form4', db=db)
        report = {**result, 'event_ids': sorted(event_ids), 'email_deliveries': 0}
        if event_ids:
            report['canonical_sha256'] = _canonical_hash(db, metadata['key'], event_ids)
        elif result['status'] == 'existing':
            report['existing_transactions_sha256'] = _existing_transaction_hash(db, metadata['key'])
        if receipt is None:
            receipt = DirectFeedPublication(document_id=document.id)
            db.add(receipt)
        receipt.source_hash, receipt.metadata_hash = document.content_hash or '', metadata_hash
        receipt.source_generation = control.generation
        receipt.publish_since = control.publish_since.isoformat()
        receipt.status, receipt.report_json = result['status'], dumps(report)
        receipt.updated_at = datetime.now(timezone.utc)
        db.flush()
        return {**report, 'receipt_id': receipt.id}


def publish_batch(db, *, limit=100, retry_held=False):
    if not 1 <= limit <= 200:
        raise ValueError('Publication limit must be between 1 and 200')
    if db.new or db.dirty or db.deleted:
        raise ValueError('Feed writer session has unrelated pending changes')
    require_selected_source(db, 'sec_form4', 'sec_edgar')
    query = select(DirectFeedDocument.id).outerjoin(DirectFeedPublication,
        DirectFeedPublication.document_id == DirectFeedDocument.id).where(
        DirectFeedDocument.feed == 'sec_form4', DirectFeedDocument.status == 'parsed')
    if not retry_held:
        query = query.where(DirectFeedPublication.id.is_(None))
    else:
        query = query.where((DirectFeedPublication.id.is_(None)) | (DirectFeedPublication.status == 'held'))
    ids = list(db.scalars(query.order_by(DirectFeedDocument.id).limit(limit)))
    db.rollback()  # Each filing has its own atomic transaction/ownership check.
    results = []
    for document_id in ids:
        result = publish_document(db, document_id)
        results.append({'document_id': document_id, **result})
    return {'status': 'partial' if any(r['status'] == 'held' for r in results) else 'ok',
            'processed': len(results), 'inserted_events': sum(r.get('inserted_events', 0) for r in results),
            'results': results, 'email_deliveries': 0}
