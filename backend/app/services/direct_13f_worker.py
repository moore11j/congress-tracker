"""Guarded, source-bound institutional publication; never sends email."""
from datetime import datetime, timezone
import hashlib
import json
from zoneinfo import ZoneInfo

from sqlalchemy import select

from app.models import Event, InstitutionalFiling, InstitutionalPositionChange, InstitutionalActivityEvent, InstitutionalPosition
from app.services.direct_feed_store import DirectFeedDocument, DirectFeedRevision, dumps
from app.services.direct_feed_worker import DirectFeedPublication, _source_url
from app.services.direct_13f_publication import _project_new_13f, _filing_digest, _positions_digest, _receipt
from app.services.feed_source_control import writer_transaction
from app.services.feed_pnl_enrichment import enqueue_feed_pnl_enrichment_for_event
from app.services.feed_cache_epoch import bump_feed_events_epoch
from app.services.institutional_activity import INSTITUTIONAL_EVENT_SOURCE


def _state_hash(db, filing, holder_event_ids=()):
    models = (InstitutionalPositionChange, InstitutionalActivityEvent)
    state = {'filing': _filing_digest(filing), 'positions': _positions_digest(db, filing),
             'receipt': _receipt(filing)}
    for model in models:
        rows = db.scalars(select(model).where(model.cik == filing.cik,
            model.report_year == filing.report_year, model.report_quarter == filing.report_quarter).order_by(model.id))
        state[model.__tablename__] = [{c.name: getattr(row, c.name) for c in model.__table__.columns
            if c.name not in {'created_at', 'updated_at'}} for row in rows]
    events = list(db.scalars(select(Event).where(Event.id.in_(holder_event_ids)).order_by(Event.id)))
    if len(events) != len(holder_event_ids):
        return None
    # Holder evidence is immutable. Cluster events can legitimately change when
    # another holder files, so do not bind shared cluster state to this receipt.
    fields = ('id', 'source_provider', 'source_filing_id', 'source_document_url',
              'symbol', 'ts', 'event_date', 'trade_type', 'transaction_type',
              'amount_min', 'amount_max', 'payload_json')
    state['holder_events'] = [{key: getattr(row, key) for key in fields} for row in events]
    return hashlib.sha256(dumps(state).encode()).hexdigest()


def publish_13f_document(db, document_id, *, identifier_documents, comparison_documents, prepared_evidence=None, reference_documents=()):
    """Atomic projection and receipt; evidence must be explicit and bounded.

    Whole-filing parser/identity/peer-value holds remain. Complete originals may
    wait for prior-quarter or mapping evidence without generating any alerts.
    Replaying such a receipt re-evaluates readiness; a published receipt is fixed.
    """
    if len(reference_documents) > 5000 or len(dumps(reference_documents).encode()) > 20_000_000:
        raise ValueError('Reference evidence exceeds bounds')
    if len(identifier_documents) > 50 or not 1 <= len(comparison_documents) <= 500:
        raise ValueError('Supply bounded identifier and nonempty peer evidence')
    if sum(len(d['raw']) for d in (*identifier_documents, *comparison_documents)) > 100_000_000:
        raise ValueError('Evidence batch exceeds 100 MB')
    for peer in comparison_documents:
        if peer.get('feed') != 'sec_13f':
            raise ValueError('Wrong comparison evidence feed')
        _source_url(peer['metadata'])
    with writer_transaction(db, 'sec_13f', 'sec_edgar') as control:
        staged = db.scalar(select(DirectFeedDocument).where(DirectFeedDocument.id == document_id).with_for_update())
        if staged is None or staged.feed != 'sec_13f':
            raise ValueError('No staged institutional document')
        metadata = json.loads(staged.metadata_json)
        _source_url(metadata)
        if metadata['key'] != staged.source_key or metadata['url'] != staged.source_url:
            raise ValueError('Staged filing identity mismatch')
        metadata_hash = hashlib.sha256(dumps(metadata).encode()).hexdigest()
        receipt = db.scalar(select(DirectFeedPublication).where(DirectFeedPublication.document_id == staged.id))
        def held(reason, result=None):
            report = result or {'status': 'held', 'reason': reason, 'inserted_filings': 0,
                                'inserted_positions': 0, 'feed_events': 0}
            # Persist a first hold so an unresolved filing cannot monopolize
            # every scheduled batch. Never replace a prior publication receipt.
            if receipt is None:
                db.add(DirectFeedPublication(document_id=staged.id, source_hash=staged.content_hash or '',
                    metadata_hash=metadata_hash, publish_since=control.publish_since.isoformat(),
                    source_generation=control.generation, status='held', report_json=dumps(report),
                    updated_at=datetime.now(timezone.utc)))
                db.flush()
            return report
        if receipt and (receipt.source_hash != staged.content_hash or receipt.metadata_hash != metadata_hash
                        or receipt.publish_since != control.publish_since.isoformat()):
            return held('Source or publication boundary changed; reconciliation required')
        revision = db.scalar(select(DirectFeedRevision).where(DirectFeedRevision.document_id == staged.id,
                                                            DirectFeedRevision.content_hash == staged.content_hash))
        if staged.status != 'parsed' or revision is None or revision.source_bytes is None:
            return held('Source is not verified or original transport bytes are missing')
        if hashlib.sha256(revision.source_bytes).hexdigest() != staged.content_hash:
            raise ValueError('Source checksum changed')
        if metadata['filing_date'] >= datetime.now(timezone.utc).astimezone(ZoneInfo('America/New_York')).date().isoformat():
            return held('Publish completed filing days only')
        filing = db.scalar(select(InstitutionalFiling).where(InstitutionalFiling.accession_number == metadata['key']))
        prior_report = json.loads(receipt.report_json) if receipt else {}
        if receipt and prior_report.get('canonical_sha256') and (filing is None or
                prior_report['canonical_sha256'] != _state_hash(db, filing, prior_report.get('holder_event_ids', []))):
            return held('Published canonical state changed; reconciliation required')
        # Revalidate peers on every explicit replay, including terminal receipts:
        # newly available contradictory evidence must not earn an "existing" pass.
        result = _project_new_13f(db, {'feed': 'sec_13f', 'metadata': metadata,
            'raw': revision.source_bytes, 'content_hash': staged.content_hash}, publish_since=control.publish_since,
            identifier_documents=identifier_documents, comparison_documents=comparison_documents, prepared_evidence=prepared_evidence, reference_documents=reference_documents)
        result.pop('production_writes', None)  # This entry point can write its selected database.
        if result['status'] == 'held':
            return held(result.get('reason', 'Source requires reconciliation'), result)
        if receipt and receipt.status in {'published', 'existing'} and not result.get('enriched_positions'):
            return {**result, 'status': 'existing', 'receipt_id': receipt.id}
        filing = db.scalar(select(InstitutionalFiling).where(InstitutionalFiling.accession_number == metadata['key']))
        events = []
        for event in db.scalars(select(Event).where(Event.source_provider == INSTITUTIONAL_EVENT_SOURCE)):
            proof = json.loads(event.payload_json or '{}').get('sec_verification', {})
            if proof.get('accession') == metadata['key']:
                events.append(event)
        if len(events) != result.get('feed_events', 0):
            raise ValueError('Institutional event receipt count mismatch')
        for event in events:
            enqueue_feed_pnl_enrichment_for_event(db, event, source='direct_sec_13f',
                reason='event_insert', use_current_session=True)
        if events:
            bump_feed_events_epoch(reason='direct_sec_13f', db=db)
        holder_event_ids = sorted(e.id for e in events
            if json.loads(e.payload_json or '{}').get('sec_verification', {}).get('scope') == 'holder_pair')
        state = result.get('derived_state', '')
        missing_prior_identity = (state == 'historical_no_alerts' and db.scalar(select(InstitutionalPosition.id).where(
            InstitutionalPosition.filing_id == filing.id, InstitutionalPosition.normalized_symbol.is_(None),
            (InstitutionalPosition.put_call.is_(None)) | (InstitutionalPosition.put_call == '')).limit(1)) is not None)
        status = 'waiting' if state.startswith('waiting_') or missing_prior_identity else ('published' if result['status'] == 'projected' else 'existing')
        report = {**result, 'status': status, 'event_ids': sorted(e.id for e in events),
            'holder_event_ids': holder_event_ids, 'email_deliveries': 0,
            'evidence': {'identifiers': [d['content_hash'] for d in identifier_documents],
                         'comparisons': [d['content_hash'] for d in comparison_documents]},
            'canonical_sha256': _state_hash(db, filing, holder_event_ids)}
        # Repeated waiting attempts with identical evidence/state are no-ops.
        if receipt and prior_report.get('canonical_sha256') == report['canonical_sha256'] and prior_report.get('evidence') == report['evidence']:
            return {**result, 'status': status, 'receipt_id': receipt.id}
        if receipt is None:
            receipt = DirectFeedPublication(document_id=staged.id)
            db.add(receipt)
        receipt.source_hash, receipt.metadata_hash = staged.content_hash, metadata_hash
        receipt.source_generation, receipt.publish_since = control.generation, control.publish_since.isoformat()
        receipt.status, receipt.report_json = status, dumps(report)
        receipt.updated_at = datetime.now(timezone.utc)
        db.flush()
        return {**report, 'receipt_id': receipt.id}
