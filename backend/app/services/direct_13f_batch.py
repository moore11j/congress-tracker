"""Bounded institutional publication from captured evidence, without network I/O."""
import hashlib
import json
from pathlib import Path

from sqlalchemy import func, select

from app.services.direct_feed_store import DirectFeedDocument, DirectFeedRevision
from app.services.direct_feed_worker import DirectFeedPublication, _source_url
from app.services.direct_13f_worker import publish_13f_document
from app.services.feed_source_control import require_selected_source
from app.services.direct_13f_evidence import Prepared13FEvidence


def load_identifier_manifest(path):
    """Load an operator-reviewed manifest; paths must stay beside the manifest."""
    path = Path(path).resolve(strict=True)
    if path.stat().st_size > 1_000_000:
        raise ValueError('Identifier manifest exceeds size limit')
    manifest = json.loads(path.read_bytes())
    entries = manifest['documents']
    if not isinstance(entries, list) or not 1 <= len(entries) <= 50:
        raise ValueError('Supply 1 to 50 reviewed identifier documents')
    documents, seen, total = [], set(), 0
    for entry in entries:
        source = (path.parent / entry['source_file']).resolve(strict=True)
        if source.parent != path.parent or not source.is_file() or source in seen:
            raise ValueError('Identifier path escapes manifest directory or is duplicated')
        size = source.stat().st_size
        total += size
        if size > 30_000_000 or total > 50_000_000:
            raise ValueError('Identifier evidence exceeds size limit')
        raw = source.read_bytes()
        if len(raw) != size or hashlib.sha256(raw).hexdigest() != entry['content_hash']:
            raise ValueError('Identifier evidence checksum changed')
        documents.append({'raw': raw, 'content_hash': entry['content_hash'], 'url': entry['url']})
        seen.add(source)
    # XML/header identities and availability are checked for each target filing
    # by nport_identifiers, not trusted from the manifest's parsed observations.
    return documents


def staged_comparisons(db, *, byte_budget):
    """Use a bounded saved cohort; absence of matching peers is not value proof."""
    query = select(DirectFeedDocument, DirectFeedRevision.id, func.length(DirectFeedRevision.source_bytes)).join(
        DirectFeedRevision, (DirectFeedRevision.document_id == DirectFeedDocument.id) &
        (DirectFeedRevision.content_hash == DirectFeedDocument.content_hash)).where(
        DirectFeedDocument.feed == 'sec_13f', DirectFeedDocument.status == 'parsed',
        DirectFeedRevision.source_bytes.is_not(None)).order_by(DirectFeedDocument.id.desc()).limit(500)
    rows = list(db.execute(query))
    if not rows or sum(size for _, _, size in rows) > byte_budget:
        raise ValueError('Missing or oversized staged comparison cohort; supply a bounded staging batch')
    documents = []
    for doc, revision_id, size in rows:
        metadata = json.loads(doc.metadata_json)
        _source_url(metadata)
        if metadata['key'] != doc.source_key or metadata['url'] != doc.source_url:
            raise ValueError('Staged comparison identity changed')
        raw = db.scalar(select(DirectFeedRevision.source_bytes).where(DirectFeedRevision.id == revision_id))
        if len(raw) != size or hashlib.sha256(raw).hexdigest() != doc.content_hash:
            raise ValueError('Staged comparison checksum changed')
        documents.append({'feed': 'sec_13f', 'metadata': metadata, 'raw': raw, 'content_hash': doc.content_hash})
    return documents


def publish_13f_batch(db, *, identifier_documents, limit=100, retry_waiting=False):
    if not 1 <= limit <= 200:
        raise ValueError('Publication limit must be between 1 and 200')
    if db.new or db.dirty or db.deleted:
        raise ValueError('Feed writer session has unrelated pending changes')
    require_selected_source(db, 'sec_13f', 'sec_edgar')
    query = select(DirectFeedDocument.id).outerjoin(DirectFeedPublication,
        DirectFeedPublication.document_id == DirectFeedDocument.id).where(
        DirectFeedDocument.feed == 'sec_13f', DirectFeedDocument.status == 'parsed')
    if retry_waiting:
        query = query.where(DirectFeedPublication.status.in_(('waiting', 'held')))
    else:
        query = query.where(DirectFeedPublication.id.is_(None))
    ids = list(db.scalars(query.order_by(DirectFeedDocument.id).limit(limit)))
    if not ids:
        db.rollback()
        return {'status': 'ok', 'processed': 0, 'feed_events': 0, 'results': [], 'email_deliveries': 0}
    comparisons = staged_comparisons(db, byte_budget=100_000_000 - sum(len(d['raw']) for d in identifier_documents))
    db.rollback()  # Recheck durable ownership inside every filing transaction.
    evidence=Prepared13FEvidence.build(identifier_documents,comparisons)
    results = [dict(document_id=document_id, **publish_13f_document(db, document_id,
        identifier_documents=identifier_documents, comparison_documents=comparisons, prepared_evidence=evidence)) for document_id in ids]
    return {'status': 'partial' if any(r['status'] in {'held', 'waiting'} for r in results) else 'ok',
        'processed': len(results), 'feed_events': sum(r.get('feed_events', 0) for r in results),
        'results': results, 'email_deliveries': 0}
