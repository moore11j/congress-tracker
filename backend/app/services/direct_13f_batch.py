"""Bounded institutional publication from captured evidence, without network I/O."""
import hashlib
import json
from pathlib import Path

from sqlalchemy import func, select

from app.services.direct_feed_store import DirectFeedDocument, DirectFeedRevision
from app.services.direct_feed_worker import DirectFeedPublication, _source_url
from app.services.direct_13f_worker import publish_13f_document
from app.services.feed_source_control import require_selected_source
from app.services.direct_13f_evidence import Prepared13FEvidence, PreparedComparisonCache


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


# Inventory bounds protect memory and work; they are never a truncation policy.
MAX_SCAN_DOCUMENTS = 10_000
MAX_SCAN_BYTES = 100_000_000
MAX_SOURCE_BYTES = 30_000_000
FETCH_DOCUMENTS = 25
FETCH_BYTES = 8_000_000
MAX_PERIOD_DOCUMENTS = 500


def _staged_sources(db):
    """Read every current parsed original or refuse an incomplete inventory."""
    query = select(DirectFeedDocument.id, DirectFeedDocument.metadata_json,
        DirectFeedDocument.source_key, DirectFeedDocument.source_url,
        DirectFeedDocument.content_hash, DirectFeedRevision.id.label('revision_id'),
        func.length(DirectFeedRevision.source_bytes).label('size')).outerjoin(
        DirectFeedRevision, (DirectFeedRevision.document_id == DirectFeedDocument.id) &
        (DirectFeedRevision.content_hash == DirectFeedDocument.content_hash)).where(
        DirectFeedDocument.feed == 'sec_13f', DirectFeedDocument.status == 'parsed'
        ).order_by(DirectFeedDocument.id.desc()).limit(MAX_SCAN_DOCUMENTS + 1)
    rows = list(db.execute(query).mappings())
    if not rows or len(rows) > MAX_SCAN_DOCUMENTS:
        raise ValueError('Missing or oversized complete staged source inventory')
    if any(row['size'] is None or not 0 < row['size'] <= MAX_SOURCE_BYTES for row in rows):
        raise ValueError('Missing or oversized staged original source')
    if sum(row['size'] for row in rows) > MAX_SCAN_BYTES:
        raise ValueError('Complete staged source scan exceeds byte bounds')
    offset = 0
    while offset < len(rows):
        chunk, size = [], 0
        for row in rows[offset:offset + FETCH_DOCUMENTS]:
            if chunk and size + row['size'] > FETCH_BYTES:
                break
            chunk.append(row)
            size += row['size']
        originals = dict(db.execute(select(DirectFeedRevision.id, DirectFeedRevision.source_bytes)
            .where(DirectFeedRevision.id.in_([row['revision_id'] for row in chunk]))).all())
        for row in chunk:
            metadata = json.loads(row['metadata_json'])
            _source_url(metadata)
            if metadata['key'] != row['source_key'] or metadata['url'] != row['source_url']:
                raise ValueError('Staged comparison identity changed')
            raw = originals.pop(row['revision_id'], None)
            if raw is None or len(raw) != row['size'] or hashlib.sha256(raw).hexdigest() != row['content_hash']:
                raise ValueError('Staged comparison checksum changed')
            yield {'feed': 'sec_13f', 'metadata': metadata, 'raw': raw, 'content_hash': row['content_hash']}
        offset += len(chunk)


def staged_comparison_groups(db, *, byte_budget, report_periods=None, comparison_cache=None):
    """Complete original-bound period cohorts, with no global newest-N window.

    All inventory/byte/period bounds fail before publication. Invalid originals
    remain in every requested group for the existing evidence validator; saved
    parsed_json is never trusted to exclude a source.
    """
    from app.services.direct_feed_collection import parse_document
    from app.clients.direct_sources import DirectSourceError
    groups = {period: [] for period in sorted(report_periods)} if report_periods else {None: []}
    # Each independently prepared quarter has the same identifier+peer budget.
    # The complete original scan remains bounded globally by MAX_SCAN_BYTES.
    retained_bytes = {period: 0 for period in groups}
    for document in _staged_sources(db):
        periods = groups
        if report_periods:
            if comparison_cache is not None:
                if not isinstance(comparison_cache, PreparedComparisonCache):
                    raise TypeError('Invalid comparison parse cache')
                period = comparison_cache.period(document)
                if period is not None:
                    periods = [period] if period in groups else []
            else:
                try:
                    _, parsed, _ = parse_document('sec_13f', document['raw'], document['metadata'])
                except DirectSourceError:
                    pass
                else:
                    period = parsed['metadata']['report_period']
                    periods = [period] if period in groups else []
        if not periods:
            continue
        for period in periods:
            retained_bytes[period] += len(document['raw'])
            if retained_bytes[period] > byte_budget:
                raise ValueError('Missing or oversized relevant staged comparison cohort exceeds evidence budget')
            groups[period].append(document)
            if len(groups[period]) > MAX_PERIOD_DOCUMENTS:
                raise ValueError('Complete staged period cohort exceeds document bounds')
    if any(not documents for documents in groups.values()):
        raise ValueError('Missing staged comparison period evidence')
    return groups


def staged_comparisons(db, *, byte_budget, report_periods=None):
    """Compatibility reader for callers requiring one bounded evidence object."""
    groups = staged_comparison_groups(db, byte_budget=byte_budget, report_periods=report_periods)
    documents = list({id(d): d for group in groups.values() for d in group}.values())
    if len(documents) > MAX_PERIOD_DOCUMENTS:
        raise ValueError('Combined comparison cohort exceeds document bounds; use period groups')
    if sum(len(d['raw']) for d in documents) > byte_budget:
        raise ValueError('Combined comparison cohort exceeds evidence budget; use period groups')
    return documents


def publish_13f_batch(db, *, identifier_documents, limit=100, retry_waiting=False, use_staged_references=False):
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
    target_bytes = db.scalar(select(func.sum(func.length(DirectFeedRevision.source_bytes))).join(
        DirectFeedDocument, (DirectFeedDocument.id == DirectFeedRevision.document_id) &
        (DirectFeedDocument.content_hash == DirectFeedRevision.content_hash)).where(
            DirectFeedDocument.id.in_(ids))) or 0
    if target_bytes > 100_000_000:
        raise ValueError('Target source scan exceeds byte bounds')
    from app.services.direct_13f_priors import _saved_source
    from app.clients.direct_sources import DirectSourceError
    report_periods = set()
    reference_scopes = {}
    target_periods = {}
    for document_id in ids:
        try:
            _, _, parsed, _ = _saved_source(db, db.get(DirectFeedDocument, document_id))
        except DirectSourceError:
            # Preserve existing per-document hold behavior if a target cannot
            # be trusted. Never use cached metadata to omit comparison evidence.
            report_periods = None
            break
        target_periods[document_id] = parsed['metadata']['report_period']
        report_periods.add(target_periods[document_id])
        reference_scopes[document_id] = {(r['cusip'].strip().upper(), parsed['metadata']['report_period']) for r in parsed['positions']}
    comparison_cache = PreparedComparisonCache()
    groups = staged_comparison_groups(db,
        byte_budget=100_000_000 - sum(len(d['raw']) for d in identifier_documents),
        report_periods=report_periods, comparison_cache=comparison_cache)
    references = []
    if use_staged_references:
        from app.services.institutional_reference import load_reference_batch
        references = load_reference_batch(db, set().union(*reference_scopes.values()) if reference_scopes else set())
    db.rollback()  # Recheck durable ownership inside every filing transaction.
    # Prepare every complete group before the first writer transaction. Mixed
    # quarter batches retain input order without fitting both quarters into500.
    evidence = {period: Prepared13FEvidence.build(identifier_documents, documents, comparison_cache=comparison_cache)
                for period, documents in groups.items()}
    comparisons = list({id(d): d for group in groups.values() for d in group}.values())
    def group_key(document_id):
        return target_periods[document_id] if report_periods is not None else None
    results = [dict(document_id=document_id, **publish_13f_document(db, document_id,
        identifier_documents=identifier_documents, comparison_documents=groups[group_key(document_id)],
        prepared_evidence=evidence[group_key(document_id)],
        reference_documents=[r for r in references if (r['query']['cusip'], r['query']['date']) in reference_scopes.get(document_id, set())])) for document_id in ids]
    return {'status': 'partial' if any(r['status'] in {'held', 'waiting'} for r in results) else 'ok',
        'comparison_periods': sorted(report_periods) if report_periods is not None else None,
        'comparison_group_counts': {str(period): len(group) for period, group in groups.items()},
        'reference_documents': len(references),
        'comparison_documents': len(comparisons), 'comparison_bytes': sum(len(d['raw']) for d in comparisons),
        'processed': len(results), 'feed_events': sum(r.get('feed_events', 0) for r in results),
        'results': results, 'email_deliveries': 0}
