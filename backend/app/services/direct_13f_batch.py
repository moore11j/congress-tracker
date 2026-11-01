"""Bounded institutional publication from captured evidence, without network I/O."""
import hashlib
from datetime import datetime, timezone
import json
from pathlib import Path

from sqlalchemy import func, select

from app.services.direct_feed_store import DirectFeedDocument, DirectFeedRevision
from app.services.direct_feed_worker import DirectFeedPublication, _source_url
from app.services.direct_13f_worker import publish_13f_document
from app.services.feed_source_control import require_selected_source, writer_transaction, FeedSourceMismatch
from app.models import InsightsSnapshot
from app.services.direct_13f_evidence import Prepared13FEvidence, PreparedComparisonCache, prepare_streamed_peer_groups


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
MAX_TARGET_BYTES = 100_000_000
FETCH_DOCUMENTS = 25
FETCH_BYTES = 8_000_000
MAX_PERIOD_DOCUMENTS = 500


def _staged_sources(db, *, retain_originals=True):
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
    if retain_originals and sum(row['size'] for row in rows) > MAX_SCAN_BYTES:
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


RETRY_POSITION_KEY = 'direct-13f-retry-position:v1'


def _retry_position(db, generation):
    row = db.get(InsightsSnapshot, RETRY_POSITION_KEY, populate_existing=True)
    if row is None:
        return None, None
    state = json.loads(row.payload_json)
    if (row.source != 'sec_edgar' or not isinstance(state, dict)
            or state.get('version') != 1 or type(state.get('generation')) is not int
            or type(state.get('last_document_id')) is not int or state['last_document_id'] <= 0):
        raise ValueError('Invalid institutional retry position')
    return (state['last_document_id'] if state['generation'] == generation else None), row.payload_json


def _retry_ids(db, query, limit, position):
    after = query.where(DirectFeedDocument.id > position) if position is not None else query
    ids = list(db.scalars(after.order_by(DirectFeedDocument.id).limit(limit)))
    if position is not None and len(ids) < limit:
        ids.extend(db.scalars(query.where(DirectFeedDocument.id <= position)
            .order_by(DirectFeedDocument.id).limit(limit-len(ids))))
    return ids


def _save_retry_position(db, generation, expected_state, document_id):
    # This is operational rotation state. Canonical filing receipts and their
    # timestamps remain immutable when repeated evidence changes nothing.
    with writer_transaction(db, 'sec_13f', 'sec_edgar') as control:
        if control.generation != generation:
            raise FeedSourceMismatch('Institutional source generation changed during retry')
        row = db.get(InsightsSnapshot, RETRY_POSITION_KEY, populate_existing=True)
        if (row.payload_json if row else None) != expected_state:
            return False  # A newer completed batch owns the rotation position.
        payload = json.dumps({'version':1, 'generation':generation,
                              'last_document_id':document_id}, sort_keys=True)
        if row is not None and row.payload_json == payload:
            return False
        now = datetime.now(timezone.utc)
        if row is None:
            row = InsightsSnapshot(kind=RETRY_POSITION_KEY, source='sec_edgar',
                                   fetched_at=now, payload_json=payload)
            db.add(row)
        else:
            row.source, row.fetched_at, row.payload_json = 'sec_edgar', now, payload
        return True


def _bound_target_work(db, ids, retry_ids):
    """Defer whole target filings, preserving each queue's ordered prefix.

    This bounds target work only. Every comparison original is still verified;
    deferred targets are never omitted from the complete peer evidence scan.
    """
    sizes = dict(db.execute(select(DirectFeedDocument.id,
        func.length(DirectFeedRevision.source_bytes)).outerjoin(DirectFeedRevision,
        (DirectFeedRevision.document_id == DirectFeedDocument.id) &
        (DirectFeedRevision.content_hash == DirectFeedDocument.content_hash))
        .where(DirectFeedDocument.id.in_(ids))).all())
    if len(sizes) != len(ids) or any(size is None or not 0 < size <= MAX_SOURCE_BYTES for size in sizes.values()):
        raise ValueError('Missing or oversized target original source')
    if sum(sizes.values()) <= MAX_TARGET_BYTES:
        return ids, retry_ids
    retry_set = set(retry_ids)
    queues = [[document_id for document_id in ids if document_id not in retry_set], retry_ids]
    selected = [[], []]
    remaining = MAX_TARGET_BYTES
    def take_prefix(index, budget):
        nonlocal remaining
        for document_id in queues[index][len(selected[index]):]:
            size = sizes[document_id]
            if size > budget:
                break
            selected[index].append(document_id)
            budget -= size
            remaining -= size
    share = MAX_TARGET_BYTES // 2 if all(queues) else MAX_TARGET_BYTES
    for index in range(2):
        take_prefix(index, min(share, remaining))
    # Each nonempty queue gets a reserved share before spare bytes are reused.
    # Never advance a retry cursor past a deferred filing, even if later ones fit.
    for index in range(2):
        take_prefix(index, remaining)
    if any(queue and not chosen for queue, chosen in zip(queues, selected)):
        raise ValueError('Target work budget cannot preserve queue progress')
    return selected[0] + selected[1], selected[1]


def _validate_batch_options(limit, retry_waiting, include_waiting):
    if not 1 <= limit <= 200:
        raise ValueError('Publication limit must be between 1 and 200')
    if include_waiting and (retry_waiting or limit < 2):
        raise ValueError('Combined publication requires at least two slots and no retry-only mode')


def select_13f_batch(db, *, generation, limit=100, retry_waiting=False, include_waiting=False):
    """Read the bounded ordered target plan without advancing retry state."""
    _validate_batch_options(limit, retry_waiting, include_waiting)
    if db.new or db.dirty or db.deleted:
        raise ValueError('Feed writer session has unrelated pending changes')
    expected_state = None
    query = select(DirectFeedDocument.id).outerjoin(DirectFeedPublication,
        DirectFeedPublication.document_id == DirectFeedDocument.id).where(
        DirectFeedDocument.feed == 'sec_13f', DirectFeedDocument.status == 'parsed')
    retry_ids = []
    if retry_waiting or include_waiting:
        position, expected_state = _retry_position(db, generation)
        retry_query = query.where(DirectFeedPublication.status.in_(('waiting', 'held')))
    if include_waiting:
        # Reserve half the batch for rotating retries and half for first visits.
        # Unused slots flow to the other group; originals are scanned only once.
        # Publish new priors first so a waiting current filing can use them now.
        new_ids = list(db.scalars(query.where(DirectFeedPublication.id.is_(None))
            .order_by(DirectFeedDocument.id).limit(limit)))
        retry_ids = _retry_ids(db, retry_query, max(limit // 2, limit - len(new_ids)), position)
        ids = new_ids[:limit - len(retry_ids)] + retry_ids
    elif retry_waiting:
        retry_ids = _retry_ids(db, retry_query, limit, position)
        ids = retry_ids
    else:
        ids = list(db.scalars(query.where(DirectFeedPublication.id.is_(None))
            .order_by(DirectFeedDocument.id).limit(limit)))
    if ids:
        ids, retry_ids = _bound_target_work(db, ids, retry_ids)
    return ids, retry_ids, expected_state


def prepare_13f_batch(db, *, ids, identifier_documents, use_staged_references=False, checkpoint=lambda phase: None):
    """Prepare complete original-bound evidence; no writes or transaction changes."""
    if not ids or len(ids) > 200 or len(set(ids)) != len(ids):
        raise ValueError("Preparation requires 1 to 200 distinct targets")
    if db.new or db.dirty or db.deleted:
        raise ValueError("Preparation session has unrelated pending changes")
    from app.services.direct_13f_priors import _saved_source
    from app.clients.direct_sources import DirectSourceError
    report_periods = set()
    reference_scopes = {}
    target_periods = {}
    value_scopes = {}
    for document_id in ids:
        checkpoint('targets')
        try:
            _, _, parsed, _ = _saved_source(db, db.get(DirectFeedDocument, document_id))
        except DirectSourceError:
            # Preserve existing per-document hold behavior if a target cannot
            # be trusted. Never use cached metadata to omit comparison evidence.
            report_periods = None
            break
        target_periods[document_id] = parsed['metadata']['report_period']
        report_periods.add(target_periods[document_id])
        # Reference lookup normalizes identifiers; peer value checks preserve
        # the original parser keys to keep existing comparison semantics.
        value_scopes.setdefault(target_periods[document_id], set()).update(r['cusip'] for r in parsed['positions'])
        reference_scopes[document_id] = {(r['cusip'].strip().upper(), parsed['metadata']['report_period']) for r in parsed['positions']}
    def sources():
        for document in _staged_sources(db, retain_originals=False):
            checkpoint('complete_peer_scan')
            yield document
    groups = prepare_streamed_peer_groups(sources(),
        scopes=value_scopes if report_periods is not None else None, spill_to_disk=True)
    try:
        checkpoint('references_and_evidence')
        references = []
        if use_staged_references:
            from app.services.institutional_reference import load_reference_batch
            references = load_reference_batch(db, set().union(*reference_scopes.values()) if reference_scopes else set())
        # All originals are checked before the first writer transaction. Mixed
        # quarter batches retain complete peer summaries and target order.
        evidence = {period: Prepared13FEvidence.build_for_cohort(identifier_documents, cohort)
                    for period, cohort in groups.items()}
        comparisons = {record.identity: record for cohort in groups.values() for record in cohort.records}
        checkpoint('complete')
        return dict(groups=groups, evidence=evidence, references=references,
            target_periods=target_periods, reference_scopes=reference_scopes,
            summary={
                'comparison_periods': sorted(report_periods) if report_periods is not None else None,
                'comparison_group_counts': {str(period): len(group) for period, group in groups.items()},
                'reference_documents': len(references),
                'comparison_documents': len(comparisons),
                'comparison_bytes': sum(record.source_bytes for record in comparisons.values()),
            })

    except BaseException:
        for cohort in groups.values():
            cohort.close()
        raise


def publish_13f_batch(db, *, identifier_documents, limit=100, retry_waiting=False, use_staged_references=False, include_waiting=False):
    _validate_batch_options(limit, retry_waiting, include_waiting)
    if db.new or db.dirty or db.deleted:
        raise ValueError('Feed writer session has unrelated pending changes')
    generation = require_selected_source(db, 'sec_13f', 'sec_edgar').generation
    ids, retry_ids, expected_state = select_13f_batch(db, generation=generation, limit=limit,
        retry_waiting=retry_waiting, include_waiting=include_waiting)
    if not ids:
        db.rollback()
        return {'status': 'ok', 'processed': 0, 'feed_events': 0, 'results': [], 'email_deliveries': 0}
    prepared = prepare_13f_batch(db, ids=ids, identifier_documents=identifier_documents,
        use_staged_references=use_staged_references)
    try:
        db.rollback()  # Recheck durable ownership inside every filing transaction.
        groups, evidence, references = prepared['groups'], prepared['evidence'], prepared['references']
        target_periods, reference_scopes = prepared['target_periods'], prepared['reference_scopes']
        report_periods = prepared['summary']['comparison_periods']
        def group_key(document_id):
            return target_periods[document_id] if report_periods is not None else None
        results = [dict(document_id=document_id, **publish_13f_document(db, document_id,
            identifier_documents=identifier_documents, comparison_documents=groups[group_key(document_id)],
            prepared_evidence=evidence[group_key(document_id)],
            reference_documents=[r for r in references if (r['query']['cusip'], r['query']['date']) in reference_scopes.get(document_id, set())])) for document_id in ids]
        if retry_ids:
            _save_retry_position(db, generation, expected_state, retry_ids[-1])
        return {'status': 'partial' if any(r['status'] in {'held', 'waiting'} for r in results) else 'ok',
            **prepared['summary'],
            'processed': len(results), 'feed_events': sum(r.get('feed_events', 0) for r in results),
            'results': results, 'email_deliveries': 0}

    finally:
        for cohort in prepared['groups'].values():
            cohort.close()
