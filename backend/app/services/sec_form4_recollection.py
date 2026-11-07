"""Verify already published identical lots without changing canonical records."""
import hashlib
import json

from sqlalchemy import select

from app.models import InsiderTransactionNormalized
from app.services.direct_feed_store import dumps


def reconcile_published_original(db, document, raw, parsed):
    """Return exact receipt-bound row identities, or retain ordinary ambiguity.

    Only collection uses this fallback after a successful whole-original parse.
    It does not authorize a new publication, adopt legacy lots, or repair rows.
    """
    from app.services.direct_feed_worker import DirectFeedPublication, _canonical_hash, _source_url
    from app.services.feed_source_control import FeedSourceControl

    if document.feed != 'sec_form4':
        return None
    metadata = json.loads(document.metadata_json)
    _source_url(metadata)
    if metadata['key'] != document.source_key or metadata['url'] != document.source_url:
        return None
    receipt = db.scalar(select(DirectFeedPublication).where(
        DirectFeedPublication.document_id == document.id))
    control = db.get(FeedSourceControl, 'sec_form4')
    if (receipt is None or receipt.status != 'published' or control is None
            or control.provider != 'sec_edgar' or receipt.source_generation != control.generation
            or receipt.publish_since != control.publish_since.isoformat()
            or receipt.source_hash != hashlib.sha256(raw).hexdigest()
            or receipt.source_hash != document.content_hash
            or receipt.metadata_hash != hashlib.sha256(dumps(metadata).encode()).hexdigest()):
        return None
    report = json.loads(receipt.report_json)
    event_ids = report.get('event_ids')
    if (not isinstance(event_ids, list) or not event_ids or len(set(event_ids)) != len(event_ids)
            or not report.get('canonical_sha256')
            or report['canonical_sha256'] != _canonical_hash(db, metadata['key'], event_ids)):
        return None
    source = parsed['transactions']
    if parsed['filing']['accession_number'] != metadata['key'] or not source:
        return None
    rows = list(db.scalars(select(InsiderTransactionNormalized).where(
        InsiderTransactionNormalized.accession_number == metadata['key'])))
    by_hash = {row.normalized_hash: row for row in rows}
    hashes = [item['normalized_hash'] for item in source]
    if (len(rows) != len(source) or len(by_hash) != len(rows)
            or len(event_ids) != len(source)
            or len(report.get('normalized_hashes') or []) != len(source)
            or len(set(hashes)) != len(hashes) or set(hashes) != set(by_hash)
            or set(hashes) != set(report.get('normalized_hashes') or [])):
        return None
    matched = []
    for index, item in enumerate(source, 1):
        row = by_hash[item['normalized_hash']]
        if row.is_duplicate or any(getattr(row, key, object()) != value for key, value in item.items()):
            return None
        matched.append({'row': index, 'existing_id': row.id})
    return {'matched': matched, 'unmatched': [], 'ambiguous': [], 'existing_only_ids': []}
