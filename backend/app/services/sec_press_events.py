"""Atomic selected SEC earnings-release events; no delivery or source transport."""
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import os

from sqlalchemy import cast, func, or_, select
from sqlalchemy.dialects.postgresql import JSONB

from app.models import Event, Security
from app.services.direct_feed_store import DirectFeedDocument, DirectFeedRevision, dumps
from app.services.direct_feed_worker import DirectFeedPublication
from app.services.feed_source_control import require_selected_source
from app.services.sec_earnings_store import reconcile_staged_earnings

FEED = 'sec_earnings_release'


def _aware(value):
    return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)


def _event_hash(event):
    values = {k: getattr(event, k) for k in ('id', 'event_type', 'symbol', 'source', 'source_provider',
        'source_filing_id', 'source_document_url', 'payload_json')}
    values.update({k: _aware(getattr(event, k)).isoformat() if getattr(event, k) else None for k in ('ts', 'event_date')})
    return hashlib.sha256(dumps(values).encode()).hexdigest()


def _source_state(db, document):
    """Bind repeat checks to saved transport bytes and the first observation."""
    metadata = json.loads(document.metadata_json)
    values = {'parsed_sha256': hashlib.sha256((document.parsed_json or '').encode()).hexdigest()}
    for feed, doc_id, digest in [
        (FEED, document.id, document.content_hash),
        ('sec_earnings_company', metadata['company_document_id'], metadata['company_sha256']),
        ('sec_earnings_index', metadata['index_document_id'], metadata['index_sha256'])]:
        parent = db.get(DirectFeedDocument, doc_id)
        revision = db.scalar(select(DirectFeedRevision).where(DirectFeedRevision.document_id == doc_id,
                                                            DirectFeedRevision.content_hash == digest))
        if parent is None or parent.feed != feed or revision is None or revision.source_bytes is None or hashlib.sha256(revision.source_bytes).hexdigest() != digest:
            raise ValueError('Published SEC release source evidence missing or changed')
        values[feed] = {'sha256': digest, 'observed_at': _aware(revision.fetched_at).isoformat()}
    return hashlib.sha256(dumps(values).encode()).hexdigest()


def publish_sec_release(db, document_id, *, now=None):
    """Caller commits event, source receipt and reconciliation together."""
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        raise ValueError('Publication clock must be aware')
    control = require_selected_source(db, FEED, 'sec_edgar')
    try:
        cutoff = datetime.fromisoformat(os.environ['SEC_EARNINGS_PUBLISH_AFTER'].replace('Z', '+00:00'))
        if cutoff.tzinfo is None:
            raise ValueError('Naive cutoff')
        cutoff = cutoff.astimezone(timezone.utc)
    except (KeyError, ValueError):
        raise ValueError('SEC earnings publication requires an explicit aware activation timestamp') from None
    document = db.get(DirectFeedDocument, document_id)
    if document is None or document.feed != FEED:
        raise ValueError('No supported SEC release document')
    metadata_hash = hashlib.sha256(document.metadata_json.encode()).hexdigest()
    source_state = _source_state(db, document)
    saved = db.scalar(select(DirectFeedPublication).where(DirectFeedPublication.document_id == document.id))
    if saved and saved.status == 'published':
        report = json.loads(saved.report_json)
        event = db.get(Event, report['event_id'])
        checks = {'source_hash': saved.source_hash == document.content_hash,
                  'metadata_hash': saved.metadata_hash == metadata_hash,
                  'activation': report.get('publish_after') == cutoff.isoformat(),
                  'filing_boundary': saved.publish_since == control.publish_since.isoformat(),
                  'source_state': report.get('source_state_sha256') == source_state,
                  'event_state': event is not None and _event_hash(event) == report['event_sha256']}
        if not all(checks.values()):
            changed = ','.join(key for key, valid in checks.items() if not valid)
            raise ValueError(f'Published SEC release state changed ({changed}); explicit reconciliation required')
        return {'status': 'unchanged', 'event_id': event.id, 'inserted_events': 0}
    metadata = json.loads(document.metadata_json)
    symbol = metadata['filing']['symbol']
    security = db.scalar(select(Security).where(Security.symbol == symbol).with_for_update())
    if security is None:
        raise ValueError('SEC release security missing')
    result = reconcile_staged_earnings(db, document_id, security_id=security.id)
    parsed = json.loads(document.parsed_json)
    reason = result.get('reason') if result['status'] == 'held' else None
    filed = date.fromisoformat(parsed['filing_date'])
    if filed < control.publish_since or filed < now.date() - timedelta(days=7) or filed > now.date():
        reason = 'outside_recent_publication_boundary'
    revision = db.scalar(select(DirectFeedRevision).where(DirectFeedRevision.document_id == document.id,
                                                         DirectFeedRevision.content_hash == document.content_hash))
    observed = _aware(revision.fetched_at) if revision else None
    accepted = datetime.fromisoformat(parsed['sec_accepted_at']) if parsed.get('sec_accepted_at') else None
    clock_evidence = parsed.get('acceptance_evidence') or {}
    earliest_observation = datetime.fromisoformat(clock_evidence['observation_not_before']) if clock_evidence.get('observation_not_before') else accepted
    if observed is None or accepted is None or earliest_observation is None or not accepted <= earliest_observation <= observed <= now:
        reason = reason or 'invalid_observation_order'
    if accepted is not None and accepted < cutoff:
        reason = 'before_activation_timestamp'
    if not reason:
        # Existing provider projections may have truncated text or different
        # titles. A nearby press event is not proof that this is a new event.
        since = datetime.combine(filed-timedelta(days=3), datetime.min.time(), tzinfo=timezone.utc)
        related = db.scalar(select(Event.id).where(Event.symbol == symbol, Event.event_type == 'press_release',
            or_(Event.source_filing_id == parsed['canonical_key'], Event.source_document_url == parsed['release']['url'],
                (Event.event_date >= since) & (Event.event_date < since+timedelta(days=7)),
                Event.event_date.is_(None))).limit(1))
        if related is not None:
            reason = 'existing_press_event_requires_reconciliation'
    if saved is None:
        saved = DirectFeedPublication(document_id=document.id)
        db.add(saved)
    saved.source_hash, saved.metadata_hash = document.content_hash, metadata_hash
    saved.source_generation, saved.publish_since = control.generation, control.publish_since.isoformat()
    saved.updated_at = now
    saved.status, saved.report_json = 'held', dumps({'status': 'held', 'reason': 'publication_in_progress'})
    if reason:
        report = {'status': 'held', 'reason': reason, 'inserted_events': 0}
        saved.status, saved.report_json = 'held', dumps(report)
        db.flush()
        return report
    source_url = parsed['release']['url']
    payload = {'title': f'{symbol} earnings release filed with the SEC',
        'summary': f'Earnings release filed {filed.isoformat()}. First observed by Walnut {observed.date().isoformat()}.',
        'url': source_url, 'publisher': 'SEC EDGAR', 'provider': 'sec_edgar', 'symbol': symbol,
        'data_category': 'press_releases', 'published_at': None, 'filing_date': filed.isoformat(),
        'sec_accepted_at': accepted.isoformat(), 'first_observed_at': observed.isoformat(),
        'acceptance_evidence': clock_evidence,
        'source_availability': {'basis': 'direct_publication', 'date': observed.date().isoformat(),
                                'observed_at': observed.isoformat()},
        'source_verification': {'feed': FEED, 'sha256': document.content_hash,
                                'text_sha256': parsed['release']['text_sha256']},
        'can_confirm': False, 'is_market_trade': False, 'direction': 'neutral',
        'content_event_key': parsed['canonical_key']}
    event = Event(event_type='press_release', symbol=symbol, ts=observed, event_date=accepted,
        source='sec_edgar_press_release', source_provider='sec_edgar', data_source='sec_edgar',
        source_filing_id=parsed['canonical_key'], source_document_url=source_url,
        parser_version='sec_earnings_v2', impact_score=0, payload_json=dumps(payload))
    db.add(event); db.flush()
    report = {'status': 'published', 'event_id': event.id, 'event_sha256': _event_hash(event),
              'source_state_sha256': source_state, 'publish_after': cutoff.isoformat(), 'inserted_events': 1}
    saved.status, saved.report_json = 'published', dumps(report)
    db.flush()
    return report


def sync_sec_release_events(db, symbols, *, limit=20):
    control = require_selected_source(db, FEED, 'sec_edgar')
    limit = min(100, max(1, limit))
    if not symbols:
        return 0
    if db.get_bind().dialect.name == 'postgresql':
        metadata = cast(DirectFeedDocument.metadata_json, JSONB)
        symbol, filed = metadata['filing']['symbol'].astext, metadata['filing']['filing_date'].astext
    else:
        symbol = func.json_extract(DirectFeedDocument.metadata_json, '$.filing.symbol')
        filed = func.json_extract(DirectFeedDocument.metadata_json, '$.filing.filing_date')
    start = max(control.publish_since, datetime.now(timezone.utc).date()-timedelta(days=7)).isoformat()
    ids = list(db.scalars(select(DirectFeedDocument.id).outerjoin(DirectFeedPublication,
        DirectFeedPublication.document_id == DirectFeedDocument.id).where(
        DirectFeedDocument.feed == FEED, DirectFeedDocument.status == 'parsed', symbol.in_(symbols),
        filed >= start, DirectFeedPublication.id.is_(None)).order_by(DirectFeedDocument.id).limit(limit)))
    return sum(publish_sec_release(db, doc_id)['inserted_events'] for doc_id in ids)
