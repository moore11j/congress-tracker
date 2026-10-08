"""Atomic official Congress publication with complete filing row bindings."""
from datetime import date, datetime, timezone
import hashlib
import json
from zoneinfo import ZoneInfo

from sqlalchemy import select, or_

from app.models import Event, Filing, Member, Security, Transaction
from app.services.direct_feed_store import DirectFeedDocument, DirectFeedRevision, dumps
from app.services.direct_feed_worker import DirectFeedPublication
from app.services.direct_feed_collection import parse_document
from app.services.direct_congress_reconciliation import document_identity, resolve_direct_member, reconcile_direct_congress, _key
from app.services.direct_congress_repair import CongressRowBinding, filing_state, _record, _event_matches
from app.services.feed_source_control import writer_transaction, DIRECT_PROVIDERS
from app.services.feed_pnl_enrichment import enqueue_feed_pnl_enrichment_for_event
from app.services.feed_cache_epoch import bump_feed_events_epoch
from app.backfill_events_from_trades import _congress_event_from_transaction


def _hash(value):
    return hashlib.sha256(dumps(value).encode()).hexdigest()


def _context(db, url, member):
    state = filing_state(db, url)
    stored = db.scalar(select(Member).where(Member.bioguide_id == member['bioguide_id']))
    if stored and (stored.chamber != member['chamber'] or stored.state != member['state']):
        raise ValueError('Existing member chamber/state conflicts with disclosed identity')
    if stored:
        rows = list(db.scalars(select(Event).where(Event.member_bioguide_id == stored.bioguide_id).order_by(Event.id).limit(20001)))
        trades = list(db.scalars(select(Transaction).where(Transaction.member_id == stored.id).order_by(Transaction.id).limit(20001)))
        if len(rows) > 20000 or len(trades) > 20000:
            raise ValueError('Member population exceeds bounded Congress publication scope')
        state['events'] = list({e['id']: e for e in state['events'] + [_record(r) for r in rows]}.values())
        state['member_trades'] = [_record(r) for r in trades]
        state['member_securities'] = {r.id: _record(r) for r in db.scalars(select(Security).where(Security.id.in_({t.security_id for t in trades if t.security_id})))}
    return state, stored


def _canonical_hash(db, url):
    state = filing_state(db, url)
    # Outcomes and mutable ranking/enrichment fields do not define identity.
    state.pop('outcomes')
    event_fields = ('id', 'event_type', 'ts', 'event_date', 'symbol', 'member_bioguide_id', 'chamber',
                    'source_provider', 'source_filing_id', 'source_document_url', 'trade_type',
                    'transaction_type', 'amount_min', 'amount_max', 'payload_json')
    state['events'] = [{k: row[k] for k in event_fields} for row in state['events']]
    state['bindings'] = [_record(r) for r in db.scalars(select(CongressRowBinding).where(
        CongressRowBinding.source_url == url).order_by(CongressRowBinding.source_line_ref))]
    return _hash(state)


def _project(db, document, metadata, raw, directory, boundary):
    _, parsed, reasons = parse_document(document.feed, raw, metadata)
    if reasons:
        return {'status': 'held', 'reasons': reasons}
    member_result = resolve_direct_member(metadata, document.feed.split('_')[0], directory)
    if member_result['status'] != 'resolved':
        return member_result
    member = member_result['member']
    day = date.fromisoformat(metadata['filing_date'])
    if day < boundary or day >= datetime.now(timezone.utc).astimezone(ZoneInfo('America/New_York')).date():
        return {'status': 'held', 'reason': 'Filing falls outside completed publication days'}
    rows = parsed['transactions']
    if any(r['amendment_flag'] or r['asset_type_normalized'] in {'unresolved', 'option', 'etf', 'etn'}
           or r['transaction_type_normalized'] not in {'purchase', 'sale'}
           or (r['asset_type_normalized'] == 'stock' and not r['ticker_normalized'])
           or (r['asset_type_normalized'] != 'stock' and r['ticker_normalized']) for r in rows):
        return {'status': 'held', 'reason': 'Instrument/action/amendment needs explicit publication handling'}
    state, stored = _context(db, metadata['url'], member)
    plan = reconcile_direct_congress(parsed, member, **{k: state[k] for k in ('filings', 'transactions', 'events', 'members', 'securities')})
    if plan['status'] == 'held':
        return plan
    existing_bindings = list(db.scalars(select(CongressRowBinding).where(CongressRowBinding.source_url == metadata['url'])))
    if existing_bindings:
        matches = {r['source_line_ref']: r for r in plan['matched']}
        normalized = {r['source_line_ref']: r['normalized_hash'] for r in rows}
        if plan['status'] != 'existing' or len(existing_bindings) != len(rows) or any(
                b.source_line_ref not in matches or b.source_hash != document.content_hash
                or b.normalized_hash != normalized[b.source_line_ref]
                or b.transaction_id != matches[b.source_line_ref]['transaction_id']
                or ([b.event_id] if b.event_id else []) != matches[b.source_line_ref]['event_ids'] for b in existing_bindings):
            return {'status': 'held', 'reason': 'Prior repair bindings conflict with source/canonical population'}
    if plan['status'] == 'new':
        # Catch unprojected provider transactions too, including legacy URLs
        # that were missing or different. Do not guess cross-filing identity.
        source_keys = {_key(day=r['transaction_date'], owner=r['owner_normalized'], action=r['transaction_type_normalized'],
            lower=r['amount_low'], upper=r['amount_high'], symbol=r['ticker_normalized'], description=r['issuer_name_raw'] or r['security_name_raw']) for r in rows}
        for trade in state.get('member_trades', []):
            security = state.get('member_securities', {}).get(trade['security_id'], {})
            key = _key(day=trade['trade_date'], owner=trade['owner_type'], action=trade['transaction_type'],
                lower=trade['amount_range_min'], upper=trade['amount_range_max'], symbol=security.get('symbol'), description=trade['description'] or security.get('name'))
            if key in source_keys:
                return {'status': 'held', 'reason': 'Source economics overlap another legacy transaction; verify filing identity'}
    matched = {r['source_line_ref']: r for r in plan['matched']}
    event_by_id = {e['id']: e for e in state['events']}
    securities = {}
    for row in rows:
        if row['asset_type_normalized'] == 'stock':
            if plan['status'] == 'existing':
                match = matched[row['source_line_ref']]
                if len(match['event_ids']) != 1 or not _event_matches(event_by_id[match['event_ids'][0]], row, member,
                        match['transaction_id'], plan['filing_ids'][0], correct_disclosure=True):
                    return {'status': 'held', 'reason': 'Existing public event conflicts with verified source row'}
            security = db.scalar(select(Security).where(Security.symbol == row['ticker_normalized']))
            if security and security.asset_class.lower() not in {'stock', 'stocks', 'equity'}:
                return {'status': 'held', 'reason': 'Existing security classification conflicts'}
            securities[row['ticker_normalized']] = security
        elif plan['status'] == 'existing' and matched[row['source_line_ref']]['event_ids']:
            return {'status': 'held', 'reason': 'Existing non-stock event requires explicit reconciliation'}
    event_ids, transaction_ids = [], []
    if plan['status'] == 'new':
        if stored is None:
            stored = Member(**{k: member[k] for k in ('bioguide_id', 'first_name', 'last_name', 'chamber', 'party', 'state')})
            db.add(stored); db.flush()
        filing = Filing(member_id=stored.id, source=DIRECT_PROVIDERS[document.feed], filing_date=day,
                        document_url=metadata['url'], document_hash=document.content_hash)
        db.add(filing); db.flush()
    for row in rows:
        event_id = None
        if plan['status'] == 'new':
            security = securities.get(row['ticker_normalized'])
            if row['asset_type_normalized'] == 'stock' and security is None:
                security = Security(symbol=row['ticker_normalized'], name=row['issuer_name_raw'] or row['security_name_raw'] or row['ticker_normalized'], asset_class='stock')
                db.add(security); db.flush()
                securities[row['ticker_normalized']] = security
            tx = Transaction(filing_id=filing.id, member_id=stored.id, security_id=security.id if security else None,
                owner_type=row['owner_normalized'], transaction_type=row['transaction_type_normalized'],
                trade_date=date.fromisoformat(str(row['transaction_date'])[:10]), report_date=day,
                amount_range_min=row['amount_low'], amount_range_max=row['amount_high'],
                description=row['issuer_name_raw'] or row['security_name_raw'])
            db.add(tx); db.flush()
            transaction_id = tx.id
            if security:
                event = _congress_event_from_transaction(tx, filing, stored, security)
                event.source_provider = DIRECT_PROVIDERS[document.feed]
                event.source_filing_id = row['normalized_hash']
                event.source_document_url = metadata['url']
                event.parser_version = row['parser_version']
                event.provider_priority = 10
                payload = json.loads(event.payload_json)
                payload.update(source_line_ref=row['source_line_ref'], normalized_hash=row['normalized_hash'],
                    source_sha256=document.content_hash, source_transaction_type=row['transaction_type_raw'],
                    source_asset_type=row['asset_type_raw'])
                event.payload_json = dumps(payload)
                db.add(event); db.flush()
                enqueue_feed_pnl_enrichment_for_event(db, event, source='direct_congress', reason='event_insert', use_current_session=True)
                event_id = event.id
        else:
            match = matched[row['source_line_ref']]
            transaction_id = match['transaction_id']
            event_id = match['event_ids'][0] if match['event_ids'] else None
        if not existing_bindings:
            db.add(CongressRowBinding(source_url=metadata['url'], source_line_ref=row['source_line_ref'],
                source_hash=document.content_hash, normalized_hash=row['normalized_hash'], transaction_id=transaction_id, event_id=event_id))
        transaction_ids.append(transaction_id)
        if event_id is not None:
            event_ids.append(event_id)
    db.flush()
    if plan['status'] == 'new' and event_ids:
        bump_feed_events_epoch(reason='direct_congress', db=db)
    return {'status': 'published' if plan['status'] == 'new' else 'existing', 'event_ids': sorted(event_ids),
            'transaction_ids': sorted(transaction_ids), 'inserted_events': len(event_ids) if plan['status'] == 'new' else 0,
            'inserted_transactions': len(rows) if plan['status'] == 'new' else 0,
            'non_stock_rows': sum(r['asset_type_normalized'] != 'stock' for r in rows)}


def publish_document(db, document_id, *, directory):
    if db.new or db.dirty or db.deleted:
        raise ValueError('Publisher session has unrelated pending changes')
    # Lookup only establishes which lock to acquire; identity is re-read under
    # the lock. Source control remains the authority, never document status.
    feed = db.scalar(select(DirectFeedDocument.feed).where(DirectFeedDocument.id == document_id))
    if feed not in {'house_ptr', 'senate_ptr'}:
        raise ValueError('No supported Congress source document')
    with writer_transaction(db, feed, DIRECT_PROVIDERS[feed]) as control:
        document = db.scalar(select(DirectFeedDocument).where(DirectFeedDocument.id == document_id).with_for_update().execution_options(populate_existing=True))
        if document.feed != feed:
            raise ValueError('Staged feed changed')
        metadata = json.loads(document.metadata_json)
        identity = document_identity(metadata['url'])
        if not identity or identity[0] != feed.split('_')[0] or identity[-1] != metadata['filing_id'] or metadata['url'] != document.source_url or metadata['key'] != document.source_key:
            raise ValueError('Staged Congress source identity differs')
        metadata_hash = _hash(metadata)
        revision = db.scalar(select(DirectFeedRevision).where(DirectFeedRevision.document_id == document.id, DirectFeedRevision.content_hash == document.content_hash))
        if revision is None or revision.source_bytes is None or hashlib.sha256(revision.source_bytes).hexdigest() != document.content_hash:
            raise ValueError('Captured source bytes missing or changed')
        receipt = db.scalar(select(DirectFeedPublication).where(DirectFeedPublication.document_id == document.id))
        if receipt:
            if receipt.source_hash != document.content_hash or receipt.metadata_hash != metadata_hash or receipt.publish_since != control.publish_since.isoformat():
                return {'status': 'held', 'reason': 'Source changed after publication attempt'}
            if receipt.status in {'published', 'existing'}:
                prior = json.loads(receipt.report_json)
                if document.status != 'parsed' or prior['canonical_sha256'] != _canonical_hash(db, metadata['url']):
                    return {'status': 'held', 'reason': 'Published source/canonical state changed'}
                return {'status': 'existing', 'inserted_events': 0, 'inserted_transactions': 0, 'email_deliveries': 0}
        result = ({'status': 'held', 'reason': 'Staged source is not parsed'} if document.status != 'parsed'
                  else _project(db, document, metadata, revision.source_bytes, directory, control.publish_since))
        report = {**result, 'email_deliveries': 0, 'directory_sha256': _hash(directory)}
        if result['status'] in {'published', 'existing'}:
            report['canonical_sha256'] = _canonical_hash(db, metadata['url'])
        if receipt is None:
            receipt = DirectFeedPublication(document_id=document.id); db.add(receipt)
        receipt.source_hash, receipt.metadata_hash = document.content_hash, metadata_hash
        receipt.source_generation, receipt.publish_since = control.generation, control.publish_since.isoformat()
        receipt.status, receipt.report_json, receipt.updated_at = result['status'], dumps(report), datetime.now(timezone.utc)
        db.flush()
        return report


def publish_batch(db, *, feed, directory, limit=100, retry_held=False):
    if feed not in {'house_ptr', 'senate_ptr'} or not 1 <= limit <= 200:
        raise ValueError('Select a Congress feed and limit between 1 and 200')
    if db.new or db.dirty or db.deleted:
        raise ValueError('Publisher session has unrelated pending changes')
    from app.services.feed_source_control import require_selected_source
    require_selected_source(db, feed, DIRECT_PROVIDERS[feed])
    query = select(DirectFeedDocument.id).outerjoin(DirectFeedPublication,
        DirectFeedPublication.document_id == DirectFeedDocument.id).where(
        DirectFeedDocument.feed == feed, DirectFeedDocument.status == 'parsed')
    query = query.where(or_(DirectFeedPublication.id.is_(None), DirectFeedPublication.status == 'held')
        if retry_held else DirectFeedPublication.id.is_(None))
    ids = list(db.scalars(query.order_by(DirectFeedDocument.id).limit(limit)))
    db.rollback()
    results = [{'document_id': identifier, **publish_document(db, identifier, directory=directory)} for identifier in ids]
    return {'status': 'partial' if any(r['status'] == 'held' for r in results) else 'ok',
            'processed': len(results), 'inserted_events': sum(r.get('inserted_events', 0) for r in results),
            'results': results, 'email_deliveries': 0}
