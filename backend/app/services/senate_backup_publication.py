"""Transaction-bound secondary Senate publication with retained source identity.

No collection, source selection, network requests or email delivery occurs here.
Official row bindings remain exclusively the official parser's responsibility.
"""
from datetime import date, datetime, timezone
import json
from zoneinfo import ZoneInfo

from sqlalchemy import select

from app.models import Event, Filing, Member, Security, Transaction
from app.services.direct_feed_store import DirectFeedDocument, dumps
from app.services.direct_feed_worker import DirectFeedPublication
from app.services.direct_congress_worker import _canonical_hash, _hash
from app.services.feed_source_control import writer_transaction
from app.services.senate_backup_collection import FEED
from app.services.senate_backup_evidence import PROVIDER, bind_original
from app.services.senate_backup_reconciliation import inspect_staged
from app.services.feed_pnl_enrichment import enqueue_feed_pnl_enrichment_for_event
from app.services.feed_cache_epoch import bump_feed_events_epoch
from app.backfill_events_from_trades import _congress_event_from_transaction


def _receipt_bindings_match(db, prepared, prior):
    """Use captured bindings to disambiguate identical lots, never merge them."""
    from app.services.direct_congress_repair import filing_state, _event_matches
    from app.services.direct_congress_reconciliation import _key
    state = filing_state(db, prepared['document_url'])
    bindings = prior['bindings']
    rows = {row['secondary_row_id']:row for row in prepared['transactions']}
    trades = {row['id']:row for row in state['transactions']}
    events = {row['id']:row for row in state['events']}
    securities = {row['id']:row for row in state['securities']}
    if (len(bindings) != len(rows) or len(trades) != len(rows)
            or {b['secondary_row_id'] for b in bindings} != set(rows)
            or {b['transaction_id'] for b in bindings} != set(trades)
            or sorted(e for b in bindings for e in b['event_ids']) != sorted(events)
            or len(state['filings']) != 1):
        return False
    for binding in bindings:
        row, trade = rows[binding['secondary_row_id']], trades[binding['transaction_id']]
        security = securities.get(trade['security_id'], {})
        expected = _key(day=row['transaction_date'], owner=row['owner_normalized'],
            action=row['transaction_type_normalized'], lower=row['amount_low'], upper=row['amount_high'],
            symbol=row['ticker_normalized'], description=row['issuer_name_raw'])
        actual = _key(day=trade['trade_date'], owner=trade['owner_type'], action=trade['transaction_type'],
            lower=trade['amount_range_min'], upper=trade['amount_range_max'],
            symbol=security.get('symbol'), description=trade['description'] or security.get('name'))
        if expected != actual or str(trade['report_date'])[:10] != row['disclosure_date']:
            return False
        if row['asset_type_normalized']=='stock':
            if (len(binding['event_ids']) != 1 or not _event_matches(events[binding['event_ids'][0]],
                    row, prior['member'], trade['id'], state['filings'][0]['id'], correct_disclosure=True)):
                return False
        elif trade['security_id'] is not None or binding['event_ids']:
            return False
    return True


def verify_original_recovery(db, document, raw):
    """Require exact original/extraction agreement before adopting backup writes.

    The original source may recover after the snapshot expires. Its own captured
    bytes establish freshness for this verification; no secondary fetch is needed.
    """
    staged = db.scalar(select(DirectFeedDocument).where(
        DirectFeedDocument.feed == FEED, DirectFeedDocument.source_key == document.source_key))
    if staged is None:
        return None
    receipt = db.scalar(select(DirectFeedPublication).where(
        DirectFeedPublication.document_id == staged.id))
    if receipt is None or receipt.status not in {'published', 'existing'}:
        return None
    from app.services.senate_backup_reconciliation import load_staged_evidence
    try:
        _, prepared, _, _ = load_staged_evidence(db, staged.id, expected_hash=receipt.source_hash)
    except ValueError:
        return {'status':'held', 'reason':'Secondary publication source receipt is missing or changed'}
    prior = json.loads(receipt.report_json)
    if (prepared['document_url'] != document.source_url
            or prior['canonical_sha256'] != _canonical_hash(db, document.source_url, include_bindings=False)):
        return {'status':'held', 'reason':'Secondary publication identity changed before original recovery'}
    try:
        proof = bind_original(prepared, raw)
    except ValueError:
        return {'status':'held', 'reason':'Recovered original disagrees with secondary publication evidence'}
    if proof['status'] != 'matched':
        return {'status':'held', 'reason':'Recovered original has ambiguous secondary row identities'}
    return None


def _project(db, document, prepared, plan, now):
    rows, member = prepared['transactions'], plan['member']
    securities = {}
    for row in rows:
        if row['asset_type_normalized'] == 'stock':
            symbol = row['ticker_normalized']
            security = db.scalar(select(Security).where(Security.symbol == symbol))
            if security and (security.asset_class or '').lower() not in {'stock','stocks','equity'}:
                return {'status':'held', 'reason':'Existing security classification conflicts'}
            securities[symbol] = security
    bindings = []
    if plan['status'] == 'existing':
        bindings = plan['matched']
    else:
        stored = db.scalar(select(Member).where(Member.bioguide_id == member['bioguide_id']))
        if stored is None:
            stored = Member(**{key:member[key] for key in
                ('bioguide_id','first_name','last_name','chamber','party','state')})
            db.add(stored); db.flush()
        day = date.fromisoformat(prepared['source_record']['filingDate'])
        filing = Filing(member_id=stored.id, source=PROVIDER, filing_date=day,
            document_url=document.source_url, document_hash=document.content_hash)
        db.add(filing); db.flush()
        for row in rows:
            security = securities.get(row['ticker_normalized'])
            if row['asset_type_normalized'] == 'stock' and security is None:
                security = Security(symbol=row['ticker_normalized'], name=row['issuer_name_raw'], asset_class='stock')
                db.add(security); db.flush()
                securities[security.symbol] = security
            trade = Transaction(filing_id=filing.id, member_id=stored.id,
                security_id=security.id if security else None, owner_type=row['owner_normalized'],
                transaction_type=row['transaction_type_normalized'],
                trade_date=date.fromisoformat(row['transaction_date']), report_date=day,
                amount_range_min=row['amount_low'], amount_range_max=row['amount_high'],
                description=row['issuer_name_raw'])
            db.add(trade); db.flush()
            event_ids = []
            if security is not None:
                event = _congress_event_from_transaction(trade, filing, stored, security)
                event.source_provider = PROVIDER
                event.source_filing_id = row['secondary_row_id']
                event.source_document_url = document.source_url
                event.parser_version = row['parser_version']
                event.provider_priority = 20
                event.ts = now
                payload = json.loads(event.payload_json)
                payload.update(source_line_ref=None, secondary_row_id=row['secondary_row_id'],
                    source_sha256=document.content_hash, source_transaction_type=row['transaction_type_raw'],
                    source_asset_type=row['asset_type_raw'], partial_sale=row['partial_sale'])
                payload['source_verification'] = dict(feed=FEED, provider=PROVIDER,
                    evidence_kind='secondary_extraction', original_verified=False,
                    claimed_original_sha256=prepared['claimed_original_sha256'],
                    secondary_row_sha256=row['evidence_sha256'],
                    snapshot_sha256=plan['latest_snapshot_sha256'],
                    snapshot_source_checked_at=plan['latest_source_checked_at'],
                    evidence_snapshot_sha256=prepared['snapshot_manifest_sha256'],
                    source_observed_at=prepared['observed_at'],
                    reported_available_at=prepared['reported_available_at'])
                # Execution availability is arrival in Walnut, not the provider's
                # synthetic end-of-day timestamp or the earlier transaction date.
                payload['source_availability'] = dict(date=now.date().isoformat(),
                    basis='direct_publication', observed_at=now.isoformat(),
                    source_discovered_at=prepared['observed_at'])
                event.payload_json = dumps(payload)
                db.add(event); db.flush()
                enqueue_feed_pnl_enrichment_for_event(db, event, source=FEED,
                    reason='event_insert', use_current_session=True)
                event_ids.append(event.id)
            bindings.append(dict(secondary_row_id=row['secondary_row_id'],
                                 transaction_id=trade.id, event_ids=event_ids))
        if any(binding['event_ids'] for binding in bindings):
            bump_feed_events_epoch(reason=FEED, db=db)
    db.flush()
    event_ids = sorted(identifier for binding in bindings for identifier in binding['event_ids'])
    return dict(status='published' if plan['status']=='new' else 'existing', bindings=bindings,
        event_ids=event_ids, transaction_ids=sorted(b['transaction_id'] for b in bindings),
        inserted_events=len(event_ids) if plan['status']=='new' else 0,
        inserted_transactions=len(rows) if plan['status']=='new' else 0,
        non_stock_rows=sum(r['asset_type_normalized']!='stock' for r in rows))


def publish_document(db, document_id, *, directory, now=None):
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError('Publication clock must be timezone aware')
    now = now.astimezone(timezone.utc)
    with writer_transaction(db, 'senate_ptr', PROVIDER) as control:
        document = db.scalar(select(DirectFeedDocument).where(DirectFeedDocument.id==document_id)
            .with_for_update().execution_options(populate_existing=True))
        if document is None or document.feed != FEED:
            raise ValueError('Not a staged Senate backup filing')
        # Staging is deliberately quarantined. Immutable evidence and the full
        # current canonical population, not a mutable parsed flag, authorize use.
        plan = inspect_staged(db, document_id, directory, now=now)
        prepared = json.loads(document.parsed_json)
        if now < datetime.fromisoformat(prepared['observed_at']):
            raise ValueError('Publication cannot precede source observation')
        day = date.fromisoformat(prepared['source_record']['filingDate'])
        if not control.publish_since <= day < now.astimezone(ZoneInfo('America/New_York')).date():
            return {'status':'held', 'reason':'Filing falls outside completed publication days'}
        receipt = db.scalar(select(DirectFeedPublication).where(DirectFeedPublication.document_id==document_id))
        if receipt and receipt.status in {'published','existing'}:
            prior = json.loads(receipt.report_json)
            if (receipt.source_hash != document.content_hash
                    or receipt.publish_since != control.publish_since.isoformat()
                    or prior['canonical_sha256'] != _canonical_hash(db, document.source_url, include_bindings=False)):
                return {'status':'held', 'reason':'Published source/canonical state changed'}
            # Complete unchanged canonical/source hashes plus persisted one-to-one
            # bindings preserve distinct identical lots across repeated imports.
            permitted_reasons = {'Multiple lots or unverified legacy instrument/member/date identity'}
            if (plan.get('member') != prior['member']
                    or set(plan.get('reasons', [])) - permitted_reasons
                    or not _receipt_bindings_match(db, prepared, prior)):
                return {'status':'held', 'reason':'Published secondary bindings changed'}
            return {**prior, 'status':'existing', 'inserted_events':0, 'inserted_transactions':0}
        result = plan if plan['status'] not in {'new','existing'} else _project(db, document, prepared, plan, now)
        report = {**result, 'email_deliveries':0, 'directory_sha256':_hash(directory), 'member':plan.get('member'),
                  'latest_snapshot_sha256':plan.get('latest_snapshot_sha256')}
        if result['status'] in {'published','existing'}:
            report['bindings'] = sorted(report['bindings'], key=lambda b:b['transaction_id'])
            report['canonical_sha256'] = _canonical_hash(db, document.source_url, include_bindings=False)
        if receipt is None:
            receipt = DirectFeedPublication(document_id=document_id); db.add(receipt)
        receipt.source_hash = document.content_hash
        receipt.metadata_hash = _hash({'filing_id':prepared['filing_id'], 'url':document.source_url})
        receipt.source_generation = control.generation
        receipt.publish_since = control.publish_since.isoformat()
        receipt.status, receipt.report_json, receipt.updated_at = result['status'], dumps(report), now
        db.flush()
        return report
