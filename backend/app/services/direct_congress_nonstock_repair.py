"""Original-bound planning and archived repair of non-equity Congress duplicates.

Production application requires a reviewed hash and paused source generation.
Rows with public events or securities require a consumer-aware repair instead.
"""
from collections import defaultdict

from app.services.direct_congress_reconciliation import document_identity, _key
from app.services.direct_congress_repair import _digest


def plan_nonstock_repair(parsed, member, state):
    held=lambda reason:dict(status='held',reason=reason,database_writes=0)
    rows,metadata=parsed['transactions'],parsed['metadata']
    if (not rows or any(row['asset_type_normalized'] not in {'corporate_bond','municipal_bond','treasury_bill'}
            or row['ticker_normalized'] or row['amendment_flag']
            or row['owner_normalized'] not in {'self','spouse','joint','dependent'}
            or row['transaction_type_normalized'] not in {'purchase','sale'} for row in rows)):
        return held('Requires complete identified non-equity debt rows without amendments')
    if state['events'] or state['outcomes'] or state['securities']:
        return held('Existing public events, outcomes or securities require consumer-aware repair')
    if len(state['filings'])!=1:
        return held('Requires exactly one canonical filing')
    filing=state['filings'][0]
    members={item['id']:item for item in state['members']}
    if (document_identity(filing['document_url'])!=document_identity(metadata['url'])
            or members.get(filing['member_id'],{}).get('bioguide_id')!=member['bioguide_id']
            or members[filing['member_id']].get('chamber')!=member['chamber']):
        return held('Canonical filing identity disagrees with the original')
    filed=metadata['filing_date']
    old_date=str(filing['filing_date'])[:10]
    source={}
    for row in rows:
        key=_key(day=row['transaction_date'],owner=row['owner_normalized'],
            action=row['transaction_type_normalized'],lower=row['amount_low'],upper=row['amount_high'],
            symbol=None,description=row['issuer_name_raw'] or row['security_name_raw'])
        if key in source:
            return held('Separate identical original lots require explicit row reconciliation')
        source[key]=row
    groups=defaultdict(list)
    for row in state['transactions']:
        if (row['filing_id']!=filing['id'] or row['member_id']!=filing['member_id']
                or row['security_id'] is not None):
            return held('Canonical transaction identity or instrument differs')
        key=_key(day=row['trade_date'],owner=row['owner_type'],action=row['transaction_type'],
            lower=row['amount_range_min'],upper=row['amount_range_max'],symbol=None,description=row['description'])
        groups[key].append(row)
    if set(source)!=set(groups):
        return held('Complete canonical and original economics differ')
    keep,retire,bindings=[],[],[]
    for key,row in source.items():
        group=groups[key]
        survivors=[item for item in group if str(item['report_date'])[:10]==filed]
        if (len(survivors)!=1 or len(group)<2 or old_date==filed
                or any(str(item['report_date'])[:10]!=old_date for item in group if item not in survivors)):
            return held('Requires one correctly dated survivor and only old-date duplicate copies per original row')
        survivor=survivors[0]
        keep.append(survivor['id'])
        retire.extend(dict(id=item['id'],canonical_id=survivor['id']) for item in group if item['id']!=survivor['id'])
        bindings.append(dict(source_line_ref=row['source_line_ref'],normalized_hash=row['normalized_hash'],
                             transaction_id=survivor['id'],event_id=None))
    return dict(status='planned',repair_kind='nonstock_duplicate_and_filing_date',
        before_hash=_digest(state),filing_id=filing['id'],previous_date=old_date,official_date=filed,
        preserve_transaction_ids=sorted(keep),retire_transactions=sorted(retire,key=lambda r:r['id']),
        bindings=sorted(bindings,key=lambda r:r['source_line_ref']),
        archive_filing_before_change=True,inserted_events=0,database_writes=0,
        remaining_gate='archival_application_reference_checks_and_reviewed_generation')


def _repair(db, document, directory, *, expected_before_hash=None, inspect_only=False):
    from datetime import date
    import hashlib
    import json
    from sqlalchemy import select, func
    from app.db import Base
    from app.models import Filing, Transaction, Event
    from app.services.direct_feed_collection import parse_document
    from app.services.direct_feed_store import dumps
    from app.services.direct_congress_reconciliation import resolve_direct_member, reconcile_direct_congress
    from app.services.direct_congress_repair import (
        filing_state, CongressRepairArchive, CongressRowBinding, CongressRepairReceipt, _record, _evidence_hash)
    if db.new or db.dirty or db.deleted:
        raise ValueError('Repair session has unrelated pending changes')
    if (document['feed'] not in {'senate_ptr','house_ptr'}
            or hashlib.sha256(document['raw']).hexdigest()!=document['content_hash']):
        raise ValueError('Official repair source identity or checksum differs')
    _,parsed,reasons=parse_document(document['feed'],document['raw'],document['metadata'])
    if reasons:return dict(status='held',reason='Official parser holds the complete filing')
    identity=resolve_direct_member(document['metadata'],document['feed'].split('_')[0],directory)
    if identity['status']!='resolved':return identity
    url=document['metadata']['url']
    state=filing_state(db,url)
    receipt=db.get(CongressRepairReceipt,url)
    if receipt is not None:
        prior=json.loads(receipt.report_json)
        if (receipt.source_hash!=document['content_hash'] or receipt.after_hash!=_digest(state)
                or prior.get('repair_kind')!='nonstock_duplicate_and_filing_date'
                or prior.get('evidence_hash')!=_evidence_hash(db,url)):
            raise ValueError('Existing repair proof or canonical state differs')
        return dict(status='existing',inserted_events=0,withdrawn_transactions=0,emails=0)
    plan=plan_nonstock_repair(parsed,identity['member'],state)
    if plan['status']!='planned':return plan
    if db.scalar(select(func.count()).select_from(CongressRowBinding).where(CongressRowBinding.source_url==url)):
        return dict(status='held',reason='Existing official row bindings require explicit reconciliation')
    retired={row['id'] for row in plan['retire_transactions']}
    # Legacy payloads can encode IDs as strings. Refuse those references too,
    # even when the event has no member or document link.
    from sqlalchemy import String
    if db.get_bind().dialect.name == 'postgresql':
        from sqlalchemy.dialects.postgresql import JSONB
        reference = Event.payload_json.cast(JSONB)['transaction_id'].astext
    else:
        reference = func.json_extract(Event.payload_json, '$.transaction_id').cast(String)
    if db.scalar(select(func.count()).select_from(Event).where(reference.in_([str(i) for i in retired]))):
        return dict(status='held',reason='Retired transaction has a public event reference')
    # Refuse every schema-declared transaction reference, in addition to the
    # complete event/payload reference scan already performed by filing_state.
    for table in Base.metadata.sorted_tables:
        for column in table.columns:
            if any(key.target_fullname=='transactions.id' for key in column.foreign_keys):
                if db.scalar(select(func.count()).select_from(table).where(column.in_(retired))):
                    return dict(status='held',reason='Retired transaction has a stored dependent reference')
    if inspect_only:return {**plan,'source_hash':document['content_hash'],'emails':0}
    if not expected_before_hash or expected_before_hash!=plan['before_hash']:
        raise ValueError('Canonical population changed since reviewed repair')
    with db.begin_nested():
        filing=db.get(Filing,plan['filing_id'])
        db.add(CongressRepairArchive(entity_type='nonstock_filing_before_date',original_id=filing.id,
            canonical_id=filing.id,source_hash=document['content_hash'],source_url=url,record_json=dumps(_record(filing))))
        filing.filing_date=date.fromisoformat(plan['official_date'])
        for item in plan['retire_transactions']:
            row=db.get(Transaction,item['id'])
            db.add(CongressRepairArchive(entity_type='transaction',original_id=row.id,
                canonical_id=item['canonical_id'],source_hash=document['content_hash'],source_url=url,
                record_json=dumps(_record(row))))
            db.delete(row)
        for binding in plan['bindings']:
            db.add(CongressRowBinding(source_url=url,source_hash=document['content_hash'],**binding))
        db.flush()
        after=filing_state(db,url)
        check=reconcile_direct_congress(parsed,identity['member'],
            **{key:after[key] for key in ('filings','transactions','securities','members','events')})
        if check['status']!='existing' or after['events'] or after['outcomes']:
            raise ValueError('Non-equity repair failed exact complete-source reconciliation')
        db.add(CongressRepairReceipt(source_url=url,source_hash=document['content_hash'],
            before_hash=expected_before_hash,after_hash=_digest(after),
            report_json=dumps({**plan,'evidence_hash':_evidence_hash(db,url)})))
        db.flush()
    return dict(status='rehearsed',preserved_transactions=len(plan['preserve_transaction_ids']),
        withdrawn_transactions=len(retired),updated_filings=1,inserted_events=0,emails=0)


def inspect_nonstock_repair(db,document,directory):
    return _repair(db,document,directory,inspect_only=True)


def rehearse_nonstock_repair(db,document,directory,*,expected_before_hash):
    bind=db.get_bind()
    if bind.dialect.name!='sqlite' or bind.url.database!=':memory:':
        raise ValueError('Non-equity rehearsal requires in-memory SQLite')
    from app.services.feed_source_control import lock_feed
    lock_feed(db,document['feed'])
    return _repair(db,document,directory,expected_before_hash=expected_before_hash)


def apply_reviewed_nonstock_repair(db,document,directory,*,expected_before_hash,expected_generation):
    import json
    from sqlalchemy import text
    from app.services.feed_source_control import FeedSourceControl,lock_feed
    from app.services.direct_feed_store import DirectFeedDocument,dumps
    from app.services.direct_congress_repair import _lock_repair_tables
    from app.services.direct_feed_collection import parse_document
    from app.services.feed_cache_epoch import bump_feed_events_epoch
    if db.get_bind().dialect.name!='postgresql' or db.in_transaction() or db.new or db.dirty or db.deleted:
        raise ValueError('Production repair requires a fresh PostgreSQL session')
    if not expected_before_hash or document['feed'] not in {'house_ptr','senate_ptr'}:
        raise ValueError('Reviewed official source and population hash required')
    with db.begin():
        db.execute(text("SET LOCAL lock_timeout='100ms'"))
        db.execute(text("SET LOCAL statement_timeout='15s'"))
        lock_feed(db,document['feed'])
        control=db.get(FeedSourceControl,document['feed'],populate_existing=True)
        if control is None or control.provider!='paused' or control.generation!=expected_generation:
            raise ValueError('Repair requires the reviewed paused source generation')
        _lock_repair_tables(db)
        staged=db.get(DirectFeedDocument,document['document_id'],populate_existing=True)
        if (staged is None or staged.feed!=document['feed'] or staged.status not in {'parsed','quarantined'}
                or staged.content_hash!=document['content_hash']
                or dumps(json.loads(staged.metadata_json))!=dumps(document['metadata'])):
            raise ValueError('Staged source changed since review')
        result=_repair(db,document,directory,expected_before_hash=expected_before_hash)
        if result['status']=='rehearsed':
            _,parsed,reasons=parse_document(document['feed'],document['raw'],document['metadata'])
            if reasons:raise ValueError('Official source changed during repair')
            staged.status,staged.error,staged.parsed_json='parsed',None,dumps(parsed)
            bump_feed_events_epoch(reason='official_congress_nonstock_repair',db=db)
            result={**result,'status':'applied'}
    return result
