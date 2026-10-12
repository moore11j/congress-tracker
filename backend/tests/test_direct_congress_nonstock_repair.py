import base64
from copy import deepcopy

import pytest

from app.services.direct_feed_collection import parse_document
from app.services.direct_congress_nonstock_repair import plan_nonstock_repair
from test_senate_backup_evidence import SNAPSHOT,case
from test_direct_congress_worker import db
from test_senate_backup_collection import bundle


def evidence():
    filing,_=case(0)
    metadata=dict(filing_id=filing['docId'],url=filing['sourceUrl'],filing_date='2026-10-07',
        report_date='2026-10-07',member_name='John Fetterman',
        report_title='Periodic Transaction Report for 10/07/2026',amendment_flag=False)
    raw=base64.b64decode(SNAPSHOT['originals_base64'][filing['docId']])
    _,parsed,reasons=parse_document('senate_ptr',raw,metadata)
    assert not reasons and len(parsed['transactions'])==2
    member=dict(id=1,bioguide_id='F000479',chamber='senate')
    state=dict(filings=[dict(id=1,member_id=1,document_url=metadata['url'],filing_date='2026-10-06')],
        members=[member],securities=[],events=[],outcomes=[],transactions=[])
    for index,row in enumerate(parsed['transactions']):
        for day,offset in [('2026-10-06',0),('2026-10-07',2)]:
            state['transactions'].append(dict(id=index+offset+1,filing_id=1,member_id=1,security_id=None,
                trade_date=row['transaction_date'],report_date=day,owner_type=row['owner_normalized'],
                transaction_type=row['transaction_type_normalized'],amount_range_min=row['amount_low'],
                amount_range_max=row['amount_high'],description=row['issuer_name_raw']))
    return parsed,member,state


def test_actual_two_bond_original_plans_four_synthetic_copies_without_mutation():
    args=evidence();before=deepcopy(args)
    result=plan_nonstock_repair(*args)
    assert result['status']=='planned' and result['official_date']=='2026-10-07'
    assert result['preserve_transaction_ids']==[3,4]
    assert result['retire_transactions']==[dict(id=1,canonical_id=3),dict(id=2,canonical_id=4)]
    assert all(binding['event_id'] is None for binding in result['bindings'])
    assert result['database_writes']==result['inserted_events']==0
    assert plan_nonstock_repair(*args)==result and args==before


@pytest.mark.parametrize('damage',['stock','amendment','identical_original_lot','extra_economics',
    'wrong_member','security','event','outcome','multiple_filings','only_old_rows','two_survivors','third_date'])
def test_ambiguous_or_referenced_population_is_held(damage):
    parsed,member,state=evidence()
    if damage=='stock':parsed['transactions'][0]['asset_type_normalized']='stock'
    if damage=='amendment':parsed['transactions'][0]['amendment_flag']=True
    if damage=='identical_original_lot':parsed['transactions'].append(deepcopy(parsed['transactions'][0]))
    if damage=='extra_economics':state['transactions'][0]['amount_range_max']=999999
    if damage=='wrong_member':state['filings'][0]['member_id']=7
    if damage=='security':state['transactions'][0]['security_id']=9
    if damage=='event':state['events']=[dict(id=9)]
    if damage=='outcome':state['outcomes']=[dict(id=9)]
    if damage=='multiple_filings':state['filings']*=2
    if damage=='only_old_rows':state['transactions']=[r for r in state['transactions'] if r['report_date']=='2026-10-06']
    if damage=='two_survivors':state['transactions'][0]['report_date']='2026-10-07'
    if damage=='third_date':state['transactions'][0]['report_date']='2026-10-05'
    assert plan_nonstock_repair(parsed,member,state)['status']=='held'


def setup(db):
    import hashlib
    from app.models import Member,Filing,Transaction
    from test_direct_congress_repair import load_rows
    parsed,member,state=evidence()
    state['members'][0].update(first_name='John',last_name='Fetterman',state='PA',party='Democrat')
    state['filings'][0].update(source='senate_fmp',document_hash='legacy')
    for key,model in [('members',Member),('filings',Filing),('transactions',Transaction)]:
        load_rows(db,model,state[key])
    db.commit()
    raw=base64.b64decode(SNAPSHOT['originals_base64'][case(0)[0]['docId']])
    metadata=parsed['metadata'];metadata.update(key=case(0)[0]['docId'],filer_type='John Fetterman (Senator)')
    document=dict(feed='senate_ptr',raw=raw,content_hash=hashlib.sha256(raw).hexdigest(),metadata=metadata)
    people=[dict(id={'bioguide':'F000479'},name={'first':'John','last':'Fetterman'},
        terms=[dict(type='sen',state='PA',party='Democrat',start='2023-01-03',end='2029-01-03')])]
    return document,people


def test_archived_repair_preserves_correct_rows_and_repeats(db):
    import json
    from sqlalchemy import select
    from app.models import Transaction
    from app.services.direct_congress_repair import filing_state,CongressRepairArchive,CongressRowBinding
    from app.services.direct_congress_nonstock_repair import inspect_nonstock_repair,rehearse_nonstock_repair
    document,people=setup(db)
    before=filing_state(db,document['metadata']['url'])
    plan=inspect_nonstock_repair(db,document,people)
    assert plan['status']=='planned' and filing_state(db,document['metadata']['url'])==before
    result=rehearse_nonstock_repair(db,document,people,expected_before_hash=plan['before_hash']);db.commit()
    assert result['withdrawn_transactions']==result['preserved_transactions']==2
    assert result['inserted_events']==0
    after=filing_state(db,document['metadata']['url'])
    assert [row['id'] for row in after['transactions']]==[3,4]
    assert str(after['filings'][0]['filing_date'])=='2026-10-07'
    assert after['transactions']==[row for row in before['transactions'] if row['id'] in (3,4)]
    archived=list(db.scalars(select(CongressRepairArchive)))
    assert len(archived)==3
    old_filing=next(row for row in archived if row.entity_type=='nonstock_filing_before_date')
    assert json.loads(old_filing.record_json)['filing_date']=='2026-10-06'
    assert len(list(db.scalars(select(CongressRowBinding))))==2
    assert rehearse_nonstock_repair(db,document,people,expected_before_hash=plan['before_hash'])['status']=='existing'
    assert filing_state(db,document['metadata']['url'])==after


@pytest.mark.parametrize('damage',['source','before_hash','archive_failure'])
def test_repair_rejects_stale_review_or_rolls_back_all_changes(db,monkeypatch,damage):
    from sqlalchemy import event as sql_event
    from app.services.direct_congress_repair import filing_state,CongressRepairReceipt
    from app.services.direct_congress_nonstock_repair import inspect_nonstock_repair,rehearse_nonstock_repair
    document,people=setup(db)
    before=filing_state(db,document['metadata']['url']);plan=inspect_nonstock_repair(db,document,people)
    expected=plan['before_hash']
    if damage=='source':document['raw']+=b'!'
    if damage=='before_hash':expected='0'*64
    if damage=='archive_failure':
        def fail(session,*args):
            if any(isinstance(row,CongressRepairReceipt) for row in session.new):raise ValueError('injected archive interruption')
        sql_event.listen(db,'before_flush',fail)
    with pytest.raises(ValueError):rehearse_nonstock_repair(db,document,people,expected_before_hash=expected)
    assert filing_state(db,document['metadata']['url'])==before


def test_repair_proof_detects_later_archive_change(db):
    from sqlalchemy import select
    from app.services.direct_congress_repair import CongressRepairArchive
    from app.services.direct_congress_nonstock_repair import inspect_nonstock_repair,rehearse_nonstock_repair
    document,people=setup(db);plan=inspect_nonstock_repair(db,document,people)
    rehearse_nonstock_repair(db,document,people,expected_before_hash=plan['before_hash']);db.commit()
    db.scalar(select(CongressRepairArchive)).record_json='{}';db.commit()
    with pytest.raises(ValueError,match='proof'):inspect_nonstock_repair(db,document,people)


def test_repaired_actual_bond_original_is_adopted_by_both_publishers(db,bundle):
    from datetime import date
    from sqlalchemy import select,func
    from app.models import Event,EmailDelivery
    from app.services.direct_feed_store import discover,record_document,DirectFeedDocument
    from app.services.direct_congress_repair import filing_state
    from app.services.direct_congress_nonstock_repair import inspect_nonstock_repair,rehearse_nonstock_repair
    from app.services import direct_congress_worker as official,senate_backup_publication as secondary
    from app.services.senate_backup_collection import stage_bundle,FEED
    from test_senate_backup_publication import switch
    from test_senate_backup_evidence import NOW
    document,people=setup(db);plan=inspect_nonstock_repair(db,document,people)
    rehearse_nonstock_repair(db,document,people,expected_before_hash=plan['before_hash']);db.commit()
    before=filing_state(db,document['metadata']['url'])
    doc=discover(db,'senate_ptr',document['metadata'])
    source,parsed,reasons=parse_document('senate_ptr',document['raw'],document['metadata'])
    assert not reasons
    record_document(db,doc,document['raw'],source,parsed);db.commit()
    switch(db,'official_senate',boundary=date(2026,10,7))
    result=official.publish_document(db,doc.id,directory=people)
    assert result['status']=='existing' and result['transaction_ids']==[3,4]
    stage_bundle(db,bundle,now=NOW);db.commit()
    secondary_id=db.scalar(select(DirectFeedDocument.id).where(
        DirectFeedDocument.feed==FEED,DirectFeedDocument.source_key==case(0)[0]['docId']))
    switch(db,generation=1,boundary=date(2026,10,7))
    result=secondary.publish_document(db,secondary_id,directory=people,now=NOW)
    assert result['status']=='existing' and result['transaction_ids']==[3,4]
    assert filing_state(db,document['metadata']['url'])==before
    assert db.scalar(select(func.count()).select_from(Event))==0
    assert db.scalar(select(func.count()).select_from(EmailDelivery))==0

@pytest.mark.parametrize('as_string',[False,True])
def test_legacy_payload_reference_without_member_or_document_holds_repair(db,as_string):
    import json
    from app.models import Event
    from app.services.direct_congress_nonstock_repair import inspect_nonstock_repair
    document,people=setup(db)
    db.add(Event(source='senate',event_type='purchase',ts=__import__('datetime').datetime(2026,10,7),payload_json=json.dumps({'transaction_id':'1' if as_string else 1})))
    db.commit()
    assert inspect_nonstock_repair(db,document,people)['status']=='held'


def test_disabled_repair_cli_never_reads_directory_or_database(monkeypatch,capsys):
    import json
    from app.jobs import repair_direct_congress as job
    monkeypatch.delenv('DIRECT_CONGRESS_REPAIR_ENABLED',raising=False)
    job.main(['--kind','nonstock','--apply','--document-id','1','--source-sha256','0'*64,
        '--directory','absent.json','--directory-sha256','0'*64])
    assert json.loads(capsys.readouterr().out)=={'status':'disabled'}
