from copy import deepcopy
import json
from pathlib import Path

import pytest

from app.services.senate_backup_reconciliation import reconcile, resolve_member
from test_senate_backup_evidence import case, prepare
from test_senate_backup_collection import bundle


def directory():
    return [dict(id={'bioguide':'W000802'},name={'first':'Sheldon','last':'Whitehouse'},
        terms=[dict(type='sen',state='RI',party='Democrat',start='2025-01-03',end='2031-01-03')])]


def evidence():
    return prepare(*case())


def canonical():
    source=json.loads((Path(__file__).with_name('fixtures')/'congress_whitehouse_reconciliation.json').read_text())
    return {key:source[key] for key in ('filings','transactions','securities','members','events')}


def clean():
    source=canonical()
    source['transactions']=[r for r in source['transactions'] if 23181<=r['id']<=23185]
    source['events']=[r for r in source['events'] if 461243<=r['id']<=461247]
    return source


def plan(source=None,prepared=None):
    source=clean() if source is None else source
    return reconcile(prepared or evidence(),directory(),**source,
        member_trades=source['transactions'],member_securities={s['id']:s for s in source['securities']})


def test_independent_name_term_and_claim_agree():
    result=resolve_member(evidence(),directory())
    assert result['status']=='resolved' and result['member']['bioguide_id']=='W000802'
    assert result['identity_basis']=='independent_name_and_active_senate_term'


@pytest.mark.parametrize('damage',['id','name','former','house','ambiguous','duplicate_term'])
def test_claimed_id_cannot_override_independent_identity(damage):
    source=evidence();people=directory()
    if damage=='id':source['source_record']['memberId']='F000479'
    if damage=='name':source['source_record']['filerFirst']='Someone'
    if damage=='former':people[0]['terms'][0]['end']='2026-10-01'
    if damage=='house':people[0]['terms'][0]['type']='rep'
    if damage=='ambiguous':people=people*2
    if damage=='duplicate_term':people[0]['terms']*=2
    assert resolve_member(source,people)['status']=='held'


def test_actual_secondary_order_adopts_same_canonical_transactions_and_events():
    source=clean();prepared=evidence();before=deepcopy((source,prepared))
    result=plan(source,prepared)
    assert result['status']=='existing' and not result['publishable']
    assert {m['transaction_id'] for m in result['matched']}==set(range(23181,23186))
    assert {e for m in result['matched'] for e in m['event_ids']}==set(range(461243,461248))
    assert all('source_line_ref' not in m for m in result['matched'])
    assert (source,prepared)==before and plan(source,prepared)==result
    prepared['transactions'].reverse()
    assert plan(source,prepared)==result


def test_original_duplicate_population_is_held_without_adding_trades():
    result=plan(canonical())
    assert result['status']=='held' and len(result['ambiguous_secondary_rows'])==5
    assert not result['new_secondary_rows']


@pytest.mark.parametrize('damage',['amount','date','member','type','symbol','missing_event','extra_event','extra_filing'])
def test_incompatible_canonical_records_cannot_be_adopted(damage):
    source=clean()
    if damage=='amount':source['events'][0]['amount_min']=1
    if damage=='date':source['events'][0]['event_date']='2026-09-30'
    if damage=='member':source['events'][0]['member_bioguide_id']='F000479'
    if damage=='type':source['securities'][0]['asset_class']='option'
    if damage=='symbol':source['events'][0]['symbol']='NVDA'
    if damage=='missing_event':source['events'].pop()
    if damage=='extra_event':source['events'].append({**source['events'][0],'id':999999})
    if damage=='extra_filing':source['filings'].append({**source['filings'][0],'id':999999})
    assert plan(source)['status']=='held'


def test_unlinked_legacy_trade_economics_prevent_new_duplicate():
    source=clean();trades=source['transactions'];source['filings']=[];source['transactions']=[];source['events']=[]
    result=reconcile(evidence(),directory(),**source,member_trades=trades,
        member_securities={s['id']:s for s in source['securities']})
    assert result['status']=='held' and any('overlap' in reason for reason in result['reasons'])


def test_new_population_is_only_a_read_only_plan():
    source=clean();source['filings']=[];source['transactions']=[];source['events']=[]
    result=plan(source)
    assert result['status']=='new' and len(result['new_secondary_rows'])==5
    assert not result['publishable']


@pytest.mark.parametrize('hold',['amendment_reconciliation_required','paper_original_review_required'])
def test_source_holds_survive_successful_canonical_matching(hold):
    prepared=evidence();prepared['publication_holds'].append(hold)
    result=plan(prepared=prepared)
    assert result['status']=='held' and hold in result['reasons']


def test_unverified_ticker_is_not_cleared_by_matching_legacy_record():
    prepared=evidence();prepared['transactions'][0]['publication_holds'].append('secondary_resolver_requires_independent_evidence')
    result=plan(prepared=prepared)
    assert result['status']=='held' and 'secondary_resolver_requires_independent_evidence' in result['reasons']


@pytest.mark.parametrize('first,last,identifier,nickname',[
    ('James','Banks','B001299','Jim'),('William F','Hagerty, IV','H000601','Bill'),
    ('M. Michael','Rounds','R000605','Mike'),('Timothy P','Sheehy','S001232','Tim'),
    ('Thomas H','Tuberville','T000278','Tommy')])
def test_reviewed_exact_names_still_require_independent_id_and_term(first,last,identifier,nickname):
    source=evidence();source['source_record'].update(filerFirst=first,filerLast=last,memberId=identifier)
    people=directory();people[0]['id']['bioguide']=identifier
    people[0]['name']={'first':nickname,'last':last.split(',')[0]}
    assert resolve_member(source,people)['status']=='resolved'
    assert resolve_member(source,people)['reviewed_name_sources']
    people[0]['terms'][0]['end']='2025-01-03'
    assert resolve_member(source,people)['status']=='held'


def test_known_nickname_does_not_authorize_unreviewed_spelling():
    source=evidence();source['source_record'].update(filerFirst='Timothy Q',filerLast='Sheehy',memberId='S001232')
    people=directory();people[0]['id']['bioguide']='S001232';people[0]['name']={'first':'Tim','last':'Sheehy'}
    assert resolve_member(source,people)['status']=='held'


@pytest.mark.parametrize('damage',[None,'raw','parsed','manifest','stale','retired'])
def test_staged_inspector_revalidates_sources_and_does_not_write(bundle,damage):
    from datetime import timedelta
    from sqlalchemy import create_engine,select,func
    from sqlalchemy.orm import Session
    from app.db import Base
    from app.models import Event,EmailDelivery
    from app.services.direct_feed_store import DirectFeedDocument,DirectFeedRevision
    from app.services.senate_backup_collection import stage_bundle,FEED
    from app.services.senate_backup_reconciliation import inspect_staged
    from test_senate_backup_evidence import NOW
    engine=create_engine('sqlite:///:memory:');Base.metadata.create_all(engine)
    with Session(engine) as db:
        stage_bundle(db,bundle,now=NOW);db.commit()
        doc=db.scalar(select(DirectFeedDocument).where(DirectFeedDocument.feed==FEED,
            DirectFeedDocument.source_key==evidence()['filing_id']))
        rev=db.scalar(select(DirectFeedRevision).where(DirectFeedRevision.document_id==doc.id))
        if damage=='raw':rev.source_bytes+=b'!'
        if damage=='parsed':doc.parsed_json='{}'
        if damage=='manifest':doc.metadata_json=json.dumps({'snapshot_sha256':'0'*64})
        if damage=='retired':
            db.add(DirectFeedDocument(feed=FEED+'_snapshot',source_key='latest-other-scope',
                source_url='https://huggingface.co/',checked_at=NOW+timedelta(days=10)))
        db.commit()
        identifier=doc.id
        before=db.scalar(select(func.count()).select_from(DirectFeedRevision))
        if damage:
            with pytest.raises(ValueError):
                inspect_staged(db,identifier,directory(),now=NOW+timedelta(days=2) if damage=='stale' else NOW)
        else:
            result=inspect_staged(db,identifier,directory(),now=NOW)
            assert result['status']=='new' and not result['publishable']
            assert result==inspect_staged(db,identifier,directory(),now=NOW)
        assert not db.new and not db.dirty and not db.deleted
        assert db.scalar(select(func.count()).select_from(DirectFeedRevision))==before
        assert db.scalar(select(func.count()).select_from(Event))==0
        assert db.scalar(select(func.count()).select_from(EmailDelivery))==0
    engine.dispose()
