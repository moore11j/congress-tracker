from test_direct_13f_publication import db, SINCE
from test_institutional_source_reconciliation import seed, planned, peers
from datetime import datetime, timezone, timedelta
import copy
import json
import pytest
from sqlalchemy import select
from app.models import InstitutionalPosition, InstitutionalFiling, Event
from app.services.direct_feed_rehearsal import digest
from app.services.institutional_repair_bundle import (MODELS, canonical, build_bundle,
    verify_sources, rehearse_bundle, InstitutionalRepairReceipt)
from app.services import institutional_activity as activity


def state(db):
    return {name:[canonical(model,row) for row in db.scalars(select(model).order_by(model.id))] for name,model in MODELS.items()}


def package(db):
    doc,filing=seed(db)
    plan=planned(db,doc,filing)
    before=state(db);after=copy.deepcopy(before)
    after['institutional_positions'][0]['value_usd']=100_000_000
    bundle=build_bundle(before,after,[plan],scopes=[('AAA',2026,2)],publish_since=SINCE)
    verify_sources(bundle,[doc],comparison_documents=peers())
    db.rollback()
    return doc,bundle,before


def test_atomic_apply_repeat_and_rollback_with_source_overwrite_protection(db):
    doc,bundle,before=package(db)
    first=rehearse_bundle(db,bundle,expected_plan_hash=bundle['sha256'])
    assert first['updated']==2
    after=state(db);db.rollback()
    repeat=rehearse_bundle(db,bundle,expected_plan_hash=bundle['sha256'])
    assert repeat['updated']==0
    assert state(db)==after;db.rollback()
    filing=db.get(InstitutionalFiling,before['institutional_filings'][0]['id'])
    metadata=json.loads(filing.raw_metadata_json)
    assert metadata['_walnut_position_source']=='sec_edgar_reconciled'
    assert metadata['_walnut_position_repair']['source_sha256']==doc['content_hash']
    payload={'symbol':'AAA','cusip':'000361105','shares':10_000_000,'value':100_000}
    with pytest.raises(ValueError,match='complete reconciled SEC quarter'):
        activity.upsert_positions_for_filing(db,filing=filing,rows=[payload])
    db.rollback()
    assert state(db)==after;db.rollback()
    undone=rehearse_bundle(db,bundle,expected_plan_hash=bundle['sha256'],rollback=True)
    assert undone['updated']==2 and state(db)==before;db.rollback()
    again=rehearse_bundle(db,bundle,expected_plan_hash=bundle['sha256'],rollback=True)
    assert again['updated']==0
    assert db.scalar(select(InstitutionalRepairReceipt.status))=='rolled_back'


@pytest.mark.parametrize('drift',['amount','extra_position','missing_position','parent','rollback_drift'])
def test_whole_population_drift_aborts_without_partial_corrections(db,drift):
    _,bundle,before=package(db)
    if drift=='rollback_drift':
        rehearse_bundle(db,bundle,expected_plan_hash=bundle['sha256'])
    position=db.scalar(select(InstitutionalPosition))
    if drift in {'amount','rollback_drift'}:position.value_usd+=1
    elif drift=='extra_position':
        db.add(InstitutionalPosition(filing_id=position.filing_id,cik=position.cik,report_year=2026,report_quarter=2,filing_date=position.filing_date,normalized_symbol='AAA',cusip='999999999',value_usd=1))
    elif drift=='missing_position':db.delete(position)
    else:db.get(InstitutionalFiling,position.filing_id).form_type='13F-HR/A'
    db.commit();changed=state(db);db.rollback()
    with pytest.raises(ValueError,match='population changed'):
        rehearse_bundle(db,bundle,expected_plan_hash=bundle['sha256'],rollback=drift=='rollback_drift')
    assert state(db)==changed


def test_tampered_bundle_cannot_use_previous_review_hash(db):
    _,bundle,before=package(db)
    bundle['operations'][0]['changes']['raw_metadata_json']='{}'
    with pytest.raises(ValueError,match='bundle changed'):
        rehearse_bundle(db,bundle,expected_plan_hash=bundle['sha256'])
    assert state(db)==before


def test_source_bytes_reverified_independently(db):
    doc,bundle,before=package(db)
    doc['raw']+=b' altered'
    with pytest.raises(ValueError):verify_sources(bundle,[doc],comparison_documents=peers())
    assert state(db)==before


@pytest.mark.parametrize('mutation',['insert','date','mapping','unsupported_amount','event_timing','event_payload_date'])
def test_bundle_builder_rejects_unsupported_runtime_output(db,mutation):
    doc,filing=seed(db);plan=planned(db,doc,filing)
    if mutation.startswith('event'):
        db.add(Event(event_type='institutional_new_position',symbol='AAA',source='13F filing',ts=datetime(2026,8,1),payload_json='{}',source_filing_id='institutional:1:test:2026q2'));db.commit()
    before=state(db);after=copy.deepcopy(before)
    after['institutional_positions'][0]['value_usd']=100_000_000
    if mutation=='insert':after['institutional_positions'].append({**after['institutional_positions'][0],'id':999})
    elif mutation=='date':after['institutional_positions'][0]['filing_date']='2026-11-01'
    elif mutation=='mapping':after['institutional_positions'][0]['normalized_symbol']='OTHER'
    elif mutation=='unsupported_amount':after['institutional_positions'][0]['value_usd']+=1
    elif mutation=='event_timing':after['events'][0]['ts']='2026-11-01T00:00:00+00:00'
    else:after['events'][0]['payload_json']='{"filing_date":"2026-11-01"}'
    with pytest.raises(ValueError):build_bundle(before,after,[plan],scopes=[('AAA',2026,2)],publish_since=SINCE)


def test_utc_canonicalization_keeps_meaning_and_payload_bytes():
    row={'id':1,'ts':'2026-08-01T09:00:00-06:00','payload_json':'{"filing_date": "2026-08-01"}'}
    result=canonical(Event,row)
    assert result['ts']=='2026-08-01T15:00:00+00:00' and result['payload_json']==row['payload_json']


def test_failed_postcondition_rolls_back_every_update_and_receipt(db,monkeypatch):
    from app.services import institutional_repair_bundle as repair
    _,bundle,before=package(db)
    original=repair._check_population
    def fail_after_write(session,plan,phase):
        if phase=='after':raise ValueError('Injected failed postcondition')
        return original(session,plan,phase)
    monkeypatch.setattr(repair,'_check_population',fail_after_write)
    with pytest.raises(ValueError,match='postcondition'):
        rehearse_bundle(db,bundle,expected_plan_hash=bundle['sha256'])
    assert state(db)==before
    assert db.scalar(select(InstitutionalRepairReceipt)) is None


def staged(db,doc):
    from app.services.direct_feed_store import DirectFeedDocument,DirectFeedRevision
    row=DirectFeedDocument(feed='sec_13f',source_key=doc['metadata']['key'],source_url=doc['metadata']['url'],
        metadata_json=json.dumps(doc['metadata']),content_hash=doc['content_hash'],status='parsed')
    db.add(row);db.flush()
    revision=DirectFeedRevision(document_id=row.id,content_hash=doc['content_hash'],source_text=doc['raw'].decode(),source_bytes=doc['raw'],parsed_json='{}')
    db.add(revision);db.commit()
    return row,revision


def test_staged_original_application_and_readonly_receipt_inspection(db):
    from app.services.institutional_repair_bundle import _transaction,inspect_staged_bundle
    doc,bundle,before=package(db);staged(db,doc)
    assert inspect_staged_bundle(db,bundle,expected_plan_hash=bundle['sha256'])['status']=='ready'
    assert state(db)==before;db.rollback()
    applied=_transaction(db,bundle,expected_plan_hash=bundle['sha256'],rollback=False,staged=True,expected_generation=0)
    assert applied['updated']==2
    assert inspect_staged_bundle(db,bundle,expected_plan_hash=bundle['sha256'])['status']=='existing';db.rollback()
    _transaction(db,bundle,expected_plan_hash=bundle['sha256'],rollback=True,staged=True,expected_generation=0)
    assert state(db)==before


@pytest.mark.parametrize('drift',['bytes','metadata','status','published','generation'])
def test_staged_source_or_generation_drift_cannot_change_canonical_rows(db,drift):
    from app.services.institutional_repair_bundle import _transaction
    from app.services.direct_feed_worker import DirectFeedPublication
    doc,bundle,before=package(db);row,revision=staged(db,doc)
    if drift=='bytes':revision.source_bytes=b'changed'
    elif drift=='metadata':row.metadata_json=json.dumps({**doc['metadata'],'name':'Changed'})
    elif drift=='status':row.status='quarantined'
    elif drift=='published':db.add(DirectFeedPublication(document_id=row.id,source_hash=doc['content_hash'],metadata_hash='x',source_generation=0,publish_since='2026-10-01',status='existing',report_json='{}',updated_at=datetime.now(timezone.utc)))
    db.commit()
    with pytest.raises(ValueError):
        _transaction(db,bundle,expected_plan_hash=bundle['sha256'],rollback=False,staged=True,expected_generation=1 if drift=='generation' else 0)
    assert state(db)==before and db.scalar(select(InstitutionalRepairReceipt)) is None


def test_wall_time_limit_rolls_back_complete_bundle(db,monkeypatch):
    from app.services import institutional_repair_bundle as repair
    _,bundle,before=package(db)
    ticks=iter([1,47]);monkeypatch.setattr(repair.time,'monotonic',lambda:next(ticks))
    with pytest.raises(ValueError,match='bounded transaction'):
        rehearse_bundle(db,bundle,expected_plan_hash=bundle['sha256'])
    assert state(db)==before and db.scalar(select(InstitutionalRepairReceipt)) is None
