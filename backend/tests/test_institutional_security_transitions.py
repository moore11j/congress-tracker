from copy import deepcopy
from datetime import date, datetime, timezone
import json
import pytest
from sqlalchemy import select, func
from app.models import InstitutionalPosition, InstitutionalHolder, InstitutionalFiling, InstitutionalPositionChange, Event, EmailDelivery
from app.services import institutional_security_transitions as continuity
from app.services.institutional_activity import _derived_activity_for_holder_from_positions, process_filing_changes_and_events_symbol_batch
from test_direct_13f_worker import db
from test_direct_13f_reference_mapping import run, snapshot
from test_direct_13f_publication import document, CIK


def pair(db, symbol, old, new, *, shares=10_000_000, value=100_000_000, prior_value=100_000_000):
    for cusip in {old,new}:
        db.add(InstitutionalPosition(filing_id=999,cik='0000000999',cusip=cusip,normalized_symbol=symbol,symbol=symbol,
            shares=1,value_usd=1,report_year=2025,report_quarter=4,filing_date=date(2026,2,1)))
    db.commit()
    prior=document(quarter=2,serial=2,rows=[(old,10_000_000,prior_value,'')])
    current=document(rows=[(new,shares,value,'')])
    run(db,prior)
    db.get(InstitutionalHolder,CIK).quality_score=100
    db.commit()
    return current


@pytest.mark.parametrize('symbol,old,new',[('XOM','30231G102','30233Q108'),('OKE','682680103','30609A109')])
def test_one_for_one_preserves_original_holdings_no_false_buy_exit_and_identical_repeat(db,symbol,old,new):
    current=pair(db,symbol,old,new)
    result=run(db,current)
    assert result['derived_state']=='published' and result['feed_events']==0
    rows=list(db.scalars(select(InstitutionalPosition).where(InstitutionalPosition.cik==CIK)))
    assert {(p.cusip,p.shares,p.value_usd) for p in rows}=={(old,10_000_000,100_000_000),(new,10_000_000,100_000_000)}
    changes=list(db.scalars(select(InstitutionalPositionChange)))
    assert all(c.change_type not in {'new_position','exit'} for c in changes)
    assert not _derived_activity_for_holder_from_positions(db,CIK,page=0,limit=50)['items']
    filing=db.scalar(select(InstitutionalFiling).where(InstitutionalFiling.report_quarter==3))
    proof=json.loads(filing.raw_metadata_json)['_walnut_direct_13f']
    assert proof['security_transition_evidence'][0]['symbol']==symbol
    db.commit();saved=snapshot(db)
    assert run(db,current)['feed_events']==0 and snapshot(db)==saved
    assert db.scalar(select(func.count()).select_from(EmailDelivery))==0


@pytest.mark.parametrize('shares,value,kind,delta',[(30_000_000,300_000_000,'increase',20_000_000),(1_000_000,10_000_000,'decrease',-9_000_000)])
def test_real_share_change_is_retained_across_publisher_holder_and_symbol_batch(db,shares,value,kind,delta):
    current=pair(db,'XOM','30231G102','30233Q108',shares=shares,value=value,prior_value=200_000_000)
    assert run(db,current)['derived_state']=='published'
    change=db.scalar(select(InstitutionalPositionChange))
    assert change.change_type==kind and change.shares_delta==delta
    items=_derived_activity_for_holder_from_positions(db,CIK,page=0,limit=50)['items']
    assert len(items)==1 and items[0]['change_type']==kind and items[0]['shares_delta']==delta
    events=list(db.scalars(select(Event)))
    if kind == 'decrease':assert events
    for event in events:
        proof=json.loads(event.payload_json)['security_transition_evidence'][0]
        assert proof['old_cusip']=='30231G102' and proof['new_cusip']=='30233Q108'
        assert proof['observed_at'] and proof['sources'][0]['sha256']
    filing=db.scalar(select(InstitutionalFiling).where(InstitutionalFiling.report_quarter==3))
    db.commit()
    from app.services.feed_source_control import writer_transaction
    saved=snapshot(db)
    with writer_transaction(db,'sec_13f','sec_edgar'):
        result=process_filing_changes_and_events_symbol_batch(db,filing)
    assert result['source_owned_unchanged']==1 and snapshot(db)==saved
    changes=list(db.scalars(select(InstitutionalPositionChange)))
    assert len(changes)==1 and changes[0].shares_delta==delta


@pytest.mark.parametrize('case',['unknown','future','mixed','wrong_period'])
def test_unverified_or_out_of_period_transition_remains_held(db,monkeypatch,case):
    old,new='30231G102','30233Q108'
    if case=='unknown':new='30233Q109'
    current=pair(db,'XOM',old,new)
    if case in {'future','wrong_period'}:
        registry=deepcopy(continuity._registry())
        for row in registry:
            if row['symbol']=='XOM':row['observed_at' if case=='future' else 'effective_date']='2099-01-01T00:00:00+00:00' if case=='future' else '2027-01-01'
        monkeypatch.setattr(continuity,'_registry',lambda:registry)
    if case=='mixed':current=document(rows=[(old,5_000_000,50_000_000,''),(new,5_000_000,50_000_000,'')])
    result=run(db,current)
    assert result['derived_state']=='waiting_security_transition' and result['feed_events']==0
    assert _derived_activity_for_holder_from_positions(db,CIK,page=0,limit=50)['reason']=='direct_pair_not_published'


def test_published_transition_proof_cannot_be_silently_changed(db):
    current=pair(db,'OKE','682680103','30609A109');run(db,current)
    filing=db.scalar(select(InstitutionalFiling).where(InstitutionalFiling.report_quarter==3))
    meta=json.loads(filing.raw_metadata_json);meta['_walnut_direct_13f']['security_transition_evidence'][0]['share_ratio']='2'
    filing.raw_metadata_json=json.dumps(meta);db.commit()
    result=run(db,current)
    assert result['status']=='held' and 'changed' in result['reason']
    with pytest.raises(ValueError,match='Unverified recorded'):
        _derived_activity_for_holder_from_positions(db,CIK,page=0,limit=50)


def test_original_sources_are_checksum_bound_and_not_available_before_review(tmp_path):
    from pathlib import Path
    import shutil
    source=Path(continuity.__file__).resolve().parents[2]/'config/security_transitions'
    shutil.copytree(source,tmp_path/'source')
    manifest=tmp_path/'source/manifest.json'
    registry=continuity.reviewed_transitions(manifest)
    before=[{'symbol':'XOM','cusip':'30231G102'}];after=[{'symbol':'XOM','cusip':'30233Q108'}]
    assert continuity.comparison_plan(before,after,prior_period='2026-06-30',current_period='2026-09-30',observed_by='2026-10-01T00:00:00+00:00',transitions=registry)['status']=='held'
    (tmp_path/'source/xom-transition-form25.htm').write_bytes(b'changed')
    with pytest.raises(ValueError,match='checksum'):continuity.reviewed_transitions(manifest)
