from datetime import datetime, timezone
import json
from copy import deepcopy
import pytest
from sqlalchemy import select, func
from app.models import InstitutionalFiling, InstitutionalPosition, Event, EmailDelivery
from app.services.direct_feed_store import dumps, DirectFeedDocument
from app.services.direct_feed_worker import DirectFeedPublication
from app.services import direct_13f_worker as worker
from app.services.institutional_reference import capture_reference
from test_direct_13f_worker import db, stage
from test_direct_13f_publication import document
from test_institutional_reference import payload, Client

CUSIP='000361108'

def reference(period, *, symbol='NEW', observed='2026-10-08T05:00:00+00:00'):
    value=payload();value['results'][0]['ticker']=symbol
    return capture_reference(Client(value),cusip=CUSIP,report_period=period,observed_at=observed)

def docs():
    return [document(quarter=2,serial=2,rows=[(CUSIP,10_000_000,100_000_000,''),('000361106',10_000_000,100_000_000,'')]),
            document(rows=[(CUSIP,30_000_000,300_000_000,'')])]

def run(db,doc,refs=()):
    document_id=db.scalar(select(DirectFeedDocument.id).where(DirectFeedDocument.feed=='sec_13f',DirectFeedDocument.source_key==doc['metadata']['key']))
    db.commit()
    if document_id is None:document_id=stage(db,doc)
    return worker.publish_13f_document(db,document_id,identifier_documents=[],comparison_documents=[doc],reference_documents=refs)

def snapshot(db):
    from app.db import Base
    return dumps([[t.name,sorted([dict(r._mapping) for r in db.execute(select(t))],key=dumps)] for t in Base.metadata.sorted_tables])

def test_late_references_enrich_owned_nulls_preserve_facts_and_repeat_without_delivery(db):
    prior,current=docs()
    run(db,prior);assert run(db,current)['status']=='waiting'
    before={p.id:(p.filing_id,p.cusip,p.shares,p.value_usd,p.filing_date) for p in db.scalars(select(InstitutionalPosition))}
    refs=[reference('2026-06-30'),reference('2026-09-30')]
    assert run(db,prior,refs)['enriched_positions']==1
    result=run(db,current,refs)
    assert result['status']=='published' and result['enriched_positions']==1 and result['feed_events']>0
    assert {p.id:(p.filing_id,p.cusip,p.shares,p.value_usd,p.filing_date) for p in db.scalars(select(InstitutionalPosition))}==before
    events=list(db.scalars(select(Event)))
    for event in events:
        value=json.loads(event.payload_json)
        assert value['identifier_reference_evidence']
        assert all(r['provider']=='massive_reference' for r in value['identifier_reference_evidence'])
        assert event.ts.replace(tzinfo=timezone.utc)>=datetime.fromisoformat(refs[0]['observed_at'])
    saved=snapshot(db)
    assert run(db,prior,refs)['enriched_positions']==0
    assert run(db,current,refs)['feed_events']==0
    assert snapshot(db)==saved
    assert db.scalar(select(func.count()).select_from(EmailDelivery))==0


def test_reference_mapped_prior_does_not_become_undated_identity_for_another_period(db):
    prior,current=docs()
    run(db,prior,[reference('2026-06-30')])
    result=run(db,current,[]) # same CUSIP needs period-specific reference, not a copied prior fact
    assert result['derived_state']=='waiting_equity_symbol_mapping'
    current_row=db.scalar(select(InstitutionalPosition).where(InstitutionalPosition.report_quarter==3))
    assert current_row.normalized_symbol is None
    assert db.scalar(select(func.count()).select_from(Event))==0


@pytest.mark.parametrize('case',['future','wrong_period','conflict','tamper'])
def test_reference_holds_do_not_change_waiting_canonical_state(db,case):
    prior,current=docs();run(db,prior);run(db,current)
    ref=reference('2026-09-30')
    if case=='future':ref=reference('2026-09-30',observed='2099-10-10T05:00:00+00:00')
    if case=='wrong_period':ref=reference('2026-06-30')
    if case=='conflict':
        db.add(InstitutionalPosition(filing_id=999,cik='0000000999',cusip=CUSIP,normalized_symbol='OLD',symbol='OLD',shares=1,value_usd=1,report_year=2025,report_quarter=4,filing_date=datetime(2026,2,1).date()));db.commit()
    if case=='tamper':ref['response']['results'][0]['ticker']='BAD'
    before=snapshot(db)
    if case=='tamper':
        with pytest.raises(ValueError,match='checksum'):run(db,current,[ref])
    else:
        result=run(db,current,[ref]);assert result['enriched_positions']==0
    assert snapshot(db)==before
    assert db.scalar(select(func.count()).select_from(Event))==0


def test_actual_batch_retries_unmapped_prior_and_current_from_saved_references_only(db):
    from app.services.direct_13f_batch import publish_13f_batch
    from app.services.institutional_reference import stage_reference
    prior,current=docs();stage(db,prior);stage(db,current)
    first=publish_13f_batch(db,identifier_documents=[])
    assert all(r['status']=='waiting' for r in first['results'])
    for period in ('2026-06-30','2026-09-30'):
        stage_reference(db,reference(period));db.commit()
    result=publish_13f_batch(db,identifier_documents=[],retry_waiting=True,use_staged_references=True)
    assert result['reference_documents']==2 and result['feed_events']>0
    assert sum(r.get('enriched_positions',0) for r in result['results'])==2
    saved=snapshot(db)
    assert publish_13f_batch(db,identifier_documents=[],retry_waiting=True,use_staged_references=True)['processed']==0
    assert snapshot(db)==saved


def test_saved_reference_tamper_aborts_batch_before_any_canonical_change(db):
    from app.services.direct_13f_batch import publish_13f_batch
    from app.services.institutional_reference import stage_reference, FEED
    from app.services.direct_feed_store import DirectFeedRevision
    prior,current=docs();stage(db,prior);stage(db,current)
    stage_reference(db,reference('2026-09-30'));db.commit()
    source=db.scalar(select(DirectFeedDocument).where(DirectFeedDocument.feed==FEED))
    revision=db.scalar(select(DirectFeedRevision).where(DirectFeedRevision.document_id==source.id))
    revision.source_bytes+=b'changed';db.commit()
    with pytest.raises(ValueError,match='checksum'):
        publish_13f_batch(db,identifier_documents=[],use_staged_references=True)
    db.rollback()
    assert db.scalar(select(func.count()).select_from(InstitutionalFiling))==0
    assert db.scalar(select(func.count()).select_from(DirectFeedPublication))==0


def test_reference_enrichment_does_not_overwrite_unfingerprinted_symbol_drift(db):
    prior,current=docs();run(db,prior);run(db,current)
    row=db.scalar(select(InstitutionalPosition).where(InstitutionalPosition.report_quarter==3))
    row.symbol='UNREVIEWED';db.commit()
    result=run(db,current,[reference('2026-09-30')])
    assert result['status']=='held' and 'symbol requires reconciliation' in result['reason']
    assert row.symbol=='UNREVIEWED' and row.normalized_symbol is None


def test_public_holder_fallback_cannot_bypass_waiting_mapping_or_transition(db):
    from app.services.institutional_activity import activity_for_holder
    from test_direct_13f_publication import CIK
    prior,current=docs();run(db,prior);run(db,current)
    result=activity_for_holder(db,CIK)
    assert result['items']==[] and result['reason']=='direct_pair_not_published'
    assert result['status']=='unavailable'
    refs=[reference('2026-06-30'),reference('2026-09-30')]
    run(db,prior,refs);run(db,current,refs)
    result=activity_for_holder(db,CIK)
    assert result['items'] and result.get('reason') is None


def test_mapped_spinoff_position_waits_for_distribution_treatment_instead_of_buy_signal(db):
    prior=document(quarter=2,serial=2,rows=[('000361106',10_000_000,100_000_000,'')])
    current=document(rows=[('60744M106',10_000_000,100_000_000,'')])
    value=payload();value['results'][0]['ticker']='MBGL'
    ref=capture_reference(Client(value),cusip='60744M106',report_period='2026-09-30',observed_at='2026-10-08T05:00:00+00:00')
    run(db,prior);result=run(db,current,[ref])
    assert result['derived_state']=='waiting_distribution_treatment' and result['feed_events']==0
    assert db.scalar(select(func.count()).select_from(Event))==0
    saved=snapshot(db)
    assert run(db,current,[ref])['derived_state']=='waiting_distribution_treatment'
    assert snapshot(db)==saved
