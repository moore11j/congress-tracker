"""Missing-symbol recovery uses complete dated SEC sources, with real worker repeats."""
from datetime import date
from pathlib import Path
import gzip
import hashlib
import json

import pytest
from sqlalchemy import select, func
from app.models import InstitutionalFiling, InstitutionalPosition, Event, EmailDelivery
from app.services.direct_feed_store import DirectFeedDocument
from app.services import direct_13f_worker as worker
from test_direct_13f_worker import db, stage
from test_direct_13f_reference_mapping import CUSIP, docs, run, snapshot
from test_direct_13f_evidence import nport
from test_direct_13f_publication import document

def identifier(**kwargs):
    source = nport(**kwargs)
    source['raw'] = source['raw'].replace(b'000361105', CUSIP.encode())
    source['content_hash'] = hashlib.sha256(source['raw']).hexdigest()
    return source

def publish(db, doc, sources):
    ident = db.scalar(select(DirectFeedDocument.id).where(
        DirectFeedDocument.feed == 'sec_13f', DirectFeedDocument.source_key == doc['metadata']['key']))
    db.commit()
    if ident is None:
        ident = stage(db, doc)
    return worker.publish_13f_document(db, ident, identifier_documents=sources, comparison_documents=[doc])

def test_late_supplied_dated_nport_recovers_owned_pair_and_repeats(db):
    prior, current = docs()
    run(db, prior); assert run(db, current)['status'] == 'waiting'
    facts = {p.id: (p.filing_id,p.cusip,p.shares,p.value_usd,p.filing_date)
             for p in db.scalars(select(InstitutionalPosition))}
    source = identifier(symbol='NEW')
    assert publish(db,prior,[source])['enriched_positions'] == 1
    result = publish(db,current,[source])
    assert result['status'] == 'published' and result['enriched_positions'] == 1 and result['feed_events'] > 0
    assert {p.id: (p.filing_id,p.cusip,p.shares,p.value_usd,p.filing_date)
            for p in db.scalars(select(InstitutionalPosition))} == facts
    for filing in db.scalars(select(InstitutionalFiling)):
        proofs = json.loads(filing.raw_metadata_json)['_walnut_direct_13f']['identifier_evidence']
        assert len(proofs) == 1 and proofs[0]['source_sha256'] == source['content_hash']
        assert proofs[0]['cusip'] == CUSIP and proofs[0]['symbol'] == 'NEW'
    before = snapshot(db)
    assert publish(db,prior,[source])['enriched_positions'] == 0
    assert publish(db,current,[source])['feed_events'] == 0
    assert snapshot(db) == before
    assert db.scalar(select(func.count()).select_from(EmailDelivery)) == 0

@pytest.mark.parametrize('case',['future','conflict','non_equity','derivative','checksum','wrong_url','unrelated'])
def test_unqualified_identifier_does_not_enrich_or_create_events(db,case):
    prior,current=docs();run(db,prior);run(db,current)
    source=identifier(symbol='NEW')
    if case=='future':source=identifier(symbol='NEW',filed='20261007')
    if case=='non_equity':source=identifier(symbol='NEW',asset='DBT')
    if case=='derivative':source=identifier(symbol='NEW',derivative=True)
    if case=='checksum':source['raw']+=b'tamper'
    if case=='wrong_url':source['url']=source['url'].replace('/1592900/','/1/')
    if case=='unrelated':source=nport(symbol='NEW')
    if case=='conflict':
        db.add(InstitutionalPosition(filing_id=999,cik='999',cusip=CUSIP,symbol='OLD',normalized_symbol='OLD',
            shares=1,value_usd=1,report_year=2026,report_quarter=1,filing_date=date(2026,2,1)))
        db.commit()
    if case in {'checksum','wrong_url'}:
        from app.clients.direct_sources import DirectSourceError
        with pytest.raises(DirectSourceError):publish(db,current,[source])
        db.rollback()
    else:
        assert publish(db,current,[source])['enriched_positions']==0
    # Worker receipts can record a newly inspected evidence set. Canonical
    # holdings, facts and events must remain unchanged.
    assert all(p.normalized_symbol is None for p in db.scalars(select(InstitutionalPosition).where(
        InstitutionalPosition.cusip==CUSIP,InstitutionalPosition.filing_id!=999)))
    assert db.scalar(select(func.count()).select_from(Event))==0

def test_symbol_drift_still_requires_reconciliation(db):
    prior,current=docs();run(db,prior);run(db,current)
    row=db.scalar(select(InstitutionalPosition).where(InstitutionalPosition.report_quarter==3))
    row.symbol='UNREVIEWED';db.commit()
    result=publish(db,current,[identifier(symbol='NEW')])
    assert result['status']=='held' and 'symbol requires reconciliation' in result['reason']
    assert row.symbol=='UNREVIEWED' and row.normalized_symbol is None

def test_published_pair_never_changes_when_conflicting_identifiers_arrive(db):
    prior,current=docs();source=identifier(symbol='NEW')
    publish(db,prior,[source]);publish(db,current,[source]);before=snapshot(db)
    result=publish(db,current,[identifier(symbol='OTHER')])
    assert result['status']=='existing' and result['feed_events']==0
    assert snapshot(db)==before

def test_published_dependent_protects_prior_missing_option_symbol(db):
    prior=document(quarter=2,serial=2,rows=[('000361106',100,1000,''),(CUSIP,10,100,'PUT')])
    current=document(rows=[('000361106',200,2000,'')])
    run(db,prior);assert run(db,current)['status']=='published'
    before=snapshot(db)
    result=publish(db,prior,[identifier(symbol='NEW')])
    assert result['status']=='held' and 'Published pair depends' in result['reason']
    assert snapshot(db)==before

def actual_nport():
    path=Path(__file__).parent/'fixtures/nport_identifier_enrichment/0002066578-26-003341.source.gz'
    raw=gzip.decompress(path.read_bytes())
    expected='fd403420fb788d29d8effdece5b309f2ea94cf017c9dd0ef148ccff2c1c6816b'
    assert hashlib.sha256(raw).hexdigest()==expected
    return dict(raw=raw,content_hash=expected,
        url='https://www.sec.gov/Archives/edgar/data/92500/0002066578-26-003341.txt')

def test_complete_retained_nport_enriches_current_but_never_backdates_prior(db):
    source=actual_nport();cusip='G39104107'
    prior=document(quarter=2,serial=2,rows=[(cusip,100,1000,'')])
    current=document(rows=[(cusip,200,2000,'')])
    run(db,prior);run(db,current)
    assert publish(db,prior,[source])['enriched_positions']==0
    result=publish(db,current,[source])
    assert result['enriched_positions']==1 and result['derived_state']=='waiting_equity_symbol_mapping'
    rows=list(db.scalars(select(InstitutionalPosition).where(InstitutionalPosition.cusip==cusip).order_by(InstitutionalPosition.report_quarter)))
    assert [p.normalized_symbol for p in rows]==[None,'GTES']
    filing=db.get(InstitutionalFiling,rows[1].filing_id)
    proofs=json.loads(filing.raw_metadata_json)['_walnut_direct_13f']['identifier_evidence']
    assert proofs and all(p['source_sha256']==source['content_hash'] and p['filing_date']=='2026-09-28' for p in proofs)
    before=snapshot(db);assert publish(db,current,[source])['enriched_positions']==0
    assert snapshot(db)==before and db.scalar(select(func.count()).select_from(Event))==0


def test_complete_nport_disambiguation_keeps_both_identifier_and_reference_proofs(db):
    source=actual_nport();cusip='98980F104'
    for symbol in ('ZI','ZOM'):
        db.add(InstitutionalPosition(filing_id=999,cik='999',cusip=cusip,symbol=symbol,normalized_symbol=symbol,
            shares=1,value_usd=1,report_year=2026,report_quarter=1,filing_date=date(2026,2,1)))
    db.commit()
    prior=document(quarter=2,serial=2,rows=[(cusip,100,1000,'')])
    current=document(rows=[(cusip,200,2000,'')])
    run(db,prior);run(db,current)
    ref=json.loads((Path(__file__).parent/'fixtures/nport_identifier_enrichment/gtm-reference-september.json').read_bytes())
    assert run(db,current,[ref])['enriched_positions']==0
    ident=db.scalar(select(DirectFeedDocument.id).where(DirectFeedDocument.source_key==current['metadata']['key']))
    db.commit()
    result=worker.publish_13f_document(db,ident,identifier_documents=[source],comparison_documents=[current],reference_documents=[ref])
    assert result['enriched_positions']==1 and result['feed_events']==0
    position=db.scalar(select(InstitutionalPosition).where(InstitutionalPosition.report_quarter==3))
    assert position.normalized_symbol=='GTM'
    filing=db.get(InstitutionalFiling,position.filing_id)
    proof=json.loads(filing.raw_metadata_json)['_walnut_direct_13f']
    assert proof['identifier_evidence'] and all(r['symbol']=='GTM' and r['source_sha256']==source['content_hash'] for r in proof['identifier_evidence'])
    assert proof['reference_evidence'][0]['symbol']=='GTM'
    assert {p.normalized_symbol for p in db.scalars(select(InstitutionalPosition).where(
        InstitutionalPosition.filing_id==999,InstitutionalPosition.cusip==cusip))}=={'ZI','ZOM'}
    before=snapshot(db)
    repeated=worker.publish_13f_document(db,ident,identifier_documents=[source],comparison_documents=[current],reference_documents=[ref])
    assert repeated['enriched_positions']==0 and snapshot(db)==before
