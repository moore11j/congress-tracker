from test_direct_13f_publication import db, document, CIK, SINCE
from test_direct_13f_empty_historical import shell
from datetime import date
import copy,hashlib,json
import pytest
from sqlalchemy import select
from app.models import InstitutionalPosition
from app.services.direct_feed_collection import parse_document
from app.services.direct_feed_rehearsal import snapshot,digest
from app.services.institutional_source_reconciliation import plan_historical_positions,apply_position_rehearsal,value_kind
from decimal import Decimal


def peers(value=100_000_000):
    output=[]
    for i in (7,8):
        doc=document(quarter=2,serial=i,filed='2026-08-01',rows=[('000361105',10_000_000,value,'')])
        cik=f'{i:010}'
        doc['raw']=doc['raw'].replace(CIK.encode(),cik.encode())
        doc['metadata']['cik']=cik
        doc['metadata']['url']=f'https://www.sec.gov/Archives/edgar/data/{i}/{doc["metadata"]["key"]}.txt'
        doc['content_hash']=hashlib.sha256(doc['raw']).hexdigest();output.append(doc)
    return output


def seed(db,*,rows=None):
    doc=document(quarter=2,serial=2,rows=rows)
    filing=shell(db,doc)
    _,parsed,reasons=parse_document('sec_13f',doc['raw'],doc['metadata']);assert not reasons
    for i,row in enumerate(parsed['positions']):
        db.add(InstitutionalPosition(filing_id=filing.id,cik=CIK,report_year=2026,report_quarter=2,
            filing_date=filing.filing_date,cusip=row['cusip'],symbol='AAA',normalized_symbol='AAA',
            shares=row['shares'],value_usd=row['valueUsd']/1000,put_call=row.get('putCall') or None))
    db.commit()
    return doc,filing


def planned(db,doc,filing,**kwargs):
    return plan_historical_positions(doc,snapshot(filing),[snapshot(r) for r in db.scalars(
        select(InstitutionalPosition).where(InstitutionalPosition.filing_id==filing.id).order_by(InstitutionalPosition.id))],
        publish_since=SINCE,comparison_documents=kwargs.get('comparisons',peers()))


def test_source_bound_apply_reconciles_and_repeats_with_same_ids(db):
    doc,filing=seed(db)
    plan=planned(db,doc,filing)
    assert plan['proposal']['kinds']=={'verified_unit_difference':1}
    before=snapshot(filing)
    ids=list(db.scalars(select(InstitutionalPosition.id)))
    first=apply_position_rehearsal(db,doc,plan,expected_plan_hash=plan['sha256'],comparison_documents=peers());db.commit()
    assert first['updated']==1 and first['inserted']==first['deleted']==0
    after=[snapshot(r) for r in db.scalars(select(InstitutionalPosition))]
    assert after[0]['value_usd']==100_000_000
    repeated=apply_position_rehearsal(db,doc,plan,expected_plan_hash=plan['sha256'],comparison_documents=peers());db.commit()
    assert repeated['updated']==0 and repeated['skipped']==1
    assert [snapshot(r) for r in db.scalars(select(InstitutionalPosition))]==after
    assert snapshot(filing)==before and list(db.scalars(select(InstitutionalPosition.id)))==ids


@pytest.mark.parametrize('invalid',['current','owned','shares','weight','ownership','duplicate','source_hash','wrong_url','no_peers','independent_units','amendment','period'])
def test_whole_source_and_population_guards(db,invalid):
    doc,filing=seed(db);rows=list(db.scalars(select(InstitutionalPosition)))
    fs=snapshot(filing);positions=[snapshot(r) for r in rows];comparison=peers()
    if invalid=='current':fs['filing_date']='2026-10-01'
    elif invalid=='owned':fs['raw_metadata_json']='{"_walnut_direct_13f":{"complete":true}}'
    elif invalid=='shares':positions[0]['shares']+=1
    elif invalid=='weight':positions[0]['portfolio_weight']=1
    elif invalid=='ownership':positions[0]['ownership_pct']=1
    elif invalid=='duplicate':positions.append(copy.deepcopy(positions[0]))
    elif invalid=='source_hash':doc['content_hash']='0'*64
    elif invalid=='wrong_url':doc['metadata']['url']='https://example.com/source.txt'
    elif invalid=='no_peers':comparison=[]
    elif invalid=='independent_units':comparison=peers(value=100_000)
    elif invalid=='amendment':fs['is_amendment']=True
    elif invalid=='period':fs['report_quarter']=3
    with pytest.raises(ValueError):plan_historical_positions(doc,fs,positions,publish_since=SINCE,comparison_documents=comparison)
    assert snapshot(filing)==snapshot(db.get(type(filing),filing.id))
    assert [snapshot(r) for r in db.scalars(select(InstitutionalPosition))]==[snapshot(r) for r in rows]


def test_stale_population_aborts_before_any_position_is_corrected(db):
    doc,filing=seed(db,rows=[('000361105',10_000_000,100_000_000,''),('000361106',20_000_000,200_000_000,'')])
    plan=planned(db,doc,filing)
    rows=list(db.scalars(select(InstitutionalPosition).order_by(InstitutionalPosition.id)))
    rows[1].shares+=1;db.commit();before=[snapshot(r) for r in rows]
    with pytest.raises(ValueError):apply_position_rehearsal(db,doc,plan,expected_plan_hash=plan['sha256'],comparison_documents=peers())
    assert [snapshot(r) for r in rows]==before


def test_rehashed_unsupported_change_is_rejected_by_original_rebuild(db):
    doc,filing=seed(db);plan=planned(db,doc,filing)
    plan['proposal']['operations'][0]['changes']['normalized_symbol']='FAKE'
    plan['sha256']=digest({k:v for k,v in plan.items() if k!='sha256'})
    with pytest.raises(ValueError):apply_position_rehearsal(db,doc,plan,expected_plan_hash=plan['sha256'],comparison_documents=peers())
    assert db.scalar(select(InstitutionalPosition.normalized_symbol))=='AAA'


@pytest.mark.parametrize('stored,exact,expected',[(16404869.999999998,'16404870','exact_scaled_float_roundtrip'),(16404869.99,'16404870',None),(float('nan'),'1',None),(float('inf'),'1',None),(1.0000000000000002,'1',None),(123,'123000','verified_unit_difference')])
def test_only_exact_documented_value_transforms(stored,exact,expected):
    assert value_kind(stored,Decimal(exact))==expected
