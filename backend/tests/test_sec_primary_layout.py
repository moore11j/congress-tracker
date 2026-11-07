"""Complete original statement layout, explicit zero and sign reconciliation."""
from copy import deepcopy
from datetime import date, datetime, timezone
from functools import lru_cache
from pathlib import Path
import gzip
import hashlib
import json
import socket
from unittest.mock import patch

import pytest
from lxml import html
from app.services import sec_primary_income as income, sec_inline_financials as inline, sec_fundamentals as sec

ROOT=Path(__file__).parent/'fixtures/sec_primary_layout'
MANIFEST=json.loads((ROOT/'manifest.json').read_bytes())
NOW=datetime(2026,11,7,4,tzinfo=timezone.utc)
DAY=date(2026,11,5)
CIK='0000804328'


@lru_cache(maxsize=3)
def raw(name):
    entry=MANIFEST['sources'][name]
    value=gzip.decompress((ROOT/entry['file']).read_bytes())
    assert len(value)==entry['bytes'] and hashlib.sha256(value).hexdigest()==entry['sha256']
    return value


def planned(company=None):
    return income.plan(json.loads(raw('facts.json')),company or json.loads(raw('company.json')),
                       symbol='QCOM',cik=CIK,available_by=DAY)


class Client:
    def __init__(self):self.calls=[]
    def get(self,url):
        self.calls.append(url)
        names=[n for n,v in MANIFEST['sources'].items() if v['url']==url]
        assert len(names)==1,'Unexpected source request'
        return raw(names[0])


@lru_cache(maxsize=1)
def acquired():
    client=Client()
    with patch.object(socket.socket,'connect',side_effect=AssertionError('Offline fixture attempted network')):
        result=inline.acquire(raw('facts.json'),raw('company.json'),symbol='QCOM',cik=CIK,observed_at=NOW,client=client)
    assert client.calls==[MANIFEST['filing']['url']]
    return result


def project(item=None):
    return sec.project_fundamentals(raw('facts.json'),raw('company.json'),symbol='QCOM',cik=CIK,
                                    observed_at=NOW,filing_supplement=item)


def test_complete_statement_reconciles_current_income_without_filling_unproved_ratios():
    before=project();after=project(acquired())
    old=json.loads(before['source_evidence_json']);proof=json.loads(after['source_evidence_json'])
    assert all(after[k]==before[k] for k in old['calculations'])
    assert old['leverage_preparation']['ebitda']['status']=='incomplete'
    assert proof['leverage_preparation']['ebitda']['value']==11733000000
    assert proof['net_debt_policy']['reason']=='no_verified_finance_inclusive_debt'
    assert after.get('net_debt_to_ebitda') is None and after.get('roe') is None
    assert after==project(acquired())
    verified=proof['inline_filing_supplement']['primary_consolidated_income']
    assert income.cached_valid(verified,cik=CIK)
    point=verified['rows'][0]
    assert point['value']==13628000000 and point['pretax']==9414000000 and point['tax']==-4214000000
    assert point['issuer_jurisdiction']['registry_name']=='QUALCOMM INC/DE'
    assert point['reported_zero_discontinued']['value']==0


def test_other_printed_columns_require_explicit_zero_and_same_period_reconciliation():
    rows=income.statement_rows(raw('primary.htm'),MANIFEST['filing'],'QUALCOMM INC/DE',jurisdiction='DE')
    assert {r['end'] for r in rows.values()}=={'2026-09-27','2025-09-28'}
    assert {r['value'] for r in rows.values()}=={13628000000,5541000000}
    # The older column reports nonzero discontinued operations and stays held.
    assert all(r['end']!='2024-09-29' for r in rows.values())


@pytest.mark.parametrize('change',[
    'cover_name','cover_context','statement_owner','allocation_label','missing_zero',
    'zero_nil','zero_format','zero_nonzero','zero_context','zero_currency','tax_sign',
    'tax_label','hidden_income','missing_duration','mixed_duration','ambiguous_period',
])
def test_changed_complete_source_cannot_pass_current_period(change):
    tree=html.fromstring(raw('primary.htm'))
    target=next(n for n in inline.nodes(tree,'nonfraction') if n.get('name')=='us-gaap:NetIncomeLoss' and n.get('contextref')=='c-1' and n.xpath('ancestor::table'))
    table=target.xpath('ancestor::table')[-1]
    zero=next(n for n in inline.nodes(table,'nonfraction') if n.get('name')=='us-gaap:IncomeLossFromDiscontinuedOperationsNetOfTaxAttributableToReportingEntity' and n.get('contextref')=='c-1')
    tax=next(n for n in inline.nodes(table,'nonfraction') if n.get('name')=='us-gaap:IncomeTaxExpenseBenefit' and n.get('contextref')=='c-1')
    name=next(n for n in tree.iter() if n.tag=='ix:nonnumeric' and n.get('name')=='dei:EntityRegistrantName')
    if change=='cover_name':name.text='Different Entity Inc/DE'
    elif change=='cover_context':name.set('contextref','missing')
    elif change=='statement_owner':
        for n in tree.iter():
            if n.text and n.text.strip()=='QUALCOMM Incorporated':n.text='Unrelated Subsidiary Incorporated'
    elif change=='allocation_label':
        row=target.xpath('ancestor::tr')[-1]
        cell=row.xpath('./td|./th')[0];cell.clear();cell.text='Net income attributable to parent'
    elif change=='missing_zero':zero.getparent().remove(zero)
    elif change=='zero_nil':zero.set('xsi:nil','true')
    elif change=='zero_format':zero.attrib.pop('format')
    elif change=='zero_nonzero':zero.set('format',tax.get('format'));zero.text='1'
    elif change=='zero_context':zero.set('contextref','c-2')
    elif change=='zero_currency':zero.set('unitref','usdPerShare')
    elif change=='tax_sign':tax.attrib.pop('sign')
    elif change=='tax_label':
        row=tax.xpath('ancestor::tr')[-1]
        cell=row.xpath('./td|./th')[0];cell.clear();cell.text='Income tax expense'
    elif change=='hidden_income':
        parent=target.getparent();wrapper=html.Element('ix:hidden');parent.replace(target,wrapper);wrapper.append(target)
    elif change in {'missing_duration','mixed_duration'}:
        for n in table.iter():
            if n.text and n.text.strip()=='Year Ended':n.text='' if change=='missing_duration' else 'Three months ended'
    elif change=='ambiguous_period':
        context=deepcopy(next(n for n in inline.nodes(tree,'context') if n.get('id')=='c-1'))
        context.set('id','competing');inline.nodes(context,'startdate')[0].text='2025-09-28';tree.append(context)
    changed=html.tostring(tree,encoding='utf-8')
    try:result=income.qualify(planned(),{MANIFEST['filing']['url']:changed},issuer='QUALCOMM INC/DE')
    except ValueError:return
    assert not result['rows']


@pytest.mark.parametrize('change',['missing_jurisdiction','wrong_jurisdiction','different_issuer'])
def test_metadata_name_is_not_an_unconditional_alias(change):
    company=json.loads(raw('company.json'))
    if change=='missing_jurisdiction':company.pop('stateOfIncorporation')
    elif change=='wrong_jurisdiction':company['stateOfIncorporation']='MD'
    else:company['name']='Unrelated Issuer INC/DE'
    assert not income.qualify(planned(company),{MANIFEST['filing']['url']:raw('primary.htm')},issuer=company['name'])['rows']


@pytest.mark.parametrize('change',['nonzero','bool_zero','zero_context','zero_tag','registry_name','jurisdiction','owner_cik','owner_heading'])
def test_cached_new_evidence_rejects_corruption(change):
    proof=deepcopy(acquired()['evidence']['primary_consolidated_income']);row=proof['rows'][0]
    if change=='nonzero':row['reported_zero_discontinued']['value']=1
    elif change=='bool_zero':row['reported_zero_discontinued']['value']=False
    elif change=='zero_context':row['reported_zero_discontinued']['context']='other'
    elif change=='zero_tag':row['reported_zero_discontinued']['tag']='us-gaap:EarningsPerShareBasic'
    elif change=='registry_name':row['issuer_jurisdiction']['registry_name']='Other INC/DE'
    elif change=='jurisdiction':row['issuer_jurisdiction']['jurisdiction']='MD'
    elif change=='owner_cik':row['issuer_jurisdiction']['cik']='0000000001'
    elif change=='owner_heading':row['heading']=['Unrelated Company','Consolidated statements of income']
    assert not income.cached_valid(proof,cik=CIK)


def test_normal_preparation_retains_new_proof_and_repeats_without_requests(monkeypatch):
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from app.models import InsightsSnapshot
    from app.services import sec_fundamentals_preparation as prep, sec_directory
    monkeypatch.setattr(socket.socket,'connect',lambda *a,**k:pytest.fail('Unexpected network'))
    monkeypatch.setenv('SEC_FUNDAMENTALS_WARMING_ENABLED','1')
    monkeypatch.setattr(sec_directory,'directory',lambda:{'QCOM':{'cik':CIK}})
    class Clock(datetime):
        @classmethod
        def now(cls,tz=None):return NOW
    engine=create_engine('sqlite://');InsightsSnapshot.__table__.create(engine)
    factory=sessionmaker(bind=engine);client=Client()
    try:
        with patch.object(prep,'SessionLocal',factory),patch.object(prep,'datetime',Clock),patch.object(prep,'DirectSourceClient',lambda:client):
            first=prep.prepare('QCOM')
            assert first['calculation_evidence']['leverage_preparation']['ebitda']['value']==11733000000
            assert first['values'].get('net_debt_to_ebitda') is None
            count=len(client.calls)
            assert prep.prepare('QCOM')==first and len(client.calls)==count
        with factory() as db:
            row=db.get(InsightsSnapshot,'sec-current-fundamentals:QCOM:v1');saved=json.loads(row.payload_json)
            saved['calculation_evidence']['inline_filing_supplement']['primary_consolidated_income']['rows'][0]['reported_zero_discontinued']['value']=1
            row.payload_json=json.dumps(saved);db.commit()
        with patch.object(prep,'SessionLocal',factory):assert prep._cached('QCOM',NOW) is None
    finally:engine.dispose()
