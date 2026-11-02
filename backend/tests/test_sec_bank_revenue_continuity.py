from copy import deepcopy
from datetime import date, datetime, timezone
from functools import lru_cache
from pathlib import Path
from unittest.mock import patch
import gzip
import hashlib
import json
from urllib.parse import parse_qs, urlsplit
import pytest
from lxml import html
from app.services import sec_bank_revenue as bank, sec_bank_revenue_continuity as continuity
from app.services import sec_fundamentals as sec, sec_financial_statements as statements
from app.services import sec_inline_financials as inline

FIXTURES = Path(__file__).parent/'fixtures/sec_bank_revenue_continuity'
NOW = datetime(2026, 11, 2, 3, tzinfo=timezone.utc)
AVAILABLE = date(2026, 11, 1)


@lru_cache
def originals():
    manifest=json.loads((FIXTURES/'sources.json').read_bytes())
    result={}
    for name,entry in manifest.items():
        raw=gzip.decompress((FIXTURES/(name+'.gz')).read_bytes())
        assert len(raw)==entry['bytes'] and hashlib.sha256(raw).hexdigest()==entry['sha256']
        result[name]=raw
    return result


@lru_cache
def records():
    raw=originals();data=json.loads(raw['BNY-facts.json']);company=json.loads(raw['BNY-company.json'])
    ordinary=sec._records(data,'revenue',AVAILABLE)
    combined=bank.apply({'revenue':{}},data,company,available_by=AVAILABLE)['revenue']
    return ordinary,combined,company


def test_complete_original_bank_inputs_and_annual_anchors():
    raw=originals();ordinary,combined,company=records()
    selected=continuity.select(ordinary,combined,company,AVAILABLE)
    assert selected is not None
    current=sec._ttm(selected,date(2026,9,30));prior=sec._ttm(selected,date(2025,9,30))
    assert current['value']==21962000000 and prior['value']==19748000000
    sources=json.loads((FIXTURES/'bank-originals.json').read_bytes())
    annual=json.loads((FIXTURES/'bank-annual-exhibit.json').read_bytes())
    index=html.fromstring(raw['BNY-annual-index.htm'])
    listed=[]
    for row in index.xpath('//table//tr'):
        if 'EX-13.1' in [' '.join(c.itertext()).strip() for c in row.xpath('./td')]:
            href=row.xpath('.//a/@href');assert len(href)==1
            listed.append('https://www.sec.gov'+parse_qs(urlsplit(href[0]).query)['doc'][0])
    assert listed==[annual['url']]
    required={}
    for point in current['inputs']+prior['inputs']:
        for item in point['inputs']:
            required[(item['accession'],item['tag'],item['start'],item['end'])]=item['value']
        for anchor in point['revenue_selection']['annual_anchors']:
            item=anchor['reported']
            required[(item['accession'],item['tag'],item['start'],item['end'])]=item['value']
    assert len(required)==12
    checked=set()
    for source in sources:
        # The SEC document index binds Exhibit13.1 to this annual accession.
        # Do not alter the application's separate linked-document admission.
        name='BNY-annual-exhibit.htm' if source['form']=='10-K' else source['file']
        tree=html.fromstring(raw[name]);contexts={}
        assert tree.get('xmlns:ix')=='http://www.xbrl.org/2013/inlineXBRL'
        for context in inline.nodes(tree,'context'):
            if inline.nodes(context,'segment') or inline.nodes(context,'scenario'):continue
            ids=inline.nodes(context,'identifier');starts=inline.nodes(context,'startdate');ends=inline.nodes(context,'enddate')
            if len(ids)==len(starts)==len(ends)==1 and ids[0].get('scheme')=='http://www.sec.gov/CIK' and ids[0].text_content().strip().zfill(10)==source['cik']:
                contexts[context.get('id')]=(starts[0].text_content().strip(),ends[0].text_content().strip())
        units={u.get('id') for u in inline.nodes(tree,'unit') if [m.text_content().strip() for m in inline.nodes(u,'measure')]==['iso4217:USD'] and not inline.nodes(u,'divide')}
        seen={}
        for node in inline.nodes(tree,'nonfraction'):
            period=contexts.get(node.get('contextref'));tag=node.get('name','')
            if not period or node.get('unitref') not in units or not tag.startswith('us-gaap:'):continue
            key=(source['accession'],tag.removeprefix('us-gaap:'),*period)
            if key in required:seen.setdefault(key,set()).add(inline._number(node,tree))
        for key,expected in required.items():
            if key[0]==source['accession']:
                assert seen.get(key)=={expected},key
                checked.add(key)
    assert checked==required.keys()


def test_recovered_metrics_preserve_existing_values_and_public_statement_contract():
    raw=originals();kwargs=dict(symbol='BNY',cik='0001390777',observed_at=NOW)
    with patch.object(continuity,'select',return_value=None):
        before=sec.project_fundamentals(raw['BNY-facts.json'],raw['BNY-company.json'],**kwargs)
    after=sec.project_fundamentals(raw['BNY-facts.json'],raw['BNY-company.json'],**kwargs)
    assert sec.project_fundamentals(raw['BNY-facts.json'],raw['BNY-company.json'],**kwargs)==after
    assert {k:v for k,v in before.items() if k!='source_evidence_json'}=={k:after[k] for k in before if k!='source_evidence_json'}
    assert set(after)-set(before)=={'revenue_growth','net_margin','fcf_margin'}
    assert after['revenue_growth']==pytest.approx((21962/19748-1)*100)
    evidence=json.loads(after['source_evidence_json'])
    assert evidence['revenue_basis']==bank.BASIS
    for row in evidence['current']['revenue']['inputs']:
        selection=row['revenue_selection']
        assert selection['method']=='reconciled_bank_reported_total_continuity_v1'
        assert len(selection['annual_anchors'])==2 and selection['basis_spliced'] is False
    assert after.get('net_debt_to_ebitda') is None
    panel=statements.project(raw['BNY-facts.json'],raw['BNY-company.json'],**kwargs)
    assert panel['summary']['revenueTtm']==21962000000 and panel['revenueBasis']==bank.BASIS


@pytest.mark.parametrize('kind',['value','filed','accession','currency','missing_anchor','conflicting_total',
    'missing_current','missing_prior','contract_component','current_total','newer_filing','ambiguous_filing','wrong_form','overlap_conflict'])
def test_continuity_refuses_unproved_or_superseded_inputs(kind):
    ordinary,combined,company=deepcopy(records())
    annual=(date(2024,1,1),date(2024,12,31))
    if kind=='value':ordinary[annual]['value']+=1
    if kind=='filed':ordinary[annual]['filed']='2026-02-24'
    if kind=='accession':ordinary[annual]['accession']='0001390777-26-999999'
    if kind=='currency':ordinary[annual]['unit']='EUR'
    if kind=='missing_anchor':ordinary.pop(annual)
    if kind=='conflicting_total':ordinary[annual]=None
    if kind=='missing_current':combined[date(2026,1,1),date(2026,9,30)]=None
    if kind=='missing_prior':combined[date(2024,1,1),date(2024,9,30)]=None
    if kind=='contract_component':
        for row in ordinary.values():row['tag']='RevenueFromContractWithCustomerExcludingAssessedTax'
    if kind=='current_total':
        key=(date(2026,1,1),date(2026,9,30));ordinary[key]={**combined[key],'tag':'Revenues'}
    if kind=='overlap_conflict':
        key=(date(2025,1,1),date(2025,9,30));ordinary[key]={**combined[key],'tag':'Revenues','value':1}
    if kind in ('newer_filing','ambiguous_filing','wrong_form'):
        recent=company['filings']['recent']
        recent['form'].append('20-F' if kind=='wrong_form' else '10-Q')
        recent['reportDate'].append('2026-10-31' if kind=='newer_filing' else '2026-09-30')
        recent['filingDate'].append('2026-11-01' if kind!='ambiguous_filing' else '2026-10-30')
        recent['accessionNumber'].append('0001390777-26-999999')
    assert continuity.select(ordinary,combined,company,AVAILABLE) is None


def test_stale_components_and_unchanged_inputs_are_preserved():
    values=deepcopy(records());original=deepcopy(values)
    assert continuity.select(*values,date(2027,4,1)) is None
    assert values==original
    result=continuity.select(*values,AVAILABLE)
    assert result is not None and values==original
