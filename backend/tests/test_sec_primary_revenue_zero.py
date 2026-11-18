from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
import gzip
import hashlib
import json
import socket

import pytest
from lxml import html
from app.services import sec_inline_financials as inline, sec_fundamentals as sec
from app.services import sec_primary_revenue_zero as zero

ROOT = Path(__file__).parent/'fixtures/sec_primary_revenue_zero'
NOW = datetime(2026,11,16,4,30,tzinfo=timezone.utc)
CIK = '0001818838'


def originals():
    manifest=json.loads((ROOT/'sources.json').read_bytes())
    raw={}
    for name,meta in manifest.items():
        raw[name]=gzip.decompress((ROOT/meta['file']).read_bytes())
        assert len(raw[name])==meta['bytes'] and hashlib.sha256(raw[name]).hexdigest()==meta['sha256']
    return manifest,raw


def acquire(raw,manifest):
    calls=[]
    urls={manifest[k]['url']:v for k,v in raw.items()}
    class Client:
        issuer_document_urls=frozenset()
        def get(self,url):
            calls.append(url)
            return urls[url]
    supplement=inline.acquire(raw['facts'],raw['company'],symbol='ADAG',cik=CIK,observed_at=NOW,client=Client())
    return supplement,calls


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    monkeypatch.setenv('SEC_FINANCIAL_DISCOVERY_READ_ENABLED','0')
    monkeypatch.setattr(socket.socket,'connect',lambda *a,**k:pytest.fail('Network forbidden'))


def test_complete_originals_prove_comparative_zero_and_both_operating_equations():
    manifest,raw=originals();supplement,calls=acquire(raw,manifest)
    proof=supplement['evidence']['primary_comparative_revenue_zeros']
    assert len(proof)==1 and proof[0]['methodology']==zero.VERSION
    assert proof[0]['value']==0 and proof[0]['start']=='2025-01-01' and proof[0]['end']=='2025-06-30'
    assert proof[0]['columns'][0]['operating_facts'][2]['value']==-15688257
    assert proof[0]['columns'][1]['operating_facts'][2]['value']==-16586539
    value=sec.project_fundamentals(raw['facts'],raw['company'],symbol='ADAG',cik=CIK,observed_at=NOW,filing_supplement=supplement)
    evidence=json.loads(value['source_evidence_json'])
    assert evidence['current']['revenue']['value']==9290815
    assert value['net_margin']==pytest.approx(-20511005/9290815*100)
    assert value['operating_margin']==pytest.approx(-22336577/9290815*100)
    assert value['current_ratio']==pytest.approx(132408623/26361197)
    assert value['roe']==pytest.approx(-20511005/((37911789+89453418)/2)*100)
    assert not {'free_cash_flow','fcf_margin','revenue_growth','net_debt_to_ebitda'} & value.keys()
    from types import SimpleNamespace
    from app.services.fundamentals_cache import fundamentals_summary_from_cache_row
    before={k:v for k,v in value.items() if k not in {'net_margin','operating_margin'}}
    summary=fundamentals_summary_from_cache_row(SimpleNamespace(**value),now=NOW)
    assert summary==fundamentals_summary_from_cache_row(SimpleNamespace(**before),now=NOW)
    assert summary['status']=='unavailable'
    assert acquire(raw,manifest)==(supplement,calls)
    assert set(calls)=={manifest['cover']['url'],manifest['current']['url']}
    annual=html.fromstring(raw['annual'])
    tagged=[n for n in inline.nodes(annual,'nonfraction') if n.get('name')=='us-gaap:'+zero.REVENUE]
    assert any(inline._number(n,annual)==7670902 for n in tagged)


def mutate(raw,defect):
    tree=html.fromstring(raw)
    revenue=next(n for n in inline.nodes(tree,'nonfraction') if n.get('name')=='us-gaap:'+zero.REVENUE)
    table=revenue.xpath('ancestor::table')[-1]
    row=revenue.xpath('ancestor::tr')[-1]
    cell=next(c for c in row.xpath('./td') if ''.join(c.itertext()).strip()=='\u2014')
    prior_rd=next(n for n in inline.nodes(table,'nonfraction') if n.get('name')=='us-gaap:ResearchAndDevelopmentExpense' and '2025' in n.get('contextref',''))
    if defect=='blank':
        for n in cell.iter():n.text='' ; n.tail=''
    elif defect=='nil':
        cell.clear();n=html.Element('ix:nonfraction');n.attrib.update({'name':'us-gaap:'+zero.REVENUE,'contextref':prior_rd.get('contextref'),'unitref':prior_rd.get('unitref'),'xsi:nil':'true'});cell.append(n)
    elif defect=='rounding_mismatch':prior_rd.text='12,015,185'
    elif defect=='wrong_column_period':prior_rd.set('contextref',revenue.get('contextref'))
    elif defect=='wrong_currency':prior_rd.set('unitref','unknown')
    elif defect=='wrong_expense_concept':prior_rd.set('name','us-gaap:CapitalExpenditures')
    elif defect=='hidden_zero':cell.set('style','display:none')
    elif defect=='hidden_expense':prior_rd.set('style','display:none')
    elif defect=='ambiguous_span':cell.set('colspan','2')
    elif defect=='additional_revenue':
        extra=deepcopy(row);row.addnext(extra)
    elif defect=='adjusted_heading':
        for node in tree.iter():
            if node.text and 'UNAUDITED INTERIM CONDENSED CONSOLIDATED STATEMENTS OF COMPREHENSIVE LOSS' in node.text:node.text=node.text.replace('UNAUDITED','ADJUSTED')
    elif defect=='wrong_header_year':
        for node in table.iter():
            if node.text=='2025':node.text='2024'
    elif defect=='negative_expense':prior_rd.set('sign','-')
    else:raise AssertionError(defect)
    return html.tostring(tree)


@pytest.mark.parametrize('defect',['blank','nil','rounding_mismatch','wrong_column_period','wrong_currency',
    'wrong_expense_concept','hidden_zero','hidden_expense','ambiguous_span','additional_revenue',
    'adjusted_heading','wrong_header_year','negative_expense'])
def test_ambiguous_or_incomplete_primary_statement_cannot_supply_zero(defect):
    manifest,raw=originals();raw['current']=mutate(raw['current'],defect)
    try:
        supplement,_=acquire(raw,manifest)
    except (inline.InlineFactsError,ValueError):
        return
    assert not supplement['evidence'].get('primary_comparative_revenue_zeros')
    facts=supplement['facts'].get(zero.REVENUE,{}).get('units',{}).get('USD',[])
    assert not any(p.get('start')=='2025-01-01' and p['end']=='2025-06-30' and p.get('val')==0 for p in facts)


def test_cache_version_rotates_known_affected_symbol_only():
    from app.services import sec_fundamentals_preparation as prep
    assert zero.VERSION in prep.version_for_symbol('ADAG')
    assert zero.VERSION not in prep.version_for_symbol('ADSE')


def test_net_margin_remains_outside_the_existing_alert_metric_contract():
    from app.services.custom_alert_rules import validate_conditions
    with pytest.raises(ValueError,match='Choose a supported metric'):
        validate_conditions([dict(metric='net_margin',operator='gt',comparison_type='value',comparison_value=-222)])


@pytest.mark.parametrize('defect',['proof_value','arithmetic','period','fact','source_hash','missing_proof'])
def test_tampered_zero_cannot_enter_projection_or_saved_cache(defect):
    manifest,raw=originals();supplement,_=acquire(raw,manifest)
    value=sec.project_fundamentals(raw['facts'],raw['company'],symbol='ADAG',cik=CIK,observed_at=NOW,filing_supplement=supplement)
    evidence=json.loads(value['source_evidence_json'])
    assert zero.cached_valid(evidence)
    proof=supplement['evidence']['primary_comparative_revenue_zeros'][0]
    if defect=='proof_value':proof['value']=1
    elif defect=='arithmetic':proof['columns'][0]['operating_facts'][0]['value']+=1
    elif defect=='period':proof['end']='2025-03-31'
    elif defect=='fact':
        for p in supplement['facts'][zero.REVENUE]['units']['USD']:
            if p['end']=='2025-06-30':p['val']=1
    elif defect=='source_hash':supplement['evidence']['sha256']='0'*64
    elif defect=='missing_proof':supplement['evidence'].pop('primary_comparative_revenue_zeros')
    with pytest.raises(ValueError):
        sec.project_fundamentals(raw['facts'],raw['company'],symbol='ADAG',cik=CIK,observed_at=NOW,filing_supplement=supplement)
    evidence['inline_filing_supplement']=supplement['evidence']
    if defect!='fact':assert not zero.cached_valid(evidence)
