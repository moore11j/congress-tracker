"""Complete originals, conflict preservation and retained-consumer checks."""
from copy import deepcopy
from datetime import datetime,timezone
from functools import lru_cache
from pathlib import Path
from unittest.mock import patch
import gzip,hashlib,json,socket
import pytest
from lxml import html,etree
from app.services import sec_common_equity as equity,sec_inline_financials as inline,sec_fundamentals as sec,sec_financial_statements as statements

ROOT=Path(__file__).parent/'fixtures/sec_common_equity'
MANIFEST=json.loads((ROOT/'sources.json').read_text())
NOW=datetime(2026,11,9,7,tzinfo=timezone.utc);CIK='0000922224'
@lru_cache(maxsize=4)
def raw(name):
    m=MANIFEST[name];data=gzip.decompress((ROOT/m['file']).read_bytes())
    assert len(data)==m['bytes'] and hashlib.sha256(data).hexdigest()==m['sha256']
    return data
class Client:
    def __init__(self):self.calls=[]
    def get(self,url):
        self.calls.append(url);matches=[n for n,m in MANIFEST.items() if m['url']==url]
        assert len(matches)==1,'Unretained source '+url
        return raw(matches[0])
def inputs():return json.loads(raw('PPLC-facts.gz')),json.loads(raw('PPLC-company.gz'))
@lru_cache(maxsize=1)
def acquired():
    with patch.object(socket.socket,'connect',side_effect=AssertionError('Unexpected network')):
        c=Client();item=inline.acquire(raw('PPLC-facts.gz'),raw('PPLC-company.gz'),symbol='PPLC',cik=CIK,observed_at=NOW,client=c)
        assert len(c.calls)==len(set(c.calls))==2
        return item
def project(item):return sec.project_fundamentals(raw('PPLC-facts.gz'),raw('PPLC-company.gz'),symbol='PPLC',cik=CIK,observed_at=NOW,filing_supplement=item)
def planned():
    payload,company=inputs();payload=inline.merge(payload,acquired(),available_by=NOW.date())
    return payload,company,equity.plan(payload,company,cik=CIK,available_by=NOW.date())

def test_full_originals_restore_only_roe_and_all_readers_repeat():
    before_item=deepcopy(acquired());before_item.pop('common_equity');before_item['evidence'].pop('common_equity_reconciliation')
    before=project(before_item);after=project(acquired())
    assert 'roe' not in before
    assert {k:v for k,v in after.items() if k not in {'roe','source_evidence_json'}}=={k:v for k,v in before.items() if k!='source_evidence_json'}
    assert after['roe']==1315000000/((15209000000+14413000000)/2)*100
    assert after.get('ev_to_ebitda') is None and after.get('net_debt_to_ebitda') is None
    proof=acquired()['evidence']['common_equity_reconciliation']
    assert len(proof['rows'])==2 and equity.cached_valid(proof,cik=CIK)
    assert project(acquired())==after
    panel=statements.project(raw('PPLC-facts.gz'),raw('PPLC-company.gz'),symbol='PPLC',cik=CIK,observed_at=NOW,filing_supplement=acquired())
    assert panel['sourceEvidence']['inlineFilingSupplement']['common_equity_reconciliation']==proof
    from types import SimpleNamespace
    from app.services.fundamentals_cache import fundamentals_summary_from_cache_row
    summary=fundamentals_summary_from_cache_row(SimpleNamespace(**after),now=NOW)
    assert summary['metrics']['return_on_equity']['provenance']['version']==equity.VERSION
    assert equity.BASIS in summary['context']['source_note']

@pytest.mark.parametrize('change',['raw','proof','displayed_proof','missing_original'])
def test_consumer_rechecks_original_and_proof(change):
    item=deepcopy(acquired());h=item['common_equity'];url=next(iter(h['documents']))
    if change=='raw':h['documents'][url]=h['documents'][url].replace(b'14,413',b'14,414').replace(b'15,209',b'15,210')
    elif change=='proof':h['evidence']['rows'][0]['value']+=1
    elif change=='displayed_proof':item['evidence']['common_equity_reconciliation']=None
    else:del h['documents'][url]
    with pytest.raises((ValueError,KeyError)):project(item)

@pytest.mark.parametrize('change',['unknown_equity','preferred','wrong_tag','wrong_context','hidden','nil','owner','subtotal','treasury_sign','asset_conflict'])
def test_complete_statement_rejects_changes(change):
    payload,company,plan=planned();filing=plan['filings'][-1]
    tree=html.fromstring(raw('PPLC-0000922224-26-000052.gz'))
    chosen=[n for n in tree.iter('ix:nonfraction') if n.get('name')=='us-gaap:AdditionalPaidInCapitalCommonStock']
    table=next(n.xpath('ancestor::table')[-1] for n in chosen if 'PPL Corporation and Subsidiaries' in __import__('app.services.sec_tabular_financials',fromlist=['preceding_text']).preceding_text(n.xpath('ancestor::table')[-1]))
    if change in {'unknown_equity','preferred','subtotal'}:
        node=next(n for n in table.iter('ix:nonfraction') if n.get('name')=='us-gaap:AdditionalPaidInCapitalCommonStock')
        row=deepcopy(node.xpath('ancestor::tr')[-1]);first=row.xpath('./td|./th')[0];first.clear();first.text={'unknown_equity':'Other equity','preferred':'Preferred stock','subtotal':'Total Equity'}[change]
        original=node.xpath('ancestor::tr')[-1];original.addnext(row)
    elif change=='owner':company['name']='Different Corporation'
    else:
        tag='TreasuryStockCommonValue' if change=='treasury_sign' else 'LiabilitiesAndStockholdersEquity' if change=='asset_conflict' else 'AdditionalPaidInCapitalCommonStock'
        for node in list(table.iter('ix:nonfraction')):
            if node.get('name')!='us-gaap:'+tag:continue
            if change=='wrong_tag':node.set('name','us-gaap:PreferredStockValue')
            elif change=='wrong_context':node.set('contextref','unowned')
            elif change=='hidden':
                parent=node.getparent();hidden=html.fromstring('<ix:hidden></ix:hidden>');parent.replace(node,hidden);hidden.append(node)
            elif change=='nil':node.set('xsi:nil','true')
            elif change=='treasury_sign':node.set('sign','-')
            else:node.text='1'
    try:rows=equity.statement_rows(etree.tostring(tree,method='html'),filing,issuer=company['name'])
    except ValueError:rows={}
    assert not rows

@pytest.mark.parametrize('change',['method','value','tag','balance','assets','liability_items','basis','source','cik','duplicate','label','order','column','liability_tag'])
def test_cached_proof_rejects_corruption(change):
    p=deepcopy(acquired()['evidence']['common_equity_reconciliation']);r=p['rows'][0]
    if change=='method':p['methodology']='other'
    elif change=='value':r['value']+=1
    elif change=='tag':r['components']['common']['tag']='PreferredStockValue'
    elif change=='balance':r['components']['balance']['value']+=1
    elif change=='assets':r['assets']['value']+=1
    elif change=='liability_items':r['components']['current']['items'][0]['value']+=1
    elif change=='basis':r['basis']='Reported parent equity'
    elif change=='source':r['source_url']='https://example.com/a'
    elif change=='cik':r['source_cik']='0000000001'
    elif change=='label':r['components']['paid']['label']='Preferred equity'
    elif change=='order':r['components']['paid']['row']=0
    elif change=='column':r['column']['end']='2026-09-29'
    elif change=='liability_tag':r['components']['current']['items'][0]['tag']='CommonStockValue'
    else:p['rows'][1]=deepcopy(r)
    assert not equity.cached_valid(p,cik=CIK)

def test_no_ticker_allowlist_and_reported_conflicts_or_negative_values_take_precedence():
    payload,company,plan=planned();company['tickers']=['OTHER']
    assert equity.plan(payload,company,cik=CIK,available_by=NOW.date())==plan
    for value in (-1,100):
        copy=deepcopy(payload)
        copy['facts']['us-gaap']['StockholdersEquity']={'units':{'USD':[dict(end=f['report_date'],val=value,filed=f['filed'],accn=f['accession'],form=f['form']) for f in plan['filings']]}}
        assert equity.plan(copy,company,cik=CIK,available_by=NOW.date()) is None
    copy=deepcopy(payload);f=plan['filings'][0]
    copy['facts']['us-gaap']['StockholdersEquity']={'units':{'USD':[dict(end=f['report_date'],val=v,filed=f['filed'],accn=f['accession'],form=f['form']) for v in (1,2)]}}
    next_plan=equity.plan(copy,company,cik=CIK,available_by=NOW.date())
    assert all(p['report_date']!=f['report_date'] for p in next_plan['filings'])

def test_stable_alert_method_rebaselines_without_changing_other_metrics():
    from app.models import FundamentalsCache
    from app.services.custom_alert_rules import _fundamental_basis
    p=project(acquired());row=FundamentalsCache(symbol='PPLC',provider='sec_edgar',source_evidence_json=p['source_evidence_json'])
    original=_fundamental_basis(row,{'roe','revenue_growth'})
    assert original['roe']['equity_measurement']==equity.VERSION
    evidence=json.loads(row.source_evidence_json);evidence['inline_filing_supplement']['common_equity_reconciliation']['rows'][0]['value']+=1
    row.source_evidence_json=json.dumps(evidence);assert _fundamental_basis(row,{'roe','revenue_growth'})==original
    del evidence['inline_filing_supplement']['common_equity_reconciliation'];row.source_evidence_json=json.dumps(evidence)
    changed=_fundamental_basis(row,{'roe','revenue_growth'})
    assert changed['roe']!=original['roe'] and changed['revenue_growth']==original['revenue_growth']

def test_qualified_original_cache_cannot_be_mutated_by_a_consumer():
    payload,company,plan=planned();filing=plan['filings'][-1];data=raw('PPLC-0000922224-26-000052.gz')
    result=equity.statement_rows(data,filing,issuer=company['name']);original=deepcopy(result)
    result['2026-09-30']['value']=0
    assert equity.statement_rows(data,filing,issuer=company['name'])==original

def test_current_facts_conflict_and_changed_interest_hold_the_derivation():
    payload,company,plan=planned();f=plan['filings'][0];documents=acquired()['common_equity']['documents']
    for tag,value in [('CommonStockValue',1),('MinorityInterest',1),('PreferredStockValue',1)]:
        changed=deepcopy(payload)
        changed['facts']['us-gaap'][tag]={'units':{'USD':[dict(end=f['report_date'],val=value,filed=f['filed'],accn=f['accession'],form=f['form'])]}}
        result=equity.qualify(plan,documents,changed,issuer=company['name'],available_by=NOW.date())
        assert all(r['end']!=f['report_date'] for r in result['rows'])

def test_source_score_and_watchlist_recovery_repeat_crossing_rollback(monkeypatch):
    import test_sec_tagged_foreign_consumers as consumer
    urls={m['url']:raw(n) for n,m in MANIFEST.items()}
    monkeypatch.setattr(consumer,'originals',lambda symbol:(raw('PPLC-facts.gz'),raw('PPLC-company.gz'),CIK,urls))
    values=project(acquired());metrics={k:values[k] for k in json.loads(values['source_evidence_json'])['calculations']}
    monkeypatch.setattr(consumer,'EXPECTED',{'PPLC':{'metrics':metrics}});monkeypatch.setattr(consumer,'NOW',NOW)
    consumer.test_original_source_score_and_alert_recovery_repeat_crossing_rollback('PPLC','roe',monkeypatch)
