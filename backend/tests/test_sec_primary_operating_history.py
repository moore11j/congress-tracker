"""Complete originals through acquisition, projection and retained proof checks."""
from copy import deepcopy
from datetime import date, datetime, timezone
from functools import lru_cache
from pathlib import Path
import gzip
import hashlib
import json
import pytest

from app.services import sec_primary_operating_history as primary
from app.services import sec_operating_history as history
from app.services import sec_inline_financials as inline
from app.services import sec_fundamentals as sec

ROOT = Path(__file__).parent/'fixtures/sec_primary_operating_subtotal'
SOURCES = json.loads((ROOT/'sources.json').read_bytes())
COMPANYFACTS = ROOT.parent/'sec_primary_operating_companyfacts'
FULL_INPUTS = json.loads((COMPANYFACTS/'manifest.json').read_bytes())['rows']
AVAILABLE = date(2026, 11, 24)
NOW = datetime(2026, 11, 25, 18, tzinfo=timezone.utc)


@lru_cache(maxsize=2)
def inputs(symbol):
    entries = [e for e in SOURCES if e['source']['symbol'] == symbol]
    captured = {}
    for source in FULL_INPUTS:
        if source['symbol'] != symbol:
            continue
        raw = gzip.decompress((COMPANYFACTS/source['file']).read_bytes())
        assert len(raw) == source['bytes'] and hashlib.sha256(raw).hexdigest() == source['sha256']
        captured[source['kind']] = json.loads(raw)
    company, payload = captured['company'], captured['facts']
    assert str(company['cik']).zfill(10) == str(payload['cik']).zfill(10) == entries[0]['source']['cik']
    documents = {}
    for e in entries:
        s = e['source']; raw = gzip.decompress((ROOT/e['file']).read_bytes())
        assert len(raw) == s['bytes'] and hashlib.sha256(raw).hexdigest() == s['sha256']
        documents[s['url']] = raw
    return payload, company, documents


class RetainedClient:
    def __init__(self, documents): self.documents, self.calls = documents, []
    def get(self, url):
        assert url not in self.calls, 'Original fetched twice'
        self.calls.append(url)
        return self.documents[url]


@lru_cache(maxsize=2)
def acquired(symbol):
    payload, company, documents = inputs(symbol)
    client = RetainedClient(documents)
    result = history.acquire(payload, company, cik=str(company['cik']).zfill(10), available_by=AVAILABLE, client=client)
    assert set(client.calls) == set(documents) and len(client.calls) == 3
    return result


@lru_cache(maxsize=2)
def full_acquired(symbol):
    payload, company, documents = inputs(symbol)
    client = RetainedClient(documents)
    result = inline.acquire(json.dumps(payload).encode(), json.dumps(company).encode(), symbol=symbol,
        cik=str(company['cik']).zfill(10), observed_at=NOW, client=client)
    assert set(client.calls) == set(documents) and len(client.calls) == 3
    return result


def project(symbol, supplement):
    payload, company, _ = inputs(symbol)
    return sec.project_fundamentals(json.dumps(payload).encode(), json.dumps(company).encode(), symbol=symbol,
        cik=str(company['cik']).zfill(10), observed_at=NOW, filing_supplement=supplement)


@pytest.mark.parametrize('symbol,amounts', [('DMLP',(103781000.,55403000.)),('FIP',(-22871000.,-71147000.))])
def test_full_original_history_recovery_and_repeat(symbol, amounts):
    payload, company, documents = inputs(symbol); cik=str(company['cik']).zfill(10)
    result=acquired(symbol);assert result and history.cached_valid(result['evidence'],cik=cik)
    supplement={'operating_history_only':True,'operating_history':result,
                'evidence':{'cik':cik,'methodology':primary.VERSION,'operating_statement_history':result['evidence']}}
    original_operating=sec._records(payload,'operating_income',AVAILABLE)
    records={'operating_income':deepcopy(original_operating),'net_income':{'untouched':None}}
    output=history.apply(records,payload,company,cik=cik,available_by=AVAILABLE,supplement=supplement)
    assert records['operating_income']==original_operating and output['net_income']==records['net_income']
    assert all(output['operating_income'][key]==value for key,value in original_operating.items())
    if symbol=='DMLP':
        assert len(original_operating)==37 and max(end for start,end in original_operating)==date(2016,12,31)
    assert tuple(sec._ttm(output['operating_income'],end)['value'] for end in [date(2026,9,30),date(2025,9,30)])==amounts
    assert all(p['derived'] and p['source_kind']==primary.VERSION and p['original_statement']['primary_reconciliation']
               for key,p in output['operating_income'].items() if key not in original_operating)
    assert history.apply(records,payload,company,cik=cik,available_by=AVAILABLE,supplement=supplement)==output
    changed=deepcopy(supplement);changed['operating_history']['evidence']['rows'][0]['value']+=1
    with pytest.raises(inline.InlineFactsError):history.apply(records,payload,company,cik=cik,available_by=AVAILABLE,supplement=changed)


@pytest.mark.parametrize('mutation',['existing','conflict','missing_cost','wrong_accession','future','foreign','stale'])
def test_no_request_for_unqualified_or_existing_series(mutation):
    payload,company,documents=deepcopy(inputs('DMLP')); available=AVAILABLE
    facts=payload['facts']['us-gaap']; revenue=next(facts[t] for t in sec.TAGS['revenue'] if t in facts)
    if mutation in {'existing','conflict'}:
        facts['OperatingIncomeLoss']=deepcopy(revenue)
        if mutation=='conflict':
            rows=facts['OperatingIncomeLoss']['units']['USD']; p=deepcopy(rows[0]);p['val']+=1;rows.append(p)
    elif mutation=='missing_cost':del facts['CostsAndExpenses']
    elif mutation=='wrong_accession':
        for p in facts['CostsAndExpenses']['units']['USD']:p['accn']='0000000000-26-000001'
    elif mutation=='future':
        recent=company['filings']['recent'];recent['filingDate']=['2027-11-25']*len(recent['filingDate'])
    elif mutation=='stale':available=date(2027,9,1)
    else:
        recent=company['filings']['recent'];i=recent['form'].index('10-Q');recent['form'][i]='20-F'
    client=RetainedClient({})
    assert primary.acquire(payload,company,cik=str(company['cik']).zfill(10),available_by=available,client=client) is None
    assert client.calls==[]


def test_point_in_time_plan_uses_only_filings_already_available():
    payload,company,_=inputs('DMLP');available=date(2026,1,1)
    result=primary.plan(payload,company,cik=str(company['cik']).zfill(10),available_by=available)
    assert result and result['period_end']=='2025-09-30'
    assert all(row['filing']['filingDate']<=str(available) for row in result['rows'])
    assert all(row['filing']['accessionNumber']!='0001437749-26-035295' for row in result['rows'])


@pytest.mark.parametrize('conflict',[False,True])
def test_reported_quarter_inside_comparison_window_holds_entire_derived_series(conflict):
    payload,company,_=deepcopy(inputs('DMLP'))
    units=payload['facts']['us-gaap']['OperatingIncomeLoss']['units']['USD']
    row=deepcopy(units[-1])
    row.update(start='2024-04-01',end='2024-06-30',filed='2024-08-01',
               accn='0001437749-24-000001',form='10-Q',val=1000)
    row.pop('frame',None);units.append(row)
    if conflict:
        other=deepcopy(row);other['val']=2000;units.append(other)
    client=RetainedClient({})
    assert history.acquire(payload,company,cik=str(company['cik']).zfill(10),available_by=AVAILABLE,client=client) is None
    assert client.calls==[]


def test_existing_projected_comparison_value_is_not_overwritten():
    payload,company,_=inputs('DMLP');item=acquired('DMLP')
    supplement={'operating_history':item,'evidence':{'operating_statement_history':item['evidence']}}
    records={'operating_income':{(date(2024,4,1),date(2024,6,30)):None}}
    assert primary.apply(records,payload,company,cik=str(company['cik']).zfill(10),available_by=AVAILABLE,supplement=supplement) is records


@pytest.mark.parametrize('mutation',['amount','cost','expense','hash','unit','cik','period','bridge','geometry'])
def test_cached_proof_corruption_rejected(mutation):
    proof=deepcopy(acquired('FIP')['evidence']);row=proof['rows'][0];p=row['primary_reconciliation']
    if mutation=='amount':row['value']+=1
    elif mutation=='cost':p['cost_total']['value']+=1
    elif mutation=='expense':p['expenses'][0]['tag']='us-gaap:InterestExpense'
    elif mutation=='hash':row['source_sha256']='bad'
    elif mutation=='unit':row['unit']='EUR'
    elif mutation=='cik':row['source_cik']='0000000001'
    elif mutation=='period':row['end']='2027-09-30'
    elif mutation=='bridge':p['bridge']['pretax']['value']+=1
    else:row['geometry']['checked_numeric_cells']=0
    assert not history.cached_valid(proof,cik=proof['cik'])


@pytest.mark.parametrize('symbol',['DMLP','FIP'])
def test_full_acquisition_projection_cache_and_source_label(symbol,monkeypatch):
    from types import SimpleNamespace
    import socket
    from app.services.fundamentals_cache import fundamentals_summary_from_cache_row
    from app.services.sec_tagged_foreign import cached_currency_valid
    monkeypatch.setattr(socket.socket,'connect',lambda *a,**k:pytest.fail('Unexpected provider call'))
    before=project(symbol,None);after=project(symbol,full_acquired(symbol))
    old=json.loads(before['source_evidence_json']);evidence=json.loads(after['source_evidence_json'])
    assert all(after[k]==before[k] for k in old['calculations'])
    assert before.get('operating_margin') is None and before.get('operating_margin_expansion') is None
    assert after['operating_margin'] is not None and after['operating_margin_expansion'] is not None
    assert after.get('net_debt_to_ebitda')==before.get('net_debt_to_ebitda')
    assert after.get('roe')==before.get('roe')
    assert cached_currency_valid(evidence)
    assert project(symbol,full_acquired(symbol))==after
    summary=fundamentals_summary_from_cache_row(SimpleNamespace(**after),now=NOW)
    assert summary['metrics']['operating_margin_expansion']['provenance']['version']==primary.VERSION
    changed=deepcopy(evidence);changed['inline_filing_supplement']['operating_statement_history']['rows'][0]['value']+=1
    assert not cached_currency_valid(changed)


@pytest.mark.parametrize('symbol',['DMLP','FIP'])
def test_score_alert_recovery_repeat_and_rollback(symbol,monkeypatch):
    import test_sec_tagged_foreign_consumers as consumer
    payload,company,documents=inputs(symbol);cik=str(company['cik']).zfill(10)
    monkeypatch.setattr(consumer,'originals',lambda name:(json.dumps(payload).encode(),json.dumps(company).encode(),cik,documents))
    values=project(symbol,full_acquired(symbol));metrics={k:values[k] for k in json.loads(values['source_evidence_json'])['calculations']}
    monkeypatch.setattr(consumer,'EXPECTED',{symbol:{'metrics':metrics}});monkeypatch.setattr(consumer,'NOW',NOW)
    consumer.test_original_source_score_and_alert_recovery_repeat_crossing_rollback(symbol,'operating_margin',monkeypatch)


def test_basis_change_rebaselines_without_using_current_numbers():
    from app.models import FundamentalsCache
    from app.services.custom_alert_rules import _fundamental_basis
    evidence={'methodology':sec.VERSION,'inline_filing_supplement':{'operating_statement_history':{'methodology':primary.VERSION,'rows':[]}}}
    row=FundamentalsCache(symbol='TEST',provider='sec_edgar',source_evidence_json=json.dumps(evidence))
    before=_fundamental_basis(row,{'operating_margin','revenue_growth'})
    assert before['operating_margin']['operating_measurement']==[primary.VERSION]
    evidence['inline_filing_supplement']['operating_statement_history']['rows']=[{'value':999}]
    row.source_evidence_json=json.dumps(evidence);assert _fundamental_basis(row,{'operating_margin','revenue_growth'})==before
    evidence['inline_filing_supplement']={};row.source_evidence_json=json.dumps(evidence)
    after=_fundamental_basis(row,{'operating_margin','revenue_growth'})
    assert before['operating_margin']!=after['operating_margin'] and before['revenue_growth']==after['revenue_growth']
