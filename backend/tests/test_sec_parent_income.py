from copy import deepcopy
import json
import pytest
from test_sec_leverage import prepared_sources
from test_sec_fundamentals import project


def parent_sources(nci=2):
    facts,_=prepared_sources();gaap=facts['facts']['us-gaap']
    income=gaap.pop('NetIncomeLoss')['units']['USD']
    gaap['ProfitLoss']={'units':{'USD':[{**r,'val':r['val']+nci} for r in income]}}
    gaap['NetIncomeLossAttributableToNoncontrollingInterest']={'units':{'USD':[{**r,'val':nci} for r in income]}}
    return facts,income


@pytest.mark.parametrize('nci',[2,0,-2])
def test_parent_income_matches_reported_control_with_positive_zero_or_negative_nci(nci):
    facts,_=parent_sources(nci);original=deepcopy(facts)
    baseline=project(prepared_sources()[0]);result=project(facts)
    assert {k:v for k,v in result.items() if k!='source_evidence_json'}=={k:v for k,v in baseline.items() if k!='source_evidence_json'}
    e=json.loads(result['source_evidence_json']);point=e['current']['net_income']['inputs'][0]
    assert point['derived'] and point['formula'].startswith('reported consolidated')
    assert [x['tag'] for x in point['inputs']]==['ProfitLoss','NetIncomeLossAttributableToNoncontrollingInterest']
    assert 'noncontrolling interest' in e['calculations']['net_margin']
    assert 'noncontrolling interest' in e['calculations']['roe']
    assert e['leverage_preparation']['ebitda']['status']=='components_complete'
    assert result==project(facts) and facts==original


@pytest.mark.parametrize('defect',['missing','filed','accession','period','currency','conflict','redeemable','redeemable_conflict'])
def test_incomplete_or_ambiguous_parent_components_are_held(defect):
    facts,_=parent_sources();g=facts['facts']['us-gaap'];info=g['NetIncomeLossAttributableToNoncontrollingInterest'];row=info['units']['USD'][-1]
    if defect=='missing':del g['NetIncomeLossAttributableToNoncontrollingInterest']
    if defect=='filed':row['filed']='2026-08-02'
    if defect=='accession':row['accn']='0000000001-26-000099'
    if defect=='period':row['start']='2026-04-01'
    if defect=='currency':info['units']['EUR']=info['units'].pop('USD')
    if defect=='conflict':info['units']['USD'].append({**row,'val':99})
    if defect=='redeemable':g['NetIncomeLossAttributableToRedeemableNoncontrollingInterest']={'units':{'USD':[{**row,'val':1}]}}
    if defect=='redeemable_conflict':g['NetIncomeLossAttributableToRedeemableNoncontrollingInterest']={'units':{'USD':[{**row,'val':0},{**row,'val':1}]}}
    result=project(facts)
    assert 'net_margin' not in result
    e=json.loads(result['source_evidence_json'])
    assert e['leverage_preparation']['ebitda']['status']=='incomplete'


def test_reported_parent_wins_and_its_conflicts_are_never_filled():
    facts,income=parent_sources();facts['facts']['us-gaap']['NetIncomeLoss']={'units':{'USD':deepcopy(income)}}
    control=project(prepared_sources()[0]);result=project(facts)
    assert result['net_margin']==control['net_margin']
    assert 'noncontrolling' not in json.loads(result['source_evidence_json'])['calculations']['net_margin']
    rows=facts['facts']['us-gaap']['NetIncomeLoss']['units']['USD'];rows.append({**rows[-1],'val':999})
    assert 'net_margin' not in project(facts)



def test_market_valuations_retain_derived_parent_income_basis(monkeypatch):
    from app.services import sec_fundamentals as sec, sec_inline_financials as inline
    from app.clients.direct_sources import DirectSourceClient
    from app.clients.massive_stocks import MassiveStocksClient
    from test_sec_fundamentals import sources, NOW
    from datetime import datetime
    facts,_ = parent_sources()
    company = sources()[1]
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None): return NOW
    monkeypatch.setattr(sec, 'datetime', Clock)
    monkeypatch.setattr(sec, 'company_directory', lambda day: {'ABC': {'cik':'0000000001'}})
    monkeypatch.setattr(DirectSourceClient, 'get', lambda self,url: json.dumps(facts if 'companyfacts' in url else company).encode())
    monkeypatch.setattr(inline, 'acquire', lambda *a, **k: None)
    monkeypatch.setenv('STOCK_PRICE_PROVIDER', 'massive')
    monkeypatch.setenv('MASSIVE_API_KEY', 'test-only')
    monkeypatch.setattr(MassiveStocksClient, 'details', lambda self,s: {'cik':'1', 'market_cap':1300})
    monkeypatch.setattr(MassiveStocksClient, 'bars', lambda *a,**k: [])
    result = sec.fetch_fundamentals('ABC')
    assert result['trailing_pe'] == 100 and result['earnings_yield'] == 1
    e=json.loads(result['source_evidence_json'])
    for field in ('net_margin','roe','earnings_yield','trailing_pe'):
        assert 'noncontrolling interest' in e['calculations'][field]


def test_original_inline_parent_components_remain_explicit_evidence():
    from test_sec_inline_financials import document, fact, parse, URL
    from app.services import sec_fundamentals as sec
    from test_sec_fundamentals import sources, NOW
    raw=document(primary=fact('70',tag='RevenueFromContractWithCustomerExcludingAssessedTax'),extra=fact('9',tag='ProfitLoss')+fact('2',tag='NetIncomeLossAttributableToNoncontrollingInterest'))
    supplement=parse(raw)
    assert supplement['facts']['ProfitLoss']['units']['USD'][0]['val'] == 9
    assert supplement['facts']['NetIncomeLossAttributableToNoncontrollingInterest']['units']['USD'][0]['val'] == 2
    data,_ = parent_sources()
    for tag in ('ProfitLoss','NetIncomeLossAttributableToNoncontrollingInterest'):
        data['facts']['us-gaap'][tag]['units']['USD'] = [r for r in data['facts']['us-gaap'][tag]['units']['USD'] if r['end']!='2026-06-30']
    result=sec.project_fundamentals(json.dumps(data).encode(),json.dumps(sources()[1]).encode(),
        symbol='ABC',cik='1',observed_at=NOW,filing_supplement=supplement)
    income=json.loads(result['source_evidence_json'])['current']['net_income']
    assert income['value'] == 13
    closing=next(r for r in income['inputs'] if r['end']=='2026-06-30')
    assert closing['value'] == 7
    assert all(r['source_url']==URL for r in closing['inputs'])
