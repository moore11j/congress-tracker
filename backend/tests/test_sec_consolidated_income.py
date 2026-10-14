from copy import deepcopy
from datetime import date
import json
import pytest
from app.services import sec_parent_income as subject
from test_sec_leverage import prepared_sources, panel

KEY=(date(2026,1,1),date(2026,6,30))
def fact(value,tag):
    return dict(value=value,tag=tag,start='2026-01-01',end='2026-06-30',filed='2026-07-28',accession='0001-26-1',unit='USD',source_cik='0000000001')
def records(nci=2):
    return {'net_income':{KEY:fact(10,'NetIncomeLoss')},'leverage_net_income':{},
            'parent_income_noncontrolling':{KEY:fact(nci,'NetIncomeLossAttributableToNoncontrollingInterest')}}

@pytest.mark.parametrize('nci',[2,0,-2])
def test_reported_parent_plus_nci_derives_consolidated_without_changing_parent(nci):
    raw=records(nci);before=deepcopy(raw);out,proof=subject.consolidated_for_leverage(raw)
    assert raw==before and subject.consolidated_for_leverage(raw)==(out,proof)
    assert out['leverage_net_income'][KEY]['value']==10+nci
    assert out['net_income']==raw['net_income'] and proof['derived_periods']==1
    assert out['leverage_net_income'][KEY]['inputs']==[raw['net_income'][KEY],raw['parent_income_noncontrolling'][KEY]]
    after,_=subject.apply(out)
    assert after['leverage_net_income']==out['leverage_net_income']

@pytest.mark.parametrize('change',['missing','conflict','filed','accession','start','end','currency','cik','derived_parent','nonfinite','overflow','redeemable','redeemable_conflict','identity'])
def test_missing_or_incompatible_allocation_never_becomes_zero(change):
    raw=records();nci=raw['parent_income_noncontrolling'][KEY]
    if change=='missing':raw['parent_income_noncontrolling'].clear()
    elif change=='conflict':raw['parent_income_noncontrolling'][KEY]=None
    elif change in ['filed','accession','start','end']:nci[change]='wrong'
    elif change=='currency':nci['unit']='EUR'
    elif change=='cik':nci['source_cik']='2'
    elif change=='derived_parent':raw['net_income'][KEY]['derived']=True
    elif change=='nonfinite':nci['value']=float('nan')
    elif change=='overflow':nci['value']=1e308;raw['net_income'][KEY]['value']=1e308
    elif change=='redeemable':raw['parent_income_redeemable']={KEY:fact(1,'other')}
    elif change=='redeemable_conflict':raw['parent_income_redeemable']={KEY:None}
    elif change=='identity':nci['accession']=raw['net_income'][KEY]['accession']=''
    out,_=subject.consolidated_for_leverage(raw)
    assert not out['leverage_net_income']

@pytest.mark.parametrize('reported',[None,12])
def test_reported_consolidated_or_reported_conflict_is_not_overwritten(reported):
    raw=records();raw['leverage_net_income'][KEY]=None if reported is None else fact(reported,'ProfitLoss')
    out,proof=subject.consolidated_for_leverage(raw)
    assert out['leverage_net_income']==raw['leverage_net_income'] and proof['derived_periods']==0

def test_consolidated_ttm_changes_leverage_but_not_parent_ratios():
    facts,_=prepared_sources();before,_=panel(facts)
    g=facts['facts']['us-gaap']
    for row in g['ProfitLoss']['units']['USD']:row['val']+=2
    result,proof=panel(facts)
    assert proof['ebitda']['value']==pytest.approx(22.8)
    assert 'consolidated' in proof['ebitda']['income_basis']
    assert {k:v for k,v in before.items() if k!='source_evidence_json'}=={k:v for k,v in result.items() if k!='source_evidence_json'}
    assert all(f['tag']=='ProfitLoss' for f in proof['ebitda']['components']['leverage_net_income']['inputs'])
    assert 'net_debt_to_ebitda' not in result

def test_missing_consolidated_and_nci_holds_leverage_but_retains_shareholder_ratios():
    facts,_=prepared_sources();del facts['facts']['us-gaap']['ProfitLoss']
    result,proof=panel(facts)
    assert 'net_margin' in result and proof['ebitda']['status']=='incomplete'
    assert all('net_debt_to_ebitda' not in r for r in proof['candidates'])

def test_explicit_nci_restores_missing_consolidated_ttm():
    facts,_=prepared_sources();g=facts['facts']['us-gaap'];del g['ProfitLoss']
    g['NetIncomeLossAttributableToNoncontrollingInterest']={'units':{'USD':[{**r,'val':2} for r in g['NetIncomeLoss']['units']['USD']]}}
    result,proof=panel(facts)
    assert proof['ebitda']['value']==pytest.approx(22.8)
    assert proof['income_reconciliation']['derived_periods']==len(g['NetIncomeLoss']['units']['USD'])
    assert all(f['tag']=='derived_consolidated_net_income' for f in proof['ebitda']['components']['leverage_net_income']['inputs'])
