from copy import deepcopy
from datetime import date
import pytest
from app.services import sec_statement_income as subject
from app.services.sec_fundamentals_preparation import version_for_symbol


def fixture(symbol='NVDA', index=0):
    profile=subject._reviews()[symbol];proof=profile['periods'][index]
    key=tuple(date.fromisoformat(proof[k]) for k in ('start','end'))
    fact={k:proof[k] for k in ('start','end','filed','accession','value')}
    fact.update(tag='NetIncomeLoss',unit='USD',source_cik=profile['cik'])
    return {'net_income':{key:fact},'leverage_net_income':{}},key,profile,proof


@pytest.mark.parametrize('symbol',['NVDA','JNJ'])
@pytest.mark.parametrize('index',[0,1,2])
def test_exact_source_review_preserves_parent_and_repeats(symbol,index):
    records,key,profile,proof=fixture(symbol,index);before=deepcopy(records)
    output,evidence=subject.apply(records,symbol=symbol,cik=profile['cik'])
    assert records==before and output['net_income']==before['net_income']
    promoted=output['leverage_net_income'][key]
    assert promoted['value']==proof['value'] and promoted['consolidated_statement_review']['source_sha256']==proof['source_sha256']
    assert subject.apply(records,symbol=symbol,cik=profile['cik'])==(output,evidence)
    assert evidence['accepted_periods']==[promoted['consolidated_statement_review']]
    assert version_for_symbol(symbol).endswith(subject.VERSION)


@pytest.mark.parametrize('field',['start','end','filed','accession','value','unit','source_cik','source_sha256','source_url','context_id','tag','derived','missing','conflict','nci','redeemable','nci_conflict'])
def test_changed_or_unreviewed_sources_remain_held(field):
    records,key,profile,proof=fixture();fact=records['net_income'][key]
    if field=='missing':records['net_income'].clear()
    elif field=='conflict':records['net_income'][key]=None
    elif field in {'nci','redeemable','nci_conflict'}:
        name='parent_income_redeemable' if field=='redeemable' else 'parent_income_noncontrolling'
        records[name]={key:None if field=='nci_conflict' else {**fact,'value':1}}
    elif field=='derived':fact[field]=True
    elif field=='value':fact[field]+=1
    else:fact[field]='changed'
    before=deepcopy(records)
    output,evidence=subject.apply(records,symbol='NVDA',cik=profile['cik'])
    assert not output['leverage_net_income'] and not evidence['accepted_periods']
    assert records==before and evidence['holds']


@pytest.mark.parametrize('reported',[None,{'value':123}])
def test_explicit_consolidated_observation_or_conflict_takes_priority(reported):
    records,key,profile,proof=fixture();records['leverage_net_income'][key]=reported
    output,evidence=subject.apply(records,symbol='NVDA',cik=profile['cik'])
    assert output['leverage_net_income'][key]==reported and not evidence['accepted_periods']


def test_unknown_issuer_and_future_period_do_not_inherit_review():
    records,key,profile,proof=fixture()
    assert subject.apply(records,symbol='HCA',cik='0000860730')==(records,None)
    with pytest.raises(ValueError,match='cik_mismatch'):subject.apply(records,symbol='NVDA',cik='1')
    fact=records['net_income'].pop(key);future=(date(2026,1,26),date(2026,10,25))
    records['net_income'][future]={**fact,'start':str(future[0]),'end':str(future[1])}
    output,evidence=subject.apply(records,symbol='NVDA',cik=profile['cik'])
    assert not output['leverage_net_income'] and not evidence['accepted_periods']


def test_registry_preserves_actual_original_proof_and_no_arithmetic_gap():
    for symbol,profile in subject._reviews().items():
        assert len(profile['periods'])==3 and profile['basis']
        for proof in profile['periods']:
            assert proof['pretax']-proof['tax']==proof['value'] and proof['unexplained_difference']==0
            assert len(proof['source_sha256'])==64 and proof['context'] and proof['table_index']>=0
            assert proof['url'].startswith('https://www.sec.gov/Archives/edgar/data/'+str(int(profile['cik']))+'/')
