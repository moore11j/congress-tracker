from copy import deepcopy
from datetime import date
import pytest
from app.services import sec_parent_equity as subject

KEY = (None, date(2026, 6, 30))
CIK = subject.PROFILES['AMAL'][0]


def records(symbol='AMAL'):
    # Original AMAL June 2026 balance sheet, thousands of USD.
    amounts = {'equity_common':315, 'equity_paid_in':296491, 'equity_retained':616925,
        'equity_aoci':-40796, 'equity_treasury':37942, 'equity_inclusive':834993,
        'equity_assets':9411541, 'equity_liabilities':8576548}
    tags = subject.tags_for(symbol, subject.PROFILES[symbol][0])
    return {'equity':{}, **{name:{KEY:{'value':v*1000, 'tag':tags[name][0],
        'unit':'USD', 'start':None, 'end':'2026-06-30', 'filed':'2026-08-04',
        'accession':'0001823608-26-000182', 'priority':0}} for name,v in amounts.items()}}


def apply(rows, payload=None, symbol='AMAL'):
    cik=subject.PROFILES[symbol][0]
    return subject.apply(rows, symbol=symbol, cik=cik, payload=payload or {'cik':cik})


def test_actual_equity_has_both_reconciliations_and_repeat_identity():
    rows=records(); before=deepcopy(rows)
    result,proof=apply(rows); point=result['equity'][KEY]
    assert point['value']==point['balance_reconciliation']==834993000
    assert len(point['inputs'])==8 and not proof['holds']
    assert point['derivation']==subject.VERSION and len(proof['reviewed_sources'])==2
    assert rows==before and apply(rows)==(result,proof)
    point['inputs']['equity_common']['value']=0
    assert rows==before


@pytest.mark.parametrize('name',tuple(n for n in records() if n != 'equity'))
@pytest.mark.parametrize('mode',['missing','conflict'])
def test_every_component_is_required(name,mode):
    rows=records()
    rows[name]={} if mode=='missing' else {KEY:None}
    result,proof=apply(rows)
    assert not result['equity']


@pytest.mark.parametrize('name',('equity_inclusive','equity_assets','equity_liabilities','equity_aoci'))
def test_changed_component_or_structure_does_not_balance(name):
    rows=records(); rows[name][KEY]['value']+=1000
    result,proof=apply(rows)
    assert not result['equity'] and proof['holds'][0]['reason']=='equity_or_balance_reconciliation_failed'


@pytest.mark.parametrize('field,value', [('unit','EUR'),('start','2026-01-01'),('end','2026-03-31'),
    ('filed','2026-08-05'),('accession','other'),('source_cik','0000320193'),('derived',True),('tag','Other')])
def test_incompatible_component_is_held(field,value):
    rows=records();rows['equity_retained'][KEY][field]=value
    assert not apply(rows)[0]['equity']


@pytest.mark.parametrize('value',[float('nan'),float('inf'),float('-inf'),True,'0',10**400])
def test_invalid_number_is_held(value):
    rows=records();rows['equity_retained'][KEY]['value']=value
    assert not apply(rows)[0]['equity']


@pytest.mark.parametrize('tag',subject.GUARD_TAGS)
@pytest.mark.parametrize('value',[None,1])
def test_minority_or_preferred_structure_change_blocks_even_if_totals_match(tag,value):
    rows=records(); point=deepcopy(rows['equity_common'][KEY]);point.update(tag=tag,value=value)
    rows['equity_guard_'+tag]={KEY:point if value is not None else None}
    result,proof=apply(rows)
    assert not result['equity'] and proof['holds'][0]['reason']=='changed_preferred_or_minority_structure'


def test_explicit_compatible_zero_is_retained_as_evidence():
    rows=records();point=deepcopy(rows['equity_common'][KEY]);point.update(tag='MinorityInterest',value=0)
    rows['equity_guard_MinorityInterest']={KEY:point}
    assert apply(rows)[0]['equity'][KEY]['allocation_guards']['MinorityInterest']==point


@pytest.mark.parametrize('value',[None,{'value':999}])
def test_reported_parent_and_conflict_are_authoritative(value):
    rows=records();rows['equity'][KEY]=value
    result,proof=apply(rows)
    assert result['equity'][KEY]==value and not proof['derived_periods']


def test_later_unchanged_comparative_requires_matching_original_observation():
    rows=records();point=rows['equity_inclusive'][KEY];point.update(filed='2026-09-01',accession='later')
    raw={'end':'2026-06-30','filed':'2026-08-04','accn':'0001823608-26-000182','val':834993000,'form':'10-Q'}
    payload={'cik':CIK,'facts':{'us-gaap':{point['tag']:{'units':{'USD':[raw]}}}}}
    result,proof=apply(rows,payload)
    fact=result['equity'][KEY]['inputs']['equity_inclusive']
    assert fact['accession']=='0001823608-26-000182' and fact['later_identical_confirmation']==point
    assert not apply(rows)[0]['equity']
    raw['val']+=1
    assert not apply(rows,payload)[0]['equity']


def test_conflicting_same_filing_duplicate_not_dropped():
    rows=records();point=rows['equity_inclusive'][KEY];point.update(filed='2026-09-01',accession='later')
    raw={'end':'2026-06-30','filed':'2026-08-04','accn':'0001823608-26-000182','val':834993000,'form':'10-Q'}
    payload={'cik':CIK,'facts':{'us-gaap':{point['tag']:{'units':{'USD':[raw,{**raw,'val':1}]}}}}}
    assert not apply(rows,payload)[0]['equity']


@pytest.mark.parametrize('symbol,cik',[('TFC','0000092230'),('AMAL','0000320193')])
def test_unreviewed_issuers_unchanged(symbol,cik):
    rows=records();assert subject.apply(rows,symbol=symbol,cik=cik,payload={})==(rows,None)


def test_payload_identity_is_not_optional():
    with pytest.raises(ValueError,match='identity'):apply(records(),{'cik':'other'})


def test_cache_versions_preserve_existing_fusb_policy_and_other_issuers():
    from app.services.sec_fundamentals_preparation import version_for_symbol,VERSION
    from app.services.sec_bank_revenue import REVIEWED_VERSION
    assert version_for_symbol('FUSB')==VERSION+'_'+REVIEWED_VERSION+'_'+subject.VERSION
    assert version_for_symbol('AMAL')==VERSION+'_'+subject.VERSION
    assert version_for_symbol('FRST')==VERSION+'_'+subject.VERSION
    assert version_for_symbol('AAPL')==VERSION


def test_negative_retained_or_aoci_is_valid_but_treasury_sign_is_not_flipped():
    rows=records();rows['equity_retained'][KEY]['value']=-1000
    value=sum(rows[n][KEY]['value']*c for n,c in subject.COEFFICIENTS.items())
    rows['equity_inclusive'][KEY]['value']=value
    rows['equity_assets'][KEY]['value']=value+rows['equity_liabilities'][KEY]['value']
    assert apply(rows)[0]['equity'][KEY]['value']==value
    rows['equity_treasury'][KEY]['value']*=-1
    assert not apply(rows)[0]['equity']


def test_future_complete_period_reconciles_but_new_preferred_is_held():
    rows=records();future=(None,date(2026,9,30))
    for values in rows.values():
        if KEY in values:
            point=values.pop(KEY);point.update(end='2026-09-30',filed='2026-11-04',accession='0001823608-26-000999');values[future]=point
    assert apply(rows)[0]['equity'][future]['value']==834993000
    point=deepcopy(rows['equity_common'][future]);point.update(tag='PreferredStockValue',value=10)
    rows['equity_guard_PreferredStockValue']={future:point}
    assert not apply(rows)[0]['equity']
