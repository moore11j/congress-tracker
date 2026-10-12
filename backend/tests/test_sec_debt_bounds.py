from copy import deepcopy
from datetime import date
import json
import pytest
from app.services import sec_leverage as leverage
from test_sec_leverage import prepared_sources, panel


def fact(value, **changes):
    return {'value':value,'start':None,'end':'2026-07-03','filed':'2026-08-03',
        'accession':'0001097864-26-000017','unit':'USD','source_cik':'0001097864',**changes}


def test_guarantee_mistag_cannot_become_total_debt_in_projector():
    data,_=prepared_sources();gaap=data['facts']['us-gaap']
    template=gaap['DebtAndCapitalLeaseObligations']['units']['USD'][-1]
    template['val']=1.6
    gaap['LongTermDebtNoncurrent']={'units':{'USD':[{**template,'val':3657.3}]}}
    before=deepcopy(data)
    values,result=panel(data)
    assert data==before and 'net_debt_to_ebitda' not in values
    assert not any(c['basis'] in {'reported_debt_including_finance_leases','reported_debt_plus_operating_leases'} for c in result['candidates'])
    reason=result['debt_reconciliations']['leverage_debt_with_finance_leases']
    assert reason['status']=='aggregate_below_reported_component'
    assert any(c['component']=='leverage_noncurrent_borrowings' for c in reason['contradictions'])
    assert result['holds']['reported_debt_plus_operating_leases']['reason']=='conflicting_reported_debt_total'


@pytest.mark.parametrize('total,part',[
    ('leverage_borrowings','leverage_noncurrent_borrowings'),
    ('leverage_debt_with_finance_leases','leverage_borrowings'),
    ('leverage_debt_with_finance_leases','leverage_current_debt'),
    ('leverage_debt_with_finance_leases','leverage_noncurrent_debt_with_finance_leases'),
    ('leverage_debt_with_finance_leases','leverage_noncurrent_borrowings'),
    ('leverage_debt_with_finance_leases','leverage_finance_leases'),
    ('leverage_noncurrent_debt_with_finance_leases','leverage_noncurrent_borrowings'),
    ('leverage_noncurrent_debt_with_finance_leases','leverage_finance_leases_noncurrent'),
    ('leverage_current_debt','leverage_finance_leases_current'),
])
def test_each_included_component_is_a_lower_bound(total,part):
    instant={total:fact(1),part:fact(10)}
    result=leverage._debt_bounds(instant)
    assert result[total]['reported_aggregate']['value']==1
    assert instant[total] is None and instant[part]['value']==10


@pytest.mark.parametrize('change',[{'filed':'2026-08-04'},{'accession':'other'},
    {'end':'2026-06-30'},{'unit':'EUR'},{'source_cik':'0000000001'},{'start':'2026-01-01'}])
def test_incompatible_component_does_not_invalidate_an_independent_total(change):
    instant={'leverage_debt_with_finance_leases':fact(1),'leverage_noncurrent_borrowings':fact(10,**change)}
    assert leverage._debt_bounds(instant)=={}
    assert instant['leverage_debt_with_finance_leases']['value']==1


@pytest.mark.parametrize('component',[None,0,1,1.001,float('nan'),float('inf'),-5])
def test_absent_invalid_zero_and_subcent_components_do_not_create_false_conflicts(component):
    instant={'leverage_debt_with_finance_leases':fact(1),
        'leverage_noncurrent_borrowings':None if component is None else fact(component)}
    assert leverage._debt_bounds(instant)=={}


def test_operating_leases_are_not_assumed_included_in_finance_debt():
    instant={'leverage_debt_with_finance_leases':fact(1),'leverage_operating_leases':fact(100)}
    assert leverage._debt_bounds(instant)=={}


def test_finance_leases_are_not_assumed_included_in_borrowings():
    instant={'leverage_borrowings':fact(1),'leverage_finance_leases':fact(100),'leverage_current_debt':fact(100)}
    assert leverage._debt_bounds(instant)=={}


def test_independent_valid_borrowings_candidate_survives_conflicting_finance_total():
    data,_=prepared_sources();gaap=data['facts']['us-gaap']
    gaap['DebtAndCapitalLeaseObligations']['units']['USD'][-1]['val']=1
    values,result=panel(data)
    assert any(c['basis']=='reported_borrowings' and c['status']=='ratio_candidate' for c in result['candidates'])
    assert result==panel(data)[1]
    assert 'net_debt_to_ebitda' not in values
