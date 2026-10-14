from copy import deepcopy
from datetime import date
import pytest
from app.services import sec_leverage as leverage
from test_sec_debt_bounds import fact


def records(**values):
    return {name: {(None, date(2026,7,3)): fact(value)} for name,value in values.items()}


def project(rows):
    return leverage.project(rows,end=date(2026,7,3),start='2025-07-04')


def test_goodyear_subtotal_omits_short_term_debt_but_maturity_sum_survives():
    rows=records(leverage_cash=861, leverage_debt_with_finance_leases=6831,
        leverage_current_debt=1418, leverage_noncurrent_debt_with_finance_leases=5772,
        leverage_operating_leases=1023)
    before=deepcopy(rows);result=project(rows)
    candidates={c['basis']:c for c in result['candidates']}
    assert 'reported_debt_including_finance_leases' not in candidates
    assert 'reported_debt_plus_operating_leases' not in candidates
    assert candidates['current_plus_noncurrent_debt_including_finance_and_operating_leases']['debt']==8213
    assert result['debt_reconciliations']['leverage_debt_with_finance_leases']['contradictions'][0]['sum']==7190
    assert rows==before and result==project(rows)


def test_sonoco_finance_leases_are_not_added_twice():
    rows=records(leverage_cash=168648,leverage_current_debt=968752,
        leverage_noncurrent_debt_with_finance_leases=3484464,leverage_noncurrent_borrowings=3484464,
        leverage_finance_leases_noncurrent=41044,leverage_operating_leases=312414)
    before=deepcopy(rows);result=project(rows)
    candidates={c['basis']:c for c in result['candidates']}
    assert 'current_debt_plus_noncurrent_borrowings_and_finance_leases' not in candidates
    assert 'current_debt_plus_noncurrent_borrowings_finance_and_operating_leases' not in candidates
    assert candidates['current_plus_noncurrent_debt_including_finance_and_operating_leases']['debt']==4765630
    assert result['holds']['current_debt_plus_noncurrent_borrowings_and_finance_leases']['reason']=='conflicting_debt_addition'
    assert rows==before and result==project(rows)


@pytest.mark.parametrize('inclusive,addition',[(60,10),(50,0),(60.001,10),(70,10),(55,10)])
def test_total_reconciliation_allows_equal_and_explicit_zero_but_not_either_direction_of_conflict(inclusive,addition):
    rows=records(leverage_cash=5,leverage_borrowings=50,leverage_finance_leases=addition,
        leverage_debt_with_finance_leases=inclusive,leverage_operating_leases=2)
    result=project(rows);candidates={c['basis']:c for c in result['candidates']}
    expected=abs(inclusive-50-addition)<=.01
    assert ('borrowings_plus_finance_leases' in candidates)==expected
    assert ('borrowings_plus_finance_and_operating_leases' in candidates)==expected
    assert candidates['reported_debt_including_finance_leases']['debt']==inclusive


@pytest.mark.parametrize('change',[{'filed':'2026-08-04'},{'accession':'other'},
    {'end':'2026-06-30'},{'unit':'EUR'},{'source_cik':'0000000001'},{'start':'2026-01-01'}])
def test_incompatible_reference_cannot_disprove_an_independent_decomposition(change):
    instant={'leverage_debt_with_finance_leases':fact(55,**change),
        'leverage_borrowings':fact(50),'leverage_finance_leases':fact(10),
        'leverage_current_debt':fact(20),'leverage_noncurrent_debt_with_finance_leases':fact(40)}
    assert leverage._debt_addition_checks(instant)=={}
    assert leverage._debt_bounds(instant)=={}


@pytest.mark.parametrize('missing',['leverage_borrowings','leverage_finance_leases','leverage_debt_with_finance_leases'])
def test_missing_component_is_not_inferred_as_zero(missing):
    instant={'leverage_borrowings':fact(50),'leverage_finance_leases':fact(10),
        'leverage_debt_with_finance_leases':fact(55)}
    instant[missing]=None
    assert leverage._debt_addition_checks(instant)=={}


@pytest.mark.parametrize('bad',[float('nan'),float('inf'),-1])
def test_invalid_components_do_not_disprove_an_independent_total(bad):
    instant={'leverage_borrowings':fact(50),'leverage_finance_leases':fact(bad),
        'leverage_debt_with_finance_leases':fact(55)}
    assert leverage._debt_addition_checks(instant)=={}


def test_overflowing_sum_is_held_without_serializing_nonfinite_value():
    instant={'leverage_borrowings':fact(1e308),'leverage_finance_leases':fact(1e308),
        'leverage_debt_with_finance_leases':fact(1e308)}
    result=leverage._debt_addition_checks(instant)['leverage_borrowings']
    assert result['status']=='conflicting_debt_addition' and result['sum'] is None
