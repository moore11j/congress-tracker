from copy import deepcopy
from datetime import datetime, timezone
import json
from types import SimpleNamespace

import pytest
from sqlalchemy import select, func
from app.models import Event, FundamentalsCache
from app.services import sec_net_debt_policy as policy, sec_fundamentals as sec
from app.services import sec_fundamentals_preparation as prep
from app.services.fundamentals_cache import fundamentals_summary_from_cache_row
from test_sec_leverage import prepared_sources
from test_sec_fundamentals_preparation import fixture
from test_custom_alert_fundamental_basis import scenario

NOW = datetime(2026, 10, 15, 5, tzinfo=timezone.utc)


def sources():
    data, company = prepared_sources()
    company['sic'] = '3571'
    return data, company


def project(data, company):
    return sec.project_fundamentals(json.dumps(data).encode(), json.dumps(company).encode(),
        symbol='ABC', cik='1', observed_at=NOW)


def test_finance_leases_included_operating_leases_excluded_with_full_provenance():
    data, company = sources(); original = deepcopy(data)
    result = project(data, company)
    assert result['net_debt_to_ebitda'] == pytest.approx(55/20.8)
    evidence = json.loads(result['source_evidence_json'])
    p = evidence['net_debt_policy']
    assert p['value'] != pytest.approx(75/20.8)
    assert p['value'] != pytest.approx(45/20.8)
    assert p['version'] == policy.VERSION
    assert p['selected_basis'] == 'reported_debt_including_finance_leases'
    assert p['candidate']['cash_input']['accession']
    assert p['candidate']['debt_inputs'][0]['tag'] == 'DebtAndCapitalLeaseObligations'
    assert evidence['calculations']['net_debt_to_ebitda'] == policy.FORMULA
    assert result == project(data, company) and data == original
    row = SimpleNamespace(**{**result, 'provider': 'free_direct'})
    summary = fundamentals_summary_from_cache_row(row, now=NOW)
    assert policy.NOTE in summary['context']['source_note']
    assert summary['metrics']['net_debt_to_ebitda']['provenance']['version'] == policy.VERSION


@pytest.mark.parametrize('sic', ['6022','6199','6311','6798','0','9999','',None,'unknown'])
def test_financial_or_unverified_industry_cannot_use_industrial_policy(sic):
    data, company = sources(); company['sic'] = sic
    result = project(data, company)
    assert 'net_debt_to_ebitda' not in result
    assert json.loads(result['source_evidence_json'])['net_debt_policy']['status'] == 'unavailable'


def test_borrowings_and_leases_without_independent_overlap_proof_are_held():
    data, company = sources()
    data['facts']['us-gaap'].pop('DebtAndCapitalLeaseObligations')
    result = project(data, company)
    assert 'net_debt_to_ebitda' not in result
    evidence = json.loads(result['source_evidence_json'])
    assert any(r['basis']=='borrowings_plus_finance_leases' for r in evidence['leverage_preparation']['candidates'])
    assert evidence['net_debt_policy']['reason'] == 'no_verified_finance_inclusive_debt'


def test_lease_inclusive_maturity_sum_without_separate_lease_balance_is_usable():
    data, company = sources(); g = data['facts']['us-gaap']
    point = g['DebtAndCapitalLeaseObligations']['units']['USD'][0]
    g['DebtCurrent'] = {'units': {'USD': [{**point, 'val': 20}]}}
    g['LongTermDebtAndCapitalLeaseObligations'] = {'units': {'USD': [{**point, 'val': 40}]}}
    g.pop('DebtAndCapitalLeaseObligations'); g.pop('FinanceLeaseLiability')
    result = project(data, company)
    assert result['net_debt_to_ebitda'] == pytest.approx(55/20.8)
    assert json.loads(result['source_evidence_json'])['net_debt_policy']['selected_basis'].startswith('current_plus_noncurrent')


@pytest.mark.parametrize('total,expected', [(55,55/20.8), (60,55/20.8), (65,None)])
def test_contradicted_subtotal_is_held_but_conflicting_complete_totals_are_not_cherry_picked(total,expected):
    data, company = sources(); g=data['facts']['us-gaap']
    point=g['DebtAndCapitalLeaseObligations']['units']['USD'][0];point['val']=total
    g['DebtCurrent']={'units':{'USD':[{**point,'val':20}]}}
    g['LongTermDebtAndCapitalLeaseObligations']={'units':{'USD':[{**point,'val':40}]}}
    result=project(data,company)
    if expected is None:
        assert 'net_debt_to_ebitda' not in result
        assert json.loads(result['source_evidence_json'])['net_debt_policy']['reason']=='conflicting_complete_debt_representations'
    else:assert result['net_debt_to_ebitda']==pytest.approx(expected)


@pytest.mark.parametrize('change', ['currency','version','negative_ebitda','zero_ebitda','mismatch','duplicate','nonfinite'])
def test_policy_rejects_incompatible_or_invalid_prepared_evidence(change):
    data, company = sources();result=project(data,company)
    p=json.loads(result['source_evidence_json'])['leverage_preparation']
    row=next(r for r in p['candidates'] if r['basis']=='reported_debt_including_finance_leases')
    if change=='currency':p['reporting_currency']='EUR'
    if change=='version':p['version']='obsolete'
    if change=='negative_ebitda':p['ebitda']['value']=-1
    if change=='zero_ebitda':p['ebitda']['value']=0
    if change=='mismatch':row['net_debt_to_ebitda']=999
    if change=='duplicate':p['candidates'].append(deepcopy(row))
    if change=='nonfinite':row['debt']=float('nan')
    assert policy.select(p,company)['status']=='unavailable'


def test_net_cash_preserved_and_no_forced_zero_or_market_data():
    data, company=sources()
    data['facts']['us-gaap']['CashAndCashEquivalentsAtCarryingValue']['units']['USD'][0]['val']=100
    assert project(data,company)['net_debt_to_ebitda']==pytest.approx(-40/20.8)


def test_preparation_adds_ratio_without_canonical_writes_or_repeated_requests(fixture):
    factory,data,company,calls=fixture
    d,c=sources();data.update(d);company.update(c)
    prepared=prep.prepare('ABC')
    assert prepared['values']['net_debt_to_ebitda']==pytest.approx(55/20.8)
    count=len(calls)
    assert prep.prepare('ABC')==prepared and len(calls)==count
    assert all('sec.gov' in url for url in calls)
    with factory() as db:
        assert db.scalar(select(func.count()).select_from(FundamentalsCache))==0
        assert db.scalar(select(func.count()).select_from(Event))==0


def test_same_provider_debt_policy_rebaseline_does_not_send_correction_alert(scenario):
    db,rule,row,run=scenario
    row.provider='free_direct';row.source_evidence_json=json.dumps({'methodology':'free_fundamentals_v1'})
    assert run(0)['initialized']==1
    data,company=sources();result=project(data,company)
    row.net_debt_to_ebitda=result['net_debt_to_ebitda'];row.source_evidence_json=result['source_evidence_json']
    corrected=run(5);assert corrected['initialized']==1 and corrected['triggered']==0
    assert run(10)['triggered']==0
    row.net_debt_to_ebitda=1;assert run(15)['triggered']==0
    row.net_debt_to_ebitda=3;assert run(20)['triggered']==1
    assert run(25)['triggered']==0


def test_policy_version_rebaselines_even_if_formula_is_unchanged(scenario):
    db, rule, row, run = scenario
    data, company = sources()
    value = project(data, company)
    row.provider = 'free_direct'
    evidence = json.loads(value['source_evidence_json'])
    row.source_evidence_json = json.dumps(evidence)
    assert run(0)['triggered'] == 0
    evidence['net_debt_policy']['version'] = 'reviewed_future_policy'
    row.source_evidence_json = json.dumps(evidence)
    row.net_debt_to_ebitda = 3
    result = run(5)
    assert result['initialized'] == 1 and result['triggered'] == 0


def test_original_bound_rounding_uses_precise_maturity_sum_without_inventing_lease_amounts():
    from test_sec_debt_precision import fixture_sources, acquire, Source, project as precision_project
    data,company=fixture_sources();company['sic']='3571'
    from test_sec_inline_financials import ACC
    for concept in data['facts']['us-gaap'].values():
        for point in concept['units'].get('USD', []):
            if point.get('filed') == '2026-08-01':
                point['accn'] = ACC
    without=precision_project(data,company)
    assert 'net_debt_to_ebitda' not in without
    supplement=acquire(data,company,Source())
    result=precision_project(data,company,supplement)
    p=json.loads(result['source_evidence_json'])['net_debt_policy']
    assert p['status']=='available'
    assert p['candidate']['debt']==90381e6
    assert p['rounding_reconciliation']['canonical_selection'] is True
    assert p['selected_basis']=='current_plus_noncurrent_debt_including_finance_leases'


@pytest.mark.parametrize('change',['missing','precision','other_filing','changed_total','different_cash'])
def test_rounding_cannot_rescue_unbound_or_contradictory_candidate(change):
    from test_sec_debt_precision import fixture_sources, acquire, Source, project as precision_project
    data,company=fixture_sources();company['sic']='3571'
    from test_sec_inline_financials import ACC
    for concept in data['facts']['us-gaap'].values():
        for point in concept['units'].get('USD', []):
            if point.get('filed') == '2026-08-01':
                point['accn'] = ACC
    result=precision_project(data,company,acquire(data,company,Source()))
    p=json.loads(result['source_evidence_json'])['leverage_preparation']
    proof=p['debt_precision_reconciliation']
    aggregate=next(x for x in p['candidates'] if x['basis']=='reported_debt_including_finance_leases')
    if change=='missing':p.pop('debt_precision_reconciliation')
    if change=='precision':proof['inputs'][0]['reported_precision'].pop('decimals')
    if change=='other_filing':proof['inputs'][0]['accession']='other'
    if change=='changed_total':proof['inputs'][0]['value']+=1000000000
    if change=='different_cash':aggregate['cash_input']['accession']='other'
    assert policy.select(p,company)['status']=='unavailable'
