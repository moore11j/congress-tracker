from copy import deepcopy
from datetime import date
import json

import pytest

from app.services import sec_leverage as leverage
from test_sec_fundamentals import sources, project


def prepared_sources():
    facts, company = sources()
    gaap = facts['facts']['us-gaap']
    income = gaap['NetIncomeLoss']['units']['USD']
    for tag, factor in [('IncomeTaxExpenseBenefit', .2), ('InterestExpenseNonoperating', .1),
                        ('DepreciationDepletionAndAmortization', .3)]:
        gaap[tag] = {'units': {'USD': [{**row, 'val': row['val']*factor} for row in income]}}
    instant = gaap['AssetsCurrent']['units']['USD'][-1]
    for tag, value in [('DebtLongtermAndShorttermCombinedAmount', 50),
                       ('DebtAndCapitalLeaseObligations', 60), ('FinanceLeaseLiability', 10),
                       ('OperatingLeaseLiability', 20), ('CashAndCashEquivalentsAtCarryingValue', 5)]:
        # Match the filing identity of the source income fixture.
        gaap[tag] = {'units': {'USD': [{**instant, 'val': value} ]}}
    return facts, company


def panel(facts):
    result = project(facts)
    return result, json.loads(result['source_evidence_json'])['leverage_preparation']


def test_ttm_candidates_preserve_debt_definitions_and_do_not_publish_ratio():
    result, p = panel(prepared_sources()[0])
    assert 'net_debt_to_ebitda' not in result
    assert p['ebitda']['value'] == pytest.approx(20.8)
    assert p['ebitda']['status'] == 'components_complete'
    rows = {x['basis']: x for x in p['candidates']}
    assert rows['reported_borrowings']['net_debt_to_ebitda'] == pytest.approx(45/20.8)
    assert rows['reported_debt_including_finance_leases']['debt'] == 60
    # Finance leases included in an aggregate must not be added to it again.
    assert rows['reported_debt_plus_operating_leases']['debt'] == 80
    assert rows['borrowings_plus_finance_and_operating_leases']['debt'] == 80
    assert rows['reported_borrowings']['cash_input']['accession']
    assert len(p['ebitda']['components']['leverage_da']['inputs']) == 3
    assert p['period_date'] == '2026-06-30'
    assert p['status'] == 'preparation_only'


@pytest.mark.parametrize('tag', ['IncomeTaxExpenseBenefit', 'InterestExpenseNonoperating',
    'DepreciationDepletionAndAmortization'])
def test_missing_expense_is_not_zero_or_operating_income_substitute(tag):
    facts, _ = prepared_sources()
    del facts['facts']['us-gaap'][tag]
    _, p = panel(facts)
    assert p['ebitda']['status'] == 'incomplete'
    assert all('net_debt_to_ebitda' not in row for row in p['candidates'])


def test_combined_da_and_other_is_not_accepted_as_da():
    facts, _ = prepared_sources()
    gaap = facts['facts']['us-gaap']
    gaap['DepreciationAmortizationAndAccretionNet'] = gaap.pop('DepreciationDepletionAndAmortization')
    _, p = panel(facts)
    assert 'leverage_da' in p['ebitda']['missing_components']


@pytest.mark.parametrize('kind', ['filed', 'accession', 'start', 'currency'])
def test_incompatible_income_leg_is_held(kind):
    facts, _ = prepared_sources()
    info = facts['facts']['us-gaap']['IncomeTaxExpenseBenefit']
    row = info['units']['USD'][-1]
    if kind == 'filed': row['filed'] = '2026-08-02'
    if kind == 'accession': row['accn'] = '0000000001-26-000099'
    if kind == 'start': row['start'] = '2026-04-01'
    if kind == 'currency': info['units']['EUR'] = info['units'].pop('USD')
    _, p = panel(facts)
    assert p['ebitda']['status'] in {'incomplete', 'incompatible_filing_components'}
    assert all('net_debt_to_ebitda' not in row for row in p['candidates'])


def test_tax_benefit_and_net_cash_are_preserved_but_negative_da_is_held():
    facts, _ = prepared_sources()
    gaap = facts['facts']['us-gaap']
    for row in gaap['IncomeTaxExpenseBenefit']['units']['USD']: row['val'] *= -1
    gaap['CashAndCashEquivalentsAtCarryingValue']['units']['USD'][-1]['val'] = 100
    _, p = panel(facts)
    assert p['ebitda']['value'] == pytest.approx(15.6)
    assert p['candidates'][0]['net_debt_to_ebitda'] < 0
    gaap['DepreciationDepletionAndAmortization']['units']['USD'][-1]['val'] *= -1
    _, held = panel(facts)
    assert held['ebitda']['status'] == 'negative_expense_component'


def test_missing_lease_does_not_create_zero_or_suppress_independent_borrowings():
    facts, _ = prepared_sources()
    del facts['facts']['us-gaap']['FinanceLeaseLiability']
    _, p = panel(facts)
    assert 'borrowings_plus_finance_leases' in p['holds']
    assert p['holds']['borrowings_plus_finance_leases']['components'] == ['leverage_finance_leases']
    assert any(row['basis']=='reported_borrowings' for row in p['candidates'])


@pytest.mark.parametrize('kind', ['stale', 'conflict', 'negative', 'future'])
def test_invalid_cash_cannot_produce_net_debt(kind):
    facts, _ = prepared_sources()
    rows = facts['facts']['us-gaap']['CashAndCashEquivalentsAtCarryingValue']['units']['USD']
    if kind == 'stale': rows[-1]['end'] = '2026-03-31'
    if kind == 'conflict': rows.append({**rows[-1], 'val': 999})
    if kind == 'negative': rows[-1]['val'] = -1
    if kind == 'future': rows[-1]['filed'] = '2026-10-09'
    _, p = panel(facts)
    assert not p['candidates']


def test_nonpositive_ebitda_and_explicit_zero_are_distinct_from_missing():
    facts, _ = prepared_sources()
    for tag in ['NetIncomeLoss', 'IncomeTaxExpenseBenefit', 'InterestExpenseNonoperating',
                'DepreciationDepletionAndAmortization']:
        for row in facts['facts']['us-gaap'][tag]['units']['USD']: row['val'] = 0
    _, p = panel(facts)
    assert p['ebitda']['status'] == 'components_complete'
    assert p['ebitda']['value'] == 0
    assert all('net_debt_to_ebitda' not in row for row in p['candidates'])


def test_adding_preparation_components_does_not_change_existing_financial_fields():
    baseline = project(sources()[0])
    result, p = panel(prepared_sources()[0])
    assert {k:v for k,v in result.items() if k!='source_evidence_json'} == {
        k:v for k,v in baseline.items() if k!='source_evidence_json'}
    assert result == project(prepared_sources()[0])
    assert 'current_plus_noncurrent_debt_including_finance_leases' in p['holds']


def test_income_restatement_cannot_mix_with_unreconciled_balance_sheet():
    facts, _ = prepared_sources()
    for tag in ['NetIncomeLoss', 'IncomeTaxExpenseBenefit', 'InterestExpenseNonoperating',
                'DepreciationDepletionAndAmortization']:
        for row in facts['facts']['us-gaap'][tag]['units']['USD']:
            if row['end'] == '2026-06-30':
                row.update(accn='0000000001-26-000099', filed='2026-08-02')
    _, p = panel(facts)
    assert p['ebitda']['status'] == 'components_complete'
    assert p['candidates']
    assert all(row.get('ratio_hold') == 'income_balance_filing_mismatch' for row in p['candidates'])
    assert all('net_debt_to_ebitda' not in row for row in p['candidates'])



def maturity_sources():
    facts, _ = prepared_sources()
    gaap = facts['facts']['us-gaap']
    template = gaap['CashAndCashEquivalentsAtCarryingValue']['units']['USD'][-1]
    for tag, value in [('FinanceLeaseLiabilityCurrent', 3), ('FinanceLeaseLiabilityNoncurrent', 7),
            ('OperatingLeaseLiabilityCurrent', 4), ('OperatingLeaseLiabilityNoncurrent', 16),
            ('DebtCurrent', 13), ('LongTermDebtNoncurrent', 40)]:
        gaap[tag] = {'units': {'USD': [{**template, 'val': value}]}}
    return facts


def test_maturity_components_reconstruct_totals_with_original_filing_evidence():
    facts = maturity_sources()
    gaap = facts['facts']['us-gaap']
    del gaap['FinanceLeaseLiability'], gaap['OperatingLeaseLiability']
    _, p = panel(facts)
    assert all(r['status']=='complete_maturity_sum' for r in p['lease_reconciliations'].values())
    row = next(r for r in p['candidates'] if r['basis']=='borrowings_plus_finance_and_operating_leases')
    assert row['debt']==80
    assert row['debt_inputs'][1]['derived'] is True
    assert [f['value'] for f in row['debt_inputs'][1]['inputs']]==[3,7]
    assert len({f['accession'] for f in row['debt_inputs'][1]['inputs']})==1
    assert panel(facts)[1]==p


def test_current_debt_never_adds_current_finance_lease_twice():
    _, p = panel(maturity_sources())
    rows={r['basis']:r for r in p['candidates']}
    row=rows['current_debt_plus_noncurrent_borrowings_and_finance_leases']
    assert row['debt']==60  # 13 already contains the current finance lease of 3.
    assert [f['tag'] for f in row['debt_inputs']]==['DebtCurrent','LongTermDebtNoncurrent','FinanceLeaseLiabilityNoncurrent']
    assert rows['current_debt_plus_noncurrent_borrowings_finance_and_operating_leases']['debt']==80
    assert all(r['status']=='reported_total_verified' for r in p['lease_reconciliations'].values())


@pytest.mark.parametrize('defect', ['absent','stale','filing','negative','conflict'])
def test_incomplete_or_conflicting_maturity_components_cannot_create_total(defect):
    facts = maturity_sources()
    gaap=facts['facts']['us-gaap']
    del gaap['FinanceLeaseLiability']
    row=gaap['FinanceLeaseLiabilityNoncurrent']['units']['USD'][0]
    if defect=='absent': del gaap['FinanceLeaseLiabilityNoncurrent']
    if defect=='stale': row['end']='2026-03-31'
    if defect=='filing': row['accn']='0000000001-26-000099'
    if defect=='negative': row['val']=-1
    if defect=='conflict': gaap['FinanceLeaseLiabilityNoncurrent']['units']['USD'].append({**row,'val':9})
    _, p=panel(facts)
    assert 'borrowings_plus_finance_leases' in p['holds']
    assert not any(r['basis']=='borrowings_plus_finance_and_operating_leases' for r in p['candidates'])
    # The independently reported debt total remains usable.
    assert any(r['basis']=='reported_debt_including_finance_leases' for r in p['candidates'])


def test_conflicting_reported_total_is_held_without_mutating_input():
    facts=maturity_sources(); original=deepcopy(facts)
    facts['facts']['us-gaap']['FinanceLeaseLiability']['units']['USD'][0]['val']=12
    changed=deepcopy(facts)
    _, p=panel(facts)
    assert p['lease_reconciliations']['leverage_finance_leases']['status']=='conflicting_lease_total'
    assert 'borrowings_plus_finance_leases' in p['holds']
    assert 'current_debt_plus_noncurrent_borrowings_and_finance_leases' in p['holds']
    assert facts==changed
    assert original!=changed


def test_explicit_zero_noncurrent_finance_lease_is_required_for_complete_debt():
    facts=maturity_sources(); gaap=facts['facts']['us-gaap']
    del gaap['FinanceLeaseLiability']
    gaap['FinanceLeaseLiabilityNoncurrent']['units']['USD'][0]['val']=0
    _, p=panel(facts)
    basis='current_debt_plus_noncurrent_borrowings_and_finance_leases'
    assert next(r['debt'] for r in p['candidates'] if r['basis']==basis)==53
    del gaap['FinanceLeaseLiabilityNoncurrent']
    _, p=panel(facts)
    assert basis in p['holds']



def mixed_ttm_records():
    from datetime import timedelta
    end=date(2026,6,30); annual_start=date(2025,1,1); annual_end=date(2025,12,31)
    prior_end=date(2025,6,30); current_start=date(2026,1,1)
    records={}
    for index,name in enumerate(leverage.DURATION,1):
        def fact(start,stop,value,accession):
            return {'start':str(start),'end':str(stop),'value':value,'unit':'USD',
                'filed':'2026-08-01' if accession=='current' else '2026-02-01',
                'accession':accession,'tag':leverage.DURATION[name][0],'source_cik':'0000000001'}
        records[name]={(annual_start,annual_end):fact(annual_start,annual_end,index*10,'annual'),
            (current_start,end):fact(current_start,end,index*8,'current'),
            (annual_start,prior_end):fact(annual_start,prior_end,index*5,'current')}
        if name in {'leverage_net_income','leverage_da'}:
            records[name][(prior_end+timedelta(days=1),end)]=fact(prior_end+timedelta(days=1),end,index*13,'current')
    return records,end,'2025-07-01'


def test_reported_and_derived_ttm_align_only_with_exact_common_filing_proof():
    records,end,start=mixed_ttm_records(); original=deepcopy(records)
    result=leverage._ebitda(records,end,start)
    assert result['status']=='components_complete'
    assert result['value']==130
    assert result['reported_ttm_reconciliation']['selected_reported_components']['leverage_net_income']['method']=='reported_annual'
    assert all(v['method']=='annual_plus_ytd_minus_prior_ytd' for v in result['components'].values())
    assert result==leverage._ebitda(records,end,start)
    assert records==original


@pytest.mark.parametrize('defect',['value','closing_filing','annual_filing','missing_leg','currency'])
def test_reported_ttm_does_not_bypass_restatement_or_missing_leg(defect):
    records,end,start=mixed_ttm_records()
    point=records['leverage_net_income'][(date(2025,7,1),end)]
    if defect=='value': point['value']+=1
    if defect=='closing_filing': point['accession']='different'
    if defect=='annual_filing': records['leverage_tax'][(date(2025,1,1),date(2025,12,31))]['accession']='restated'
    if defect=='missing_leg': del records['leverage_da'][(date(2025,1,1),date(2025,6,30))]
    if defect=='currency': point['unit']='EUR'
    assert leverage._ebitda(records,end,start)['status']=='incompatible_filing_components'
