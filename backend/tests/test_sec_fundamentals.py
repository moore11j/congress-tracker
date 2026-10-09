from datetime import datetime, timezone
import json
from copy import deepcopy

import pytest

from app.services.sec_fundamentals import project_fundamentals, SecFundamentalsError

NOW = datetime(2026, 10, 8, 18, tzinfo=timezone.utc)


def sources():
    periods = [('2024-01-01', '2024-12-31', '2025-02-01', 100),
               ('2025-01-01', '2025-12-31', '2026-02-01', 120),
               ('2024-01-01', '2024-06-30', '2024-08-01', 50),
               ('2025-01-01', '2025-06-30', '2025-08-01', 60),
               ('2026-01-01', '2026-06-30', '2026-08-01', 70)]
    gaap = {}
    for tag, factor in [('RevenueFromContractWithCustomerExcludingAssessedTax', 1), ('GrossProfit', .6),
                        ('OperatingIncomeLoss', .2), ('NetIncomeLoss', .1),
                        ('NetCashProvidedByUsedInOperatingActivities', .3), ('PaymentsToAcquirePropertyPlantAndEquipment', .05)]:
        gaap[tag] = {'units': {'USD': [dict(start=s, end=e, filed=f, val=v*factor,
            accn=f'0000000001-{f[:4][-2:]}-000001', form='10-K' if '12-31' in e else '10-Q') for s,e,f,v in periods]}}
    for tag, values in [('StockholdersEquity', [100,120]), ('AssetsCurrent', [80,100]), ('LiabilitiesCurrent', [40,50])]:
        gaap[tag] = {'units': {'USD': [dict(end=e, filed=f, val=v, form='10-Q', accn='0000000001-26-000001')
            for e,f,v in zip(['2025-06-30','2026-06-30'],['2025-08-01','2026-08-01'],values)]}}
    return {'cik': 1, 'facts': {'us-gaap': gaap}}, {'cik': '1', 'name': 'Sample', 'tickers': ['ABC'], 'exchanges': ['NYSE']}


def project(facts, company=None, **kw):
    return project_fundamentals(json.dumps(facts).encode(), json.dumps(company or sources()[1]).encode(),
        symbol='ABC', cik='1', observed_at=kw.get('now', NOW))


def test_ttm_math_and_exact_source_provenance():
    result = project(*sources())
    assert result['revenue_growth'] == pytest.approx((130/110-1)*100)
    assert result['roe'] == pytest.approx(13/110*100)
    assert result['operating_margin'] == 20
    assert result['current_ratio'] == 2
    assert result['free_cash_flow'] == 32.5
    evidence = json.loads(result['source_evidence_json'])
    assert evidence['current']['revenue']['value'] == 130
    assert evidence['period_start'] == '2025-07-01'
    assert len(evidence['current']['revenue']['inputs']) == 3
    assert evidence['available_by'] == '2026-10-07'
    assert all('accession' in row for row in evidence['current']['revenue']['inputs'])
    assert 'forward_pe' not in result and 'ev_to_ebitda' not in result and 'eps_ttm' not in result


def test_restatement_availability_does_not_use_same_day_or_future_filings():
    facts, _ = sources()
    rows = facts['facts']['us-gaap']['RevenueFromContractWithCustomerExcludingAssessedTax']['units']['USD']
    row = {**rows[-1], 'filed': '2026-10-08', 'val': 999, 'form': '10-Q/A'}
    rows.append(row)
    assert json.loads(project(facts)['source_evidence_json'])['current']['revenue']['value'] == 130
    next_day = NOW.replace(day=9)
    assert json.loads(project(facts, now=next_day)['source_evidence_json'])['current']['revenue']['value'] == 1059
    # UTC midnight is still the prior New York filing day.
    assert json.loads(project(facts, now=next_day.replace(hour=2))['source_evidence_json'])['current']['revenue']['value'] == 130


def test_wrong_currency_period_and_nonfinite_inputs_are_missing():
    facts, _ = sources()
    gaap = facts['facts']['us-gaap']
    gaap['GrossProfit']['units']['EUR'] = gaap['GrossProfit']['units'].pop('USD')
    gaap['OperatingIncomeLoss']['units']['USD'][-1]['start'] = '2026-04-01'
    gaap['NetIncomeLoss']['units']['USD'][-1]['val'] = float('nan')
    result = project(facts)
    assert all(name not in result for name in ('gross_margin', 'operating_margin', 'roe'))
    assert result['current_ratio'] == 2


def test_conflicting_equal_date_facts_and_incomplete_latest_period_are_held():
    facts, _ = sources()
    rows = facts['facts']['us-gaap']['RevenueFromContractWithCustomerExcludingAssessedTax']['units']['USD']
    rows.append({**rows[-1], 'val': 999})
    with pytest.raises(SecFundamentalsError):
        project(facts)


@pytest.mark.parametrize('kind', ['cik', 'symbol', 'old', 'missing_ytd'])
def test_invalid_identity_or_stale_and_incomplete_coverage_is_rejected(kind):
    facts, company = sources()
    if kind == 'cik': company['cik'] = 2
    if kind == 'symbol': company['tickers'] = ['XYZ']
    if kind == 'missing_ytd':
        rows = facts['facts']['us-gaap']['RevenueFromContractWithCustomerExcludingAssessedTax']['units']['USD']
        rows[-1]['start'] = '2026-04-01'
    with pytest.raises(SecFundamentalsError):
        project(facts, company, now=NOW.replace(year=2027) if kind == 'old' else NOW)


def test_negative_equity_does_not_create_bullish_roe():
    facts, _ = sources()
    facts['facts']['us-gaap']['StockholdersEquity']['units']['USD'][-1]['val'] = -20
    assert 'roe' not in project(facts)


def test_total_revenue_is_not_confused_with_contract_revenue_component():
    facts, _ = sources()
    gaap = facts['facts']['us-gaap']
    gaap['Revenues'] = deepcopy(gaap['RevenueFromContractWithCustomerExcludingAssessedTax'])
    for row in gaap['Revenues']['units']['USD']:
        row['val'] *= 2
    evidence = json.loads(project(facts)['source_evidence_json'])
    assert evidence['current']['revenue']['value'] == 260
    assert {r['tag'] for r in evidence['current']['revenue']['inputs']} == {'Revenues'}
