"""Reviewed fee-only bank revenue must not masquerade as company revenue."""
from copy import deepcopy
from datetime import date
import json
from types import SimpleNamespace

import pytest
from app.services import sec_bank_revenue as bank, sec_fundamentals as sec
from app.services import sec_financial_statements as statements, sec_fundamentals_preparation as prep
from test_sec_bank_revenue import bank_sources, NOW, AVAILABLE


def sources():
    data, company = bank_sources()
    data['cik'] = int(bank.REVIEWED_CIK)
    company.update(cik=bank.REVIEWED_CIK, tickers=['FUSB'])
    gaap = data['facts']['us-gaap']
    gaap[bank.REVIEWED_CONTRACT] = deepcopy(gaap[bank.FEES])
    for row in gaap[bank.REVIEWED_CONTRACT]['units']['USD']:
        row['val'] /= 2
    return data, company


def project(data, company):
    return sec.project_fundamentals(json.dumps(data).encode(), json.dumps(company).encode(),
        symbol='FUSB', cik=bank.REVIEWED_CIK, observed_at=NOW)


def test_current_fee_series_is_replaced_with_aligned_full_bank_basis():
    data, company = sources()
    original = deepcopy(data)
    result = project(data, company)
    evidence = json.loads(result['source_evidence_json'])
    assert evidence['current']['revenue']['value'] == 143
    assert evidence['prior']['revenue']['value'] == 121
    assert result['net_margin'] == pytest.approx(13 / 143 * 100)
    assert result['fcf_margin'] == pytest.approx(32.5 / 143 * 100)
    assert evidence['revenue_basis'] == bank.BASIS
    assert all(r['revenue_selection']['method'] == bank.REVIEWED_VERSION
               for r in evidence['current']['revenue']['inputs'])
    panel = statements.project(json.dumps(data).encode(), json.dumps(company).encode(),
        symbol='FUSB', cik=bank.REVIEWED_CIK, observed_at=NOW)
    assert panel['summary']['revenueTtm'] == 143
    assert panel['revenueBasis'] == bank.BASIS
    assert data == original


@pytest.mark.parametrize('kind', ['missing_interest', 'missing_fees', 'missing_contract',
    'currency', 'contract_currency', 'filing', 'filed', 'period', 'conflict',
    'contract_conflict', 'negative_fee', 'excess_fee', 'newer_contract', 'changed_tag'])
def test_unverified_current_bank_basis_never_falls_back_to_fee_only_revenue(kind):
    data, company = sources()
    gaap = data['facts']['us-gaap']
    fee = gaap[bank.FEES]['units']['USD'][-1]
    component = gaap[bank.REVIEWED_CONTRACT]['units']['USD'][-1]
    if kind == 'missing_interest': gaap.pop(bank.INTEREST)
    if kind == 'missing_fees': gaap.pop(bank.FEES)
    if kind == 'missing_contract': gaap.pop(bank.REVIEWED_CONTRACT)
    if kind == 'currency': gaap[bank.FEES]['units']['EUR'] = gaap[bank.FEES]['units'].pop('USD')
    if kind == 'contract_currency': gaap[bank.REVIEWED_CONTRACT]['units']['EUR'] = gaap[bank.REVIEWED_CONTRACT]['units'].pop('USD')
    if kind == 'filing': component['accn'] = '0000717806-26-000009'
    if kind == 'filed': component['filed'] = '2026-08-02'
    if kind == 'period': component['start'] = '2026-04-01'
    if kind == 'conflict': gaap[bank.FEES]['units']['USD'].append({**fee, 'val': 99})
    if kind == 'contract_conflict': gaap[bank.REVIEWED_CONTRACT]['units']['USD'].append({**component, 'val': 99})
    if kind == 'negative_fee': component['val'] = -1
    if kind == 'excess_fee': component['val'] = fee['val'] + 1
    if kind == 'newer_contract': gaap[bank.REVIEWED_CONTRACT]['units']['USD'].append({**component, 'end': '2026-09-30', 'filed': '2026-10-01'})
    if kind == 'changed_tag': gaap['RevenueFromContractWithCustomerExcludingAssessedTax'] = gaap.pop(bank.REVIEWED_CONTRACT)
    with pytest.raises(sec.SecFundamentalsCoverageError):
        project(data, company)


@pytest.mark.parametrize('conflict', [False, True])
def test_reported_bank_total_or_conflict_remains_authoritative(conflict):
    data, company = sources()
    gaap = data['facts']['us-gaap']
    gaap['Revenues'] = deepcopy(gaap[bank.INTEREST])
    if conflict:
        rows = gaap['Revenues']['units']['USD']
        rows.append({**rows[-1], 'val': 999})
    records = {'revenue': sec._records(data, 'revenue', AVAILABLE)}
    assert bank.apply(records, data, company, available_by=AVAILABLE) == records


@pytest.mark.parametrize('change', ['issuer', 'symbol', 'duplicate_symbol'])
def test_review_does_not_apply_to_unreviewed_identity(change):
    data, company = sources()
    if change == 'issuer': data['cik'] = company['cik'] = 1
    if change == 'symbol': company['tickers'] = ['OTHER']
    if change == 'duplicate_symbol': company['tickers'] = ['FUSB', 'FUSB']
    records = {'revenue': sec._records(data, 'revenue', AVAILABLE)}
    assert bank.apply(records, data, company, available_by=AVAILABLE) is records


def test_fusb_requires_new_ratio_and_statement_caches():
    from app.services.sec_parent_equity import VERSION as equity_version
    assert prep.version_for_symbol('FUSB') == prep.VERSION+'_'+bank.REVIEWED_VERSION+'_'+equity_version
    payload = {'source': 'sec_edgar', 'symbol': 'FUSB'}
    row = SimpleNamespace(source='sec_edgar', fetched_at=NOW, payload_json=json.dumps(payload))
    assert statements.cached_row_payload(row, 'FUSB', now=NOW) is None
    payload['preparationVersion'] = bank.REVIEWED_VERSION
    row.payload_json = json.dumps(payload)
    assert statements.cached_row_payload(row, 'FUSB', now=NOW) == payload


def test_older_unreviewed_total_is_not_spliced_into_reviewed_series():
    data, company = sources()
    data['facts']['us-gaap']['Revenues'] = {'units': {'USD': [{
        'start': '2018-01-01', 'end': '2018-12-31', 'filed': '2019-05-10',
        'val': 800000, 'accn': '0001437749-19-009456', 'form': '10-Q'}]}}
    result = project(data, company)
    evidence = json.loads(result['source_evidence_json'])
    assert evidence['current']['revenue']['value'] == 143
    records = bank.apply({'revenue': sec._records(data, 'revenue', AVAILABLE)},
                         data, company, available_by=AVAILABLE)['revenue']
    assert all(start >= bank.REVIEWED_START for start, _ in records)


def test_newer_fee_component_cannot_replace_recent_reported_total():
    data, company = sources()
    gaap = data['facts']['us-gaap']
    gaap['Revenues'] = deepcopy(gaap[bank.INTEREST])
    gaap['Revenues']['units']['USD'] = [r for r in gaap['Revenues']['units']['USD'] if r['end'] < '2026-01-01']
    with pytest.raises(sec.SecFundamentalsCoverageError):
        project(data, company)


def test_future_aligned_quarter_uses_same_bank_basis():
    data, company = sources()
    for tag in (bank.INTEREST, bank.FEES, bank.REVIEWED_CONTRACT):
        rows = data['facts']['us-gaap'][tag]['units']['USD']
        for row in list(rows):
            if row['end'].endswith('06-30'):
                rows.append({**row, 'end': row['end'][:4]+'-09-30', 'filed': '2026-10-08',
                             'accn': '0000717806-26-000009', 'val': row['val'] * 1.5})
    result = project(data, company)
    evidence = json.loads(result['source_evidence_json'])
    assert evidence['current']['revenue']['value'] == 148.5
    assert result['period_date'] == date(2026, 9, 30)
