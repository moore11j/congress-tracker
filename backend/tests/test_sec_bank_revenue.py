from copy import deepcopy
from datetime import date, datetime, timezone
import json
import pytest
from app.services import sec_bank_revenue as bank, sec_fundamentals as sec, sec_financial_statements as statements
from test_sec_fundamentals import sources

NOW = datetime(2026,10,10,23,tzinfo=timezone.utc)
AVAILABLE = date(2026,10,9)


def bank_sources():
    data, company = sources(); company['sic'] = '6035'
    gaap = data['facts']['us-gaap']
    revenue = gaap.pop('RevenueFromContractWithCustomerExcludingAssessedTax')
    gaap[bank.INTEREST] = deepcopy(revenue)
    gaap[bank.FEES] = deepcopy(revenue)
    for row in gaap[bank.FEES]['units']['USD']: row['val'] *= .1
    return data, company


def calculated(data, company):
    return bank.apply({'revenue': {}},data,company,available_by=AVAILABLE)['revenue']


def test_banking_denominator_preserves_components_and_filed_availability():
    data, company = bank_sources(); original = deepcopy(data)
    result = sec.project_fundamentals(json.dumps(data).encode(),json.dumps(company).encode(),symbol='ABC',cik='1',observed_at=NOW)
    evidence = json.loads(result['source_evidence_json'])
    assert evidence['current']['revenue']['value'] == pytest.approx(143)
    assert evidence['revenue_basis'] == bank.BASIS
    assert result['revenue_growth'] == pytest.approx((143/121-1)*100)
    assert result['net_margin'] == pytest.approx(13/143*100)
    for row in evidence['current']['revenue']['inputs']:
        assert row['tag'] == bank.TAG and row['source_kind'] == 'derived_sec_bank_revenue'
        assert [i['tag'] for i in row['inputs']] == [bank.INTEREST,bank.FEES]
        assert all(i['accession']==row['accession'] and i['filed']==row['filed'] for i in row['inputs'])
    panel = statements.project(json.dumps(data).encode(),json.dumps(company).encode(),symbol='ABC',cik='1',observed_at=NOW)
    assert panel['revenueBasis'] == bank.BASIS and panel['summary']['revenueTtm'] == pytest.approx(143)
    assert data == original


@pytest.mark.parametrize('kind', ['period','filed','accession','conflict','missing','currency'])
def test_components_cannot_be_mixed_or_missing_inferred(kind):
    data, company = bank_sources(); rows = data['facts']['us-gaap'][bank.FEES]['units']['USD']; row = rows[-1]
    if kind=='period':row['start']='2026-04-01'
    if kind=='filed':row['filed']='2026-08-02'
    if kind=='accession':row['accn']='0000000001-26-000009'
    if kind=='conflict':rows.append({**row,'val':999})
    if kind=='missing':rows.pop()
    if kind=='currency':data['facts']['us-gaap'][bank.FEES]['units']['EUR']=data['facts']['us-gaap'][bank.FEES]['units'].pop('USD')
    assert calculated(data,company).get((date(2026,1,1),date(2026,6,30))) is None


def test_explicit_zero_noninterest_income_is_valid():
    data,company=bank_sources();data['facts']['us-gaap'][bank.FEES]['units']['USD'][-1]['val']=0
    assert calculated(data,company)[date(2026,1,1),date(2026,6,30)]['value']==70


@pytest.mark.parametrize('sic', ['7372','6199','',None])
def test_nonbank_or_unclassified_companies_are_not_reinterpreted(sic):
    data,company=bank_sources();company['sic']=sic
    assert calculated(data,company)=={}


def test_reported_or_conflicting_totals_always_remain_authoritative():
    data,company=bank_sources()
    for current in [{(date(2026,1,1),date(2026,6,30)):None},{(date(2026,1,1),date(2026,6,30)):{'value':999}}]:
        records={'revenue':current}
        assert bank.apply(records,data,company,available_by=AVAILABLE) is records
    company['cik']='2'
    with pytest.raises(sec.SecFundamentalsError,match='identity'):calculated(data,company)


def test_later_partial_restatement_does_not_reuse_old_component():
    data,company=bank_sources();rows=data['facts']['us-gaap'][bank.INTEREST]['units']['USD']
    rows.append({**rows[-1],'filed':'2026-10-08','val':80,'accn':'0000000001-26-000008'})
    assert calculated(data,company)[date(2026,1,1),date(2026,6,30)] is None
    # Today's filing cannot displace the last completed filing day.
    rows[-1]['filed']='2026-10-10'
    assert calculated(data,company)[date(2026,1,1),date(2026,6,30)]['value']==77
