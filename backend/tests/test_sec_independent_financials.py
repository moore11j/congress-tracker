"""Shared dependency rules across missing, conflicted and incomplete revenue."""
from copy import deepcopy
from datetime import date
import json
import pytest
from app.services import sec_fundamentals as sec
from test_sec_fundamentals import sources, NOW


def inputs(mode='missing', symbol='GENERIC'):
    facts, company = sources()
    company['tickers'] = [symbol]
    company['filings'] = {'recent': {'form':['10-Q'], 'reportDate':['2026-06-30'],
        'filingDate':['2026-08-01'], 'accessionNumber':['0000000001-26-000001']}}
    revenue = facts['facts']['us-gaap']['RevenueFromContractWithCustomerExcludingAssessedTax']['units']['USD']
    if mode == 'missing': facts['facts']['us-gaap'].pop('RevenueFromContractWithCustomerExcludingAssessedTax')
    elif mode == 'conflict': revenue.append({**revenue[-1], 'val':999})
    elif mode == 'incomplete': revenue[-1]['start']='2026-04-01'
    elif mode == 'stale': revenue.pop()
    return facts, company


def project(facts, company, now=NOW):
    return sec.project_fundamentals(json.dumps(facts).encode(), json.dumps(company).encode(),
        symbol=company['tickers'][0], cik='1', observed_at=now)


@pytest.mark.parametrize('mode',['missing','conflict','incomplete','stale'])
@pytest.mark.parametrize('symbol',['GENERIC','UNLISTEDPOLICY'])
def test_missing_revenue_does_not_block_independent_current_fields(mode,symbol):
    facts, company=inputs(mode,symbol)
    result=project(facts,company); proof=json.loads(result['source_evidence_json'])
    assert result['current_ratio']==2
    assert result['roe']==pytest.approx(13/110*100)
    assert result['free_cash_flow']==32.5
    assert set(proof['calculations'])=={'current_ratio','roe','free_cash_flow'}
    assert proof['current']['revenue'] is None
    assert proof['financial_status']=='partial'
    assert proof['partial_financial_basis']['accession']=='0000000001-26-000001'
    assert result['period_date']==date(2026,6,30)
    assert project(facts,company)==result


@pytest.mark.parametrize('defect',['missing_capex','negative_capex','misaligned_cash','wrong_currency','conflicting_cash','latest_accession'])
def test_cash_input_failure_preserves_valid_balance_fields(defect):
    facts,company=inputs(); gaap=facts['facts']['us-gaap']
    cash=gaap['NetCashProvidedByUsedInOperatingActivities']['units']['USD']
    capex=gaap['PaymentsToAcquirePropertyPlantAndEquipment']['units']['USD']
    if defect=='missing_capex':gaap.pop('PaymentsToAcquirePropertyPlantAndEquipment')
    elif defect=='negative_capex':
        for r in capex:r['val']=-abs(r['val'])
    elif defect=='misaligned_cash':cash[-1]['start']='2026-04-01'
    elif defect=='wrong_currency':gaap['NetCashProvidedByUsedInOperatingActivities']['units']={'EUR':cash}
    elif defect=='conflicting_cash':cash.append({**cash[-1],'val':999})
    else:cash[-1]['accn']='0000000001-26-000999'
    result=project(facts,company)
    assert 'free_cash_flow' not in result
    assert result['current_ratio']==2
    assert result['roe']==pytest.approx(13/110*100)


@pytest.mark.parametrize('defect',['newer_filing','ambiguous_filing','missing_filing','stale_filing','wrong_cik'])
def test_current_identity_and_filing_checks_remain_required(defect):
    facts,company=inputs(); recent=company['filings']['recent']
    now=NOW
    if defect=='newer_filing':
        for key,value in {'form':'10-Q','reportDate':'2026-09-30','filingDate':'2026-10-06','accessionNumber':'0000000001-26-000999'}.items():recent[key].insert(0,value)
    elif defect=='ambiguous_filing':
        for key in recent:recent[key].append(recent[key][0])
        recent['accessionNumber'][-1]='0000000001-26-000999'
    elif defect=='missing_filing':company['filings']={}
    elif defect=='stale_filing':now=NOW.replace(year=2027)
    else:company['cik']='2'
    with pytest.raises(sec.SecFundamentalsError):project(facts,company,now)


def test_current_filing_day_excluded_until_completed_new_york_day():
    facts,company=inputs();recent=company['filings']['recent']
    for key,value in {'form':'10-Q','reportDate':'2026-09-30','filingDate':'2026-10-08','accessionNumber':'0000000001-26-000999'}.items():recent[key].insert(0,value)
    assert project(facts,company)['current_ratio']==2
    with pytest.raises(sec.SecFundamentalsCoverageError):project(facts,company,NOW.replace(day=9))


@pytest.mark.parametrize('currency,namespace',[('EUR','us-gaap'),('USD','ifrs-full')])
def test_non_usd_or_other_accounting_basis_not_mislabeled(currency,namespace):
    from app.services.sec_nonrevenue_financials import project as partial
    with pytest.raises(sec.SecFundamentalsCoverageError,match='currency and taxonomy'):
        partial({}, {}, symbol='ANY',cik='1',facts_raw=b'{}',company_raw=b'{}',observed_at=NOW,
            available_by=NOW.date(),currency=currency,namespace=namespace)


def test_existing_complete_calculations_unchanged_and_no_universe_cache_reset():
    from app.services.sec_fundamentals_preparation import version_for_symbol
    from test_sec_fundamentals import project as original_project
    facts,company=inputs('complete','ABC')
    expected=original_project(facts,company)
    assert project(facts,company)==expected
    assert version_for_symbol('GENERIC')=='sec_ratio_preparation_v5'


@pytest.mark.parametrize('form',['10-KT','10-QT','20-F','40-F'])
def test_newer_reporting_basis_cannot_fall_back_to_old_domestic_report(form):
    facts,company=inputs();recent=company['filings']['recent']
    recent['primaryDocument']=['old.htm']
    for key,value in {'form':form,'reportDate':'2026-09-30','filingDate':'2026-10-06','accessionNumber':'0000000001-26-000999','primaryDocument':'new.htm'}.items():recent[key].insert(0,value)
    with pytest.raises(sec.SecFundamentalsCoverageError,match='supported period rules'):project(facts,company)


@pytest.mark.parametrize('symbol,metric',[('ALOY','current_ratio'),('BCAX','current_ratio'),('BCAX','roe')])
def test_generic_issuers_complete_source_preparation_and_alert_lifecycle(symbol,metric,monkeypatch):
    from pathlib import Path
    import test_sec_nonrevenue_financials as original
    root=Path(__file__).parent/'fixtures/sec_independent_financials'
    manifest=json.loads((root/'sources.json').read_text())
    expected={'ALOY':{'current_ratio':30.677881743977704},
        'BCAX':{'current_ratio':503820000/43475000,'roe':-41.01114646158475,'free_cash_flow':-135248000.0}}
    monkeypatch.setattr(original,'ROOT',root)
    monkeypatch.setattr(original,'PROFILES',{s:manifest[s+'-current.htm']['cik'] for s in expected})
    monkeypatch.setattr(original,'EXPECTED',expected)
    original.test_actual_acquisition_preparation_score_and_alert_consumers(symbol,monkeypatch,metric)


def test_inline_missing_revenue_keeps_current_evidence_for_any_verified_issuer():
    from test_sec_inline_financials import document,fact,parse,TAG
    from app.services.sec_nonrevenue_financials import VERSION
    raw=document(fact('12',tag='NetIncomeLoss')).replace(('us-gaap:'+TAG).encode(),b'us-gaap:UnrelatedRevenueExtension')
    parsed=parse(raw)
    assert parsed['evidence']['revenue_status']=='unavailable'
    assert parsed['evidence']['partial_financial_policy']==VERSION
    assert parsed['facts']['NetIncomeLoss']['units']['USD'][0]['val']==12
