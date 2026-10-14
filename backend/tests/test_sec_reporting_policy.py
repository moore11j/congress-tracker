from copy import deepcopy
from datetime import datetime, timezone
import json
from types import SimpleNamespace
import pytest
from app.services import sec_reporting_policy as policy, sec_fundamentals as sec
from app.services import sec_financial_statements as statements, sec_inline_financials as inline
from app.services import sec_fundamentals_preparation as prep
from test_sec_fundamentals import sources

NOW = datetime(2026, 10, 14, 16, tzinfo=timezone.utc)
CIK = '0001973239'


def fixture_sources():
    data, company = sources()
    data['cik'] = int(CIK)
    company.update(cik=CIK, name='Arm Holdings plc', tickers=['ARM'])
    for content in data['facts']['us-gaap'].values():
        for rows in content['units'].values():
            for r in rows:
                r['form'] = '20-F' if r['form'] == '10-K' else '6-K'
    return data, company


def project(data, company, symbol='ARM', cik=CIK):
    return sec.project_fundamentals(json.dumps(data).encode(), json.dumps(company).encode(),
        symbol=symbol, cik=cik, observed_at=NOW)


def test_reviewed_forms_recover_current_ttm_with_explicit_basis_and_repeat():
    data, company = fixture_sources(); before = deepcopy((data, company))
    result = project(data, company)
    assert result['free_cash_flow'] == 32.5 and result['current_ratio'] == 2
    evidence = json.loads(result['source_evidence_json'])
    assert evidence['current']['revenue']['value'] == 130
    assert evidence['reviewed_reporting_policy']['cik'] == CIK
    assert evidence['reviewed_reporting_policy']['currency'] == 'USD'
    assert result == project(data, company) and (data, company) == before


@pytest.mark.parametrize('defect', ['other_issuer', 'wrong_cik', 'currency', 'today', 'conflict', 'foreign_taxonomy'])
def test_unreviewed_or_invalid_facts_cannot_supply_current_financials(defect):
    data, company = fixture_sources(); symbol, cik = 'ARM', CIK
    rows = data['facts']['us-gaap']['RevenueFromContractWithCustomerExcludingAssessedTax']['units']
    if defect == 'other_issuer':
        symbol = 'OTHER'; company['tickers'] = [symbol]
    elif defect == 'wrong_cik':
        cik = '1'; data['cik'] = 1; company['cik'] = cik
    elif defect == 'currency': rows['EUR'] = rows.pop('USD')
    elif defect == 'today': rows['USD'][-1]['filed'] = '2026-10-14'
    elif defect == 'conflict': rows['USD'].append({**rows['USD'][-1], 'val': 999})
    elif defect == 'foreign_taxonomy': data['facts']['ifrs-full'] = data['facts'].pop('us-gaap')
    with pytest.raises((sec.SecFundamentalsCoverageError, ValueError)):
        project(data, company, symbol, cik)


def test_statement_contract_preserves_direct_ordinary_eps_without_ads_conversion():
    data, company = fixture_sources()
    data['facts']['us-gaap']['EarningsPerShareDiluted'] = {'units': {'USD/shares': [
        dict(start='2025-01-01', end='2025-12-31', val=2, filed='2026-02-01', form='20-F', accn='0000000001-26-000001')]}}
    result = statements.project(json.dumps(data).encode(), json.dumps(company).encode(), symbol='ARM', cik=CIK, observed_at=NOW)
    assert len(result['annual']) == 2 and result['summary']['revenueTtm'] == 130
    assert result['annual'][-1]['eps'] == 2
    assert all(r['currency'] == 'USD' for r in result['annual']+result['quarterly'])
    assert 'ordinary share' in result['reportingBasis']
    assert all(r['eps'] is None for r in result['quarterly'])
    assert result['preparationVersion'] == policy.VERSION
    assert 'intangible' in result['reportingBasis'] and result['forecasts']['nextQuarter'] is None
    row = SimpleNamespace(source='sec_edgar', fetched_at=NOW, payload_json=json.dumps(result))
    assert statements.cached_row_payload(row, 'ARM', now=NOW) == result
    result.pop('preparationVersion'); row.payload_json = json.dumps(result)
    assert statements.cached_row_payload(row, 'ARM', now=NOW) is None
    assert policy.VERSION in prep.version_for_symbol('ARM')
    assert prep.version_for_symbol('ABC') == prep.VERSION


@pytest.mark.parametrize('form,report,filename,expected', [
    ('6-K','2026-06-30','arm-20260630.htm',True),
    ('6-K/A','2026-09-30','arm-20260930.htm',True),
    ('20-F','2026-03-31','arm-20260331.htm',True),
    ('6-K','2026-09-18','arm-20260918.htm',False),
    ('6-K','2026-06-30','earnings-release.htm',False),
    ('20-F','2026-12-31','arm-20261231.htm',False),
    ('10-Q','2026-06-30','arm-20260630.htm',False),
])
def test_only_reviewed_periodic_documents_can_trigger_acquisition(form, report, filename, expected):
    assert policy.periodic_document('ARM', CIK, form, report, filename) is expected


def test_current_facts_do_not_download_unrelated_event_six_k():
    data, company = fixture_sources()
    company['filings'] = {'recent':dict(accessionNumber=['0001973239-26-000135'],form=['6-K'],
        filingDate=['2026-09-18'],reportDate=['2026-09-18'],primaryDocument=['arm-20260918.htm'])}
    class NoNetwork:
        def get(self,url): pytest.fail('event-only filing fetched')
    assert inline.acquire(json.dumps(data).encode(),json.dumps(company).encode(),symbol='ARM',cik=CIK,observed_at=NOW,client=NoNetwork()) is None


def test_other_issuers_and_returned_review_data_are_unchanged():
    data, company = sources()
    before = project(data, company, 'ABC', '1')
    assert before['free_cash_flow'] == 32.5
    assert 'reviewed_reporting_policy' not in json.loads(before['source_evidence_json'])
    changed = policy.reviewed('ARM', CIK); changed['forms'].append('8-K')
    assert '8-K' not in policy.forms_for('ARM', CIK)
    assert policy.forms_for('ABC', '1') == inline.FORMS


@pytest.mark.parametrize('absence', ['directory', 'facts'])
def test_known_arm_absence_is_cached_without_repeated_source_requests(monkeypatch, absence):
    from sqlalchemy import create_engine, select, func
    from sqlalchemy.orm import sessionmaker
    from app.db import Base
    from app.models import InsightsSnapshot, FundamentalsCache, Event
    from app.services import sec_directory
    from app.clients.direct_sources import DirectSourceClient, DirectSourceError
    engine = create_engine('sqlite:///:memory:'); Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine)
    monkeypatch.setattr(statements, 'SessionLocal', factory)
    monkeypatch.setattr(sec_directory, 'directory', lambda: {} if absence == 'directory' else {'ARM': {'cik': CIK}})
    calls = []
    def get(self, url):
        calls.append(url)
        if 'companyfacts' in url: raise DirectSourceError('Source HTTP 404: '+url)
        return json.dumps({'cik': CIK, 'name': 'Arm', 'tickers': ['ARM']}).encode()
    monkeypatch.setattr(DirectSourceClient, 'get', get)
    try:
        result = statements.prepared('ARM'); count = len(calls)
        assert result['unavailable'] and result['preparationVersion'] == policy.VERSION
        assert statements.prepared('ARM') == result and len(calls) == count
        assert count == (0 if absence == 'directory' else 2)
        with factory() as db:
            assert db.scalar(select(func.count()).select_from(InsightsSnapshot)) == 1
            for model in (FundamentalsCache, Event):
                assert db.scalar(select(func.count()).select_from(model)) == 0
    finally:
        engine.dispose()
