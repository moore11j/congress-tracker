from datetime import date, datetime, timezone
import json
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.db import Base
from app.services import sec_directory

@pytest.fixture
def db(monkeypatch):
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    monkeypatch.setattr(sec_directory, 'SessionLocal', sessionmaker(bind=engine))
    with sessionmaker(bind=engine)() as session:
        yield session
    engine.dispose()


def test_sec_statements_reported_annual_and_ytd_cash_without_forecasts():
    from app.services import sec_financial_statements as statements
    from test_sec_fundamentals import sources
    facts, company = sources()
    revenue = facts['facts']['us-gaap']['RevenueFromContractWithCustomerExcludingAssessedTax']['units']['USD']
    # A reported second-quarter revenue period with only YTD cash/capex.
    revenue.append(dict(start='2026-04-01', end='2026-06-30', filed='2026-08-01', val=40, form='10-Q', accn='latest'))
    for tag, value in [('NetCashProvidedByUsedInOperatingActivities', 8), ('PaymentsToAcquirePropertyPlantAndEquipment', 1)]:
        facts['facts']['us-gaap'][tag]['units']['USD'].append(dict(start='2026-01-01', end='2026-03-31', filed='2026-05-01', val=value, form='10-Q', accn='prior'))
    result = statements.project(json.dumps(facts).encode(), json.dumps(company).encode(), symbol='ABC', cik='1', observed_at=datetime(2026,10,9,18,tzinfo=timezone.utc))
    assert len(result['annual']) == 2
    quarter = result['quarterly'][-1]
    assert quarter['revenue'] == 40 and quarter['operatingCashFlow'] == 13
    assert quarter['freeCashFlow'] == 10.5
    assert quarter['eps'] is None
    assert result['forecasts']['nextFiscalYear'] is None
    assert result['summary']['revenueTtm'] == 130
    assert result['sourceEvidence']['periods']['quarterly:2026-06-30']['operating_cash_flow']['method'] == 'aligned_ytd_difference'

def test_sec_statement_conflicting_or_unaligned_cash_not_zero_or_inferred():
    from app.services.sec_financial_statements import period_value
    start, end = date(2026,4,1), date(2026,6,30)
    records = {(date(2026,1,1), end): {'value': 100, 'tag': 'Cash'},
               (date(2026,1,1), date(2026,3,30)): {'value': 40, 'tag': 'Cash'}}
    assert period_value(records, start, end) is None
    records[(date(2026,1,1), date(2026,3,31))] = {'value': 40, 'tag': 'Cash'}
    assert period_value(records, start, end)['value'] == 60
    records[(start,end)] = None
    assert period_value(records, start, end) is None

def test_sec_statement_public_reader_and_worker_use_separate_source_cache(db, monkeypatch):
    from app.services import sec_financial_statements as statements, ticker_financials, data_enrichment_queue
    from app.request_priority import set_request_context, reset_request_context
    from test_sec_fundamentals import sources
    facts, company = sources()
    monkeypatch.setenv('FINANCIAL_STATEMENTS_PROVIDER', 'sec_edgar')
    monkeypatch.setattr(statements, 'SessionLocal', sessionmaker(bind=db.get_bind()))
    monkeypatch.setattr(sec_directory, 'directory', lambda: {'ABC': {'cik': '0000000001'}})
    calls, queued = [], []
    def fetch(self, url):
        calls.append(url)
        return json.dumps(facts if 'companyfacts' in url else company).encode()
    monkeypatch.setattr(statements.DirectSourceClient, 'get', fetch)
    monkeypatch.setattr(data_enrichment_queue, 'enqueue_data_enrichment_job', lambda **kw: queued.append(kw))
    # This public active-panel context used to permit synchronous FMP access.
    token = set_request_context({'path':'/api/tickers/ABC/financials', 'request_source':'client', 'route_family':'ticker', 'panel':'TickerFinancialsPanel'})
    try:
        assert ticker_financials.get_ticker_financials('ABC')['status'] == 'warming'
        assert calls == [] and queued[0]['job_type'] == 'ticker_financials'
    finally:
        reset_request_context(token)
    assert ticker_financials.get_ticker_financials('ABC')['source'] == 'sec_edgar'
    assert ticker_financials.get_ticker_financials('ABC')['source'] == 'sec_edgar'
    assert len(calls) == 2
