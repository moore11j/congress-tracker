import json
from copy import deepcopy
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.db import Base
from app.models import TickerContentCache
from app.services import fmp_news as news, ticker_content_cache as content, sec_fundamentals as fundamentals
from app.services.sec_company_filings import parse_company_filings
from app.clients.direct_sources import DirectSourceClient


def source():
    return {'cik': '1', 'tickers': ['ABC'], 'filings': {'files': [{'name':'old.json'}], 'recent': {
        'accessionNumber': ['0000000001-26-000002','0000000001-26-000001'],
        'filingDate': ['2026-10-07','2026-10-01'], 'form': ['8-K','10-Q'],
        'primaryDocument': ['earnings.htm','quarter.htm'],
        'acceptanceDateTime': ['2026-10-07T17:10:00Z', '2026-10-01T12:00:00Z']}}}


def parse(data):
    return parse_company_filings(json.dumps(data).encode(),symbol='ABC',cik='0000000001')


def test_accession_identity_dates_and_duplicate_control():
    data = source()
    for rows in data['filings']['recent'].values(): rows.append(rows[0])
    result = parse(data)
    assert len(result['items']) == 2
    assert result['items'][0]['accepted_date'] == '2026-10-07T17:10:00Z'
    assert result['items'][0]['url'] == 'https://www.sec.gov/Archives/edgar/data/1/000000000126000002/earnings.htm'
    assert result['items'][0]['source'] == 'sec_edgar_submissions'
    assert result['coverage']['older_archives_available'] is True
    assert result['coverage']['earliest_filing_date'] == '2026-10-01'


@pytest.mark.parametrize('kind',['cik','ticker','length','filename','accession','conflict','acceptance'])
def test_bad_source_cannot_create_filing_links(kind):
    data=source();recent=data['filings']['recent']
    if kind=='cik':data['cik']='2'
    if kind=='ticker':data['tickers']=['OTHER']
    if kind=='length':recent['form'].pop()
    if kind=='filename':recent['primaryDocument'][0]='../other.htm'
    if kind=='accession':recent['accessionNumber'][0]='bad'
    if kind=='conflict':recent['accessionNumber'][1]=recent['accessionNumber'][0]
    if kind=='acceptance':recent['acceptanceDateTime'][0]='not-a-date'
    with pytest.raises(ValueError):parse(data)


@pytest.fixture
def selected(monkeypatch):
    engine=create_engine('sqlite:///:memory:');Base.metadata.create_all(engine,tables=[TickerContentCache.__table__])
    factory=sessionmaker(bind=engine)
    monkeypatch.setenv('FUNDAMENTALS_PROVIDER','fmp')
    monkeypatch.setenv('SEC_FILINGS_PROVIDER','sec_edgar')
    monkeypatch.setenv('FMP_PROVIDER_DISABLED','1')
    monkeypatch.setenv('TICKER_CONTENT_SQLITE_CACHE','1')
    monkeypatch.setattr(content,'SessionLocal',factory)
    monkeypatch.setattr(news,'get_request_context',lambda: {})
    monkeypatch.setattr(fundamentals,'company_directory',lambda day: {'ABC': {'cik':'0000000001'}})
    news.clear_news_cache()
    yield factory
    news.clear_news_cache();engine.dispose()


def test_selected_route_uses_shared_cache_for_full_list_and_later_pages(selected,monkeypatch):
    calls=[]
    def get(self,url):
        calls.append(url);assert url=='https://data.sec.gov/submissions/CIK0000000001.json'
        return json.dumps(source()).encode()
    monkeypatch.setattr(DirectSourceClient,'get',get)
    monkeypatch.setattr(news,'_try_direct_symbol_search',lambda **kw: pytest.fail('FMP fallback'))
    content.db_ticker_content_cache_set('sec_filings','ABC',{'status':'ok','items':[{'url':'old-fmp'}]})
    first=news.get_sec_filings(symbol='ABC',from_date='2026-10-01',to_date='2026-10-08',limit=1)
    assert first['items'][0]['form_type']=='8-K' and first['has_next']
    news.clear_news_cache()
    second=news.get_sec_filings(symbol='ABC',from_date='2026-10-01',to_date='2026-10-08',page=1,limit=1)
    assert second['items'][0]['form_type']=='10-Q' and not second['has_next']
    assert len(calls)==1
    with selected() as db:
        row=db.query(TickerContentCache).filter_by(window_key='sec_recent').one()
        assert row.source=='sec_edgar' and row.item_count==2
        assert row.content_type=='sec_company_filings'
        legacy=db.query(TickerContentCache).filter_by(content_type='sec_filings').one()
        assert 'old-fmp' in legacy.payload_json


def test_cold_public_panel_enqueues_without_source_transport(selected,monkeypatch):
    monkeypatch.setattr(news,'get_request_context',lambda: {'path':'/api/tickers/ABC/sec-filings','source':'TickerFilingsPanel'})
    calls=[]
    monkeypatch.setattr(news,'_enqueue_news_refresh',lambda **kw: calls.append(kw) or True)
    monkeypatch.setattr(DirectSourceClient,'get',lambda *a: pytest.fail('Cold public request fetched source'))
    result=news.get_sec_filings(symbol='ABC')
    assert result['status']=='warming' and len(calls)==1
    assert calls[0]['job_type']=='sec_filings'


def test_existing_enrichment_job_and_api_share_direct_source(selected,monkeypatch):
    from types import SimpleNamespace
    from app.services.data_enrichment_queue import _process_one
    from app.main import ticker_sec_filings
    calls=[]
    def get(self,url):calls.append(url);return json.dumps(source()).encode()
    monkeypatch.setattr(DirectSourceClient,'get',get)
    _process_one(None,SimpleNamespace(job_type='sec_filings',symbol='ABC',payload_json=json.dumps({
        'from_date':'2026-10-01','to_date':'2026-10-08','page':0,'limit':100})))
    news.clear_news_cache()
    response=ticker_sec_filings('ABC',from_date='2026-10-01',to_date='2026-10-08',page=0,limit=100)
    assert response['provider']=='sec_edgar_submissions' and response['item_count']==2
    assert response['coverage']['kind']=='recent_submissions' and len(calls)==1


def test_refusal_never_returns_fmp_as_direct_source(selected,monkeypatch):
    content.db_ticker_content_cache_set('sec_filings','ABC',{'status':'ok','items':[{'url':'old-fmp'}]})
    def denied(*a):raise RuntimeError('Source refusal')
    monkeypatch.setattr(DirectSourceClient,'get',denied)
    result=news.get_sec_filings(symbol='ABC')
    assert result['status']=='unavailable' and result['items']==[]


def test_missing_primary_document_links_to_accession_index():
    data=source();data['filings']['recent']['primaryDocument'][0]=''
    assert parse(data)['items'][0]['url'].endswith('/0000000001-26-000002-index.html')


def test_official_rendering_subdirectory_is_preserved():
    data=source();data['filings']['recent']['primaryDocument'][0]='xslF345X06/form4.xml'
    assert parse(data)['items'][0]['url'].endswith('/000000000126000002/xslF345X06/form4.xml')


def test_large_issuer_is_bounded_to_latest_filings_with_explicit_coverage():
    data=source();recent=data['filings']['recent']
    for key,rows in recent.items():recent[key]=[rows[0]]*2001
    recent['accessionNumber']=[f'0000000001-26-{i:06}' for i in range(2001)]
    result=parse(data)
    assert len(result['items'])==2000 and result['coverage']['truncated'] is True
    assert result['coverage']['source_row_count']==2001
    assert result['items'][0]['accession_number']=='0000000001-26-002000'
    assert result['items'][-1]['accession_number']=='0000000001-26-000001'


@pytest.mark.parametrize('filename',['cemex.s.a.b..de.c.v..txt','lamar.advertising.co..cl.a.txt'])
def test_literal_adjacent_periods_in_official_filename_are_not_path_traversal(filename):
    data=source();data['filings']['recent']['primaryDocument'][0]=filename
    assert parse(data)['items'][0]['url'].endswith('/'+filename)

@pytest.mark.parametrize('filename',['dir/../other.htm','dir/./other.htm','%2e%2e/other.htm','dir%2fother.htm'])
def test_path_segments_and_encoded_traversal_remain_rejected(filename):
    data=source();data['filings']['recent']['primaryDocument'][0]=filename
    with pytest.raises(ValueError):parse(data)
