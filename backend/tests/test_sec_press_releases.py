from datetime import datetime, timedelta, timezone
import json
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.models import Event, ResearchSourceDocument, Security, TickerContentCache
from app.clients.direct_sources import DirectSourceClient
from app.services import fmp_news as news, sec_press_releases as press
from app.services import data_enrichment_queue as queue, sec_directory as directory
from app.services.direct_feed_store import DirectFeedDocument, DirectFeedRevision
from test_sec_earnings_materials import company, submission
from test_sec_earnings_store import index


@pytest.fixture
def selected(monkeypatch):
    engine = create_engine('sqlite:///:memory:'); Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine)
    with factory() as db:
        db.add(Security(id=1, symbol='TEST', name='Test', asset_class='stock')); db.commit()
    monkeypatch.setenv('PRESS_RELEASE_PROVIDER', 'sec_edgar')
    monkeypatch.setenv('FMP_PROVIDER_DISABLED', '1')
    monkeypatch.setattr(press, 'SessionLocal', factory)
    monkeypatch.setattr(news, 'get_request_context', lambda: {})
    monkeypatch.setattr(directory, 'directory', lambda: {'TEST': {'cik': '1234567'}})
    monkeypatch.setattr(news, '_request_ticker_press_rows', lambda **kw: pytest.fail('FMP fallback'))
    calls = []
    def get(self, url):
        calls.append(url)
        if url.endswith('.json'): return company()
        if url.endswith('-index.html'): return index()
        if url.endswith('.txt'): return submission()
        pytest.fail('Unexpected URL')
    monkeypatch.setattr(DirectSourceClient, 'get', get)
    yield factory, calls
    engine.dispose()


def test_selected_public_payload_cache_and_repeat(selected):
    factory, calls = selected
    first = news.get_press_releases(symbol='TEST', limit=1)
    assert first['provider'] == press.PROVIDER and first['status'] == 'ok'
    item = first['items'][0]
    assert item['published_at'] is None and item['filing_date'] == '2026-07-30'
    assert item['sec_accepted_at'] == '2026-07-30T20:00:00+00:00'
    assert item['source'] == press.PROVIDER and 'Filed 2026-07-30' in item['summary']
    assert first['coverage']['complete'] is False
    assert news.get_press_releases(symbol='TEST', limit=1) == first
    assert news.get_press_releases(symbol='TEST', page=1, limit=1)['items'] == []
    assert len(calls) == 3
    with factory() as db:
        assert db.scalar(select(func.count()).select_from(DirectFeedDocument)) == 3
        assert db.scalar(select(func.count()).select_from(DirectFeedRevision)) == 3
        assert db.scalar(select(func.count()).select_from(ResearchSourceDocument)) == 0
        assert db.scalar(select(func.count()).select_from(Event)) == 0


def test_existing_worker_and_api_use_direct_source(selected):
    from app.main import ticker_press_releases
    queue._process_one(None, SimpleNamespace(job_type='press_releases', symbol='TEST', payload_json='{}'))
    response = ticker_press_releases('TEST', page=0, limit=20)
    assert response['provider'] == press.PROVIDER and len(response['items']) == 1
    assert len(selected[1]) == 3
    assert queue._disabled_fmp_content_job('press_releases') is False


def test_public_cache_miss_only_enqueues_and_keeps_coverage(selected, monkeypatch):
    monkeypatch.setattr(news, '_is_public_request_context', lambda: True)
    queued = []
    monkeypatch.setattr(news, '_enqueue_news_refresh', lambda **kw: queued.append(kw) or True)
    result = news.get_press_releases(symbol='TEST')
    assert result['status'] == 'warming' and result['provider'] == press.PROVIDER
    assert queued[0]['job_type'] == 'press_releases' and not selected[1]


def test_held_only_collection_is_cached_without_retry_churn(selected, monkeypatch):
    original = DirectSourceClient.get
    def get(self, url):
        return submission(title='Test Production, Deliveries & Deployments') if url.endswith('.txt') else original(self, url)
    monkeypatch.setattr(DirectSourceClient, 'get', get)
    result = news.get_press_releases(symbol='TEST')
    assert result['items'] == [] and result['coverage']['held'] == 1
    assert result['status'] == 'ok'
    before = len(selected[1]); assert news.get_press_releases(symbol='TEST') == result
    assert len(selected[1]) == before
    from app.main import ticker_press_releases
    response = ticker_press_releases('TEST', page=0, limit=20)
    assert 'Other company releases and transcripts are not covered' in response['message']
    assert response['message'] != 'No press releases found.'


def test_press_cache_retains_api_clock_discrepancy(selected, monkeypatch):
    original = DirectSourceClient.get
    def get(self, url):
        return company(acceptanceDateTime=['2026-07-31T00:00:00Z']) if url.endswith('.json') else original(self, url)
    monkeypatch.setattr(DirectSourceClient, 'get', get)
    result = news.get_press_releases(symbol='TEST')
    assert len(result['items']) == 1
    item = result['items'][0]
    assert item['sec_accepted_at'] == '2026-07-30T20:00:00+00:00'
    assert item['acceptance_evidence']['submissions_raw'] == '2026-07-31T00:00:00Z'
    assert item['acceptance_evidence']['submissions_comparison'] == 'conflict'
    assert item['published_at'] is None
    assert news.get_press_releases(symbol='TEST') == result


def test_refusal_preserves_previous_cache_but_reports_unavailable(selected, monkeypatch):
    first = news.get_press_releases(symbol='TEST')
    def denied(*args): raise RuntimeError('refused')
    monkeypatch.setattr(DirectSourceClient, 'get', denied)
    result = news.get_press_releases(symbol='TEST', force_refresh=True)
    assert result['status'] == 'unavailable' and result['items'] == []
    assert press._cached('TEST')['items'] == first['items']


def test_stale_cache_does_not_appear_fresh(selected, monkeypatch):
    factory, calls = selected
    news.get_press_releases(symbol='TEST')
    with factory() as db:
        row = db.scalar(select(TickerContentCache)); row.fetched_at = datetime.now(timezone.utc)-timedelta(days=2); db.commit()
    monkeypatch.setattr(news, '_is_public_request_context', lambda: True)
    monkeypatch.setattr(news, '_enqueue_news_refresh', lambda **kw: False)
    result = news.get_press_releases(symbol='TEST')
    assert result['status'] == 'unavailable' and result['items'] == []
    assert len(calls) == 3


def test_new_provider_never_reuses_fmp_cache(selected):
    factory, _ = selected
    with factory() as db:
        db.add(TickerContentCache(content_type='press_releases', symbol='TEST', window_key='latest',
            cache_key='old-fmp', source='fmp', status='ok', item_count=1,
            payload_json=json.dumps({'items':[{'url':'old-fmp'}], 'status':'ok'}), fetched_at=datetime.now(timezone.utc)))
        db.commit()
    assert news.get_press_releases(symbol='TEST')['items'][0]['source'] == press.PROVIDER


def test_sec_link_is_never_extracted_as_fmp_evidence(selected, monkeypatch):
    from app.services import operational_intelligence as ops
    factory, _ = selected
    monkeypatch.setattr(ops, 'upsert_source_document', lambda *a, **kw: pytest.fail('False FMP source'))
    with factory() as db:
        security = db.get(Security, 1)
        result = ops._ingest_article(db, security=security, item={'source': press.PROVIDER}, document_type='press_release')
        assert result['skipped'] == 1
        refreshed = ops.refresh_operational_intelligence(db, security_id=1, source_types={'press_release'})
        assert refreshed['documents'] == 0 and refreshed['events'] == 0


def test_watchlist_digest_never_crawls_or_invents_press_time(selected, monkeypatch):
    from app.services import email_digests as digests
    monkeypatch.setattr(digests, '_subscription_payload', lambda _: {'watchlist_news_enabled': True})
    monkeypatch.setattr(digests, '_watchlist_market_news_enabled', lambda _: True)
    monkeypatch.setattr(digests, '_watchlist_symbols', lambda *a: ['TEST'])
    monkeypatch.setattr(digests, 'get_stock_news', lambda **kw: {'items': []})
    def items():
        return digests._watchlist_market_news_items(None, SimpleNamespace(id=1),
            since=datetime(2026, 7, 1, tzinfo=timezone.utc), subscription=SimpleNamespace(active=True))
    assert items() == [] and selected[1] == []
    news.get_press_releases(symbol='TEST')
    assert items() == [] and len(selected[1]) == 3


def test_default_refresh_is_bounded_and_documents_coverage(selected, monkeypatch):
    data = json.loads(company())
    recent = data['filings']['recent']
    for key in recent: recent[key] = recent[key]*6
    recent['accessionNumber'] = [f'0001234567-26-{i:06d}' for i in range(1,7)]
    calls = []
    def get(self, url):
        calls.append(url)
        if url.endswith('.json'): return json.dumps(data).encode()
        accession = url.rsplit('/',1)[-1].split('-index')[0].removesuffix('.txt')
        return (index() if url.endswith('-index.html') else submission()).replace(b'0001234567-26-000001', accession.encode())
    monkeypatch.setattr(DirectSourceClient, 'get', get)
    result = news.get_press_releases(symbol='TEST')
    assert len(calls) == 11  # One history plus two requests for each of five filings.
    assert result['coverage']['truncated'] and result['coverage']['filings_checked'] == 5
    assert result['coverage']['held'] == 5  # Identical source content under different accessions.
