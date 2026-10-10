from contextlib import nullcontext
from datetime import datetime, timedelta, timezone
import json
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import sessionmaker
from app.db import Base
from app.models import Event, InsightsSnapshot, ResearchSourceDocument, Security, TickerContentCache
from app.jobs import warm_sec_earnings as job
from app.services import sec_directory, sec_press_releases as press, data_enrichment_queue as queue
from app.services.direct_feed_store import DirectFeedDocument, DirectFeedRevision
from app.clients.direct_sources import DirectSourceClient, DirectSourceError
from test_sec_earnings_materials import company, submission
from test_sec_earnings_store import index


@pytest.fixture
def prepared(monkeypatch):
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine)
    with factory() as db:
        db.add(Security(id=1, symbol='TEST', name='Test', asset_class='stock'))
        db.commit()
    for module in (job, press, sec_directory):
        monkeypatch.setattr(module, 'SessionLocal', factory)
    monkeypatch.setenv('SEC_EARNINGS_WARMING_ENABLED', '1')
    monkeypatch.setenv('PRESS_RELEASE_PROVIDER', 'fmp')
    monkeypatch.setattr(job, 'collector_lock', lambda: nullcontext(True))
    monkeypatch.setattr(job, 'check_background_job_guard', lambda *a: SimpleNamespace(proceed=True))
    monkeypatch.setattr(queue, '_recently_viewed_ticker_symbols', lambda *a, **k: [])
    monkeypatch.setattr(queue, 'DEFAULT_PREWARM_SYMBOLS', ['TEST'])
    calls = []
    def fetch(self, url):
        calls.append(url)
        if url == sec_directory.URL:
            return json.dumps({'fields': ['cik', 'name', 'ticker', 'exchange'],
                               'data': [[1234567, 'Test', 'TEST', 'NYSE']]}).encode()
        return company() if url.endswith('.json') else index() if url.endswith('-index.html') else submission()
    monkeypatch.setattr(DirectSourceClient, 'get', fetch)
    yield factory, calls
    engine.dispose()


def test_preparation_stages_verified_sources_without_public_selection_or_canonical_writes(prepared):
    factory, calls = prepared
    first = job.run()
    assert first['status'] == 'ok' and first['completed_scopes'] == 1
    assert first['press_selection'] == 'fmp'
    assert first['canonical_writes'] == first['model_calls'] == first['emails'] == 0
    assert first['results'][0]['items'] == 1
    assert first['results'][0]['coverage']['complete'] is False
    assert job.run()['results'] == first['results'] and len(calls) == 4
    with factory() as db:
        assert db.scalar(select(func.count()).select_from(DirectFeedDocument)) == 3
        assert db.scalar(select(func.count()).select_from(DirectFeedRevision)) == 3
        assert db.scalar(select(func.count()).select_from(ResearchSourceDocument)) == 0
        assert db.scalar(select(func.count()).select_from(Event)) == 0
        assert not json.loads(db.get(InsightsSnapshot, job.KEY).payload_json).get('lease')
    payload = press.get_sec_releases(symbol='TEST', prepared_only=True)
    assert payload['items'][0]['published_at'] is None
    assert payload['items'][0]['filing_date'] == '2026-07-30'
    with pytest.raises(ValueError, match='not selected'):
        press.refresh_sec_releases('TEST')


def test_disabled_and_busy_guards_do_no_source_work(prepared, monkeypatch):
    monkeypatch.setenv('SEC_EARNINGS_WARMING_ENABLED', '0')
    assert job.run()['status'] == 'disabled'
    with pytest.raises(ValueError, match='disabled'):
        press.prepare_sec_releases('TEST')
    monkeypatch.setenv('SEC_EARNINGS_WARMING_ENABLED', '1')
    monkeypatch.setattr(job, 'collector_lock', lambda: nullcontext(False))
    assert job.run()['reason'] == 'source_collection_active'
    monkeypatch.setattr(job, 'check_background_job_guard', lambda *a: SimpleNamespace(
        proceed=False, to_dict=lambda: {'reason': 'db_pressure'}))
    assert job.run()['reason'] == 'db_pressure' and not prepared[1]


def test_older_exhibit_classification_cache_is_reprepared_without_duplicate_sources(prepared):
    factory, calls = prepared
    job.run()
    with factory() as db:
        row = db.scalar(select(TickerContentCache))
        payload = json.loads(row.payload_json)
        payload.pop('cache_version')
        row.payload_json = json.dumps(payload)
        db.commit()
    assert press._cached('TEST') is None
    result = job.run()
    assert result['results'][0]['items'] == 1 and len(calls) == 7
    assert press._cached('TEST')['cache_version'] == press.CACHE_VERSION
    with factory() as db:
        assert db.scalar(select(func.count()).select_from(DirectFeedDocument)) == 3
        assert db.scalar(select(func.count()).select_from(DirectFeedRevision)) == 3


def test_failed_scopes_rotate_and_live_lease_refuses_overlap(prepared, monkeypatch):
    symbols = ['T'+chr(65+i) for i in range(6)]
    monkeypatch.setattr(queue, 'DEFAULT_PREWARM_SYMBOLS', symbols)
    calls = []
    def prepare(symbol):
        assert job.run()['reason'] == 'active_warming_lease'
        calls.append(symbol)
        raise ValueError('missing identity')
    monkeypatch.setattr(press, 'prepare_sec_releases', prepare)
    assert job.run()['completed_scopes'] == 5 and calls == symbols[:5]
    calls.clear()
    job.run()
    assert calls[0] == symbols[-1]


@pytest.mark.parametrize('code', [403, 429, 500, 502, 503, 504])
def test_transport_denial_preserves_cache_and_stops_batch(prepared, monkeypatch, code):
    factory, calls = prepared
    job.run()
    before = press._cached('TEST')
    with factory() as db:
        row = db.scalar(select(TickerContentCache))
        row.fetched_at = datetime.now(timezone.utc)-timedelta(days=1)
        state = db.get(InsightsSnapshot, job.KEY)
        state.payload_json = '{}'
        db.commit()
    monkeypatch.setattr(queue, 'DEFAULT_PREWARM_SYMBOLS', ['TEST', 'ZZZ'])
    def deny(self, url):
        if url.endswith('.json'): return company()
        calls.append(url)
        raise DirectSourceError(f'Source HTTP {code}: https://www.sec.gov/Archives/test')
    monkeypatch.setattr(DirectSourceClient, 'get', deny)
    result = job.run()
    assert result['completed_scopes'] == 1 and result['status'] == 'partial'
    with factory() as db:
        assert json.loads(db.scalar(select(TickerContentCache)).payload_json) == before
        assert not json.loads(db.get(InsightsSnapshot, job.KEY).payload_json).get('lease')


def test_preparation_revoked_during_http_does_not_replace_cache(prepared, monkeypatch):
    original = DirectSourceClient.get
    def revoked(self, url):
        result = original(self, url)
        if url.endswith('-index.html'):
            monkeypatch.setenv('SEC_EARNINGS_WARMING_ENABLED', '0')
        return result
    monkeypatch.setattr(DirectSourceClient, 'get', revoked)
    assert job.run()['results'][0]['status'] == 'unavailable'
    with prepared[0]() as db:
        assert db.scalar(select(func.count()).select_from(TickerContentCache)) == 0
        assert db.scalar(select(func.count()).select_from(Event)) == 0


def test_verified_directory_absence_is_unavailable_not_perpetual_warming(prepared, monkeypatch):
    factory, calls = prepared
    monkeypatch.setattr(sec_directory, 'directory', lambda: {})
    first = job.run()
    assert first['results'][0]['status'] == 'unavailable'
    assert first['results'][0]['coverage']['reason'] == 'symbol_absent_from_sec_directory'
    assert not calls
    monkeypatch.setattr(sec_directory, 'directory', lambda: pytest.fail('Repeated absent-directory lookup'))
    monkeypatch.setattr(queue, 'enqueue_data_enrichment_job', lambda **kw: pytest.fail('Unavailable panel enqueued'))
    assert job.run()['results'] == first['results']
    from app.services import fmp_news
    monkeypatch.setattr(fmp_news, '_is_public_request_context', lambda: True)
    payload = press.get_sec_releases(symbol='TEST')
    assert payload['status'] == 'unavailable' and payload['reason'] == 'symbol_absent_from_sec_directory'
    assert payload['items'] == [] and not payload['has_next']
    with factory() as db:
        assert db.scalar(select(func.count()).select_from(TickerContentCache)) == 1
        assert db.scalar(select(func.count()).select_from(DirectFeedDocument)) == 0
        assert db.scalar(select(func.count()).select_from(Event)) == 0
