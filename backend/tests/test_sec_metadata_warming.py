from contextlib import nullcontext
import json
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from app.db import Base
from app.models import Event, InsightsSnapshot
from app.jobs import warm_sec_metadata as job
from app.services import sec_directory, sec_financial_statements, data_enrichment_queue as queue
from app.clients.direct_sources import DirectSourceError


@pytest.fixture
def factory(monkeypatch):
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    sessions = sessionmaker(bind=engine)
    for module in (job, sec_directory, sec_financial_statements):
        monkeypatch.setattr(module, 'SessionLocal', sessions)
    monkeypatch.setenv('SEC_METADATA_WARMING_ENABLED', '1')
    monkeypatch.setenv('FINANCIAL_STATEMENTS_PROVIDER', 'fmp')
    monkeypatch.setenv('COMPANY_METADATA_PROVIDER', 'fmp')
    monkeypatch.setattr(job, 'collector_lock', lambda: nullcontext(True))
    monkeypatch.setattr(job, 'check_background_job_guard', lambda *a: SimpleNamespace(proceed=True))
    monkeypatch.setattr(queue, '_recently_viewed_ticker_symbols', lambda *a, **k: [])
    monkeypatch.setattr(queue, 'DEFAULT_PREWARM_SYMBOLS', ['ABC'])
    yield sessions
    engine.dispose()


def test_disabled_pressure_and_shared_collector_hold_before_cache_work(factory, monkeypatch):
    monkeypatch.setattr(job, 'SessionLocal', lambda: pytest.fail('cache database opened'))
    monkeypatch.setenv('SEC_METADATA_WARMING_ENABLED', '0')
    assert job.run()['status'] == 'disabled'
    monkeypatch.setenv('SEC_METADATA_WARMING_ENABLED', '1')
    monkeypatch.setattr(job, 'collector_lock', lambda: nullcontext(False))
    assert job.run()['reason'] == 'source_collection_active'
    monkeypatch.setattr(job, 'check_background_job_guard', lambda *a: SimpleNamespace(
        proceed=False, to_dict=lambda: {'reason': 'db_pressure'}))
    assert job.run() == {'status': 'held', 'reason': 'db_pressure'}


def test_prepares_actual_separate_cache_repeat_preserves_public_selection(factory, monkeypatch):
    from test_sec_fundamentals import sources
    facts, company = sources()
    calls = []
    def fetch(self, url):
        calls.append(url)
        if url == sec_directory.URL:
            data = {'fields': ['cik', 'name', 'ticker', 'exchange'], 'data': [[1, 'Example', 'ABC', 'NYSE']]}
        else:
            data = facts if 'companyfacts' in url else company
        return json.dumps(data).encode()
    monkeypatch.setattr(sec_directory.DirectSourceClient, 'get', fetch)
    first = job.run()
    assert first['status'] == 'ok' and first['completed_scopes'] == 1
    assert first['metadata_selection'] == 'fmp'
    assert first['canonical_writes'] == first['emails'] == 0
    assert first['results'][0]['status'] == 'ok'
    assert len(calls) == 2
    assert job.run()['results'] == first['results']
    assert len(calls) == 2
    with factory() as db:
        assert not list(db.scalars(select(Event)))
        assert len(list(db.scalars(select(InsightsSnapshot)))) == 3
        assert 'lease' not in json.loads(db.get(InsightsSnapshot, job.KEY).payload_json)


def test_failed_scopes_rotate_and_lease_prevents_overlap(factory, monkeypatch):
    symbols = ['T'+chr(65+i) for i in range(6)]
    monkeypatch.setattr(queue, 'DEFAULT_PREWARM_SYMBOLS', symbols)
    calls = []
    def prepare(symbol):
        assert job.run()['reason'] == 'active_warming_lease'
        calls.append(symbol)
        raise DirectSourceError('financial_symbol_identity_mismatch')
    monkeypatch.setattr(sec_directory, 'symbol_metadata', prepare)
    assert job.run()['status'] == 'partial'
    assert calls == symbols[:5]
    calls.clear()
    job.run()
    assert calls[0] == symbols[-1]


def test_transport_denial_stops_batch_and_clears_lease(factory, monkeypatch):
    monkeypatch.setattr(queue, 'DEFAULT_PREWARM_SYMBOLS', ['ABC', 'DEF'])
    def deny(symbol):
        raise DirectSourceError('Source HTTP 403: https://data.sec.gov/test')
    monkeypatch.setattr(sec_directory, 'symbol_metadata', deny)
    result = job.run()
    assert result['status'] == 'partial' and result['completed_scopes'] == 1
    with factory() as db:
        state = json.loads(db.get(InsightsSnapshot, job.KEY).payload_json)
        assert 'lease' not in state and list(state['attempted_at']) == ['ABC']


