from contextlib import nullcontext
import json
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from app.db import Base
from app.models import Event, InsightsSnapshot
from app.jobs import warm_sec_research as job
from app.services import sec_directory, sec_financial_statements, data_enrichment_queue as queue
from app.clients.direct_sources import DirectSourceError


@pytest.fixture
def factory(monkeypatch):
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    sessions = sessionmaker(bind=engine)
    for module in (job, sec_directory, sec_financial_statements, queue):
        monkeypatch.setattr(module, 'SessionLocal', sessions)
    monkeypatch.setenv('SEC_RESEARCH_WARMING_ENABLED', '1')
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
    monkeypatch.setenv('SEC_RESEARCH_WARMING_ENABLED', '0')
    assert job.run()['status'] == 'disabled'
    monkeypatch.setenv('SEC_RESEARCH_WARMING_ENABLED', '1')
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
    assert first['financial_selection'] == first['metadata_selection'] == 'fmp'
    assert first['canonical_writes'] == first['emails'] == 0
    assert first['results'][0]['status'] == 'partial'
    assert len(calls) == 3
    assert job.run()['results'] == first['results']
    assert len(calls) == 3
    with factory() as db:
        assert not list(db.scalars(select(Event)))
        assert len(list(db.scalars(select(InsightsSnapshot)))) == 3
        assert 'lease' not in json.loads(db.get(InsightsSnapshot, job.KEY).payload_json)


def test_failed_scopes_rotate_and_lease_prevents_overlap(factory, monkeypatch):
    symbols = ['T'+chr(65+i) for i in range(21)]
    monkeypatch.setattr(queue, 'DEFAULT_PREWARM_SYMBOLS', symbols)
    calls = []
    def prepare(symbol):
        assert job.run()['reason'] == 'active_warming_lease'
        calls.append(symbol)
        raise DirectSourceError('financial_symbol_identity_mismatch')
    monkeypatch.setattr(sec_financial_statements, 'prepared', prepare)
    assert job.run()['status'] == 'partial'
    assert calls == symbols[:20]
    calls.clear()
    job.run()
    assert calls[0] == symbols[-1]


def test_transport_denial_stops_batch_and_clears_lease(factory, monkeypatch):
    monkeypatch.setattr(queue, 'DEFAULT_PREWARM_SYMBOLS', ['ABC', 'DEF'])
    def deny(symbol):
        raise DirectSourceError('Source HTTP 403: https://data.sec.gov/test')
    monkeypatch.setattr(sec_financial_statements, 'prepared', deny)
    result = job.run()
    assert result['status'] == 'partial' and result['completed_scopes'] == 1
    with factory() as db:
        state = json.loads(db.get(InsightsSnapshot, job.KEY).payload_json)
        assert 'lease' not in state and list(state['attempted_at']) == ['ABC']


def test_verified_directory_absence_is_unavailable_in_public_panel_not_perpetual_warming(factory, monkeypatch):
    from app.services import ticker_financials
    from app.request_priority import set_request_context, reset_request_context
    monkeypatch.setattr(sec_directory, 'directory', lambda: {})
    monkeypatch.setattr(sec_directory.DirectSourceClient, 'get', lambda *a: pytest.fail('unidentified source request'))
    first = job.run()
    assert first['results'][0]['status'] == 'unavailable'
    monkeypatch.setenv('FINANCIAL_STATEMENTS_PROVIDER', 'sec_edgar')
    monkeypatch.setattr(queue, 'enqueue_data_enrichment_job', lambda **kw: pytest.fail('unavailable cache requeued'))
    token = set_request_context({'path': '/api/tickers/ABC/financials'})
    try:
        payload = ticker_financials.get_ticker_financials('ABC')
        assert payload['status'] == 'unavailable' and payload['source'] == 'sec_edgar'
        assert not payload['annual'] and not payload['quarterly']
    finally:
        reset_request_context(token)


def test_transient_directory_failure_is_not_cached_as_missing_security(factory, monkeypatch):
    def denied():
        raise DirectSourceError('Source HTTP 403: https://www.sec.gov/files/company_tickers_exchange.json')
    monkeypatch.setattr(sec_directory, 'directory', denied)
    assert job.run()['status'] == 'partial'
    with factory() as db:
        assert db.get(InsightsSnapshot, 'sec-financials:ABC:v1') is None


def test_new_public_panel_demand_prepares_without_watch_or_general_queue(factory,monkeypatch):
    from app.services import ticker_financials
    from app.request_priority import set_request_context,reset_request_context
    from app.models import DataEnrichmentJob
    from test_sec_fundamentals import sources
    monkeypatch.setenv('FINANCIAL_STATEMENTS_PROVIDER','sec_edgar')
    monkeypatch.setenv('FMP_BACKGROUND_REFRESH_ENABLED','false')
    monkeypatch.setattr(queue,'DEFAULT_PREWARM_SYMBOLS',[])
    monkeypatch.setattr(sec_directory,'directory',lambda:{'ABC':{'cik':'0000000001'}})
    facts,company=sources();calls=[]
    def fetch(self,url):
        calls.append(url)
        return json.dumps(facts if 'companyfacts' in url else company).encode()
    monkeypatch.setattr(sec_directory.DirectSourceClient,'get',fetch)
    token=set_request_context({'path':'/api/tickers/ABC/financials'})
    try:
        assert ticker_financials.get_ticker_financials('ABC')['status']=='warming'
        assert not calls
    finally:reset_request_context(token)
    with factory() as db:
        row=db.scalar(select(DataEnrichmentJob));assert row.symbol=='ABC' and row.status=='queued'
        before=(row.id,row.dedupe_key,row.attempts,row.status)
    result=job.run()
    assert result['requested_cache_misses']==result['completed_scopes']==1
    assert result['results'][0]['scope']=='ABC' and len(calls)==2
    token=set_request_context({'path':'/api/tickers/ABC/financials'})
    try:
        panel=ticker_financials.get_ticker_financials('ABC')
        assert panel['source']=='sec_edgar' and len(panel['annual'])==2
        assert ticker_financials.get_ticker_financials('ABC')==panel
    finally:reset_request_context(token)
    repeat=job.run();assert repeat['requested_cache_misses']==repeat['completed_scopes']==0
    assert len(calls)==2
    with factory() as db:
        row=db.scalar(select(DataEnrichmentJob))
        assert (row.id,row.dedupe_key,row.attempts,row.status)==before
        assert not list(db.scalars(select(Event)))


def test_demand_is_bounded_excludes_fresh_future_other_jobs_and_keeps_watch_rotation(factory,monkeypatch):
    from datetime import datetime,timezone,timedelta
    from app.models import DataEnrichmentJob
    now=datetime.now(timezone.utc)
    watches=['W'+chr(65+i) for i in range(21)]
    requests=['Z'+chr(65+i) for i in range(21)]
    monkeypatch.setattr(queue,'DEFAULT_PREWARM_SYMBOLS',watches)
    with factory() as db:
        for symbol in requests+['CACHED','FUTURE','FINISHED','OTHER']:
            db.add(DataEnrichmentJob(symbol=symbol,job_type='ticker_meta' if symbol=='OTHER' else 'ticker_financials',
                dedupe_key=symbol,status='succeeded' if symbol=='FINISHED' else 'queued',
                next_run_at=now+timedelta(days=1) if symbol=='FUTURE' else now,priority=45))
        db.add(InsightsSnapshot(kind='sec-financials:CACHED:v1',source='sec_edgar',fetched_at=now,
            payload_json=json.dumps({'source':'sec_edgar','symbol':'CACHED','status':'unavailable'})))
        db.commit()
    seen=[]
    def prepare(symbol):
        seen.append(symbol)
        payload={'symbol':symbol,'source':'sec_edgar','status':'partial','annual':[],'quarterly':[],'updatedAt':now.isoformat()}
        with factory() as db:
            db.add(InsightsSnapshot(kind=f'sec-financials:{symbol}:v1',source='sec_edgar',fetched_at=now,payload_json=json.dumps(payload)));db.commit()
        return payload
    monkeypatch.setattr(sec_financial_statements,'prepared',prepare)
    first=job.run();assert first['completed_scopes']==20 and first['requested_cache_misses']==21
    assert seen[:10]==requests[:10] and seen[10:]==watches[:10]
    seen.clear();second=job.run()
    assert second['completed_scopes']==20 and second['requested_cache_misses']==11
    assert seen[:10]==requests[10:20] and seen[10:]==watches[10:20]
    assert not set(seen)&{'CACHED','FUTURE','FINISHED','OTHER'}
    assert first['canonical_writes']==second['emails']==0


@pytest.mark.parametrize('stale_watches', [0, 21])
def test_fresh_watched_caches_do_not_displace_pending_demand(factory, monkeypatch, stale_watches):
    from datetime import datetime, timedelta, timezone
    from app.models import DataEnrichmentJob
    now = datetime.now(timezone.utc)
    watches = ['W'+chr(65+i) for i in range(21)]
    requests = ['R'+chr(65+i) for i in range(13)]
    monkeypatch.setattr(queue, 'DEFAULT_PREWARM_SYMBOLS', watches)
    with factory() as db:
        for i, symbol in enumerate(watches):
            stamp = now-timedelta(days=2) if i < stale_watches else now
            payload = {'symbol': symbol, 'source': 'sec_edgar', 'status': 'partial',
                       'annual': [], 'quarterly': [], 'updatedAt': stamp.isoformat()}
            db.add(InsightsSnapshot(kind=f'sec-financials:{symbol}:v1', source='sec_edgar',
                                   fetched_at=stamp, payload_json=json.dumps(payload)))
        for symbol in requests:
            db.add(DataEnrichmentJob(symbol=symbol, job_type='ticker_financials', dedupe_key=symbol,
                                    status='queued', next_run_at=now, priority=45))
        db.commit()
    seen = []
    def prepare(symbol):
        seen.append(symbol)
        return {'symbol': symbol, 'source': 'sec_edgar', 'status': 'partial', 'annual': [],
                'quarterly': [], 'updatedAt': now.isoformat()}
    monkeypatch.setattr(sec_financial_statements, 'prepared', prepare)
    result = job.run()
    assert result['planned_scopes'] == result['completed_scopes'] == 20
    assert result['requested_cache_misses'] == 13
    assert seen[:10] == requests[:10]
    if stale_watches:
        assert seen[10:] == watches[:10]
    else:
        assert seen[:13] == requests
        assert seen[13:] == watches[:7]
    assert len(set(seen)) == 20
    assert result['canonical_writes'] == result['emails'] == 0
