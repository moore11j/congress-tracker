from contextlib import nullcontext
import json
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from app.db import Base
from app.models import Event, InsightsSnapshot
from app.jobs import warm_sec_fundamentals as job
from app.services import sec_directory, sec_fundamentals_preparation, data_enrichment_queue as queue
from app.clients.direct_sources import DirectSourceError


@pytest.fixture
def factory(monkeypatch):
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    sessions = sessionmaker(bind=engine)
    for module in (job, sec_directory, sec_fundamentals_preparation):
        monkeypatch.setattr(module, 'SessionLocal', sessions)
    monkeypatch.setenv('SEC_FUNDAMENTALS_WARMING_ENABLED', '1')
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
    monkeypatch.setenv('SEC_FUNDAMENTALS_WARMING_ENABLED', '0')
    assert job.run()['status'] == 'disabled'
    monkeypatch.setenv('SEC_FUNDAMENTALS_WARMING_ENABLED', '1')
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
    assert first['fundamentals_selection'] == 'fmp'
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
    symbols = ['T'+chr(65+i) for i in range(6)]
    monkeypatch.setattr(queue, 'DEFAULT_PREWARM_SYMBOLS', symbols)
    calls = []
    def prepare(symbol, **kwargs):
        assert job.run()['reason'] == 'active_warming_lease'
        calls.append(symbol)
        raise DirectSourceError('financial_symbol_identity_mismatch')
    monkeypatch.setattr(sec_fundamentals_preparation, 'prepare', prepare)
    assert job.run()['status'] == 'partial'
    assert calls == symbols[:5]
    calls.clear()
    job.run()
    assert calls[0] == symbols[-1]


def test_transport_denial_stops_batch_and_clears_lease(factory, monkeypatch):
    monkeypatch.setattr(queue, 'DEFAULT_PREWARM_SYMBOLS', ['ABC', 'DEF'])
    def deny(symbol):
        raise DirectSourceError('Source HTTP 403: https://data.sec.gov/test')
    monkeypatch.setattr(sec_fundamentals_preparation, 'prepare', deny)
    result = job.run()
    assert result['status'] == 'partial' and result['completed_scopes'] == 1
    with factory() as db:
        state = json.loads(db.get(InsightsSnapshot, job.KEY).payload_json)
        assert 'lease' not in state and list(state['attempted_at']) == ['ABC']




def test_opt_in_ranking_universe_prepares_without_starving_due_watch_symbols(factory,monkeypatch):
    from datetime import datetime,timezone,timedelta
    from app.models import FundamentalsCache
    monkeypatch.setenv('SEC_FUNDAMENTALS_UNIVERSE_WARMING_ENABLED','1')
    monkeypatch.setenv('SEC_FUNDAMENTALS_WARMING_BATCH_SIZE','1')
    now=datetime.now(timezone.utc)
    with factory() as db:
        db.add_all([FundamentalsCache(symbol='AAA',provider='fmp',status='ok',fetched_at=now),
                    FundamentalsCache(symbol='BBB',provider='fmp',status='failed',fetched_at=now)])
        db.commit()
    seen=[]
    def prepare(symbol, **kwargs):
        seen.append(symbol)
        return {'status':'partial','source':'sec_edgar','values':{},'observed_at':now.isoformat()}
    monkeypatch.setattr(sec_fundamentals_preparation,'prepare',prepare)
    assert job.run()['results'][0]['scope']=='ABC'
    second=job.run();assert second['results'][0]['scope']=='AAA'
    assert second['ranking_cache_symbols']==1 and second['canonical_writes']==0
    with factory() as db:
        state=db.get(InsightsSnapshot,job.KEY);payload=json.loads(state.payload_json)
        payload['attempted_at']['ABC']=(now-timedelta(hours=21)).isoformat()
        state.payload_json=json.dumps(payload);db.commit()
    # An old attempt cannot override the actual observation deadline.
    assert job.run()['completed_scopes']==0
    with factory() as db:
        state=db.get(InsightsSnapshot,job.KEY);payload=json.loads(state.payload_json)
        payload['scope_refresh_after']['ABC']=(now-timedelta(seconds=1)).isoformat()
        state.payload_json=json.dumps(payload);db.commit()
    assert job.run()['results'][0]['scope']=='ABC'
    assert 'BBB' not in seen


def test_universe_batch_limit_is_bounded_and_opt_in(factory,monkeypatch):
    monkeypatch.setenv('SEC_FUNDAMENTALS_WARMING_BATCH_SIZE','999')
    monkeypatch.setattr(queue,'DEFAULT_PREWARM_SYMBOLS',['T'+chr(65+i) for i in range(15)])
    monkeypatch.setattr(sec_fundamentals_preparation,'prepare',lambda s, **kwargs:{'status':'partial','source':'sec_edgar','values':{},'observed_at':'2026-10-10T00:00:00+00:00'})
    assert job.run()['completed_scopes']==5
    monkeypatch.setenv('SEC_FUNDAMENTALS_UNIVERSE_WARMING_ENABLED','1')
    result=job.run();assert result['completed_scopes']==result['batch_limit']==10


def test_new_parser_version_refreshes_priority_once_then_resumes_universe(factory,monkeypatch):
    from datetime import datetime,timezone
    from app.models import FundamentalsCache
    monkeypatch.setenv('SEC_FUNDAMENTALS_UNIVERSE_WARMING_ENABLED','1')
    monkeypatch.setenv('SEC_FUNDAMENTALS_WARMING_BATCH_SIZE','1')
    now=datetime.now(timezone.utc)
    with factory() as db:
        db.add(FundamentalsCache(symbol='AAA',provider='fmp',status='ok',fetched_at=now))
        db.add(InsightsSnapshot(kind=job.KEY,source='free_direct',fetched_at=now,payload_json=json.dumps({
            'attempted_at':{'ABC':now.isoformat()},'attempted_versions':{'ABC':'old'}})))
        db.commit()
    def prepare(symbol, **kwargs):return {'status':'partial','source':'sec_edgar','values':{},'observed_at':now.isoformat()}
    monkeypatch.setattr(sec_fundamentals_preparation,'prepare',prepare)
    assert job.run()['results'][0]['scope']=='ABC'
    assert job.run()['results'][0]['scope']=='AAA'
    with factory() as db:
        state=json.loads(db.get(InsightsSnapshot,job.KEY).payload_json)
        assert state['attempted_versions']['ABC']==sec_fundamentals_preparation.VERSION


def test_issuer_revision_refreshes_only_changed_priority_before_universe(factory,monkeypatch):
    from datetime import datetime,timezone
    from app.models import FundamentalsCache
    monkeypatch.setenv('SEC_FUNDAMENTALS_UNIVERSE_WARMING_ENABLED','1')
    monkeypatch.setenv('SEC_FUNDAMENTALS_WARMING_BATCH_SIZE','1')
    monkeypatch.setattr(queue,'DEFAULT_PREWARM_SYMBOLS',['ABC','XOM'])
    now=datetime.now(timezone.utc)
    with factory() as db:
        db.add(FundamentalsCache(symbol='AAA',provider='fmp',status='ok',fetched_at=now))
        db.add(InsightsSnapshot(kind=job.KEY,source='free_direct',fetched_at=now,payload_json=json.dumps({
            'attempted_at':{s:now.isoformat() for s in ['ABC','XOM']},
            'attempted_versions':{s:sec_fundamentals_preparation.VERSION for s in ['ABC','XOM']}})))
        db.commit()
    monkeypatch.setattr(sec_fundamentals_preparation,'prepare',lambda symbol, **kwargs:{'status':'partial','source':'sec_edgar','values':{},'observed_at':now.isoformat()})
    assert job.run()['results'][0]['scope']=='XOM'
    assert job.run()['results'][0]['scope']=='AAA'
    with factory() as db:
        state=json.loads(db.get(InsightsSnapshot,job.KEY).payload_json)
        assert state['attempted_versions']['ABC']==sec_fundamentals_preparation.VERSION
        assert state['attempted_versions']['XOM']==sec_fundamentals_preparation.version_for_symbol('XOM')


def test_metrics_are_bounded_fair_daily_and_stop_on_shared_quota(factory,monkeypatch):
    from app.services import finnhub_metrics
    from app.services.finnhub_research import FinnhubUnavailable
    from datetime import datetime,timezone
    now=datetime.now(timezone.utc)
    monkeypatch.setenv('FINNHUB_METRICS_WARMING_ENABLED','1')
    monkeypatch.setattr(queue,'DEFAULT_PREWARM_SYMBOLS',['ABC','DEF','XYZ'])
    monkeypatch.setattr(sec_fundamentals_preparation,'prepare',lambda symbol, **kwargs:{'status':'partial','source':'sec_edgar','values':{},'observed_at':now.isoformat()})
    calls=[]
    def prepare(s):
        calls.append(s)
        return {'status':'partial','values':{'roe':25}}
    monkeypatch.setattr(finnhub_metrics,'prepare',prepare)
    assert len(job.run()['metric_results'])==2 and calls==['ABC','DEF']
    assert len(job.run()['metric_results'])==1 and calls==['ABC','DEF','XYZ']
    assert job.run()['metric_results']==[]
    with factory() as db:
        row=db.get(InsightsSnapshot,job.KEY);state=json.loads(row.payload_json)
        state['metric_attempted_at']={};row.payload_json=json.dumps(state);db.commit()
    def deny(s):
        calls.append(s)
        raise FinnhubUnavailable('request_budget_exhausted')
    calls.clear();monkeypatch.setattr(finnhub_metrics,'prepare',deny)
    result=job.run()
    assert len(calls)==1 and result['status']=='partial'
    with factory() as db:
        assert json.loads(db.get(InsightsSnapshot,job.KEY).payload_json)['metric_attempted_at']=={}


def test_metric_capacity_covers_universe_without_starving_priority():
    from datetime import datetime,timedelta,timezone
    now=datetime(2026,10,10,tzinfo=timezone.utc)
    symbols=[f'S{i:04}' for i in range(1751)]
    priority=set(symbols[-48:]);attempts={};seen=[]
    for tick in range(219):
        at=now+timedelta(minutes=5*tick)
        scopes=job.metric_scopes(symbols,priority,attempts,at,8)
        if tick==0: assert set(scopes)<=priority
        seen.extend(scopes)
        for symbol in scopes:attempts[symbol]=at.isoformat()
    assert len(seen)==len(set(seen))==1751
    assert job.metric_scopes(symbols,priority,attempts,now+timedelta(hours=23),8)==[]
    assert set(job.metric_scopes(symbols,priority,attempts,now+timedelta(hours=24),8))<=priority


def test_denied_metric_symbol_does_not_starve_the_rest_of_the_universe(factory,monkeypatch):
    from app.services import finnhub_metrics
    from app.services.finnhub_research import FinnhubUnavailable
    from datetime import datetime,timezone,timedelta
    now=datetime.now(timezone.utc)
    monkeypatch.setenv('FINNHUB_METRICS_WARMING_ENABLED','1')
    monkeypatch.setenv('FINNHUB_METRICS_BATCH_SIZE','2')
    monkeypatch.setattr(queue,'DEFAULT_PREWARM_SYMBOLS',['AAPL.TO','ABC','DEF'])
    monkeypatch.setattr(sec_fundamentals_preparation,'prepare',lambda symbol, **kwargs:{
        'status':'partial','source':'sec_edgar','values':{},'observed_at':now.isoformat()})
    calls=[]
    def prepare(symbol):
        calls.append(symbol)
        if symbol=='AAPL.TO':raise FinnhubUnavailable('access_denied')
        return {'status':'partial','values':{'roe':25},'observed_at':now.isoformat()}
    monkeypatch.setattr(finnhub_metrics,'prepare',prepare)
    first=job.run()
    assert calls==['AAPL.TO','ABC']
    assert first['metric_stop_reason'] is None and first['status']=='partial'
    assert first['metric_results'][0]['reason']=='access_denied'
    assert first['canonical_writes']==first['emails']==0
    job.run();assert calls==['AAPL.TO','ABC','DEF']
    with factory() as db:
        attempts=json.loads(db.get(InsightsSnapshot,job.KEY).payload_json)['metric_attempted_at']
    assert 'AAPL.TO' in attempts
    assert job.metric_scopes(['AAPL.TO'],set(),attempts,now+timedelta(hours=23),8)==[]
    assert job.metric_scopes(['AAPL.TO'],set(),attempts,now+timedelta(days=2),8)==['AAPL.TO']


@pytest.mark.parametrize('reason',['authentication_failed','missing_api_key','rate_limited',
    'provider_cooldown','request_budget_exhausted','shared_budget_required'])
def test_global_metric_failure_stops_without_deferring_unattempted_symbols(factory,monkeypatch,reason):
    from app.services import finnhub_metrics
    from app.services.finnhub_research import FinnhubUnavailable
    from datetime import datetime,timezone
    now=datetime.now(timezone.utc)
    monkeypatch.setenv('FINNHUB_METRICS_WARMING_ENABLED','1')
    monkeypatch.setenv('FINNHUB_METRICS_BATCH_SIZE','8')
    monkeypatch.setattr(queue,'DEFAULT_PREWARM_SYMBOLS',['ABC','DEF'])
    monkeypatch.setattr(sec_fundamentals_preparation,'prepare',lambda symbol, **kwargs:{
        'status':'partial','source':'sec_edgar','values':{},'observed_at':now.isoformat()})
    calls=[]
    def denied(symbol):calls.append(symbol);raise FinnhubUnavailable(reason)
    monkeypatch.setattr(finnhub_metrics,'prepare',denied)
    result=job.run()
    assert calls==['ABC'] and result['metric_stop_reason']==reason
    with factory() as db:
        assert json.loads(db.get(InsightsSnapshot,job.KEY).payload_json)['metric_attempted_at']=={}


@pytest.mark.parametrize('configured,expected',[('999',8),('8',8),('0',1),('bad',2)])
def test_metric_configuration_has_hard_request_cap(monkeypatch,configured,expected):
    monkeypatch.setenv('FINNHUB_METRICS_BATCH_SIZE',configured)
    assert job.metric_batch_limit()==expected


def test_metric_elapsed_budget_does_not_mark_unattempted_scopes(factory,monkeypatch):
    from app.services import finnhub_metrics
    from datetime import datetime,timezone
    now=datetime.now(timezone.utc)
    monkeypatch.setenv('FINNHUB_METRICS_WARMING_ENABLED','1')
    monkeypatch.setenv('FINNHUB_METRICS_BATCH_SIZE','8')
    monkeypatch.setattr(queue,'DEFAULT_PREWARM_SYMBOLS',['ABC','DEF','XYZ'])
    monkeypatch.setattr(sec_fundamentals_preparation,'prepare',lambda s, **kwargs:{'status':'partial','source':'sec_edgar','values':{},'observed_at':now.isoformat()})
    elapsed=[0.0];calls=[]
    monkeypatch.setattr(job.time,'monotonic',lambda:elapsed[0])
    def prepare(symbol):
        calls.append(symbol);elapsed[0]=21.0
        return {'status':'partial','values':{},'observed_at':now.isoformat()}
    monkeypatch.setattr(finnhub_metrics,'prepare',prepare)
    result=job.run()
    assert calls==['ABC'] and result['metric_stop_reason']=='elapsed_budget'
    with factory() as db:
        attempts=json.loads(db.get(InsightsSnapshot,job.KEY).payload_json)['metric_attempted_at']
        assert set(attempts)=={'ABC'}
    assert job.metric_scopes(['ABC','DEF','XYZ'],set(),attempts,now,8)==['DEF','XYZ']


def test_near_expiry_metric_cache_hit_does_not_defer_refresh_another_day(factory,monkeypatch):
    from app.services import finnhub_metrics
    from datetime import datetime,timedelta,timezone
    now=datetime.now(timezone.utc);observed=now-timedelta(hours=24)+timedelta(seconds=10)
    monkeypatch.setenv('FINNHUB_METRICS_WARMING_ENABLED','1')
    monkeypatch.setattr(sec_fundamentals_preparation,'prepare',lambda s, **kwargs:{'status':'partial','source':'sec_edgar','values':{},'observed_at':now.isoformat()})
    monkeypatch.setattr(finnhub_metrics,'prepare',lambda s:{'status':'partial','values':{'roe':25},'observed_at':observed.isoformat()})
    job.run()
    with factory() as db:attempts=json.loads(db.get(InsightsSnapshot,job.KEY).payload_json)['metric_attempted_at']
    assert attempts['ABC']==observed.isoformat()
    assert job.metric_scopes(['ABC'],{'ABC'},attempts,now+timedelta(minutes=5),8)==['ABC']
