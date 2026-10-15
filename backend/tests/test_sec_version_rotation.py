from datetime import datetime, timedelta, timezone
import json
import pytest
from sqlalchemy import select
from app.models import FundamentalsCache, InsightsSnapshot, Event
from app.jobs import warm_sec_fundamentals as job
from app.services import sec_fundamentals_preparation as prep, data_enrichment_queue as queue
from app.clients.direct_sources import DirectSourceError
from test_sec_fundamentals_warming import factory


@pytest.mark.parametrize('priority,dependencies,updates,limit,expected',[
    (set(),set(),{'UL'},3,['A','B','UL']),
    ({'A'},set(),{'UL'},3,['A','B','UL']),
    ({'A','B','C'},set(),{'UL'},3,['A','B','C']),
    ({'A','B'},{'C'},{'UL'},3,['A','B','C']),
    (set(),{'C'},{'UL'},3,['A','UL','C']),
    (set(),set(),{'B','UL'},3,['A','B','C']),
    (set(),set(),set(),3,['A','B','C']),
    (set(),set(),{'UL'},1,['UL']),
    ({'A'},set(),{'UL'},1,['A']),
    (set(),set(),{'UL'},0,[]),
    (set(),set(),{'UL'},9,['A','B','C','UL']),
])
def test_one_reserved_update_slot_preserves_bounds_and_protected_work(priority,dependencies,updates,limit,expected):
    due=['A','B','C','UL'];before=list(due)
    result=job.scope_batch(due,limit,priority=priority,changed_dependencies=dependencies,changed_versions=updates)
    assert result==expected and due==before
    assert len(result)<=limit and len(result)==len(set(result))


@pytest.fixture
def rolling_universe(factory,monkeypatch):
    now=datetime(2026,10,15,21,tzinfo=timezone.utc)
    class Clock(datetime):
        @classmethod
        def now(cls,tz=None):return now
    monkeypatch.setattr(job,'datetime',Clock)
    monkeypatch.setenv('SEC_FUNDAMENTALS_UNIVERSE_WARMING_ENABLED','1')
    monkeypatch.setenv('SEC_FUNDAMENTALS_WARMING_BATCH_SIZE','10')
    monkeypatch.setenv('FINNHUB_METRICS_WARMING_ENABLED','0')
    monkeypatch.setenv('FUNDAMENTALS_PROVIDER','fmp')
    monkeypatch.setattr(queue,'DEFAULT_PREWARM_SYMBOLS',[])
    symbols=['T'+chr(65+i) for i in range(11)]+['UL']
    state={'attempted_at':{s:(now-timedelta(hours=21 if s!='UL' else 10)).isoformat() for s in symbols},
        'attempted_versions':{s:prep.version_for_symbol(s) if s!='UL' else 'old' for s in symbols},
        'scope_refresh_after':{s:(now+timedelta(hours=10) if s=='UL' else now-timedelta(hours=1)).isoformat() for s in symbols}}
    with factory() as db:
        db.add_all([FundamentalsCache(symbol=s,provider='fmp',status='ok',fetched_at=now,roe=12.5) for s in symbols])
        db.add(InsightsSnapshot(kind=job.KEY,source='free_direct',fetched_at=now,payload_json=json.dumps(state)));db.commit()
    calls=[]
    def prepare(symbol,**kwargs):
        assert kwargs['max_cache_age']==timedelta(hours=20)
        calls.append(symbol)
        return {'status':'partial','source':'sec_edgar','values':{},'observed_at':now.isoformat()}
    monkeypatch.setattr(prep,'prepare',prepare)
    return factory,now,symbols,calls,prepare


def test_new_nonpriority_adapter_advances_and_displaced_regular_work_resumes(rolling_universe):
    factory,now,symbols,calls,_=rolling_universe
    first=job.run()
    assert first['due_scopes']==12 and first['completed_scopes']==first['batch_limit']==10
    assert calls==symbols[:9]+['UL']
    assert first['canonical_writes']==first['emails']==0 and first['fundamentals_selection']=='fmp'
    second=job.run();assert second['completed_scopes']==2 and calls[-2:]==symbols[9:11]
    assert job.run()['completed_scopes']==0 and calls.count('UL')==1
    with factory() as db:
        state=json.loads(db.get(InsightsSnapshot,job.KEY).payload_json)
        assert state['attempted_versions']['UL']==prep.version_for_symbol('UL')
        assert state['scope_refresh_after']['UL']==(now+timedelta(hours=20)).isoformat()
        assert not list(db.scalars(select(Event)))
        rows=list(db.scalars(select(FundamentalsCache)))
        assert len(rows)==12 and all(r.provider=='fmp' and r.roe==12.5 and r.status=='ok' for r in rows)


def test_source_failure_keeps_normal_backoff_and_does_not_repeat_reserved_request(rolling_universe,monkeypatch):
    factory,now,symbols,calls,prepare=rolling_universe
    def fail(symbol,**kwargs):
        if symbol=='UL':
            calls.append(symbol);raise DirectSourceError('Source HTTP 429: https://data.sec.gov/test')
        return prepare(symbol,**kwargs)
    monkeypatch.setattr(prep,'prepare',fail)
    first=job.run();assert first['results'][-1]['scope']=='UL' and first['status']=='partial'
    assert first['completed_scopes']==10
    assert job.run()['completed_scopes']==2
    assert job.run()['completed_scopes']==0 and calls.count('UL')==1
