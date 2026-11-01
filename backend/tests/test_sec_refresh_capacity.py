from datetime import datetime,timedelta,timezone
import json
import pytest
from app.jobs import warm_sec_fundamentals as job
from app.services import sec_fundamentals_preparation as prep,data_enrichment_queue as queue
from app.models import InsightsSnapshot
from test_sec_fundamentals_warming import factory

@pytest.mark.parametrize('configured,expected',[('999',16),('16',16),('10',10),('1',1),('0',1),('bad',5)])
def test_sec_batch_configuration_is_bounded(monkeypatch,configured,expected):
    monkeypatch.setenv('SEC_FUNDAMENTALS_WARMING_BATCH_SIZE',configured)
    assert job.sec_batch_limit()==expected

def test_elapsed_stop_preserves_unattempted_deadlines_and_next_run(factory,monkeypatch):
    now=datetime(2026,11,1,tzinfo=timezone.utc);elapsed=[0.0];calls=[]
    class Clock(datetime):
        @classmethod
        def now(cls,tz=None):return now
    monkeypatch.setattr(job,'datetime',Clock)
    monkeypatch.setattr(job.time,'monotonic',lambda:elapsed[0])
    monkeypatch.setenv('SEC_FUNDAMENTALS_UNIVERSE_WARMING_ENABLED','1')
    monkeypatch.setenv('SEC_FUNDAMENTALS_WARMING_BATCH_SIZE','16')
    monkeypatch.setenv('FINNHUB_METRICS_WARMING_ENABLED','0')
    monkeypatch.setattr(queue,'DEFAULT_PREWARM_SYMBOLS',['ABC','DEF','XYZ'])
    def prepare(symbol,**kwargs):
        calls.append(symbol);elapsed[0]+=31
        return dict(status='partial',source='sec_edgar',values={},observed_at=now.isoformat())
    monkeypatch.setattr(prep,'prepare',prepare)
    first=job.run()
    assert calls==['ABC'] and first['planned_scopes']==3 and first['completed_scopes']==1
    assert first['sec_stop_reason']=='elapsed_budget' and first['sec_elapsed_seconds']==31
    assert first['canonical_writes']==first['emails']==0
    with factory() as db:
        state=json.loads(db.get(InsightsSnapshot,job.KEY).payload_json)
        for field in ('attempted_at','attempted_versions','scope_refresh_after'):
            assert set(state[field])=={'ABC'}
        assert 'lease' not in state
    elapsed[0]=0;second=job.run()
    assert calls==['ABC','DEF'] and second['planned_scopes']==2 and second['completed_scopes']==1

def test_expanded_universe_with_discovery_retries_and_missed_runs_stays_fresh(monkeypatch):
    now=datetime(2026,11,1,tzinfo=timezone.utc)
    symbols=[f'S{i:04}' for i in range(2623)]
    retries=set(symbols[-128:]);priority=set(symbols[:41])
    reports={}
    for configured in ('10','16'):
        monkeypatch.setenv('SEC_FUNDAMENTALS_WARMING_BATCH_SIZE',configured)
        limit=job.sec_batch_limit()
        attempts={s:(now-timedelta(minutes=(i%240)*5)).isoformat() for i,s in enumerate(symbols)}
        deadlines={s:(datetime.fromisoformat(attempts[s])+(prep.DISCOVERY_RETRY_TTL if s in retries else job.PRIORITY_REFRESH)).isoformat() for s in symbols}
        maximum=0;retry_seen=set()
        for tick in range(4*288):
            at=now+timedelta(minutes=5*tick)
            if tick%12==11:continue  # 24 missed triggers per day
            due=[s for s in symbols if datetime.fromisoformat(deadlines[s])<=at]
            due=job.order_due_scopes(due,now=at,attempts=attempts,refresh_after=deadlines,priority=priority,changed_dependencies=set())
            selected=job.scope_batch(due,limit,priority=priority,changed_dependencies=set(),changed_versions=set())
            assert len(selected)<=limit and len(selected)==len(set(selected))
            for s in selected:
                assert datetime.fromisoformat(deadlines[s])<=at
                if tick>=288 and s not in retries:
                    maximum=max(maximum,(at-datetime.fromisoformat(attempts[s])).total_seconds()/3600)
                attempts[s]=at.isoformat()
                deadlines[s]=(at+(prep.DISCOVERY_RETRY_TTL if s in retries else job.PRIORITY_REFRESH)).isoformat()
                if s in retries:retry_seen.add(s)
            if tick>=288:
                maximum=max(maximum,max((at-datetime.fromisoformat(attempts[s])).total_seconds()/3600 for s in symbols if s not in retries))
        assert retry_seen==retries
        reports[configured]=maximum
    assert reports['10']>24
    assert reports['16']<24
