from datetime import datetime,timedelta,timezone
import json
from types import SimpleNamespace
import pytest
from app.models import InsightsSnapshot
from app.jobs import warm_sec_fundamentals as job
from app.services import sec_fundamentals_preparation as prep,data_enrichment_queue as queue
from app.clients.direct_sources import DirectSourceError
from test_sec_fundamentals_warming import factory


def test_discovery_reuses_normal_worker_slot_and_twenty_minute_progress_deadline(factory,monkeypatch):
    clock=[datetime(2026,10,27,9,20,tzinfo=timezone.utc)];calls=[]
    class Clock(datetime):
        @classmethod
        def now(cls,tz=None):return clock[0]
    monkeypatch.setattr(job,'datetime',Clock)
    monkeypatch.setenv('SEC_FINANCIAL_DISCOVERY_ENABLED','1')
    monkeypatch.setenv('SEC_FUNDAMENTALS_UNIVERSE_WARMING_ENABLED','1')
    monkeypatch.setenv('FINNHUB_METRICS_WARMING_ENABLED','0')
    def prepare(symbol,**kwargs):
        calls.append((symbol,kwargs))
        return {'symbol':symbol,'source':'sec_edgar','status':'unavailable','values':{},
            'observed_at':clock[0].isoformat(),'reason':'financial_discovery_pending',
            'discovery':{'phase':'covers','pending_covers':12}}
    monkeypatch.setattr(prep,'prepare',prepare)
    result=job.run()
    assert result['completed_scopes']==1 and result['batch_limit']==5
    assert calls==[('ABC',{'max_cache_age':job.PRIORITY_REFRESH,'continue_discovery':True})]
    assert result['canonical_writes']==result['emails']==result['model_calls']==0
    with factory() as db:
        state=json.loads(db.get(InsightsSnapshot,job.KEY).payload_json)
    assert datetime.fromisoformat(state['scope_refresh_after']['ABC'])==clock[0]+prep.DISCOVERY_RETRY_TTL
    assert 'lease' not in state
    clock[0]+=timedelta(minutes=19)
    assert job.run()['completed_scopes']==0 and len(calls)==1
    clock[0]+=timedelta(minutes=1)
    assert job.run()['completed_scopes']==1 and len(calls)==2


def test_enabled_discovery_still_obeys_load_guard_before_any_database_or_source(factory,monkeypatch):
    monkeypatch.setenv('SEC_FINANCIAL_DISCOVERY_ENABLED','1')
    monkeypatch.setattr(job,'SessionLocal',lambda:pytest.fail('No database work under pressure'))
    monkeypatch.setattr(job,'check_background_job_guard',lambda *a:SimpleNamespace(
        proceed=False,to_dict=lambda:{'reason':'cpu_pressure'}))
    monkeypatch.setattr(prep,'prepare',lambda *a,**k:pytest.fail('No source work under pressure'))
    assert job.run()=={'status':'held','reason':'cpu_pressure'}


def test_discovery_source_denial_stops_current_batch_without_retrying_next_scope(factory,monkeypatch):
    monkeypatch.setenv('SEC_FINANCIAL_DISCOVERY_ENABLED','1')
    monkeypatch.setattr(queue,'DEFAULT_PREWARM_SYMBOLS',['ABC','DEF']);calls=[]
    def denied(symbol,**kwargs):
        calls.append(symbol);raise DirectSourceError('Source HTTP 403: https://www.sec.gov/example.htm')
    monkeypatch.setattr(prep,'prepare',denied)
    result=job.run()
    assert calls==['ABC'] and result['completed_scopes']==1
    with factory() as db:
        state=json.loads(db.get(InsightsSnapshot,job.KEY).payload_json)
    assert 'lease' not in state and list(state['attempted_at'])==['ABC']
