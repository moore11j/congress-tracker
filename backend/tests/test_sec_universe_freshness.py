import json
from datetime import datetime,timedelta,timezone
import pytest
from app.models import FundamentalsCache,InsightsSnapshot,Event
from sqlalchemy import select
from app.jobs import warm_sec_fundamentals as job
from app.services import sec_fundamentals_preparation as prep,data_enrichment_queue as queue,finnhub_metrics
from app.clients.direct_sources import DirectSourceError
from test_sec_priority_cache_refresh import cached_priority
from test_sec_fundamentals_warming import factory


@pytest.fixture
def broad_only(cached_priority,monkeypatch):
    factory,base,clock,calls,old=cached_priority
    monkeypatch.setattr(queue,'DEFAULT_PREWARM_SYMBOLS',[])
    with factory() as db:
        db.add(FundamentalsCache(symbol='ABC',provider='fmp',status='ok',fetched_at=base))
        db.add(InsightsSnapshot(kind='sec-current-fundamentals:AAA:v1',source='sec_edgar',fetched_at=base,
            payload_json=json.dumps(dict(version=prep.version_for_symbol('AAA'),symbol='AAA',source='sec_edgar',
                observed_at=base.isoformat(),status='unavailable',values={},reason='symbol_absent_from_sec_directory'))))
        db.add(InsightsSnapshot(kind=job.KEY,source='free_direct',fetched_at=base,payload_json=json.dumps({
            'attempted_at':{'AAA':(base-timedelta(minutes=1)).isoformat(),'ABC':base.isoformat()},
            'attempted_versions':{s:prep.version_for_symbol(s) for s in ['AAA','ABC']}})))
        db.commit()
    return factory,base,clock,calls,old


def test_broad_cache_deadline_beats_recent_attempt_and_fresh_rotation(broad_only):
    factory,base,clock,calls,old=broad_only
    clock[0]=base+timedelta(hours=2,seconds=1)
    result=job.run()
    assert result['priority_universe_size']==0
    assert result['planned_scopes']==result['completed_scopes']==1
    assert result['results'][0]['scope']=='ABC'
    assert result['results'][0]['observed_at']==clock[0].isoformat()
    assert any('companyfacts' in url for url in calls)
    calls.clear()
    clock[0]+=timedelta(minutes=5)
    assert job.run()['completed_scopes']==0
    assert calls==[]
    with factory() as db:
        state=json.loads(db.get(InsightsSnapshot,job.KEY).payload_json)
        assert datetime.fromisoformat(state['scope_refresh_after']['ABC'])==clock[0]-timedelta(minutes=5)+timedelta(hours=20)
        assert state['priority_refresh_after']=={}
        assert not list(db.scalars(select(Event)))
        assert set(db.scalars(select(FundamentalsCache.provider)))=={'fmp'}


def test_fresh_broad_scopes_do_not_block_due_finnhub_metrics(broad_only,monkeypatch):
    factory,base,clock,calls,old=broad_only
    monkeypatch.setenv('FINNHUB_METRICS_WARMING_ENABLED','1')
    seen=[]
    def metrics(symbol):
        seen.append(symbol)
        return dict(status='partial',values={},observed_at=base.isoformat())
    monkeypatch.setattr(finnhub_metrics,'prepare',metrics)
    result=job.run()
    assert result['completed_scopes']==0 and calls==[]
    assert result['completed_metric_scopes']==2 and set(seen)=={'ABC','AAA'}


def test_broad_refresh_failure_preserves_original_and_backs_off(broad_only,monkeypatch):
    factory,base,clock,calls,old=broad_only
    clock[0]=base+timedelta(hours=2,seconds=1)
    with factory() as db:before=db.get(InsightsSnapshot,'sec-current-fundamentals:ABC:v1').payload_json
    def denied(symbol,**kwargs):
        assert kwargs['max_cache_age']==timedelta(hours=20)
        raise DirectSourceError('Source HTTP 429: https://data.sec.gov/test')
    monkeypatch.setattr(prep,'prepare',denied)
    first=job.run()
    assert first['results'][0]['scope']=='ABC' and first['status']=='partial'
    clock[0]+=timedelta(minutes=5)
    assert job.run()['completed_scopes']==0
    with factory() as db:assert db.get(InsightsSnapshot,'sec-current-fundamentals:ABC:v1').payload_json==before


def test_version_change_overrides_unexpired_broad_deadline(broad_only,monkeypatch):
    factory,base,clock,calls,old=broad_only
    # Both source caches are fresh; only this scope's normalization changed.
    old_version=prep.version_for_symbol
    monkeypatch.setattr(prep,'version_for_symbol',lambda s:old_version(s)+('_new' if s=='ABC' else ''))
    result=job.run()
    assert result['results'][0]['scope']=='ABC'
    assert result['results'][0]['observed_at']==base.isoformat()
    assert any('companyfacts' in url for url in calls)


def test_broad_cache_hit_keeps_the_original_observation_deadline(broad_only):
    factory,base,clock,calls,old=broad_only
    with factory() as db:
        row=db.get(InsightsSnapshot,job.KEY);state=json.loads(row.payload_json)
        state['attempted_versions']['ABC']='old'
        row.payload_json=json.dumps(state);db.commit()
    result=job.run()
    assert result['results'][0]['scope']=='ABC'
    assert result['results'][0]['observed_at']==old['observed_at'] and calls==[]
    with factory() as db:
        state=json.loads(db.get(InsightsSnapshot,job.KEY).payload_json)
        assert datetime.fromisoformat(state['scope_refresh_after']['ABC'])==base+timedelta(hours=2)
    clock[0]=base+timedelta(hours=2,seconds=1)
    assert job.run()['results'][0]['observed_at']==clock[0].isoformat()
    assert any('companyfacts' in url for url in calls)


@pytest.mark.parametrize('deadline',['invalid','2026-10-12T00:00:00','2040-01-01T00:00:00+00:00'])
def test_corrupt_or_unbounded_deadline_cannot_hide_broad_scope(broad_only,deadline):
    factory,base,clock,calls,old=broad_only
    with factory() as db:
        row=db.get(InsightsSnapshot,job.KEY);state=json.loads(row.payload_json)
        state['scope_refresh_after']={'ABC':deadline}
        row.payload_json=json.dumps(state);db.commit()
    result=job.run()
    assert result['results'][0]['scope']=='ABC'
