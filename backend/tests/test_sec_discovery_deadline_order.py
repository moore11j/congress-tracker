from datetime import datetime,timedelta,timezone
import json
import pytest
from sqlalchemy import select,func
from app.jobs import warm_sec_fundamentals as job
from app.services import sec_fundamentals_preparation as prep,data_enrichment_queue as queue
from app.models import FundamentalsCache,InsightsSnapshot,Event
from test_sec_fundamentals_warming import factory

NOW=datetime(2026,10,27,11,40,tzinfo=timezone.utc)

def ordered(due,attempts,deadlines,priority=(),changed=()):
 return job.order_due_scopes(due,now=NOW,attempts=attempts,refresh_after=deadlines,
  priority=set(priority),changed_dependencies=set(changed))

def test_twenty_minute_retry_is_not_starved_by_twenty_hour_refresh():
 attempts={'REG':(NOW-timedelta(hours=20,minutes=5)).isoformat(),'RET':(NOW-timedelta(minutes=60)).isoformat()}
 deadlines={'REG':(NOW-timedelta(minutes=5)).isoformat(),'RET':(NOW-timedelta(minutes=40)).isoformat()}
 assert ordered(['REG','RET'],attempts,deadlines)==['RET','REG']
 assert attempts['REG']<attempts['RET'] and deadlines['RET']<deadlines['REG']

def test_uniform_refresh_intervals_retain_oldest_attempt_order_and_stable_ties():
 attempts={'CCC':(NOW-timedelta(hours=22)).isoformat(),'BBB':(NOW-timedelta(hours=21)).isoformat(),'AAA':(NOW-timedelta(hours=21)).isoformat()}
 deadlines={s:(datetime.fromisoformat(t)+job.PRIORITY_REFRESH).isoformat() for s,t in attempts.items()}
 expected=sorted(attempts,key=lambda s:(attempts[s],s))
 assert ordered(list(reversed(expected)),attempts,deadlines)==expected
 assert ordered(expected,attempts,{})==expected

def test_offset_timestamps_are_ordered_by_actual_eligible_instant():
 deadlines={'AAA':'2026-10-27T08:00:00-03:00','BBB':'2026-10-27T10:00:00+00:00'}
 assert ordered(['AAA','BBB'],{},deadlines)==['BBB','AAA']

@pytest.mark.parametrize('bad',[None,'bad','2026-10-27T10:00:00','2030-01-01T00:00:00+00:00',42])
def test_invalid_deadline_remains_finite_eligible_work_without_mutation(bad):
 deadlines={'BAD':bad,'OLD':(NOW-timedelta(minutes=1)).isoformat()};before=dict(deadlines)
 assert ordered(['BAD','OLD'],{},deadlines)==['OLD','BAD']
 assert deadlines==before

def test_overflowing_attempt_and_missing_timestamp_do_not_break_the_batch():
 assert ordered(['OLD','NEW'],{'OLD':'9999-12-31T23:59:59+00:00'},{})==['NEW','OLD']

def test_priority_and_changed_evidence_survive_version_slot_reservation():
 due=['RETRY','REG','WATCH','ISSUER','VERSION']
 deadlines={s:(NOW-timedelta(minutes=100-i)).isoformat() for i,s in enumerate(due)}
 result=ordered(due,{},deadlines,priority=['WATCH'],changed=['ISSUER'])
 assert result==['ISSUER','WATCH','RETRY','REG','VERSION']
 selected=job.scope_batch(result,3,priority={'WATCH'},changed_dependencies={'ISSUER'},changed_versions={'VERSION'})
 assert selected==['ISSUER','WATCH','VERSION'] and len(set(selected))==3
 assert job.scope_batch(result,3,priority={'WATCH'},changed_dependencies={'ISSUER'},changed_versions=set())==result[:3]

def test_ordering_cannot_add_scopes_or_expand_the_batch():
 due=['REG','RET'];result=ordered(due,{}, {'OUTSIDE':(NOW-timedelta(days=9)).isoformat()})
 assert set(result)==set(due) and due==['REG','RET']
 assert len(job.scope_batch(result,1,priority=set(),changed_dependencies=set(),changed_versions=set()))==1

def test_actual_worker_advances_retry_and_respects_deadline_and_batch(factory,monkeypatch):
 clock=[NOW];calls=[]
 class Clock(datetime):
  @classmethod
  def now(cls,tz=None):return clock[0]
 monkeypatch.setattr(job,'datetime',Clock)
 monkeypatch.setenv('SEC_FUNDAMENTALS_UNIVERSE_WARMING_ENABLED','1')
 monkeypatch.setenv('SEC_FUNDAMENTALS_WARMING_BATCH_SIZE','1')
 monkeypatch.setenv('SEC_FINANCIAL_DISCOVERY_ENABLED','1')
 monkeypatch.setenv('FINNHUB_METRICS_WARMING_ENABLED','0')
 monkeypatch.setattr(queue,'DEFAULT_PREWARM_SYMBOLS',[])
 symbols=['REG','RET','FUT']
 attempts={'REG':(NOW-timedelta(hours=20,minutes=5)).isoformat(),'RET':(NOW-timedelta(minutes=60)).isoformat(),'FUT':(NOW-timedelta(hours=1)).isoformat()}
 deadlines={'REG':(NOW-timedelta(minutes=5)).isoformat(),'RET':(NOW-timedelta(minutes=40)).isoformat(),'FUT':(NOW+timedelta(hours=19)).isoformat()}
 with factory() as db:
  db.add_all([FundamentalsCache(symbol=s,provider='fmp',status='ok',fetched_at=NOW) for s in symbols])
  db.add(InsightsSnapshot(kind=job.KEY,source='free_direct',fetched_at=NOW,payload_json=json.dumps({
   'attempted_at':attempts,'scope_refresh_after':deadlines,'attempted_versions':{s:prep.version_for_symbol(s) for s in symbols}})))
  db.commit()
 def prepare(symbol,**kwargs):
  assert job.run()['reason']=='active_warming_lease'
  assert kwargs=={'max_cache_age':job.PRIORITY_REFRESH,'continue_discovery':True}
  calls.append(symbol)
  return {'symbol':symbol,'source':'sec_edgar','status':'unavailable' if symbol=='RET' else 'partial','values':{},
   'observed_at':clock[0].isoformat(),'reason':'financial_discovery_pending' if symbol=='RET' else None,
   'discovery':{'phase':'covers','pending_covers':3} if symbol=='RET' else None}
 monkeypatch.setattr(prep,'prepare',prepare)
 first=job.run();assert calls==['RET'] and first['planned_scopes']==first['completed_scopes']==first['batch_limit']==1
 assert first['canonical_writes']==first['emails']==first['model_calls']==0
 assert job.run()['results'][0]['scope']=='REG' and calls==['RET','REG']
 assert job.run()['completed_scopes']==0
 clock[0]+=timedelta(minutes=19);assert job.run()['completed_scopes']==0
 clock[0]+=timedelta(minutes=1);assert job.run()['results'][0]['scope']=='RET'
 assert calls==['RET','REG','RET'] and 'FUT' not in calls
 with factory() as db:
  state=json.loads(db.get(InsightsSnapshot,job.KEY).payload_json)
  assert 'lease' not in state and state['scope_refresh_after']['FUT']==deadlines['FUT']
  assert datetime.fromisoformat(state['scope_refresh_after']['RET'])==clock[0]+prep.DISCOVERY_RETRY_TTL
  assert not db.scalar(select(func.count()).select_from(Event))


@pytest.mark.parametrize('attempt',[None,42,{},[],'not-a-time'])
def test_invalid_attempt_tie_break_remains_sortable_and_deterministic(attempt):
 attempts={'BAD':attempt};before=dict(attempts)
 first=ordered(['BAD','EMPTY'],attempts,{})
 assert set(first)=={'BAD','EMPTY'} and first==ordered(['EMPTY','BAD'],attempts,{})
 assert attempts==before
