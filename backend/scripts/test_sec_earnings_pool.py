import argparse,json,os,sys,uuid
from datetime import datetime,timedelta,timezone
from contextlib import ExitStack
from functools import partial
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
p=argparse.ArgumentParser();p.add_argument('--port',type=int,choices=[61982,61983],required=True);p.add_argument('--backend',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args();assert not a.output.exists()
os.environ.update(DATABASE_URL='sqlite:///:memory:',SEC_EARNINGS_WARMING_ENABLED='1',PRESS_RELEASE_PROVIDER='fmp')
sys.path.insert(0,str(a.backend.resolve()));sys.path.insert(0,str(a.backend.resolve()/'tests'))
from sqlalchemy import create_engine,text,select
from sqlalchemy.orm import sessionmaker
from app.db import Base
from app.models import InsightsSnapshot,Security,Watchlist,WatchlistItem,UserAccount,ResearchThesis,TickerContentCache,ResearchSourceDocument,Event
from app.jobs import warm_sec_earnings as job
from app.services import sec_directory,sec_press_releases,data_enrichment_queue as queue
from app.jobs.collect_direct_feeds import collector_lock
from test_sec_earnings_materials import company,submission
from test_sec_earnings_store import index
from app.services.direct_feed_store import DirectFeedDocument,DirectFeedRevision
schema='sec_earnings_pool_'+uuid.uuid4().hex
control=create_engine(f'postgresql+psycopg://postgres@127.0.0.1:{a.port}/walnut_sec_replay',connect_args={'connect_timeout':5})
engine=None;created=False;calls=[];overlaps=[]
report={'database_scope':'disposable_loopback','pool_size':2,'max_overflow':0,'http_calls':0,'emails':0}
try:
 with control.begin() as conn:
  assert conn.scalar(text('select current_database()'))=='walnut_sec_replay'
  conn.exec_driver_sql(f'CREATE SCHEMA {schema}');created=True
 engine=create_engine(control.url,pool_size=2,max_overflow=0,pool_timeout=.2,connect_args={'options':f'-c search_path={schema} -c statement_timeout=5000'})
 factory=sessionmaker(bind=engine)
 Base.metadata.create_all(engine,tables=[InsightsSnapshot.__table__,Security.__table__,UserAccount.__table__,Watchlist.__table__,WatchlistItem.__table__,ResearchThesis.__table__,TickerContentCache.__table__,ResearchSourceDocument.__table__,Event.__table__,DirectFeedDocument.__table__,DirectFeedRevision.__table__])
 with factory() as db:
  db.add(Security(id=1,symbol='TEST',name='Test',asset_class='stock'));db.commit()
 def source(self,url):
  calls.append(url)
  # A second cron process has a separate pool. Its shared lock must refuse it
  # before cache access, even while this worker uses both cache connections.
  with patch.object(job, 'collector_lock', partial(collector_lock, bind=control)):
   overlaps.append(job.run())
  assert overlaps[-1]['reason']=='source_collection_active'
  if url==sec_directory.URL:return json.dumps({'fields':['cik','name','ticker','exchange'],'data':[[1234567,'Test','TEST','NYSE']]}).encode()
  return company() if url.endswith('.json') else index() if url.endswith('-index.html') else submission()
 with ExitStack() as stack:
  stack.enter_context(patch('requests.sessions.Session.request',side_effect=AssertionError('Unexpected HTTP')))
  for module in (job,sec_directory,sec_press_releases):stack.enter_context(patch.object(module,'SessionLocal',factory))
  stack.enter_context(patch.object(job,'collector_lock',partial(collector_lock,bind=engine)))
  stack.enter_context(patch.object(job,'check_background_job_guard',lambda *a:SimpleNamespace(proceed=True)))
  stack.enter_context(patch.object(queue,'DEFAULT_PREWARM_SYMBOLS',['TEST']))
  stack.enter_context(patch.object(queue,'_recently_viewed_ticker_symbols',lambda *a,**k:[]))
  stack.enter_context(patch.object(sec_directory.DirectSourceClient,'get',source))
  first=job.run();repeat=job.run()
  assert first['status']=='ok' and first['completed_scopes']==1, first
  assert first['results']==repeat['results'] and len(calls)==4
  assert first['press_selection']=='fmp'
  with factory() as db:
   assert len(list(db.scalars(select(InsightsSnapshot))))==2
   assert not json.loads(db.get(InsightsSnapshot,job.KEY).payload_json).get('lease')
   assert len(list(db.scalars(select(DirectFeedDocument))))==3
   assert len(list(db.scalars(select(DirectFeedRevision))))==3
   assert not list(db.scalars(select(Event))) and not list(db.scalars(select(ResearchSourceDocument)))
  report.update(status='passed',prepared_panels=1,source_fixture_requests=4,canonical_writes=0,repeat_source_requests=0,overlap_refusals=len(overlaps),lease_cleared=True,public_selection='fmp')
  with factory() as db:
   cache=db.scalar(select(TickerContentCache));old_payload=cache.payload_json
   cache.fetched_at=datetime.now(timezone.utc)-timedelta(days=1);db.commit()
   assert db.scalar(text('SHOW idle_in_transaction_session_timeout'))=='0'
  with control.connect() as blocker:
   blocker.exec_driver_sql(f'SET LOCAL search_path={schema}')
   blocker.execute(text('SELECT id FROM securities WHERE id=1 FOR UPDATE'))
   blocked=job.run()
   assert blocked['results'][0]['status']=='unavailable',blocked
   with factory() as db:
    assert db.scalar(select(TickerContentCache)).payload_json==old_payload
    assert not json.loads(db.get(InsightsSnapshot,job.KEY).payload_json).get('lease')
   blocker.rollback()
  assert job.run()['results'][0]['items']==1
  with factory() as db:
   assert len(list(db.scalars(select(DirectFeedDocument))))==3
   assert len(list(db.scalars(select(DirectFeedRevision))))==3
  report.update(source_fixture_requests=len(calls),overlap_refusals=len(overlaps),row_lock_timeout_refused=True,old_cache_preserved=True,retry_preserves_source_identity=True,local_timeout_resets_after_commit=True)

finally:
 if engine:engine.dispose()
 if created:
  with control.begin() as conn:conn.exec_driver_sql(f'DROP SCHEMA {schema} CASCADE')
  report['test_schema_removed']=True
 control.dispose()
a.output.write_text(json.dumps(report,indent=2));print(json.dumps(report))
