import argparse,json,os,sys,uuid
from contextlib import ExitStack
from functools import partial
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
p=argparse.ArgumentParser();p.add_argument('--backend',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args();assert not a.output.exists()
os.environ.update(DATABASE_URL='sqlite:///:memory:',SEC_RESEARCH_WARMING_ENABLED='1',FINANCIAL_STATEMENTS_PROVIDER='fmp',COMPANY_METADATA_PROVIDER='fmp')
sys.path.insert(0,str(a.backend.resolve()));sys.path.insert(0,str(a.backend.resolve()/'tests'))
from sqlalchemy import create_engine,text,select
from sqlalchemy.orm import sessionmaker
from app.db import Base
from app.models import InsightsSnapshot,Security,Watchlist,WatchlistItem,UserAccount
from app.jobs import warm_sec_research as job
from app.services import sec_directory,sec_financial_statements,data_enrichment_queue as queue
from app.jobs.collect_direct_feeds import collector_lock
from test_sec_fundamentals import sources
schema='sec_financial_pool_'+uuid.uuid4().hex
control=create_engine('postgresql+psycopg://postgres@127.0.0.1:61982/walnut_sec_replay',connect_args={'connect_timeout':5})
engine=None;created=False;calls=[];overlaps=[]
report={'database_scope':'disposable_loopback','pool_size':2,'max_overflow':0,'http_calls':0,'emails':0}
try:
 with control.begin() as conn:
  assert conn.scalar(text('select current_database()'))=='walnut_sec_replay'
  conn.exec_driver_sql(f'CREATE SCHEMA {schema}');created=True
 engine=create_engine(control.url,pool_size=2,max_overflow=0,pool_timeout=.2,connect_args={'options':f'-c search_path={schema} -c statement_timeout=5000'})
 factory=sessionmaker(bind=engine)
 Base.metadata.create_all(engine,tables=[InsightsSnapshot.__table__,Security.__table__,UserAccount.__table__,Watchlist.__table__,WatchlistItem.__table__])
 facts,company=sources()
 def source(self,url):
  calls.append(url)
  # A second cron process has a separate pool. Its shared lock must refuse it
  # before cache access, even while this worker uses both cache connections.
  with patch.object(job, 'collector_lock', partial(collector_lock, bind=control)):
   overlaps.append(job.run())
  assert overlaps[-1]['reason']=='source_collection_active'
  data=({'fields':['cik','name','ticker','exchange'],'data':[[1,'Example','ABC','NYSE']]} if url==sec_directory.URL else facts if 'companyfacts' in url else company)
  return json.dumps(data).encode()
 with ExitStack() as stack:
  stack.enter_context(patch('requests.sessions.Session.request',side_effect=AssertionError('Unexpected HTTP')))
  for module in (job,sec_directory,sec_financial_statements):stack.enter_context(patch.object(module,'SessionLocal',factory))
  stack.enter_context(patch.object(job,'collector_lock',partial(collector_lock,bind=engine)))
  stack.enter_context(patch.object(job,'check_background_job_guard',lambda *a:SimpleNamespace(proceed=True)))
  stack.enter_context(patch.object(queue,'DEFAULT_PREWARM_SYMBOLS',['ABC']))
  stack.enter_context(patch.object(queue,'_recently_viewed_ticker_symbols',lambda *a,**k:[]))
  stack.enter_context(patch.object(sec_directory.DirectSourceClient,'get',source))
  first=job.run();repeat=job.run()
  assert first['status']=='ok' and first['completed_scopes']==1, first
  assert first['results']==repeat['results'] and len(calls)==3
  assert first['financial_selection']==first['metadata_selection']=='fmp'
  with factory() as db:
   assert len(list(db.scalars(select(InsightsSnapshot))))==3
   assert not json.loads(db.get(InsightsSnapshot,job.KEY).payload_json).get('lease')
  report.update(status='passed',prepared_panels=1,source_fixture_requests=3,repeat_source_requests=0,overlap_refusals=len(overlaps),lease_cleared=True,public_selection='fmp')
finally:
 if engine:engine.dispose()
 if created:
  with control.begin() as conn:conn.exec_driver_sql(f'DROP SCHEMA {schema} CASCADE')
  report['test_schema_removed']=True
 control.dispose()
a.output.write_text(json.dumps(report,indent=2));print(json.dumps(report))
