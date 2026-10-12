from datetime import date,datetime,timedelta,timezone
from types import SimpleNamespace
import hashlib,json,os,subprocess,sys
from pathlib import Path
import pytest
from app.jobs import prior_preflight as preflight
from app.services.direct_13f_prior_selection import normalize_candidates,select_priors

NOW=datetime(2026,10,12,6,tzinfo=timezone.utc)
ARGS=SimpleNamespace(since=date(2026,10,7),limit=20,recheck_hours=24,lookback_days=93,preview=False)

def selection(rows,priors=None):
    return select_priors(normalize_candidates(rows),priors or {},now=NOW,start=ARGS.since,limit=20,recheck_hours=24)

def receipt(*,hours=1,status='held'):
    return {'current_source_hash':'source','current_metadata_hash':hashlib.sha256(b'{}').hexdigest(),
        'checked_at':(NOW-timedelta(hours=hours)).isoformat(),'status':status,
        'prior_document_id':9,'content_hash':'prior','prior_metadata_hash':hashlib.sha256(b'{"old":true}').hexdigest()}

@pytest.fixture
def enabled(monkeypatch):monkeypatch.setenv('DIRECT_FEEDS_MODE','shadow')

@pytest.mark.parametrize('hours,expected',[(23.99,[]),(24,[]),(24.01,[1])])
def test_retry_boundary_unchanged(hours,expected):
    assert selection([(1,'source','{}',receipt(hours=hours))])['document_ids']==expected

@pytest.mark.parametrize('hours,expected',[(167.99,[]),(168,[]),(168.01,[1])])
def test_verified_prior_boundary_unchanged(hours,expected):
    assert selection([(1,'source','{}',receipt(hours=hours,status='collected'))],{9:('parsed','prior','{"old":true}')})['document_ids']==expected

@pytest.mark.parametrize('change',['source','metadata','prior_source','prior_metadata','prior_status','missing_prior'])
def test_changed_evidence_is_due_without_waiting(change):
    row=[1,'source','{}',receipt(status='collected')];priors={9:('parsed','prior','{"old":true}')}
    if change=='source':row[1]='changed'
    elif change=='metadata':row[2]='{"changed":true}'
    elif change=='prior_source':priors[9]=('parsed','changed','{"old":true}')
    elif change=='prior_metadata':priors[9]=('parsed','prior','{}')
    elif change=='prior_status':priors[9]=('quarantined','prior','{"old":true}')
    else:priors={}
    assert selection([row],priors)['document_ids']==[1]

def test_new_work_precedes_successful_retry_then_held_retry():
    rows=[(1,'source','{}',receipt(hours=169)),(2,'source','{}',receipt(hours=169,status='collected')),(3,'source','{}',None)]
    assert selection(rows,{9:('parsed','prior','{"old":true}')})['document_ids']==[3,2,1]

def test_candidate_bound_is_not_silently_truncated():
    with pytest.raises(ValueError,match='bound'):normalize_candidates([(1,'source','{}',None)]*20001)

@pytest.mark.parametrize('eligible', [0,1])
def test_preflight_only_skips_exact_empty_selection(enabled,monkeypatch,eligible):
    picked=dict(document_ids=[1] if eligible else [],eligible=eligible,candidates=1,cooling_down=1-eligible,since='2026-10-07')
    monkeypatch.setattr(preflight,'read_selection',lambda **kw:picked)
    assert preflight.early_result(ARGS,now=NOW)==({'status':'idle','selection':picked,'public_writes':0} if eligible==0 else None)

@pytest.mark.parametrize('picked',[None,{}, {'eligible':0,'document_ids':[1]}])
def test_uncertain_or_inconsistent_state_runs_full_worker(enabled,monkeypatch,picked):
    monkeypatch.setattr(preflight,'read_selection',lambda **kw:picked)
    assert preflight.early_result(ARGS,now=NOW) is None

def test_read_error_runs_full_worker_without_printing_connection(enabled,monkeypatch,capsys):
    def fail(**kw):raise RuntimeError('private connection')
    monkeypatch.setattr(preflight,'read_selection',fail)
    assert preflight.early_result(ARGS,now=NOW) is None
    assert not capsys.readouterr().out

@pytest.mark.parametrize('kind',['explicit','preview','disabled','paused'])
def test_other_modes_keep_existing_worker(enabled,monkeypatch,kind):
    args=SimpleNamespace(**vars(ARGS))
    if kind=='explicit':args.since=None
    elif kind=='preview':args.preview=True
    elif kind=='disabled':monkeypatch.setenv('DIRECT_FEEDS_MODE','off')
    else:monkeypatch.setenv('BACKGROUND_JOBS_PAUSED','true')
    monkeypatch.setattr(preflight,'read_selection',lambda **kw:pytest.fail('Unexpected read'))
    assert preflight.early_result(args,now=NOW) is None

def test_readonly_driver_query_preserves_receipt_selection(enabled,monkeypatch):
    import psycopg
    seen={};queries=[]
    class Cursor:
        def __enter__(self):return self
        def __exit__(self,*args):pass
        def execute(self,sql,params):queries.append((sql,params))
        def fetchall(self):
            return [(1,'source','{}',receipt(status='collected'))] if len(queries)==1 else [(9,'parsed','prior','{"old":true}')]
    class Connection:
        def __enter__(self):return self
        def __exit__(self,*args):seen['closed']=True
        def cursor(self):return Cursor()
    def connect(dsn,**kw):seen.update(dsn=dsn,**kw);return Connection()
    monkeypatch.setenv('DATABASE_URL','postgresql+psycopg://localhost/test')
    monkeypatch.setattr(psycopg,'connect',connect)
    result=preflight.read_selection(since=ARGS.since,limit=20,recheck_hours=24,lookback_days=93,now=NOW)
    assert result==selection([(1,'source','{}',receipt(status='collected'))],{9:('parsed','prior','{"old":true}')})
    assert seen['closed'] and seen['connect_timeout']==2 and seen['dsn']=='postgresql://localhost/test'
    assert 'default_transaction_read_only=on' in seen['options'] and 'statement_timeout=1500' in seen['options']
    assert queries[0][1]==('2026-10-07','2026-10-12') and queries[1][1]==([9],)
    assert all(sql.lstrip().startswith('SELECT') for sql,_ in queries)

def test_idle_cli_never_imports_application_or_transport(enabled):
    code='''import importlib.abc,sys,runpy
from app.jobs import prior_preflight
prior_preflight.read_selection=lambda **kw:dict(document_ids=[],eligible=0,candidates=3,cooling_down=3,since='2026-10-07')
class Block(importlib.abc.MetaPathFinder):
 def find_spec(self,name,*args):
  if name in ['sqlalchemy','app.models','app.db','app.clients.direct_sources']:raise AssertionError('Idle application import')
sys.meta_path.insert(0,Block());sys.argv=['prior','--since','2026-10-07']
runpy.run_module('app.jobs.collect_direct_13f_priors',run_name='__main__')
'''
    result=subprocess.run([sys.executable,'-c',code],cwd=Path(__file__).resolve().parents[1],env=os.environ.copy(),capture_output=True,text=True,timeout=15)
    assert result.returncode==0,result.stderr
    assert json.loads(result.stdout)['selection']['cooling_down']==3
