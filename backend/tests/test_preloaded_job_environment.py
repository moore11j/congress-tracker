"""External portfolio continuations reuse cron imports without sharing settings."""
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

SCRIPTS = Path(__file__).resolve().parents[1] / 'scripts'


@pytest.mark.skipif(sys.platform != 'linux', reason='Actual Linux import template and fork')
@pytest.mark.parametrize('cron_first', [False, True])
def test_real_cron_and_external_environments_share_imports_and_keep_child_values(tmp_path, cron_first):
    script = r'''
import functools,json,os,socket,sys
from pathlib import Path
import preloaded_jobs as w
import scheduled_jobs as j
def forbidden(*a,**k):raise AssertionError('network forbidden')
socket.socket.connect=forbidden
original=dict(os.environ)
_,manifest=j.compile_schedule((Path(sys.argv[1])/'crontab').read_text(),Path('/unused'),Path('/unused'),Path('/unused'))
cron=next(d for d in manifest.values() if d['command']=='cd /app && python -m app.jobs.warm_finnhub_news')
base={**os.environ,'DATABASE_URL':'sqlite:///:memory:','FLY_PROCESS_GROUP':'cron','SCHEDULED_JOB_PRELOAD_ENABLED':'1'}
external={**base,'WALNUT_JOB_TOKEN':'a'*32,'WALNUT_PORTFOLIO_CONTINUATION':'1'}
scheduled={**base,**cron['environment'],'WALNUT_JOB_TOKEN':'b'*32,'WALNUT_PORTFOLIO_CONTINUATION':'0'}
# PYTHONPATH is equal in production; Python was initialized with the fixture path.
external['PYTHONPATH']=scheduled['PYTHONPATH']
ordered=[scheduled,external] if sys.argv[3]=='True' else [external,scheduled]
for env in ordered:assert w.preload(env), 'cron/external import reuse rejected'
assert dict(os.environ)==original
for key in ('DATABASE_URL','PYTHONPATH','TZ','FUNDAMENTALS_PROVIDER','API_KEY'):
 assert not w.preload({**external,key:'different'}),key
real_fork=w._fork_job
def fixture_fork(module,environment,**kwargs):
 assert module in {'app.jobs.refresh_portfolio_batch','app.jobs.warm_finnhub_news'}
 return real_fork('environment_fixture',environment,working_directory=sys.argv[2],reset_database=lambda:None)
w._fork_job=fixture_fork
for index,env in enumerate(ordered):
 command=j.PORTFOLIO_COMMAND if env is external else cron['command']
 p=w.launch(command,env)
 assert p is not None and p.preloaded
 assert p.wait(timeout=5)==0
 observed=json.loads((Path(sys.argv[2])/env['WALNUT_JOB_TOKEN']).read_text())
 assert observed=={k:env.get(k) for k in ('SHELL','PATH','CRON_TZ','PYTHONPATH','WALNUT_JOB_TOKEN','WALNUT_PORTFOLIO_CONTINUATION')}
 w.forget_identity(env['WALNUT_JOB_TOKEN'])
assert dict(os.environ)==original
print('cron_external_isolation_pass')
'''
    (tmp_path/'environment_fixture.py').write_text("import os,json\nfrom pathlib import Path\nkeys=('SHELL','PATH','CRON_TZ','PYTHONPATH','WALNUT_JOB_TOKEN','WALNUT_PORTFOLIO_CONTINUATION')\nPath(os.environ['WALNUT_JOB_TOKEN']).write_text(json.dumps({k:os.getenv(k) for k in keys}))\n")
    env={**os.environ,'PYTHONPATH':os.pathsep.join([str(SCRIPTS),str(SCRIPTS.parent),str(tmp_path)]),
         'DATABASE_URL':'sqlite:///:memory:','SCHEDULED_JOB_IDENTITY_DIRECTORY':str(tmp_path/'identities')}
    result=subprocess.run([sys.executable,'-c',script,str(SCRIPTS.parent),str(tmp_path),str(cron_first)],
                          env=env,capture_output=True,text=True,timeout=35)
    assert result.returncode==0,result.stdout+result.stderr
    assert 'cron_external_isolation_pass' in result.stdout
