from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from app.db import Base
from app.jobs import reference_preflight as preflight, reference_schedule as schedule
from app.jobs import warm_institutional_reference as job

NOW = datetime(2026,10,12,18,tzinfo=timezone.utc)


def state():
    return {'universe': {'version':6,'periods':['2026-06-30','2026-09-30'],
        'weights':{'123456789:2026-09-30':2,'987654321:2026-06-30':1},
        'universe_truncated':True,'held_documents':3,'source_scan_next_before_id':500,'source_scan_documents':500},
        'universe_prepared_at':(NOW-timedelta(minutes=5)).isoformat(),
        'attempted_at':{key:(NOW-timedelta(hours=2)).isoformat() for key in ('123456789:2026-09-30','987654321:2026-06-30')}}


@pytest.fixture(autouse=True)
def enabled(monkeypatch):
    monkeypatch.setenv('INSTITUTIONAL_REFERENCE_WARMING_ENABLED','1')
    monkeypatch.setenv('DIRECT_FEEDS_MODE','shadow')


@pytest.mark.parametrize('defect',['none','missing_attempt','expired_attempt','naive_attempt','invalid_key','expired_plan','future_plan','wrong_version','quarter_rollover','missing_weights','invalid_hold'])
def test_no_due_proof_preserves_actual_planner_selections(monkeypatch,defect):
    p=state();now=NOW
    if defect=='missing_attempt':p['attempted_at'].pop('123456789:2026-09-30')
    if defect=='expired_attempt':p['attempted_at']['123456789:2026-09-30']=(NOW-timedelta(days=1)).isoformat()
    if defect=='naive_attempt':p['attempted_at']['123456789:2026-09-30']='2026-10-12T16:00:00'
    if defect=='invalid_key':p['universe']['weights']['not-a-cusip']=1
    if defect=='expired_plan':p['universe_prepared_at']=(NOW-timedelta(minutes=15)).isoformat()
    if defect=='future_plan':p['universe_prepared_at']=(NOW+timedelta(seconds=1)).isoformat()
    if defect=='wrong_version':p['universe']['version']=5
    if defect=='quarter_rollover':now=datetime(2027,1,1,tzinfo=timezone.utc)
    if defect=='missing_weights':p['universe'].pop('weights')
    if defect=='invalid_hold':p['universe']['held_documents']='0'
    original=deepcopy(p)
    monkeypatch.setattr(preflight,'read_state',lambda:(None,None,p))
    early=preflight.early_result(now=now)
    if defect!='none':assert early is None
    else:
        assert early['status']=='partial' and early['held_documents']==3 and early['universe_truncated']
        engine=create_engine('sqlite://');Base.metadata.create_all(engine)
        with Session(engine) as db:
            full=job.plan_scopes(db,now,p['attempted_at'],p['universe'])
            assert early['pending_scopes']==full['pending_scopes']==0
            assert full['scopes']==[] and not(db.new or db.dirty or db.deleted)
        engine.dispose()
    assert p==original


@pytest.mark.parametrize('age,skip',[(899,True),(900,False),(901,False)])
def test_plan_expiry_never_moves(monkeypatch,age,skip):
    p=state();p['universe_prepared_at']=(NOW-timedelta(seconds=age)).isoformat()
    monkeypatch.setattr(preflight,'read_state',lambda:(None,None,json.dumps(p)))
    assert (preflight.early_result(now=NOW) is not None)==skip


@pytest.mark.parametrize('truncated,held,exit_code',[(True,0,1),(False,1,1),(False,0,0)])
def test_cli_idle_keeps_coverage_exit_code_without_application_imports(truncated,held,exit_code):
    p=state();p['universe'].update(universe_truncated=truncated,held_documents=held)
    code="import sys,runpy,importlib.abc\nfrom datetime import datetime\nfrom app.jobs import reference_preflight\nreference_preflight.read_state=lambda:(None,None,STATE)\nreal=reference_preflight.early_result\nreference_preflight.early_result=lambda:real(now=datetime.fromisoformat(NOW))\nclass Block(importlib.abc.MetaPathFinder):\n def find_spec(self,name,*args):\n  if name=='sqlalchemy' or name=='app.models':raise AssertionError('Heavy import during proven idle plan')\nsys.meta_path.insert(0,Block())\nrunpy.run_module('app.jobs.warm_institutional_reference',run_name='__main__')\n"
    code=code.replace('STATE',repr(p)).replace('NOW',repr(NOW.isoformat()))
    result=subprocess.run([sys.executable,'-c',code],cwd=Path(__file__).resolve().parents[1],
        env=os.environ.copy(),capture_output=True,text=True,timeout=15)
    assert result.returncode==exit_code,result.stderr
    payload=json.loads(result.stdout)
    assert payload['reason']=='cached_plan_has_no_due_scopes'
    assert payload['planned_scopes']==payload['public_writes']==payload['emails']==0
