from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest
from app.jobs import reference_preflight as preflight


NOW = datetime(2026, 10, 11, 3, tzinfo=timezone.utc)


@pytest.fixture
def enabled(monkeypatch):
    monkeypatch.setenv('INSTITUTIONAL_REFERENCE_WARMING_ENABLED', '1')
    monkeypatch.setenv('DIRECT_FEEDS_MODE', 'shadow')


@pytest.mark.parametrize('seconds,expected', [(1, True), (0, False), (-1, False)])
def test_provider_retry_boundary_is_unchanged(enabled, monkeypatch, seconds, expected):
    until = (NOW + timedelta(seconds=seconds)).isoformat()
    monkeypatch.setattr(preflight, 'read_state', lambda: (until, None))
    assert preflight.early_result(now=NOW) == ({'status':'cooldown','until':until} if expected else None)


@pytest.mark.parametrize('seconds,expected', [(59, True), (60, False), (61, False)])
def test_same_minute_boundary_is_unchanged(enabled, monkeypatch, seconds, expected):
    last = (NOW - timedelta(seconds=seconds)).isoformat()
    monkeypatch.setattr(preflight, 'read_state', lambda: (None, last))
    assert preflight.early_result(now=NOW) == ({'status':'cooldown'} if expected else None)


@pytest.mark.parametrize('state', [None, ('invalid', None), ('2026-10-11T04:00:00', None), (None, None)])
def test_uncertain_state_uses_full_worker(enabled, monkeypatch, state):
    monkeypatch.setattr(preflight, 'read_state', lambda: state)
    assert preflight.early_result(now=NOW) is None


def test_read_failure_uses_full_worker(enabled, monkeypatch):
    def fail():raise RuntimeError('private connection detail')
    monkeypatch.setattr(preflight, 'read_state', fail)
    assert preflight.early_result(now=NOW) is None


def test_disabled_does_not_read_database(monkeypatch):
    monkeypatch.delenv('INSTITUTIONAL_REFERENCE_WARMING_ENABLED', raising=False)
    monkeypatch.setattr(preflight, 'read_state', lambda: pytest.fail('Unexpected DB read'))
    assert preflight.early_result() == {'status':'disabled'}


def test_read_uses_short_readonly_transaction(enabled, monkeypatch):
    import psycopg
    observed={}
    class Cursor:
        def __enter__(self):return self
        def __exit__(self, *args):pass
        def execute(self, sql, params):observed.update(sql=sql, params=params)
        def fetchone(self):return ('until', 'last')
    class Connection:
        def __enter__(self):return self
        def __exit__(self, *args):observed['closed']=True
        def cursor(self):return Cursor()
    def connect(dsn, **kwargs):
        observed.update(dsn=dsn, options=kwargs);return Connection()
    monkeypatch.setenv('DATABASE_URL', 'postgresql+psycopg://localhost/test')
    monkeypatch.setattr(psycopg, 'connect', connect)
    assert preflight.read_state() == ('until', 'last')
    assert observed['dsn'] == 'postgresql://localhost/test'
    assert observed['closed'] and observed['options']['connect_timeout']==2
    assert 'default_transaction_read_only=on' in observed['options']['options']
    assert 'statement_timeout=1500' in observed['options']['options']
    assert observed['params']==('institutional-reference:warming:v1',)


def test_cli_cooldown_exits_before_application_imports(enabled):
    code = '''import sys,runpy,importlib.abc
from datetime import datetime,timedelta,timezone
from app.jobs import reference_preflight
reference_preflight.read_state=lambda:((datetime.now(timezone.utc)+timedelta(minutes=1)).isoformat(),None)
class Block(importlib.abc.MetaPathFinder):
 def find_spec(self,name,*args):
  if name=='sqlalchemy' or name=='app.models':raise AssertionError('Heavy import on cooldown')
sys.meta_path.insert(0,Block())
runpy.run_module('app.jobs.warm_institutional_reference',run_name='__main__')
'''
    result=subprocess.run([sys.executable,'-c',code],cwd=Path(__file__).resolve().parents[1],
                          env=os.environ.copy(),capture_output=True,text=True,timeout=15)
    assert result.returncode==0, result.stderr
    assert json.loads(result.stdout)['status']=='cooldown'
