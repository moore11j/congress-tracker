"""Process isolation and durable recovery for the shared import template."""
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import uuid

import pytest

SCRIPTS = Path(__file__).resolve().parents[1] / 'scripts'
sys.path.insert(0, str(SCRIPTS))
import preloaded_jobs as warm
import scheduled_jobs as jobs


@pytest.fixture
def identity_dir(tmp_path, monkeypatch):
    monkeypatch.setenv('SCHEDULED_JOB_IDENTITY_DIRECTORY', str(tmp_path/'identities'))
    return tmp_path/'identities'


@pytest.mark.parametrize('command', [
    'cd /app && python -m app.jobs.warm_sec_research --extra',
    'cd /app && python -m app.jobs.warm_sec_research; echo extra',
    'cd /app && python -m app.jobs.deliver_strategy_event_emails',
    'cd /app && sh /app/scripts/run_enrichment_queue.sh',
    'python -m app.jobs.warm_sec_research',
])
def test_only_exact_reviewed_data_commands_use_preload(command):
    assert warm.selected_module(command) is None


def test_disabled_path_does_not_import_application_or_fork(monkeypatch):
    monkeypatch.setattr(warm, 'preload', lambda *a: pytest.fail('preload called'))
    assert warm.launch('cd /app && python -m app.jobs.warm_sec_research', {}) is None


def test_receipts_reject_invalid_tokens_and_reused_pids(identity_dir, monkeypatch):
    assert not warm.owns_process(os.getpid(), '../other')
    token=uuid.uuid4().hex
    identity_dir.mkdir()
    (identity_dir/(token+'.json')).write_text(json.dumps({'pid': os.getpid(), 'start_tick': 'old'}))
    monkeypatch.setattr(warm, '_start_tick', lambda pid: 'new')
    assert not warm.owns_process(os.getpid(), token)
    warm.forget_identity(token)
    assert not (identity_dir/(token+'.json')).exists()


LINUX=pytest.mark.skipif(sys.platform != 'linux', reason='Linux fork and process identities')


def wait_file(path, timeout=4):
    end=time.monotonic()+timeout
    while not path.exists() and time.monotonic()<end: time.sleep(.01)
    assert path.exists()
    return path.read_text()


@LINUX
def test_children_keep_independent_state_arguments_environment_and_exit_codes(tmp_path, monkeypatch, identity_dir):
    (tmp_path/'fork_state.py').write_text('values=[]\n')
    (tmp_path/'fork_fixture.py').write_text("""import os,json,sys
import fork_state
from pathlib import Path
fork_state.values.append(os.getpid())
Path(os.environ['RESULT']).write_text(json.dumps({'values':fork_state.values,'argv':sys.argv,'token':os.environ['WALNUT_JOB_TOKEN'],'pgrp':os.getpgrp(),'pid':os.getpid()}))
raise SystemExit(int(os.environ['EXIT']))
""")
    monkeypatch.syspath_prepend(str(tmp_path))
    import fork_state
    results=[]
    for i in range(2):
        token=uuid.uuid4().hex; out=tmp_path/f'result{i}'
        process=warm._fork_job('fork_fixture',{**os.environ,'RESULT':str(out),'EXIT':str(i),'WALNUT_JOB_TOKEN':token},
            working_directory=str(tmp_path),reset_database=lambda:None)
        assert process.wait(timeout=5)==i
        result=json.loads(wait_file(out));results.append(result)
        assert result['values']==[process.pid] and result['token']==token
        assert result['pgrp']==process.pid and len(result['argv'])==1
        warm.forget_identity(token)
    assert fork_state.values==[] and results[0]['pid'] != results[1]['pid']


@LINUX
def test_timeout_and_recovery_find_forked_child_even_before_pid_saved(tmp_path,monkeypatch,identity_dir):
    (tmp_path/'fork_hang.py').write_text("import signal,time\nsignal.signal(signal.SIGTERM,signal.SIG_IGN)\ntime.sleep(60)\n")
    monkeypatch.syspath_prepend(str(tmp_path));token=uuid.uuid4().hex
    process=warm._fork_job('fork_hang',{**os.environ,'WALNUT_JOB_TOKEN':token},working_directory=str(tmp_path),reset_database=lambda:None)
    try:
        assert process.pid in jobs.owned_groups(token)
        with pytest.raises(subprocess.TimeoutExpired):process.wait(timeout=.08)
        jobs.stop_process(None,token,grace_seconds=.08)
        assert process.wait(timeout=3)<0
        assert not warm.owns_process(process.pid,token)
    finally:
        jobs.stop_process(process.pid,token,grace_seconds=.01)
        process.wait(timeout=3);warm.forget_identity(token)


@LINUX
def test_failed_registration_cannot_run_job_or_leave_child(tmp_path,monkeypatch,identity_dir):
    output=tmp_path/'must-not-exist'
    (tmp_path/'fork_never.py').write_text('from pathlib import Path\nPath('+repr(str(output))+').touch()\n')
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.setattr(warm,'_record_identity',lambda *a: (_ for _ in ()).throw(OSError('registration failure')))
    with pytest.raises(OSError):
        warm._fork_job('fork_never',{**os.environ,'WALNUT_JOB_TOKEN':uuid.uuid4().hex},working_directory=str(tmp_path),reset_database=lambda:None)
    assert not output.exists()


@LINUX
def test_parent_death_before_acknowledgement_prevents_job_execution(tmp_path,identity_dir):
    output=tmp_path/'must-not-run';ready=tmp_path/'ready'
    (tmp_path/'fork_never.py').write_text('from pathlib import Path\nPath('+repr(str(output))+').touch()\n')
    script="""import os,time,sys
from pathlib import Path
import preloaded_jobs as w
def record(pid,token):
 Path(sys.argv[1]).write_text(str(pid))
 time.sleep(60)
w._record_identity=record
w._fork_job('fork_never',{**os.environ,'WALNUT_JOB_TOKEN':'a'*32},working_directory=sys.argv[2],reset_database=lambda:None)
"""
    env={**os.environ,'PYTHONPATH':os.pathsep.join([str(SCRIPTS),str(tmp_path)])}
    parent=subprocess.Popen([sys.executable,'-c',script,str(ready),str(tmp_path)],env=env)
    try:
        child=int(wait_file(ready));parent.kill();parent.wait(timeout=3)
        end=time.monotonic()+3
        while time.monotonic()<end:
            try:warm._start_tick(child)
            except (OSError,ProcessLookupError):break
            time.sleep(.02)
        else:pytest.fail('unacknowledged child stayed alive after parent death')
        assert not output.exists()
    finally:
        if parent.poll() is None:parent.kill();parent.wait(timeout=3)


@LINUX
def test_real_template_has_no_network_connections_and_child_pool_is_distinct(tmp_path,identity_dir):
    script="""import os,socket,json,sys
from pathlib import Path
import preloaded_jobs as w
def forbidden(*args,**kwargs):raise AssertionError('network during preload')
socket.socket.connect=forbidden
environment={**os.environ,'FLY_PROCESS_GROUP':'cron','DATABASE_URL':'sqlite:///:memory:','WALNUT_JOB_TOKEN':'b'*32}
original=dict(os.environ)
assert w.preload(environment)
assert dict(os.environ)==original
from app.db import engine
assert not getattr(engine.pool,'checkedout',lambda:0)()
assert w.preload({**environment,'WALNUT_JOB_TOKEN':'c'*32})
assert not w.preload({**environment,'DATABASE_URL':'sqlite:///different'})
import sqlalchemy
before=engine.pool
def reset():
 w._reset_database()
 assert engine.pool is not before
 with engine.connect() as c:assert c.scalar(sqlalchemy.text('select 1'))==1
p=w._fork_job('pool_fixture',environment,working_directory=sys.argv[1],reset_database=reset)
assert p.wait(timeout=5)==0
assert engine.pool is before
w.forget_identity('b'*32)
print('real_template_and_pool_pass')
"""
    (tmp_path/'pool_fixture.py').write_text('pass\n')
    env={**os.environ,'PYTHONPATH':os.pathsep.join([str(SCRIPTS),str(SCRIPTS.parent),str(tmp_path)])}
    r=subprocess.run([sys.executable,'-c',script,str(tmp_path)],env=env,capture_output=True,text=True,timeout=20)
    assert r.returncode==0,r.stdout+r.stderr
    assert 'real_template_and_pool_pass' in r.stdout

@LINUX
def test_stalled_child_startup_is_bounded_and_reaped(tmp_path, monkeypatch, identity_dir):
    token = uuid.uuid4().hex
    class StalledResource:
        def close(self):
            time.sleep(60)
    before = time.monotonic()
    with pytest.raises(TimeoutError):
        warm._fork_job('must_not_run', {**os.environ, 'WALNUT_JOB_TOKEN': token},
                       close_resources=(StalledResource(),), startup_timeout=.1)
    assert time.monotonic() - before < 3
    assert jobs.owned_groups(token) == set()


@LINUX
def test_fork_failure_closes_all_pipe_descriptors(monkeypatch):
    before = set(Path('/proc/self/fd').iterdir())
    def failed_fork():
        raise OSError('resource unavailable')
    monkeypatch.setattr(os, 'fork', failed_fork)
    with pytest.raises(OSError):
        warm._fork_job('must_not_run', {'WALNUT_JOB_TOKEN': 'a' * 32})
    assert set(Path('/proc/self/fd').iterdir()) == before

@LINUX
def test_other_users_processes_are_not_treated_as_owned(monkeypatch):
    def denied(self):
        raise PermissionError('unrelated process')
    monkeypatch.setattr(Path, 'read_bytes', denied)
    assert not jobs.owns_process(os.getpid(), 'not-a-job-token')

@LINUX
def test_worker_fork_path_keeps_queue_and_lane_lock_in_parent(tmp_path, identity_dir):
    fixture = tmp_path / 'queue_fixture.py'
    fixture.write_text("from pathlib import Path\nimport os\nPath(os.environ['OUTPUT']).write_text(str(os.getpid()))\n")
    manifest = {'first': {'command': 'fixture', 'lane': 'data', 'environment': {'OUTPUT': str(tmp_path/'first')}},
                'second': {'command': 'fixture', 'lane': 'data', 'environment': {'OUTPUT': str(tmp_path/'second')}}}
    queue = tmp_path/'queue'
    with jobs.database(queue) as db:
        for n,key in enumerate(manifest):
            jobs.enqueue(db, key, manifest[key], now=time.time()+n)
    code = '''import os,sys,json,functools
import preloaded_jobs as w,scheduled_jobs as j
w.selected_module=lambda command:'queue_fixture'
w.preload=lambda environment:True
w._fork_job=functools.partial(w._fork_job,working_directory=sys.argv[2],reset_database=lambda:None)
j.worker(sys.argv[1],json.loads(sys.argv[3]),'data',idle=.02)
'''
    env = {**os.environ, 'SCHEDULED_JOB_PRELOAD_ENABLED':'1',
           'PYTHONPATH':os.pathsep.join([str(SCRIPTS),str(tmp_path)])}
    log = tmp_path/'worker.log'
    with log.open('w') as output:
        process = subprocess.Popen([sys.executable,'-c',code,str(queue),str(tmp_path),json.dumps(manifest)],
                                   env=env,stdout=output,stderr=output)
        try:
            wait_file(tmp_path/'second',timeout=10)
            import fcntl
            with Path(str(queue)+'.data.lock').open('a') as other_lock:
                with pytest.raises(BlockingIOError):
                    fcntl.flock(other_lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
            end=time.monotonic()+5
            while time.monotonic()<end:
                with jobs.database(queue) as db:
                    rows=list(db.execute('SELECT runs,exit_code FROM jobs'))
                if all(row['runs']==1 and row['exit_code']==0 for row in rows):break
                time.sleep(.02)
            else:pytest.fail(log.read_text())
        finally:
            process.terminate();process.wait(timeout=5)
    receipts=[json.loads(line) for line in log.read_text().splitlines() if line.startswith('{')]
    finished=[r for r in receipts if r['event']=='scheduled_job_finished']
    assert len(finished)==2 and all(r['preloaded'] for r in finished)
    assert not list(identity_dir.glob('*.json'))

@LINUX
def test_previous_boot_receipt_cannot_own_current_process(identity_dir):
    token = 'd' * 32
    warm._record_identity(os.getpid(), token)
    assert warm.owns_process(os.getpid(), token)
    path = identity_dir/(token+'.json')
    record = json.loads(path.read_text())
    record['boot_id'] = 'previous-boot'
    path.write_text(json.dumps(record))
    assert not warm.owns_process(os.getpid(), token)
    warm.forget_identity(token)
