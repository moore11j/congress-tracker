"""Queue invariants and real Linux subprocess isolation without product data."""
import importlib.util
import json
import os
from pathlib import Path
import signal
import shlex
import shutil
import subprocess
import sys
import time

import pytest

SCRIPTS = Path(__file__).resolve().parents[1] / 'scripts'
sys.path.insert(0, str(SCRIPTS))
import scheduled_jobs as jobs
from combined_service import scheduler_enabled


def test_schedule_preserves_all_commands_and_calendars(tmp_path):
    source = (SCRIPTS.parent / 'crontab').read_text()
    result, manifest = jobs.compile_schedule(source, SCRIPTS/'scheduled_jobs.py', tmp_path/'manifest', tmp_path/'queue')
    original = [x.split(None, 5) for x in source.splitlines() if x.strip() and not x.startswith('#') and '=' not in x.split()[0]]
    converted = [x.split(None, 5) for x in result.splitlines() if x.strip() and not x.startswith('#') and '=' not in x.split()[0]]
    assert len(original) == len(converted)
    assert [x[:5] for x in original] == [x[:5] for x in converted]
    assert {x[5] for x in original} == {x['command'] for x in manifest.values()}
    assert all(x['environment']['CRON_TZ'] == 'America/Los_Angeles' for x in manifest.values())
    for definition in manifest.values():
        if 'collect_direct_feeds' in definition['command']:
            assert definition['lane'] == 'data'
        if 'run_email_digest_schedule.sh' in definition['command']:
            assert definition['lane'] == 'delivery'


def test_duplicates_coalesce_and_arrival_during_run_remains_pending(tmp_path):
    manifest = {'a': {'lane': 'data', 'command': 'true'}}
    with jobs.database(tmp_path/'queue') as db:
        jobs.enqueue(db, 'a', manifest['a'], now=1)
        jobs.enqueue(db, 'a', manifest['a'], now=2)
        first = jobs.claim(db, 'data', manifest, now=3)
        assert first['pending_at'] == 1
        jobs.enqueue(db, 'a', manifest['a'], now=4)
        jobs.enqueue(db, 'a', manifest['a'], now=5)
        assert jobs.claim(db, 'data', manifest, now=6) is None
        jobs.finish(db, first, 0, now=7)
    with jobs.database(tmp_path/'queue') as db:
        next_job = jobs.claim(db, 'data', manifest, now=8)
        assert next_job['pending_at'] == 4
        jobs.finish(db, next_job, 0)
        assert jobs.claim(db, 'data', manifest) is None


def test_lanes_are_bounded_and_oldest_waiting_job_runs_first(tmp_path):
    manifest = {key: {'lane': lane} for key,lane in [('a','data'),('b','data'),('c','delivery')]}
    with jobs.database(tmp_path/'queue') as db:
        for n,key in enumerate(manifest):
            jobs.enqueue(db, key, manifest[key], now=n+1)
        a = jobs.claim(db, 'data', manifest)
        assert a['key'] == 'a'
        assert jobs.claim(db, 'data', manifest) is None
        assert jobs.claim(db, 'delivery', manifest)['key'] == 'c'
        jobs.finish(db, a, 0)
        assert jobs.claim(db, 'data', manifest)['key'] == 'b'


def test_removed_commands_cannot_execute_and_wrong_token_cannot_finish(tmp_path):
    with jobs.database(tmp_path/'queue') as db:
        jobs.enqueue(db,'removed',{'lane':'data'})
        assert jobs.claim(db,'data',{}) is None
        jobs.enqueue(db,'a',{'lane':'data'})
        item=jobs.claim(db,'data',{'a':{'lane':'data'}})
        jobs.finish(db,{**item,'token':'wrong'},0)
        assert jobs.claim(db,'data',{'a':{}}) is None


def test_only_selected_machine_runs_scheduler():
    env={'COMBINED_SCHEDULER_ENABLED':'true','COMBINED_SCHEDULER_MACHINE_ID':'one','FLY_MACHINE_ID':'one'}
    assert scheduler_enabled(env)
    assert not scheduler_enabled({**env,'FLY_MACHINE_ID':'two'})
    assert not scheduler_enabled({**env,'COMBINED_SCHEDULER_MACHINE_ID':''})
    assert not scheduler_enabled({**env,'COMBINED_SCHEDULER_ENABLED':'false'})


@pytest.mark.skipif(sys.platform != 'linux', reason='Real flock and process groups require Linux')
def test_linux_workers_serialize_imports_and_keep_delivery_independent(tmp_path):
    import shlex
    log=tmp_path/'events'
    command=lambda tag,delay: shlex.join([sys.executable,'-c',
        "import os,time;from pathlib import Path;p=Path("+repr(str(log))+");"
        "f=p.open('a');f.write("+repr(tag+' start\n')+");f.flush();time.sleep("+str(delay)+");"
        "f.write("+repr(tag+' end\n')+");f.close()"])
    manifest={'a':{'lane':'data','command':command('a',1),'environment':{}},
              'b':{'lane':'data','command':command('b',.1),'environment':{}},
              'c':{'lane':'delivery','command':command('c',.1),'environment':{}}}
    path=tmp_path/'manifest';path.write_text(json.dumps(manifest))
    queue=tmp_path/'queue'
    with jobs.database(queue) as db:
        for index,key in enumerate(manifest):jobs.enqueue(db,key,manifest[key],now=index+1)
    workers=[subprocess.Popen([sys.executable,str(SCRIPTS/'scheduled_jobs.py'),'--queue',str(queue),
                              '--manifest',str(path),'worker',lane]) for lane in ('data','delivery')]
    try:
        deadline=time.monotonic()+15
        while time.monotonic()<deadline:
            if log.exists() and len(log.read_text().splitlines())==6:break
            time.sleep(.05)
        lines=log.read_text().splitlines()
        assert lines.index('a end') < lines.index('b start')
        assert lines.index('c end') < lines.index('a end')
    finally:
        for process in workers:process.terminate()
        for process in workers:process.wait(timeout=15)


@pytest.mark.skipif(sys.platform != 'linux', reason='Real process group recovery requires Linux')
def test_recovery_stops_only_owned_process_and_preserves_next_request(tmp_path):
    token='test-'+str(os.getpid())
    process=subprocess.Popen([sys.executable,'-c','import time;time.sleep(60)'],
                             env={**os.environ,'WALNUT_JOB_TOKEN':token},start_new_session=True)
    try:
        with jobs.database(tmp_path/'queue') as db:
            jobs.enqueue(db,'a',{'lane':'data'},now=1)
            item=jobs.claim(db,'data',{'a':{}},now=2)
            db.execute('UPDATE jobs SET token=?,pid=? WHERE key=?',(token,process.pid,'a'))
            jobs.enqueue(db,'a',{'lane':'data'},now=3)
            jobs.recover(db,'data')
            process.wait(timeout=15)
            assert process.returncode != 0
            assert jobs.claim(db,'data',{'a':{}},now=4)['pending_at']==3
    finally:
        if process.poll() is None:process.kill()
        process.wait()


@pytest.mark.skipif(sys.platform != 'linux', reason='Linux process supervision')
def test_supervisor_failure_stops_active_job_and_preserves_queue(tmp_path):
    if not shutil.which('supercronic'):
        pytest.skip('Supercronic is not installed')
    command=shlex.join([sys.executable,'-c','import time;time.sleep(60)'])
    source='* * * * * '+command+'\n'
    (tmp_path/'crontab').write_text(source)
    queue=tmp_path/'queue'
    _,manifest=jobs.compile_schedule(source,SCRIPTS/'scheduled_jobs.py',tmp_path/'manifest',queue)
    key=next(iter(manifest))
    with jobs.database(queue) as db:jobs.enqueue(db,key,manifest[key])
    code=('from pathlib import Path;from combined_service import run;run(root=Path('+repr(str(tmp_path))+'),'
          'runtime_dir=Path('+repr(str(tmp_path))+'),app_command='+repr([sys.executable,'-c','import time;time.sleep(1);raise SystemExit(7)'])+')')
    env={**os.environ,'PYTHONPATH':str(SCRIPTS),'COMBINED_SCHEDULER_ENABLED':'true',
         'COMBINED_SCHEDULER_MACHINE_ID':'test','FLY_MACHINE_ID':'test','SCHEDULED_QUEUE_PATH':str(queue)}
    result=subprocess.run([sys.executable,'-c',code],env=env,capture_output=True,text=True,timeout=35)
    assert result.returncode != 0
    with jobs.database(queue) as db:
        row=db.execute('SELECT * FROM jobs WHERE key=?',(key,)).fetchone()
        assert row['runs']==1
        assert row['started_at'] is None and row['pid'] is None
        assert row['exit_code'] != 0
