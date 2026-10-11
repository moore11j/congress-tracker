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


@pytest.mark.skipif(sys.platform != 'linux', reason='Native child CPU accounting')
def test_job_cpu_counts_descendants_and_excludes_prior_jobs(tmp_path):
    # CPU-heavy grandchild exits via a shell; the next job mostly waits.
    busy = 'import time;end=time.process_time()+.15\nwhile time.process_time()<end:pass'
    first = shlex.join([sys.executable, '-c',
        'import subprocess;subprocess.run('+repr([sys.executable, '-c', busy])+',check=True)'])
    second = shlex.join([sys.executable, '-c', 'import time;time.sleep(.3)'])
    manifest = {key: {'lane':'data','command':command,'environment':{}}
                for key,command in [('busy',first),('waiting',second)]}
    path=tmp_path/'manifest';path.write_text(json.dumps(manifest));queue=tmp_path/'queue'
    with jobs.database(queue) as db:
        for index,key in enumerate(manifest):jobs.enqueue(db,key,manifest[key],now=time.time()+index)
    output=tmp_path/'worker.log'
    with output.open('w') as log:
        process=subprocess.Popen([sys.executable,str(SCRIPTS/'scheduled_jobs.py'),'--queue',str(queue),
            '--manifest',str(path),'worker','data'],stdout=log,stderr=subprocess.STDOUT)
        try:
            deadline=time.monotonic()+30
            while time.monotonic()<deadline:
                rows=[json.loads(line) for line in output.read_text().splitlines() if line.startswith('{')]
                finished=[r for r in rows if r['event']=='scheduled_job_finished']
                if len(finished)==2:break
                time.sleep(.05)
            assert len(finished)==2
            assert finished[0]['key']=='busy' and finished[0]['cpu_seconds']>=.14
            assert finished[1]['key']=='waiting' and finished[1]['cpu_seconds']<.14
            assert all(r['exit_code']==0 for r in finished)
        finally:
            process.terminate();process.wait(timeout=15)


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


@pytest.mark.skipif(sys.platform != 'linux', reason='Linux process groups')
def test_shutdown_kills_descendant_that_outlives_group_leader(tmp_path):
    token='descendant-'+str(os.getpid())
    ready=tmp_path/'ready'
    child="import os,signal,time;from pathlib import Path;signal.signal(signal.SIGTERM,signal.SIG_IGN);Path("+repr(str(ready))+").write_text(str(os.getpid()));time.sleep(60)"
    parent='import subprocess,time;subprocess.Popen('+repr([sys.executable,'-c',child])+');time.sleep(60)'
    process=subprocess.Popen([sys.executable,'-c',parent],env={**os.environ,'WALNUT_JOB_TOKEN':token},start_new_session=True)
    try:
        deadline=time.monotonic()+5
        while not ready.exists() and time.monotonic()<deadline:time.sleep(.02)
        child_pid=int(ready.read_text())
        jobs.stop_process(process.pid,token,grace_seconds=.2)
        process.wait(timeout=5)
        deadline=time.monotonic()+3
        while jobs.owns_process(child_pid,token) and time.monotonic()<deadline:time.sleep(.02)
        assert not jobs.owns_process(child_pid,token)
    finally:
        jobs.stop_process(process.pid,token,grace_seconds=.2)
        process.wait(timeout=5)


def test_external_retry_after_completion_never_enqueues_again(tmp_path):
    manifest = jobs.MANUAL_JOBS
    with jobs.database(tmp_path/'queue') as db:
        first = jobs.dispatch_ingest(db, 'core', '123-1', manifest, now=1)
        retry = jobs.dispatch_ingest(db, 'core', '123-1', manifest, now=2)
        assert first == retry and first['target_run'] == 1
        assert jobs.wait_external_request(db, '123-1', wait_seconds=0)['status'] == 'pending'
        job = jobs.claim(db, 'data', manifest, now=3)
        jobs.finish(db, job, 0, now=4)
        receipt = jobs.wait_external_request(db, '123-1', wait_seconds=0)
        assert receipt['status'] == 'succeeded'
        jobs.dispatch_ingest(db, 'core', '123-1', manifest, now=5)
        assert jobs.claim(db, 'data', manifest) is None
        assert jobs.wait_external_request(db, '123-1', wait_seconds=0) == receipt


def test_external_requests_during_execution_wait_for_next_run(tmp_path):
    manifest = jobs.MANUAL_JOBS
    with jobs.database(tmp_path/'queue') as db:
        jobs.dispatch_ingest(db, 'core', '123-1', manifest)
        first = jobs.claim(db, 'data', manifest)
        assert jobs.dispatch_ingest(db, 'core', '124-1', manifest)['target_run'] == 2
        assert jobs.dispatch_ingest(db, 'core', '125-1', manifest)['target_run'] == 2
        jobs.finish(db, first, 0)
        assert jobs.wait_external_request(db, '124-1', wait_seconds=0)['status'] == 'pending'
        second = jobs.claim(db, 'data', manifest)
        jobs.finish(db, second, 7)
        assert jobs.wait_external_request(db, '123-1', wait_seconds=0)['status'] == 'succeeded'
        for request_id in ('124-1','125-1'):
            receipt = jobs.wait_external_request(db, request_id, wait_seconds=0)
            assert receipt['status'] == 'failed' and receipt['exit_code'] == 7
        assert jobs.claim(db, 'data', manifest) is None


def test_external_requests_serialize_with_cron_and_preserve_delivery(tmp_path):
    manifest = {**jobs.MANUAL_JOBS, 'cron': {'lane':'data'}, 'mail': {'lane':'delivery'}}
    queue = tmp_path/'queue'
    with jobs.database(queue) as db, jobs.database(queue) as second:
        jobs.enqueue(db, 'cron', manifest['cron'], now=1)
        cron = jobs.claim(db, 'data', manifest, now=2)
        jobs.dispatch_ingest(second, 'core', '123-1', manifest, now=3)
        assert jobs.claim(second, 'data', manifest) is None
        jobs.enqueue(second, 'mail', manifest['mail'])
        assert jobs.claim(second, 'delivery', manifest)['key'] == 'mail'
        jobs.finish(db, cron, 0)
        assert jobs.claim(second, 'data', manifest)['key'] == 'external-ingest-core'


def test_external_request_identity_and_commands_fail_closed(tmp_path):
    with jobs.database(tmp_path/'queue') as db:
        jobs.dispatch_ingest(db, 'core', '123-1', jobs.MANUAL_JOBS)
        with pytest.raises(ValueError, match='another job'):
            jobs.dispatch_ingest(db, 'all', '123-1', jobs.MANUAL_JOBS)
        for job, request in [('core;false','124-1'),('core','invalid'),('core','1-2;false')]:
            with pytest.raises(ValueError): jobs.dispatch_ingest(db, job, request, jobs.MANUAL_JOBS)
        altered = {**jobs.MANUAL_JOBS, 'external-ingest-core': {'lane':'data','command':'false','environment':{}}}
        with pytest.raises(ValueError, match='fixed manifest'):
            jobs.dispatch_ingest(db, 'core', '124-1', altered)
        assert db.execute('SELECT count(*) FROM external_requests').fetchone()[0] == 1


def test_wrong_finish_token_does_not_complete_external_request(tmp_path):
    with jobs.database(tmp_path/'queue') as db:
        jobs.dispatch_ingest(db, 'core', '123-1', jobs.MANUAL_JOBS)
        job=jobs.claim(db,'data',jobs.MANUAL_JOBS)
        jobs.finish(db, {**job,'token':'wrong'},0)
        assert jobs.wait_external_request(db, '123-1', wait_seconds=0)['status'] == 'pending'
        jobs.finish(db,job,-15)
        assert jobs.wait_external_request(db, '123-1', wait_seconds=0)['status'] == 'failed'



def test_institutional_schedule_is_bounded_serialized_and_does_not_enable_publication(tmp_path):
    source=(SCRIPTS.parent/'crontab').read_text()
    lines=[line for line in source.splitlines() if 'python -m app.jobs.publish_direct_13fs ' in line]
    assert len(lines)==1
    assert lines[0].split(None,5)[:5]==['37','*','*','*','*']
    _,manifest=jobs.compile_schedule(source,SCRIPTS/'scheduled_jobs.py',tmp_path/'manifest',tmp_path/'queue')
    definition=next(d for d in manifest.values() if 'python -m app.jobs.publish_direct_13fs ' in d['command'])
    command=definition['command']
    assert definition['lane']=='data'
    assert '--limit 20' in command and '--include-waiting' in command
    assert '--staged-identifiers' in command and '--staged-references' in command
    assert '--identifier-manifest /app/config/sec_13f_identifiers.json' in command
    assert 'DIRECT_13F_PUBLICATION_ENABLED' not in command
    assert 'DIRECT_13F_PUBLICATION_ENABLED' not in definition['environment']
    import tomllib
    config=tomllib.loads((SCRIPTS.parent/'fly.toml').read_text())
    assert config['env']['DIRECT_13F_PUBLICATION_ENABLED']=='false'
