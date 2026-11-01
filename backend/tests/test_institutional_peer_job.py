from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from threading import Barrier

import pytest

from app.jobs import capture_institutional_peers as capture

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
import scheduled_jobs as queue

KEY = capture.JOB_NAME
MANIFEST = {**queue.MANUAL_JOBS, 'alert-fixture': {'lane': 'delivery', 'command': 'alerts'}}


@pytest.mark.parametrize('failure', [None, 'readonly', 'transaction_close'])
def test_readonly_transaction_must_close_before_terminal(monkeypatch, failure):
    import contextlib
    import io
    from types import SimpleNamespace
    from app import db as database_module
    from app.services import institutional_peer_capture as peer
    import sqlalchemy.orm
    steps=[]
    class Connection:
        def execution_options(self, **kwargs):
            assert kwargs == {'isolation_level':'REPEATABLE READ'}
            return contextlib.nullcontext(self)
        @contextlib.contextmanager
        def begin(self):
            yield
            steps.append('transaction_closed')
            if failure=='transaction_close':raise RuntimeError('close_failed')
        def execute(self, statement):steps.append(str(statement))
        def scalar(self, statement):
            assert str(statement)=='SHOW transaction_read_only'
            return 'off' if failure=='readonly' else 'on'
    monkeypatch.setattr(database_module,'engine',SimpleNamespace(dialect=SimpleNamespace(name='postgresql'),connect=Connection))
    monkeypatch.setattr(sqlalchemy.orm,'Session',lambda bind:contextlib.nullcontext('session'))
    def export(db,output,scope):
        assert db=='session' and scope=={'fixture':True}
        assert steps==['SET TRANSACTION READ ONLY',"SET LOCAL statement_timeout='3s'",
            "SET LOCAL lock_timeout='500ms'","SET LOCAL idle_in_transaction_session_timeout='60s'"]
        steps.append('exported')
        def finish(**kwargs):
            assert steps[-1]=='transaction_closed'
            output.write(b'complete')
            return kwargs
        return SimpleNamespace(finish=finish)
    monkeypatch.setattr(peer,'export_peers',export)
    output=io.BytesIO()
    if failure:
        with pytest.raises(RuntimeError):capture.export_snapshot(output,{'fixture':True})
        assert output.getvalue()==b''
    else:
        assert capture.export_snapshot(output,{'fixture':True})==dict(transaction_read_only=True,production_writes=0)
        assert output.getvalue()==b'complete'


def request():
    return dict(version=1, request_id='a' * 32, package_sha256=capture.package_hash(),
                approved_at=100, expires_at=200,
                payload='institutional_canonical_peer_supplement', scope_sha256='a'*64,
                destination='owner_local_onedrive_migration_validation')


def test_exact_short_lived_package_approval(tmp_path):
    path = tmp_path / 'request.json'
    expected = request(); path.write_text(json.dumps(expected))
    assert capture.read_request(path, now=150) == expected


@pytest.mark.parametrize('change', ['version', 'id', 'hash', 'future', 'expired', 'duration',
                                    'payload', 'destination', 'bool', 'duplicate', 'oversize', 'scope', 'scope_missing'])
def test_unapproved_request_fails_before_capture(tmp_path, change):
    path = tmp_path / 'request.json'
    value = request()
    if change == 'version': value['version'] = True
    elif change == 'id': value['request_id'] = '../escape'
    elif change == 'hash': value['package_sha256'] = '0' * 64
    elif change == 'future': value['approved_at'] = 151
    elif change == 'expired': value['expires_at'] = 150
    elif change == 'duration': value['expires_at'] = 10000
    elif change == 'payload': value['payload'] = 'all_database'
    elif change == 'destination': value['destination'] = 'elsewhere'
    elif change == 'scope': value['scope_sha256'] = 'wrong'
    elif change == 'scope_missing': del value['scope_sha256']
    elif change == 'bool': value['approved_at'] = True
    path.write_text(json.dumps(value))
    if change == 'duplicate': path.write_text('{"version":1,"version":1}')
    if change == 'oversize': path.write_text(' ' * 4097)
    with pytest.raises(ValueError): capture.read_request(path, now=150)


@pytest.mark.parametrize('change', ['none', 'token', 'cycle', 'pid', 'key', 'second', 'finished'])
def test_live_queue_claim_is_required(tmp_path, change):
    path = tmp_path / 'queue'
    with queue.database(path) as db:
        queue.enqueue(db, KEY, MANIFEST[KEY], now=1)
        job = queue.claim(db, 'data', MANIFEST, now=2)
        db.execute('UPDATE jobs SET pid=123 WHERE key=?', (KEY,))
        token, cycle, pid = job['token'], job['cycle_id'], 123
        if change == 'token': token = 'b' * 32
        elif change == 'cycle': cycle = 'c' * 32
        elif change == 'pid': pid = 456
        elif change == 'key': db.execute("UPDATE jobs SET key='wrong'")
        elif change == 'second':
            db.execute("INSERT INTO jobs(key,lane,started_at) VALUES('other','data',2)")
        elif change == 'finished': queue.finish(db, job, 0)
    if change == 'none': capture.require_queue_ownership(path, token, cycle, pid)
    else:
        with pytest.raises(RuntimeError, match='not_exclusive'):
            capture.require_queue_ownership(path, token, cycle, pid)


def test_no_calendar_trigger_and_capture_serializes_with_imports_but_not_delivery(tmp_path):
    source = (Path(__file__).resolve().parents[1] / 'crontab').read_text()
    _, calendar = queue.compile_schedule(source, Path('script'), Path('manifest'), Path('queue'))
    assert KEY not in calendar and MANIFEST[KEY] not in calendar.values()
    with queue.database(tmp_path / 'queue') as db:
        queue.enqueue(db, KEY, MANIFEST[KEY], now=1)
        job = queue.claim(db, 'data', MANIFEST, now=2)
        queue.enqueue(db, 'external-ingest-core', MANIFEST['external-ingest-core'], now=3)
        queue.enqueue(db, 'alert-fixture', MANIFEST['alert-fixture'], now=3)
        assert queue.claim(db, 'data', MANIFEST, now=4) is None
        assert queue.claim(db, 'delivery', MANIFEST, now=4)['key'] == 'alert-fixture'
        assert queue.reserve_release(db, '123-1', MANIFEST, now=4) == {
            'status': 'blocked', 'reason': 'institutional_capture_active'}
        queue.finish(db, job, 0, now=5)
        assert queue.claim(db, 'data', MANIFEST, now=6)['key'] == 'external-ingest-core'


@pytest.mark.parametrize('iteration', range(5))
def test_actual_release_claim_race_has_one_winner(tmp_path, iteration):
    path = tmp_path / 'queue'
    with queue.database(path) as db: queue.enqueue(db, KEY, MANIFEST[KEY], now=1)
    barrier = Barrier(2)
    def claim():
        with queue.database(path) as db:
            barrier.wait(); return queue.claim(db, 'data', MANIFEST, now=2)
    def reserve():
        with queue.database(path) as db:
            barrier.wait(); return queue.reserve_release(db, '123-1', MANIFEST, now=2)
    with ThreadPoolExecutor(max_workers=2) as pool:
        claimed, reserved = pool.submit(claim), pool.submit(reserve)
        job, release = claimed.result(), reserved.result()
    assert (job is not None) != (release['status'] == 'ready')
    with queue.database(path) as db:
        assert db.execute('SELECT count(*) FROM jobs WHERE started_at IS NOT NULL').fetchone()[0] + db.execute('SELECT count(*) FROM release_hold').fetchone()[0] == 1


@pytest.mark.skipif(os.name != 'posix', reason='Production Linux process boundary')
@pytest.mark.parametrize('designated', [False, True])
def test_fresh_process_without_approval_never_imports_database(tmp_path, designated):
    script = '''
import importlib.abc,sys
class Deny(importlib.abc.MetaPathFinder):
    def find_spec(self,fullname,*args):
        if fullname in ('app.db','app.models','sqlalchemy'):
            raise AssertionError('Database import before capture approval')
sys.meta_path.insert(0,Deny())
from app.jobs import capture_institutional_peers as job
job.REQUEST=sys.argv[1]
try:job.main()
except SystemExit as exc:
    assert exc.code==75
else:raise AssertionError('Unapproved capture succeeded')
assert 'app.db' not in sys.modules and 'app.models' not in sys.modules
'''
    environment = {**os.environ, 'COMBINED_SCHEDULER_ENABLED': 'true',
                   'FLY_MACHINE_ID': 'fixture', 'COMBINED_SCHEDULER_MACHINE_ID': 'fixture',
                   'WALNUT_JOB_KEY': KEY if designated else 'wrong', 'WALNUT_JOB_QUEUE': capture.QUEUE}
    run = subprocess.run([sys.executable, '-c', script, str(tmp_path/'missing-request')],
                         env=environment, capture_output=True, text=True, timeout=10)
    assert run.returncode == 0, run.stdout + run.stderr
    assert json.loads(run.stdout)['status'] == 'held'


@pytest.mark.skipif(os.name != 'posix', reason='Production Linux queue/process limits')
@pytest.mark.parametrize('lose_claim', [False, True])
def test_real_child_owns_queue_and_closes_file_or_rejects_lost_claim(tmp_path, lose_claim):
    path=tmp_path/'queue'; approval=tmp_path/'request.json'; output=tmp_path/'output'
    scope_path=tmp_path/'scope.json'
    from app.services.institutional_peer_capture import encode,digest
    scope=dict(version=1,parent_capture_id='b'*32,parent_baseline_sha256='c'*64,
        documents=[dict(id=1,content_hash='d'*64,metadata_sha256='e'*64)],
        period_cusips={'2026-06-30':['000000001']})
    scope_path.write_bytes(encode(scope))
    value=request();value.update(approved_at=time.time()-1,expires_at=time.time()+300,scope_sha256=digest(scope))
    approval.write_text(json.dumps(value))
    with queue.database(path) as db:
        queue.enqueue(db,KEY,MANIFEST[KEY]);job=queue.claim(db,'data',MANIFEST)
    script='''
import contextlib,json,os,sqlite3,sys,time,types
from app.jobs import capture_institutional_peers as job
from app.services.institutional_peer_capture import PeerStream,POLICY,digest
job.QUEUE,job.REQUEST,job.OUTPUT=sys.argv[1:4]
job.SCOPE=sys.argv[5]
sys.modules['app.background_job_guard']=types.SimpleNamespace(check_background_job_guard=lambda _:types.SimpleNamespace(proceed=True))
sys.modules['app.jobs.collect_direct_feeds']=types.SimpleNamespace(collector_lock=lambda **_:contextlib.nullcontext(True))
def fixture(output,scope):
    if sys.argv[4]=='lose':
        with sqlite3.connect(job.QUEUE) as db:db.execute('UPDATE jobs SET token=?',('b'*32,))
        time.sleep(1.1)
    writer=PeerStream(output)
    writer.write('metadata',dict(version=1,kind='institutional_canonical_peers',
        scope_sha256=digest(scope),parent_capture_id=scope['parent_capture_id'],
        parent_baseline_sha256=scope['parent_baseline_sha256'],policy=POLICY,
        symbol_periods={'2026Q2':[],'2026Q3':[]},captured_at='2026-11-01T00:00:00+00:00',
        source_documents=1,transaction_read_only=True))
    return writer.finish(transaction_read_only=True,production_writes=0)
job.export_snapshot=fixture
job.main()
'''
    env={**os.environ,'COMBINED_SCHEDULER_ENABLED':'true','FLY_MACHINE_ID':'fixture',
         'COMBINED_SCHEDULER_MACHINE_ID':'fixture','WALNUT_JOB_KEY':KEY,'WALNUT_JOB_QUEUE':str(path),
         'WALNUT_JOB_TOKEN':job['token'],'WALNUT_JOB_CYCLE':job['cycle_id']}
    child=subprocess.Popen([sys.executable,'-c',script,str(path),str(approval),str(output),'lose' if lose_claim else 'keep',str(scope_path)],
                           env=env,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,start_new_session=True)
    with queue.database(path) as db:db.execute('UPDATE jobs SET pid=? WHERE key=?',(child.pid,KEY))
    stdout,stderr=child.communicate(timeout=20)
    assert child.returncode==(75 if lose_claim else 0),stdout+stderr
    assert json.loads(stdout)['status']==('held' if lose_claim else 'captured')
    manifest=output/(value['request_id']+'.complete.json')
    assert manifest.exists() is not lose_claim
    if not lose_claim:
        import gzip,io
        from app.services.institutional_capture_file import verify_file
        from app.services.institutional_peer_capture import receive_peers
        verified=verify_file(output,value['request_id'])
        with gzip.open(output/verified['filename'],'rb') as stream:
            proof=receive_peers(stream,io.BytesIO(),scope)
        assert proof['scope_sha256']==digest(scope) and not proof['full_cutover_ready']
