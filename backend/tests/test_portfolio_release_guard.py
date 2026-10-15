"""Real queue ownership/race checks, with no product records or network."""
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import sys
from threading import Barrier
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'scripts'))
import scheduled_jobs as queue

MANIFEST = {
    'portfolio': {'lane': 'data', 'command': queue.PORTFOLIO_COMMAND},
    'all': {'lane': 'data', 'command': 'cd /app && python -m app.ingest_run --job all'},
    'news': {'lane': 'data', 'command': 'news'},
    'alerts': {'lane': 'delivery', 'command': 'alerts'},
}


def test_reserved_release_defers_portfolio_preserves_pending_and_allows_other_lanes(tmp_path):
    path = tmp_path/'queue'
    with queue.database(path) as db:
        assert queue.reserve_release(db, '123-1', MANIFEST, now=1)['status']=='ready'
        for key in MANIFEST:
            queue.enqueue(db, key, MANIFEST[key], now=2)
        news = queue.claim(db, 'data', MANIFEST, now=3)
        assert news['key']=='news'
        queue.finish(db, news, 0, now=4)
        assert queue.claim(db, 'data', MANIFEST, now=5) is None
        assert queue.claim(db, 'delivery', MANIFEST, now=5)['key']=='alerts'
        assert {r['key']:r['pending_at'] for r in db.execute("SELECT key,pending_at FROM jobs WHERE key IN ('all','portfolio')")}=={'all':2,'portfolio':2}
    # A restart retains the reservation; wrong-owner cleanup cannot release it.
    with queue.database(path) as db:
        assert queue.release_reservation(db, '999-1')['status']=='not_owned'
        assert queue.claim(db, 'data', MANIFEST, now=6) is None
        assert queue.release_reservation(db, '123-1')['status']=='released'
        assert queue.claim(db, 'data', MANIFEST, now=7)['key']=='all'


@pytest.mark.parametrize('key', ['portfolio','all'])
@pytest.mark.parametrize('continuing', [False,True])
def test_unfinished_portfolio_or_all_blocks_release(tmp_path,key,continuing):
    with queue.database(tmp_path/'queue') as db:
        queue.enqueue(db,key,MANIFEST[key],now=1)
        job=queue.claim(db,'data',MANIFEST,now=2)
        if continuing:
            db.execute('UPDATE jobs SET started_at=NULL,continuation_at=3 WHERE key=?',(key,))
        assert queue.reserve_release(db,'123-1',MANIFEST,now=4)=={'status':'blocked','reason':'portfolio_cycle_unfinished'}
        assert db.execute('SELECT count(*) FROM release_hold').fetchone()[0]==0
        assert db.execute('SELECT cycle_id FROM jobs WHERE key=?',(key,)).fetchone()[0]==job['cycle_id']


def test_unknown_command_with_portfolio_checkpoint_is_fail_closed(tmp_path):
    with queue.database(tmp_path/'queue') as db:
        queue.enqueue(db,'legacy',{'lane':'data'},now=1)
        db.execute("UPDATE jobs SET continuation_at=3,checkpoint_json=? WHERE key='legacy'",('{"kind":"portfolio_refresh_v1"}',))
        assert queue.reserve_release(db,'123-1',MANIFEST,now=4)['status']=='blocked'


def test_expiry_restores_pending_work_and_transport_retry_does_not_extend_hold(tmp_path):
    with queue.database(tmp_path/'queue') as db:
        assert queue.reserve_release(db,'123-1',MANIFEST,seconds=60,now=1)['status']=='ready'
        assert queue.reserve_release(db,'123-1',MANIFEST,seconds=900,now=50)=={'status':'ready','existing':True}
        assert queue.reserve_release(db,'999-1',MANIFEST,now=50)=={'status':'blocked','reason':'another_release_reserved'}
        queue.enqueue(db,'portfolio',MANIFEST['portfolio'],now=2)
        assert queue.claim(db,'data',MANIFEST,now=60) is None
        assert queue.claim(db,'data',MANIFEST,now=61)['key']=='portfolio'
        assert queue.reserve_release(db,'999-1',MANIFEST,now=62)['status']=='blocked'


@pytest.mark.parametrize('owner,seconds',[('invalid',60),('123-1',59),('123-1',901)])
def test_unbounded_or_unidentified_holds_refused(tmp_path,owner,seconds):
    with queue.database(tmp_path/'queue') as db:
        with pytest.raises(ValueError):queue.reserve_release(db,owner,MANIFEST,seconds=seconds)
        assert db.execute('SELECT count(*) FROM release_hold').fetchone()[0]==0


def test_claim_and_release_reservation_have_exactly_one_winner(tmp_path):
    path=tmp_path/'queue'
    with queue.database(path) as db:
        queue.enqueue(db,'portfolio',MANIFEST['portfolio'],now=1)
    barrier=Barrier(2)
    def claim():
        with queue.database(path) as db:
            barrier.wait()
            return queue.claim(db,'data',MANIFEST,now=2)
    def reserve():
        with queue.database(path) as db:
            barrier.wait()
            return queue.reserve_release(db,'123-1',MANIFEST,now=2)
    with ThreadPoolExecutor(max_workers=2) as executor:
        claimed=executor.submit(claim);reserved=executor.submit(reserve)
        job,result=claimed.result(),reserved.result()
    assert (job is not None) != (result['status']=='ready')
    with queue.database(path) as db:
        active=db.execute('SELECT count(*) FROM jobs WHERE started_at IS NOT NULL').fetchone()[0]
        held=db.execute('SELECT count(*) FROM release_hold WHERE expires_at>2').fetchone()[0]
        assert active+held==1


def test_cli_reserves_and_cleans_up_only_on_scheduler_owner(tmp_path):
    import json
    import os
    import subprocess
    path=tmp_path/'queue'
    manifest_path=tmp_path/'manifest.json'
    manifest_path.write_text(json.dumps(MANIFEST))
    with queue.database(path) as db:
        queue.enqueue(db,'portfolio',MANIFEST['portfolio'],now=1)
    script=Path(queue.__file__)
    base=[sys.executable,str(script),'--queue',str(path),'--manifest',str(manifest_path)]
    environment={**os.environ,'COMBINED_SCHEDULER_ENABLED':'true',
                 'COMBINED_SCHEDULER_MACHINE_ID':'fixture-owner','FLY_MACHINE_ID':'fixture-owner'}
    def run(*args,env=environment):
        return subprocess.run(base+list(args),env=env,capture_output=True,text=True,timeout=10)
    denied=run('reserve-release','--owner','123-1',env={**environment,'FLY_MACHINE_ID':'other'})
    assert denied.returncode!=0 and 'active scheduler owner' in denied.stderr
    with queue.database(path) as db:
        assert db.execute('SELECT count(*) FROM release_hold').fetchone()[0]==0
    accepted=run('reserve-release','--owner','123-1','--seconds','60')
    assert accepted.returncode==0,accepted.stderr
    assert json.loads(accepted.stdout)=={'status':'ready','existing':False}
    conflict=run('reserve-release','--owner','456-1')
    assert conflict.returncode==75 and json.loads(conflict.stdout)['reason']=='another_release_reserved'
    with queue.database(path) as db:
        assert queue.claim(db,'data',MANIFEST) is None
    assert json.loads(run('release-reservation','--owner','456-1').stdout)['status']=='not_owned'
    released=run('release-reservation','--owner','123-1')
    assert released.returncode==0 and json.loads(released.stdout)['status']=='released'
    with queue.database(path) as db:
        job=queue.claim(db,'data',MANIFEST)
        assert job['key']=='portfolio' and job['pending_at']==1
    unfinished=run('reserve-release','--owner','123-2')
    assert unfinished.returncode==75
    assert json.loads(unfinished.stdout)['reason']=='portfolio_cycle_unfinished'
