"""Continuation is a logical run, with fresh prices and no lost waiting triggers."""
from copy import deepcopy
from datetime import datetime, time as day_time, timezone
import json
import os
from pathlib import Path
import sqlite3
import sys
from types import SimpleNamespace

import pytest
from sqlalchemy import select

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
import scheduled_jobs as queue
import preloaded_jobs
from app.jobs import refresh_portfolio_batch as batch
from app.services.portfolio_refresh_checkpoint import PortfolioCheckpoint


DEFINITION = {'command': queue.PORTFOLIO_COMMAND, 'lane': 'data', 'environment': {}}


def store(path, job):
    return PortfolioCheckpoint({'WALNUT_JOB_QUEUE': str(path), 'WALNUT_JOB_KEY': job['key'],
        'WALNUT_JOB_TOKEN': job['token'], 'WALNUT_JOB_CYCLE': job['cycle_id']})


def ready_checkpoint(path, job, now=10):
    state = {'kind': 'portfolio_refresh_v1', 'cycle_id': job['cycle_id'],
             'continuation': True, 'deadline': now + 1000, 'retry_after_seconds': 1}
    store(path, job).save(state)
    return state


def test_legacy_queue_migration_preserves_waiting_and_active_rows(tmp_path):
    path = tmp_path/'queue'
    with sqlite3.connect(path) as db:
        db.execute('CREATE TABLE jobs(key TEXT PRIMARY KEY,lane TEXT NOT NULL,pending_at REAL,started_at REAL,token TEXT,pid INTEGER,finished_at REAL,exit_code INTEGER,runs INTEGER NOT NULL DEFAULT 0)')
        db.execute("INSERT INTO jobs(key,lane,pending_at,started_at,token,pid,runs) VALUES ('old','data',7,8,'legacy',123,4)")
    with queue.database(path) as db:
        row = dict(db.execute("SELECT * FROM jobs WHERE key='old'").fetchone())
        assert (row['pending_at'], row['started_at'], row['token'], row['pid'], row['runs']) == (7, 8, 'legacy', 123, 4)
        assert row['cycle_id'] is None and row['checkpoint_json'] is None


def test_yield_is_fair_durable_and_preserves_a_later_trigger(tmp_path):
    path = tmp_path/'queue'; manifest = {'portfolio': DEFINITION, 'news': {'lane': 'data', 'command': 'true'}}
    with queue.database(path) as db:
        queue.enqueue(db, 'portfolio', DEFINITION, now=1)
        original = queue.claim(db, 'data', manifest, now=2)
        ready_checkpoint(path, original)
        queue.enqueue(db, 'news', manifest['news'], now=3)
        queue.enqueue(db, 'portfolio', DEFINITION, now=4)
        assert queue.continue_portfolio(db, original, DEFINITION, 75, now=5)
        assert db.execute("SELECT runs FROM jobs WHERE key='portfolio'").fetchone()[0] == 0
    # Reopening the actual queue represents a restart between completed batches.
    with queue.database(path) as db:
        news = queue.claim(db, 'data', manifest, now=7)
        assert news['key'] == 'news'
        queue.finish(db, news, 0, now=8)
        resumed = queue.claim(db, 'data', manifest, now=9)
        assert resumed['cycle_id'] == original['cycle_id'] and resumed['token'] != original['token']
        assert store(path, resumed).read()['cycle_id'] == original['cycle_id']
        queue.finish(db, resumed, 0, now=10)
        next_run = queue.claim(db, 'data', manifest, now=11)
        assert next_run['pending_at'] == 4 and next_run['cycle_id'] != original['cycle_id']
        assert store(path, next_run).read() is None


def test_external_receipt_waits_for_whole_cycle_and_retry_does_not_duplicate(tmp_path):
    path = tmp_path/'queue'; key = 'external-ingest-portfolio-simulation-refresh'
    manifest = {key: queue.MANUAL_JOBS[key]}
    with queue.database(path) as db:
        first = queue.dispatch_ingest(db, 'portfolio-simulation-refresh', '123-1', manifest, now=1)
        job = queue.claim(db, 'data', manifest, now=2); ready_checkpoint(path, job)
        assert queue.continue_portfolio(db, job, manifest[key], 75, now=3)
        assert queue.dispatch_ingest(db, 'portfolio-simulation-refresh', '123-1', manifest, now=4) == first
        second = queue.dispatch_ingest(db, 'portfolio-simulation-refresh', '123-2', manifest, now=4)
        assert second['target_run'] == 2
        assert queue.wait_external_request(db, '123-1', wait_seconds=0)['status'] == 'pending'
        resumed = queue.claim(db, 'data', manifest, now=5); queue.finish(db, resumed, 0, now=6)
        assert queue.wait_external_request(db, '123-1', wait_seconds=0)['status'] == 'succeeded'
        assert queue.wait_external_request(db, '123-2', wait_seconds=0)['status'] == 'pending'
        following = queue.claim(db, 'data', manifest, now=7); queue.finish(db, following, 0, now=8)
        assert queue.wait_external_request(db, '123-2', wait_seconds=0)['status'] == 'succeeded'


def test_wrong_token_and_unreviewed_commands_cannot_yield_or_write(tmp_path):
    path = tmp_path/'queue'; manifest = {'p': DEFINITION}
    with queue.database(path) as db:
        queue.enqueue(db, 'p', DEFINITION, now=1); job = queue.claim(db, 'data', manifest, now=2)
        state = ready_checkpoint(path, job)
        assert not queue.continue_portfolio(db, job, {**DEFINITION, 'command': 'unreviewed'}, 75, now=3)
        assert not queue.continue_portfolio(db, job, DEFINITION, 0, now=3)
        with pytest.raises(RuntimeError, match='ownership'):
            store(path, {**job, 'token': 'f'*32}).save(state)
        queue.finish(db, job, 0)
        with pytest.raises(RuntimeError, match='ownership'):
            store(path, job).save(state)


class MemoryCheckpoint:
    cycle = 'a'*32
    def __init__(self): self.state = None
    def read(self): return deepcopy(self.state)
    def save(self, state): self.state = deepcopy(state)


def test_capacity_hold_keeps_plan_pending_and_deadline_is_not_success(monkeypatch):
    checkpoint = MemoryCheckpoint()
    monkeypatch.setattr('app.background_job_guard.check_background_job_guard', lambda *_: SimpleNamespace(proceed=False))
    first = batch.run(checkpoint=checkpoint, clock=lambda: 100)
    assert first['status'] == 'yielded' and first['planning_pending']
    assert checkpoint.state['retry_after_seconds'] == 60
    expired = batch.run(checkpoint=checkpoint, clock=lambda: 100+24*3600)
    assert expired['status'] == 'failed' and not checkpoint.state['continuation']


def _public_portfolio_state(factory):
    from app.models import ReplicatedPortfolioRun, ReplicatedPortfolioPoint, ReplicatedPortfolioPosition
    result = []
    with factory() as db:
        for run in db.scalars(select(ReplicatedPortfolioRun).order_by(ReplicatedPortfolioRun.entity_id, ReplicatedPortfolioRun.lookback_days)):
            fields = {c.name: getattr(run, c.name) for c in run.__table__.c if c.name not in {'id', 'created_at', 'computed_at'}}
            for label, model in [('points', ReplicatedPortfolioPoint), ('positions', ReplicatedPortfolioPosition)]:
                rows = [{c.name: getattr(row, c.name) for c in model.__table__.c if c.name not in {'id', 'run_id'}}
                        for row in db.scalars(select(model).where(model.run_id == run.id).order_by(model.id))]
                fields[label] = rows
            result.append(fields)
    return result


def test_actual_saved_batches_match_full_run_across_midnight_and_repeat(monkeypatch, tmp_path):
    from app import compute_replicated_portfolios as compute
    from test_replicated_portfolios import _session_factory, _add_member, _add_congress_portfolio_fixture
    monkeypatch.setattr('app.background_job_guard.check_background_job_guard', lambda *_: SimpleNamespace(proceed=True))
    monkeypatch.setenv('PORTFOLIO_REFRESH_LOOKBACKS', '30,365')
    monkeypatch.setenv('PORTFOLIO_REFRESH_REPLACE_EXISTING', '1')
    monkeypatch.setattr(batch, 'BATCH_SCOPE_LIMIT', 1)
    monkeypatch.setattr(compute, '_fetch_provider_eod_close_series', lambda *a, **k: pytest.fail('Unexpected provider request'))
    def fixture():
        engine, factory = _session_factory()
        monkeypatch.setattr(compute, 'engine', engine); monkeypatch.setattr(compute, 'SessionLocal', factory)
        with factory() as db:
            for n in range(2):
                _add_member(db, f'M{n}')
                day = _add_congress_portfolio_fixture(db, member_id=f'M{n}', event_id=99020+n)
            db.commit()
        return factory, day
    before_factory, day = fixture()
    for days in (30, 365):
        report = compute.run_all_congress_portfolio_batch(batch_size=2, dry_run=False, lookback_days=days, replace_existing=True, _as_of_date=day)
        assert report['summary']['failed'] == 0
    expected = _public_portfolio_state(before_factory)
    factory, _ = fixture(); path = tmp_path/'queue'; manifest = {'p': DEFINITION}
    from unittest.mock import Mock
    schema_check = Mock(wraps=compute.Base.metadata.create_all)
    monkeypatch.setattr(compute.Base.metadata, 'create_all', schema_check)
    now = datetime.combine(day, day_time(23,59), tzinfo=timezone.utc).timestamp()
    with queue.database(path) as db:
        queue.enqueue(db, 'p', DEFINITION, now=now)
        for iteration in range(4):
            job = queue.claim(db, 'data', manifest, now=now)
            result = batch.run(checkpoint=store(path, job), clock=lambda: now)
            assert result['as_of_date'] == day.isoformat()
            if iteration < 3:
                assert result['status'] == 'yielded'
                assert queue.continue_portfolio(db, job, DEFINITION, 75, now=now)
                # A changed universe/config must not replace the saved plan.
                monkeypatch.setattr(compute, '_all_congress_member_candidates', lambda *_: pytest.fail('Unexpected replan'))
                monkeypatch.setenv('PORTFOLIO_REFRESH_LOOKBACKS', '1095')
            else:
                assert result['status'] == 'ok' and result['completed_scopes'] == 4
                repeated = batch.run(checkpoint=store(path, job), clock=lambda: now)
                assert repeated['processed_this_batch'] == 0
                queue.finish(db, job, 0, now=now)
            now += 120
    assert _public_portfolio_state(factory) == expected
    assert schema_check.call_count == 1


def test_failed_scope_is_not_hidden_by_later_success(monkeypatch):
    from app import compute_replicated_portfolios as compute
    from test_replicated_portfolios import _session_factory
    engine, factory = _session_factory()
    monkeypatch.setattr(compute, 'engine', engine); monkeypatch.setattr(compute, 'SessionLocal', factory)
    monkeypatch.setattr('app.background_job_guard.check_background_job_guard', lambda *_: SimpleNamespace(proceed=True))
    monkeypatch.setenv('PORTFOLIO_REFRESH_LOOKBACKS', '30')
    monkeypatch.setattr(compute, '_all_congress_member_candidates', lambda *_: [{'entity_id':'A'}, {'entity_id':'B'}])
    def calculate(**kwargs):
        status = 'failed' if kwargs['_planned_entities'][0]['entity_id'] == 'A' else 'created'
        return {'results': [{'status':status}]}
    monkeypatch.setattr(compute, 'run_all_congress_portfolio_batch', calculate)
    result = batch.run(checkpoint=MemoryCheckpoint(), clock=lambda: 100, cpu_clock=lambda: 0, wall_clock=lambda: 0)
    assert result['status'] == 'failed' and result['failed_scopes'] == 1
    assert result['completed_by_lookback'] == {'30': 1}


def test_preload_exact_alias_and_cycle_changes_do_not_disable_template(monkeypatch):
    assert preloaded_jobs.selected_module(queue.PORTFOLIO_COMMAND) == 'app.jobs.refresh_portfolio_batch'
    assert preloaded_jobs.selected_module(queue.PORTFOLIO_COMMAND + ' --extra') is None
    env = {'SAFE':'same', 'WALNUT_JOB_TOKEN':'a', 'WALNUT_JOB_KEY':'x', 'WALNUT_JOB_CYCLE':'first'}
    monkeypatch.setattr(preloaded_jobs, '_template_environment', {'SAFE':'same'})
    monkeypatch.setattr(preloaded_jobs, '_preload_failed', False)
    assert preloaded_jobs.preload(env)
    assert preloaded_jobs.preload({**env,'WALNUT_JOB_TOKEN':'b','WALNUT_JOB_KEY':'y','WALNUT_JOB_CYCLE':'second'})
    assert not preloaded_jobs.preload({**env,'SAFE':'changed'})


def test_two_portfolio_triggers_do_not_interleave_asof_windows(tmp_path):
    path = tmp_path/'queue'
    manifest = {'cron': DEFINITION, 'external': DEFINITION, 'all': {**DEFINITION, 'command':'cd /app && python -m app.ingest_run --job all'},
                'news': {'lane':'data','command':'true'}}
    with queue.database(path) as db:
        queue.enqueue(db,'cron',DEFINITION,now=1)
        first=queue.claim(db,'data',manifest,now=2);ready_checkpoint(path,first)
        queue.enqueue(db,'external',DEFINITION,now=3)
        queue.enqueue(db,'all',manifest['all'],now=4)
        queue.enqueue(db,'news',manifest['news'],now=5)
        assert queue.continue_portfolio(db,first,DEFINITION,75,now=6)
        news=queue.claim(db,'data',manifest,now=6.5)
        assert news['key']=='news'
        queue.finish(db,news,0,now=7)
        resumed=queue.claim(db,'data',manifest,now=8)
        assert resumed['key']=='cron' and resumed['cycle_id']==first['cycle_id']
        queue.finish(db,resumed,0,now=9)
        assert queue.claim(db,'data',manifest,now=10)['key']=='external'


def test_corrupted_plan_cannot_silently_remove_scopes(monkeypatch):
    checkpoint=MemoryCheckpoint()
    checkpoint.state={'kind':'portfolio_refresh_v1','cycle_id':checkpoint.cycle,
        'planning_pending':False,'as_of_date':'2026-10-14','methodology_version':'test',
        'benchmark':'SPY','replace_existing':True,'lookbacks':[30],'members':1,
        'scopes':[{'member':{'entity_id':'A'},'lookback_days':30}], 'cursor':0,
        'failed':[], 'completed_by_lookback':{'30':0}, 'deadline':9999999999}
    checkpoint.state['plan_sha256']=batch._plan_digest(checkpoint.state)
    checkpoint.state['scopes']=[]
    with pytest.raises(RuntimeError,match='integrity'):
        batch.run(checkpoint=checkpoint)


@pytest.mark.skipif(sys.platform != 'linux', reason='Real forked worker lifecycle')
def test_real_worker_yields_to_news_and_finishes_all_portfolio_scopes(tmp_path):
    import subprocess
    import time
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from app.db import Base
    from test_replicated_portfolios import _add_member, _add_congress_portfolio_fixture

    data_path=tmp_path/'product.sqlite3';engine=create_engine('sqlite:///'+str(data_path))
    Base.metadata.create_all(engine);factory=sessionmaker(bind=engine)
    with factory() as db:
        for n in range(2):
            _add_member(db,f'M_LINUX_{n}')
            _add_congress_portfolio_fixture(db,member_id=f'M_LINUX_{n}',event_id=99100+n)
        db.commit()
    engine.dispose()
    queue_path=tmp_path/'queue';script_root=Path(__file__).resolve().parents[1]/'scripts'
    backend=script_root.parent
    manifest={'portfolio':DEFINITION, 'news':{'command':sys.executable+' -c "pass"','environment':{},'lane':'data'}}
    with queue.database(queue_path) as db:
        queue.enqueue(db,'portfolio',DEFINITION,now=time.time())
        queue.enqueue(db,'news',manifest['news'],now=time.time()+1)
    code='''import os,sys,json,functools,socket
from types import SimpleNamespace
import scheduled_jobs as j,preloaded_jobs as w
socket.socket.connect=lambda *a,**k:(_ for _ in ()).throw(AssertionError('Network prohibited'))
import app.background_job_guard as guard
guard.check_background_job_guard=lambda *_:SimpleNamespace(proceed=True)
w._fork_job=functools.partial(w._fork_job,working_directory=sys.argv[2])
j.worker(sys.argv[1],json.loads(sys.argv[3]),'data',idle=.02)
'''
    env={**os.environ,'PYTHONPATH':os.pathsep.join([str(script_root),str(backend)]),
        'DATABASE_URL':'sqlite:///'+str(data_path),'SCHEDULED_JOB_PRELOAD_ENABLED':'1',
        'SCHEDULED_JOB_IDENTITY_DIRECTORY':str(tmp_path/'identities'),
        'PORTFOLIO_REFRESH_LOOKBACKS':'365,30,90,180,1095','PORTFOLIO_REFRESH_BATCH_SIZE':'400',
        'PORTFOLIO_REFRESH_REPLACE_EXISTING':'1'}
    output=tmp_path/'worker.log'
    with output.open('w') as stream:
        process=subprocess.Popen([sys.executable,'-c',code,str(queue_path),str(backend),json.dumps(manifest)],
            env=env,stdout=stream,stderr=stream)
        try:
            deadline=time.monotonic()+40
            while time.monotonic()<deadline:
                lines=output.read_text().splitlines()
                records=[json.loads(line) for line in lines if line.startswith('{')]
                terminal=[r for r in records if r.get('event')=='scheduled_job_finished' and r.get('key')=='portfolio']
                if terminal:break
                time.sleep(.05)
            assert terminal,output.read_text()
            assert terminal[-1]['exit_code']==0,output.read_text()
            events=[r for r in records if r.get('event') in {'scheduled_job_yielded','scheduled_job_finished'}]
            first_yield=next(i for i,r in enumerate(events) if r['event']=='scheduled_job_yielded')
            news=next(i for i,r in enumerate(events) if r['key']=='news')
            finish=next(i for i,r in enumerate(events) if r['key']=='portfolio' and r['event']=='scheduled_job_finished')
            assert first_yield<news<finish
            assert len({r['cycle_id'] for r in events if r['key']=='portfolio'})==1
            with queue.database(queue_path) as db:
                row=db.execute("SELECT * FROM jobs WHERE key='portfolio'").fetchone()
                state=json.loads(row['checkpoint_json'])
                assert row['runs']==1 and row['continuation_at'] is None and row['started_at'] is None
                assert state['cursor']==10 and not state['failed']
                assert state['completed_by_lookback']=={str(n):2 for n in [365,30,90,180,1095]}
        finally:
            if process.poll() is None:process.terminate()
            process.wait(timeout=15)
