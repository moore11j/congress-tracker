"""A slow refresh must preserve useful work and every remaining symbol."""
from contextlib import nullcontext
from datetime import date
from types import SimpleNamespace

from app import ingest_run


def test_slow_prices_cannot_consume_every_snapshot_opportunity(monkeypatch):
    elapsed = [0.0]
    captured = []
    monkeypatch.setenv('OUTCOME_LEDGER_HYDRATOR_ENABLED', 'true')
    monkeypatch.setenv('OUTCOME_LEDGER_HYDRATOR_MAX_SECONDS', '1')
    monkeypatch.setenv('OUTCOME_LEDGER_HYDRATOR_REFRESH_PRICES', 'true')
    monkeypatch.setattr(ingest_run, 'check_background_job_guard', lambda *_: SimpleNamespace(proceed=True))
    monkeypatch.setattr(ingest_run, 'ensure_price_cache_volume_columns', lambda *_: None)
    monkeypatch.setattr(ingest_run, 'ensure_outcome_ledger_schema', lambda *_: None)
    monkeypatch.setattr(ingest_run, 'SessionLocal', lambda: nullcontext(SimpleNamespace(rollback=lambda: None)))
    monkeypatch.setattr(ingest_run, 'outcome_ledger_enabled', lambda *_: True)
    monkeypatch.setattr(ingest_run, '_outcome_ledger_hydrator_symbols', lambda *a, **k: ['AAA', 'BBB', 'CCC'])
    monkeypatch.setattr(ingest_run, 'get_expected_latest_market_date', lambda: date(2026,10,15))
    monkeypatch.setattr(ingest_run.time, 'monotonic', lambda: elapsed[0])
    def refresh(*a, **k):
        elapsed[0] += 2
        return {'is_stale': False}
    monkeypatch.setattr(ingest_run, 'ensure_fresh_price_history', refresh)
    monkeypatch.setattr(ingest_run, 'get_confirmation_score_bundles_for_tickers',
        lambda db, symbols, **k: {s: {'score': 70, 'active_sources': ['insiders']} for s in symbols})
    monkeypatch.setattr(ingest_run, 'confirmation_active_source_count', lambda b: 1)
    def capture(db, symbol, bundle):
        captured.append(symbol)
        return SimpleNamespace(id=len(captured))
    monkeypatch.setattr(ingest_run, 'capture_live_confirmation_score_snapshot', capture)
    result=ingest_run._run_outcome_ledger_hydrator_job()
    assert captured, 'A completed refresh must get its capture opportunity before yielding'
    assert result['status'] != 'ok' or result['evaluated'] == result['symbol_count']


from copy import deepcopy
from pathlib import Path
import sys
import pytest
from app.jobs import hydrate_outcomes_batch as batch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
import scheduled_jobs as queue


class MemoryCheckpoint:
    cycle = 'a'*32
    def __init__(self): self.state = None
    def read(self): return deepcopy(self.state)
    def save(self, state): self.state = deepcopy(state)


def compute_fixture(monkeypatch, count=21):
    monkeypatch.setenv('OUTCOME_LEDGER_HYDRATOR_ENABLED', 'true')
    monkeypatch.setenv('OUTCOME_LEDGER_HYDRATOR_REFRESH_PRICES', 'true')
    monkeypatch.setenv('OUTCOME_LEDGER_HYDRATOR_SYMBOL_LIMIT', str(count))
    symbols = ['S'+str(i) for i in range(count)]
    seen, requests, groups = [], [], []
    db = SimpleNamespace(rollback=lambda: None)
    def prices(db, symbol, **kwargs):
        requests.append((symbol, kwargs))
        return {'is_stale': False}
    def bundles(db, symbols, **kwargs):
        groups.append(list(symbols))
        return {s: {'score': 70, 'active_sources': ['insiders']} for s in symbols}
    def capture(db, symbol, bundle):
        seen.append((symbol, deepcopy(bundle)))
        return SimpleNamespace(id=len(seen))
    compute = SimpleNamespace(engine=None, SessionLocal=lambda: nullcontext(db),
        check_background_job_guard=lambda *_: SimpleNamespace(proceed=True),
        ensure_price_cache_volume_columns=lambda *_: None, ensure_outcome_ledger_schema=lambda *_: None,
        outcome_ledger_enabled=lambda *_: True,
        _outcome_ledger_hydrator_symbols=lambda *a, **kw: list(symbols),
        get_expected_latest_market_date=lambda: date(2026,10,15),
        ensure_fresh_price_history=prices, get_confirmation_score_bundles_for_tickers=bundles,
        confirmation_active_source_count=lambda b: len(b['active_sources']),
        capture_live_confirmation_score_snapshot=capture)
    return compute, seen, requests, groups


def run(compute, checkpoint, **kwargs):
    return batch.run(compute=compute, checkpoint=checkpoint, clock=lambda: 100,
                     wall_clock=lambda: 0, cpu_clock=lambda: 0, **kwargs)


def test_all_1000_symbols_finish_in_bounded_batches_without_replanning(monkeypatch):
    compute, seen, requests, groups = compute_fixture(monkeypatch, 1000)
    checkpoint = MemoryCheckpoint()
    result = run(compute, checkpoint)
    assert result['status'] == 'yielded' and result['remaining'] == 992
    compute._outcome_ledger_hydrator_symbols = lambda *a, **kw: pytest.fail('replanned')
    monkeypatch.setenv('OUTCOME_LEDGER_HYDRATOR_MIN_SCORE', '99')
    while result['status'] == 'yielded':
        result = run(compute, checkpoint)
    assert result['status'] == 'ok'
    assert result['evaluated'] == result['captured'] == result['attempted'] == 1000
    assert [s for s, _ in seen] == ['S'+str(i) for i in range(1000)]
    assert len(requests) == 1000 and len(groups) == 125 and max(map(len, groups)) == 8
    original = deepcopy(seen)
    assert run(compute, checkpoint)['processed_this_batch'] == 0
    assert seen == original


def test_slow_provider_still_captures_then_yields_after_one_symbol(monkeypatch):
    compute, seen, requests, groups = compute_fixture(monkeypatch)
    elapsed = [0.0]
    original = compute.ensure_fresh_price_history
    def slow(*a, **kw):
        elapsed[0] += 30
        return original(*a, **kw)
    compute.ensure_fresh_price_history = slow
    state = MemoryCheckpoint()
    result = batch.run(compute=compute, checkpoint=state, clock=lambda: 100,
        wall_clock=lambda: elapsed[0], cpu_clock=lambda: 0)
    assert result['status'] == 'yielded' and result['captured'] == 1 and result['remaining'] == 20
    assert len(requests) == 1 and groups == [['S0']]


def test_guard_hold_and_expiry_never_claim_completion(monkeypatch):
    compute, seen, _, _ = compute_fixture(monkeypatch)
    compute.check_background_job_guard = lambda *_: SimpleNamespace(proceed=False)
    state = MemoryCheckpoint()
    first = run(compute, state)
    assert first['status'] == 'yielded' and first['planning_pending']
    assert state.state['retry_after_seconds'] == 60 and not seen
    expired = batch.run(compute=compute, checkpoint=state, clock=lambda: 100+24*3600)
    assert expired['status'] == 'failed' and not state.state['continuation']


@pytest.mark.parametrize('failure', ['price_stale', 'price_exception', 'missing_bundle', 'capture_none', 'capture_exception'])
def test_errors_are_explicit_and_do_not_drop_remaining_symbols(monkeypatch, failure):
    compute, seen, _, _ = compute_fixture(monkeypatch, 2)
    if failure == 'price_stale': compute.ensure_fresh_price_history = lambda *a, **kw: {'is_stale': True}
    if failure == 'price_exception':
        def broken(*a, **kw): raise RuntimeError('provider failed')
        compute.ensure_fresh_price_history = broken
    if failure == 'missing_bundle': compute.get_confirmation_score_bundles_for_tickers = lambda *a, **kw: {}
    if failure == 'capture_none': compute.capture_live_confirmation_score_snapshot = lambda *a, **kw: None
    if failure == 'capture_exception':
        def broken(*a, **kw): raise RuntimeError('capture failed')
        compute.capture_live_confirmation_score_snapshot = broken
    state = MemoryCheckpoint()
    result = run(compute, state)
    assert result['status'] == 'failed' and result['attempted'] == 2 and result['remaining'] == 0
    assert result['failed'] + result['price_failures'] == 2


def test_low_signal_is_a_completed_evaluation_not_failure(monkeypatch):
    compute, seen, _, _ = compute_fixture(monkeypatch, 3)
    compute.get_confirmation_score_bundles_for_tickers = lambda *a, **kw: {
        'S0': {'score': 39, 'active_sources': ['insiders']},
        'S1': {'score': 70, 'active_sources': []},
        'S2': {'score': 40, 'active_sources': ['insiders']}}
    result = run(compute, MemoryCheckpoint())
    assert result['status'] == 'ok' and result['skipped_low_signal'] == 2 and result['captured'] == 1
    assert [s for s, _ in seen] == ['S2']


@pytest.mark.parametrize('change', ['hash', 'cursor', 'methodology', 'market_date', 'disabled'])
def test_changed_plan_or_market_day_cannot_silently_resume(monkeypatch, change):
    compute, seen, _, _ = compute_fixture(monkeypatch)
    state = MemoryCheckpoint()
    run(compute, state)
    original = deepcopy(seen)
    if change == 'hash': state.state['symbols'][0] = 'WRONG'
    if change == 'cursor': state.state['cursor'] = -1
    if change == 'methodology': state.state['scoring_version'] = 'old'
    if change == 'market_date': compute.get_expected_latest_market_date = lambda: date(2026,10,16)
    if change == 'disabled': monkeypatch.setenv('OUTCOME_LEDGER_HYDRATOR_ENABLED', 'false')
    if change in {'hash', 'cursor', 'methodology'}:
        with pytest.raises(RuntimeError, match='integrity'): run(compute, state)
    else:
        assert run(compute, state)['status'] == 'failed'
        assert not state.state['continuation']
    assert seen == original


def test_queue_continuation_is_durable_fair_and_keeps_next_scheduled_trigger(monkeypatch, tmp_path):
    compute, seen, _, _ = compute_fixture(monkeypatch, 17)
    path = tmp_path/'queue'
    definition = {'command': queue.OUTCOME_COMMAND, 'lane': 'data', 'environment': {}}
    manifest = {'outcomes': definition, 'sec': {'command': 'true', 'lane': 'data'}}
    def checkpoint(job):
        return batch.OutcomeCheckpoint({'WALNUT_JOB_QUEUE': str(path), 'WALNUT_JOB_KEY': job['key'],
            'WALNUT_JOB_TOKEN': job['token'], 'WALNUT_JOB_CYCLE': job['cycle_id']})
    with queue.database(path) as db:
        queue.enqueue(db, 'outcomes', definition, now=1)
        job = queue.claim(db, 'data', manifest, now=2)
        result = run(compute, checkpoint(job))
        queue.enqueue(db, 'sec', manifest['sec'], now=3)
        queue.enqueue(db, 'outcomes', definition, now=4)
        assert queue.continue_checkpointed_job(db, job, definition, 75, now=5)
        assert db.execute("SELECT runs FROM jobs WHERE key='outcomes'").fetchone()[0] == 0
        assert queue.reserve_release(db, '123-1', manifest, now=6)['status'] == 'blocked'
    with queue.database(path) as db:
        sec = queue.claim(db, 'data', manifest, now=7)
        assert sec['key'] == 'sec'
        queue.finish(db, sec, 0, now=8)
        for now in (9, 12):
            resumed = queue.claim(db, 'data', manifest, now=now)
            assert resumed['cycle_id'] == job['cycle_id'] and resumed['token'] != job['token']
            result = run(compute, checkpoint(resumed))
            if result['status'] == 'yielded':
                assert queue.continue_checkpointed_job(db, resumed, definition, 75, now=now+1)
            else:
                queue.finish(db, resumed, 0, now=now+1)
        assert result['status'] == 'ok' and len(seen) == 17
        next_job = queue.claim(db, 'data', manifest, now=15)
        assert next_job['pending_at'] == 4 and next_job['cycle_id'] != job['cycle_id']
        assert checkpoint(next_job).read() is None


@pytest.mark.parametrize('wrong', ['kind', 'cycle', 'token', 'deadline', 'lane', 'command', 'exit'])
def test_queue_rejects_unowned_invalid_or_unapproved_continuation(tmp_path, wrong):
    path = tmp_path/'queue'
    definition = {'command': queue.OUTCOME_COMMAND, 'lane': 'data', 'environment': {}}
    with queue.database(path) as db:
        queue.enqueue(db, 'o', definition, now=1)
        job = queue.claim(db, 'data', {'o': definition}, now=2)
        state = {'kind': batch.KIND, 'cycle_id': job['cycle_id'], 'continuation': True, 'deadline': 100}
        checkpoint = batch.OutcomeCheckpoint({'WALNUT_JOB_QUEUE': str(path), 'WALNUT_JOB_KEY': 'o',
            'WALNUT_JOB_TOKEN': job['token'], 'WALNUT_JOB_CYCLE': job['cycle_id']})
        checkpoint.save(state)
        if wrong in {'kind','cycle','deadline'}:
            import json
            state[{'kind':'kind','cycle':'cycle_id','deadline':'deadline'}[wrong]] = {'kind':'portfolio_refresh_v1','cycle':'b'*32,'deadline':3}[wrong]
            db.execute('UPDATE jobs SET checkpoint_json=? WHERE key=?', (json.dumps(state),'o'))
        if wrong == 'token': job = {**job, 'token': 'b'*32}
        if wrong == 'lane': definition = {**definition, 'lane': 'delivery'}
        if wrong == 'command': definition = {**definition, 'command': 'unreviewed'}
        assert not queue.continue_checkpointed_job(db, job, definition, 0 if wrong == 'exit' else 75, now=5)


def test_existing_outcome_triggers_share_one_approved_queue_identity(tmp_path):
    cron = (Path(__file__).resolve().parents[1]/'crontab').read_text()
    compiled, manifest = queue.compile_schedule(cron, 'scheduled_jobs.py', tmp_path/'manifest', tmp_path/'queue')
    outcomes = [key for key, value in manifest.items() if value['command'] == queue.OUTCOME_COMMAND]
    assert len(outcomes) == 1
    assert compiled.count('enqueue ' + outcomes[0]) == 2
    assert manifest[outcomes[0]]['lane'] == 'data'
    assert queue.CHECKPOINT_COMMANDS[queue.OUTCOME_COMMAND] == batch.KIND


def test_real_snapshot_capture_matches_original_sequence_and_deduplicates_repeat(monkeypatch):
    from datetime import datetime, timezone
    from sqlalchemy import create_engine, select, func
    from sqlalchemy.orm import sessionmaker
    from app.db import Base
    from app.models import ConfirmationScoreSnapshot, EmailDelivery, Event, PriceCache
    from app.services import outcome_ledger
    from app.services.confirmation_score import CONFIRMATION_SCORING_VERSION
    from test_outcome_ledger import _bundle
    import requests

    network_calls=[]
    def deny(*a, **kw):
        network_calls.append(True)
        raise AssertionError('No network allowed in parity replay')
    monkeypatch.setattr(requests.sessions.Session, 'request', deny)
    monkeypatch.setenv('FMP_PROVIDER_DISABLED', 'true')
    fixed = datetime(2026,10,15,20,tzinfo=timezone.utc)
    symbols = ['S'+str(i) for i in range(9)]
    bundles = {s: {**_bundle(60+i, 'bullish' if i%2 else 'bearish'),
                   'ticker':s, 'scoring_version':CONFIRMATION_SCORING_VERSION} for i,s in enumerate(symbols)}
    def create():
        engine = create_engine('sqlite:///:memory:')
        Base.metadata.create_all(engine)
        factory = sessionmaker(bind=engine)
        with factory() as db:
            db.add_all(PriceCache(symbol=s, date='2026-10-15', close=100+i, price_source='test') for i,s in enumerate(symbols))
            db.commit()
        return engine, factory
    def capture(db, symbol, bundle):
        return outcome_ledger.capture_live_confirmation_score_snapshot(db, symbol, bundle, calculated_at=fixed)
    def rows(factory):
        with factory() as db:
            values = db.execute(select(ConfirmationScoreSnapshot).order_by(ConfirmationScoreSnapshot.id)).scalars().all()
            snapshots = [{c.name:getattr(row,c.name) for c in row.__table__.columns if c.name != 'created_at'} for row in values]
            assert db.scalar(select(func.count()).select_from(EmailDelivery)) == 0
            assert db.scalar(select(func.count()).select_from(Event)) == 0
            return snapshots
    original_engine, original = create()
    new_engine, new = create()
    try:
        with original() as db:
            for s in symbols:
                assert capture(db, s, bundles[s]) is not None
        expected = rows(original)
        compute, _, _, _ = compute_fixture(monkeypatch, 9)
        compute.engine = new_engine
        compute.SessionLocal = new
        compute.get_confirmation_score_bundles_for_tickers = lambda db, syms, **kw: {s:deepcopy(bundles[s]) for s in syms}
        compute.capture_live_confirmation_score_snapshot = capture
        checkpoint = MemoryCheckpoint()
        first = run(compute, checkpoint)
        assert first['status'] == 'yielded' and first['captured'] == 8
        assert run(compute, checkpoint)['status'] == 'ok'
        assert rows(new) == expected and len(expected) == 9
        repeat = MemoryCheckpoint()
        while run(compute, repeat)['status'] == 'yielded': pass
        assert rows(new) == expected
        assert not network_calls
    finally:
        original_engine.dispose()
        new_engine.dispose()


def test_manual_partial_result_is_nonzero_without_changing_other_jobs():
    assert ingest_run._payload_exit_code({'job':'outcome-ledger-hydrator', 'status':'partial'}) == 1
    assert ingest_run._payload_exit_code({'job':'daily-repair', 'status':'partial'}) == 0


def test_preload_routes_exact_outcome_command_only():
    import preloaded_jobs
    assert preloaded_jobs.selected_module(queue.OUTCOME_COMMAND) == 'app.jobs.hydrate_outcomes_batch'
    assert preloaded_jobs.selected_module(queue.OUTCOME_COMMAND+' --extra') is None
    assert preloaded_jobs.selected_module(queue.OUTCOME_COMMAND+'; echo extra') is None


def test_outcome_continuation_flag_does_not_invalidate_existing_import_template(monkeypatch):
    import preloaded_jobs
    monkeypatch.setattr(preloaded_jobs, '_preload_failed', False)
    monkeypatch.setattr(preloaded_jobs, '_template_environment', {'DATABASE_URL':'sqlite:///:memory:'})
    assert preloaded_jobs.preload({'DATABASE_URL':'sqlite:///:memory:', 'WALNUT_OUTCOME_CONTINUATION':'1'})
    assert preloaded_jobs.preload({'DATABASE_URL':'sqlite:///:memory:', 'WALNUT_OUTCOME_CONTINUATION':'0'})
    assert not preloaded_jobs.preload({'DATABASE_URL':'sqlite:///different.db', 'WALNUT_OUTCOME_CONTINUATION':'1'})
