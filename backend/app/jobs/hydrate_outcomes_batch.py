"""Capture every planned symbol across fair, durable scheduler batches."""
from __future__ import annotations

from copy import deepcopy
from datetime import date
import hashlib
import json
import os
import time
import uuid
from types import SimpleNamespace

from app.services.portfolio_refresh_checkpoint import PortfolioCheckpoint

BATCH_SYMBOL_LIMIT = 8
BATCH_WALL_SECONDS = 8.0
BATCH_CPU_SECONDS = 2.0
MAX_SYMBOLS = 10_000
KIND = 'outcome_hydration_v1'


class OutcomeCheckpoint(PortfolioCheckpoint):
    kind = KIND


class _ManualCheckpoint:
    # Explicit CLI runs cannot claim durable continuation without a scheduler.
    def __init__(self):
        self.cycle = uuid.uuid4().hex
        self.state = None
    def read(self):
        return deepcopy(self.state)
    def save(self, state):
        self.state = deepcopy(state)


def _digest(state):
    fields = {k: state[k] for k in ('symbols', 'expected_date', 'lookback_days',
              'price_lookback_days', 'refresh_prices', 'min_score', 'min_active_sources', 'scoring_version')}
    return hashlib.sha256(json.dumps(fields, sort_keys=True).encode()).hexdigest()


def _receipt(state, status, processed=0):
    return {'job': 'outcome-ledger-hydrator', 'status': status,
            'symbol_count': len(state.get('symbols', [])),
            'attempted': state.get('cursor', 0),
            'remaining': len(state.get('symbols', [])) - state.get('cursor', 0),
            **{k: state.get(k, 0) for k in ('evaluated', 'captured', 'skipped_low_signal', 'failed', 'price_failures')},
            'planning_pending': state.get('planning_pending', False),
            'processed_this_batch': processed, 'terminal_reason': state.get('terminal_reason')}


def _compute():
    # Continuing batches do not import the unrelated ingest command graph.
    from app import db as storage
    from app.background_job_guard import check_background_job_guard
    from app.services import price_lookup, confirmation_score, outcome_ledger
    def symbols(*args, **kwargs):
        from app.ingest_run import _outcome_ledger_hydrator_symbols
        return _outcome_ledger_hydrator_symbols(*args, **kwargs)
    return SimpleNamespace(engine=storage.engine, SessionLocal=storage.SessionLocal,
        ensure_price_cache_volume_columns=storage.ensure_price_cache_volume_columns,
        ensure_outcome_ledger_schema=storage.ensure_outcome_ledger_schema,
        check_background_job_guard=check_background_job_guard,
        outcome_ledger_enabled=outcome_ledger.outcome_ledger_enabled,
        _outcome_ledger_hydrator_symbols=symbols,
        get_expected_latest_market_date=price_lookup.get_expected_latest_market_date,
        ensure_fresh_price_history=price_lookup.ensure_fresh_price_history,
        get_confirmation_score_bundles_for_tickers=confirmation_score.get_confirmation_score_bundles_for_tickers,
        confirmation_active_source_count=confirmation_score.confirmation_active_source_count,
        capture_live_confirmation_score_snapshot=outcome_ledger.capture_live_confirmation_score_snapshot)


def run(*, compute=None, checkpoint=None, clock=None, wall_clock=None, cpu_clock=None):
    clock, wall_clock, cpu_clock = clock or time.time, wall_clock or time.monotonic, cpu_clock or time.process_time
    scheduled = checkpoint is not None or os.getenv('WALNUT_OUTCOME_CONTINUATION') == '1'
    checkpoint = checkpoint or (OutcomeCheckpoint() if scheduled else _ManualCheckpoint())
    state = checkpoint.read()
    now = clock()
    enabled = os.getenv('OUTCOME_LEDGER_HYDRATOR_ENABLED', 'false').strip().lower() in {'1', 'true', 'yes', 'on'}
    if not enabled:
        if state is not None:
            state.update(continuation=False, terminal_reason='hydrator_disabled_during_cycle')
            checkpoint.save(state)
            return _receipt(state, 'failed')
        return {'job': 'outcome-ledger-hydrator', 'status': 'skipped', 'reason': 'hydrator_disabled'}
    if state is None:
        state = {'kind': KIND, 'cycle_id': checkpoint.cycle, 'created_at': now,
                 'deadline': now + 24*3600, 'planning_pending': True,
                 'continuation': True, 'retry_after_seconds': 1}
        checkpoint.save(state)
    from app.services.confirmation_score import CONFIRMATION_SCORING_VERSION
    if not state['planning_pending']:
        if (state.get('plan_sha256') != _digest(state) or type(state.get('cursor')) is not int
                or not 0 <= state['cursor'] <= len(state['symbols']) <= MAX_SYMBOLS
                or state['scoring_version'] != CONFIRMATION_SCORING_VERSION):
            raise RuntimeError('Outcome hydration saved plan failed integrity checks')
        if state['cursor'] == len(state['symbols']):
            return _receipt(state, 'failed' if state['failed'] or state['price_failures'] else 'ok')
    if now >= state['deadline']:
        state.update(continuation=False, terminal_reason='hydration_deadline_exceeded')
        checkpoint.save(state)
        return _receipt(state, 'failed')
    compute = compute or _compute()
    guard = compute.check_background_job_guard('outcome-ledger-hydrator')
    if not guard.proceed:
        state.update(continuation=scheduled, retry_after_seconds=60)
        checkpoint.save(state)
        return _receipt(state, 'yielded' if scheduled else 'partial')
    wall_start, cpu_start = wall_clock(), cpu_clock()
    wall_budget = max(1, int(os.getenv('OUTCOME_LEDGER_HYDRATOR_MAX_SECONDS', '240') or 240))
    if scheduled:
        wall_budget = min(wall_budget, BATCH_WALL_SECONDS)
    def exhausted():
        return wall_clock() - wall_start >= wall_budget or (scheduled and cpu_clock() - cpu_start >= BATCH_CPU_SECONDS)
    with compute.SessionLocal() as db:
        if state['planning_pending']:
            compute.ensure_price_cache_volume_columns(compute.engine)
            compute.ensure_outcome_ledger_schema(compute.engine)
            if not compute.outcome_ledger_enabled(db):
                state.update(continuation=False, terminal_reason='outcome_ledger_disabled')
                checkpoint.save(state)
                return _receipt(state, 'failed')
            limit = max(1, int(os.getenv('OUTCOME_LEDGER_HYDRATOR_SYMBOL_LIMIT', '75') or 75))
            if limit > MAX_SYMBOLS:
                raise ValueError('Outcome hydration plan exceeds its guard; nothing was truncated')
            lookback = max(1, int(os.getenv('OUTCOME_LEDGER_HYDRATOR_EVENT_LOOKBACK_DAYS', '30') or 30))
            symbols = compute._outcome_ledger_hydrator_symbols(db, limit=limit, lookback_days=lookback)
            if len(symbols) > MAX_SYMBOLS or len(symbols) != len(set(symbols)):
                raise ValueError('Invalid outcome hydration plan; nothing was truncated')
            state.update(planning_pending=False, symbols=symbols, cursor=0, evaluated=0, captured=0,
                         skipped_low_signal=0, failed=0, price_failures=0,
                         expected_date=compute.get_expected_latest_market_date().isoformat(), lookback_days=lookback,
                         price_lookback_days=max(1, int(os.getenv('OUTCOME_LEDGER_HYDRATOR_PRICE_LOOKBACK_DAYS', '10') or 10)),
                         min_score=int(os.getenv('OUTCOME_LEDGER_HYDRATOR_MIN_SCORE', '40') or 40),
                         min_active_sources=int(os.getenv('OUTCOME_LEDGER_HYDRATOR_MIN_ACTIVE_SOURCES', '1') or 1),
                         refresh_prices=os.getenv('OUTCOME_LEDGER_HYDRATOR_REFRESH_PRICES', 'true').strip().lower() in {'1','true','yes','on'},
                         scoring_version=CONFIRMATION_SCORING_VERSION)
            state['plan_sha256'] = _digest(state)
            checkpoint.save(state)
        if date.fromisoformat(state['expected_date']) != compute.get_expected_latest_market_date():
            state.update(continuation=False, terminal_reason='market_date_changed')
            checkpoint.save(state)
            return _receipt(state, 'failed')
        processed = 0
        while state['cursor'] < len(state['symbols']):
            ready = []
            for symbol in state['symbols'][state['cursor']:state['cursor'] + BATCH_SYMBOL_LIMIT]:
                price_failed = False
                if state['refresh_prices']:
                    try:
                        price = compute.ensure_fresh_price_history(db, symbol,
                            expected_date=date.fromisoformat(state['expected_date']), lookback_days=state['price_lookback_days'])
                        price_failed = bool(price.get('is_stale'))
                    except Exception:
                        db.rollback()
                        price_failed = True
                ready.append((symbol, price_failed))
                if exhausted():
                    break
            # Finish the bounded group we refreshed even when HTTP used its budget.
            # Never compute the whole remaining population and then discard it.
            bundles = compute.get_confirmation_score_bundles_for_tickers(db, [s for s, _ in ready], lookback_days=state['lookback_days'])
            for symbol, price_failed in ready:
                state['price_failures'] += int(price_failed)
                bundle = bundles.get(symbol)
                if not isinstance(bundle, dict):
                    state['failed'] += 1
                else:
                    state['evaluated'] += 1
                    if (int(bundle.get('score') or 0) < state['min_score']
                            or compute.confirmation_active_source_count(bundle) < state['min_active_sources']):
                        state['skipped_low_signal'] += 1
                    else:
                        try:
                            snapshot = compute.capture_live_confirmation_score_snapshot(db, symbol, bundle)
                        except Exception:
                            db.rollback()
                            snapshot = None
                        state['captured' if snapshot is not None else 'failed'] += 1
                state['cursor'] += 1
                processed += 1
                state.update(continuation=state['cursor'] < len(state['symbols']), retry_after_seconds=1)
                checkpoint.save(state)
            if scheduled or exhausted():
                break
    if state['cursor'] < len(state['symbols']):
        return _receipt(state, 'yielded' if scheduled else 'partial', processed)
    state['continuation'] = False
    checkpoint.save(state)
    return _receipt(state, 'failed' if state['failed'] or state['price_failures'] else 'ok', processed)


if __name__ == '__main__':
    result = run()
    print(json.dumps(result, sort_keys=True), flush=True)
    raise SystemExit(75 if result['status'] == 'yielded' else 1 if result['status'] in {'failed', 'partial'} else 0)
