"""Finish the existing daily portfolio plan in fair, durable scheduler batches."""
from __future__ import annotations

from datetime import date, datetime, timezone
import hashlib
import json
import os
import time

from app.services.portfolio_refresh_checkpoint import PortfolioCheckpoint

DEFAULT_LOOKBACKS = [365, 30, 90, 180, 1095]
BATCH_SCOPE_LIMIT = 8
BATCH_CPU_SECONDS = 2.0
BATCH_WALL_SECONDS = 8.0
MAX_PLANNED_SCOPES = 10_000


def _plan(state, compute):
    raw = os.getenv('PORTFOLIO_REFRESH_LOOKBACKS', '')
    lookbacks = [int(v.strip()) for v in raw.split(',') if v.strip()] or list(DEFAULT_LOOKBACKS)
    if any(value not in compute.ALL_CONGRESS_SUPPORTED_LOOKBACK_DAYS for value in lookbacks):
        raise ValueError('Unsupported portfolio refresh lookback')
    batch_size = max(1, int(os.getenv('PORTFOLIO_REFRESH_BATCH_SIZE', '400') or 400))
    offset = max(0, int(os.getenv('PORTFOLIO_REFRESH_BATCH_OFFSET', '0') or 0))
    batches = max(1, int(os.getenv('PORTFOLIO_REFRESH_MAX_BATCHES', '1') or 1))
    with compute.SessionLocal() as db:
        candidates = compute._all_congress_member_candidates(db)
        selected = candidates[offset:offset + batch_size * batches]
    if len(selected) * len(lookbacks) > MAX_PLANNED_SCOPES:
        raise ValueError('Portfolio plan exceeds its scope guard; nothing was truncated')
    return {**state, 'planning_pending': False, 'methodology_version': compute.PORTFOLIO_METHODOLOGY_VERSION,
            'benchmark': compute.normalize_symbol(os.getenv('PORTFOLIO_REFRESH_BENCHMARK', 'SPY')) or 'SPY',
            'replace_existing': os.getenv('PORTFOLIO_REFRESH_REPLACE_EXISTING', '1').strip().lower() in {'1', 'true', 'yes', 'y', 'on'},
            'lookbacks': lookbacks, 'members': len(selected),
            'scopes': [{'member': member, 'lookback_days': days} for days in lookbacks for member in selected],
            'cursor': 0, 'failed': [], 'completed_by_lookback': {str(days): 0 for days in lookbacks}}


def _plan_digest(state):
    fields = {key: state[key] for key in ('as_of_date', 'methodology_version', 'benchmark',
              'replace_existing', 'lookbacks', 'members', 'scopes')}
    return hashlib.sha256(json.dumps(fields, sort_keys=True).encode()).hexdigest()


def _receipt(state, status, processed=0):
    return {'job': 'portfolio-simulation-refresh', 'status': status,
            'cycle_id': state['cycle_id'], 'as_of_date': state['as_of_date'],
            'methodology_version': state.get('methodology_version'), 'lookbacks': state.get('lookbacks', []),
            'terminal_reason': state.get('terminal_reason'),
            'planned_scopes': len(state.get('scopes', [])), 'attempted_scopes': state.get('cursor', 0), 'completed_scopes': sum(state.get('completed_by_lookback', {}).values()),
            'failed_scopes': len(state.get('failed', [])), 'processed_this_batch': processed,
            'completed_by_lookback': state.get('completed_by_lookback', {}),
            'planning_pending': state.get('planning_pending', False)}


def run(*, checkpoint=None, clock=time.time, cpu_clock=time.process_time, wall_clock=time.monotonic):
    checkpoint = checkpoint or PortfolioCheckpoint()
    state = checkpoint.read()
    now = clock()
    if state is None:
        state = {'kind': 'portfolio_refresh_v1', 'cycle_id': checkpoint.cycle,
                 'as_of_date': datetime.fromtimestamp(now, timezone.utc).date().isoformat(),
                 'created_at': now, 'deadline': now + 24 * 3600, 'planning_pending': True,
                 'continuation': True, 'retry_after_seconds': 1}
        checkpoint.save(state)
    if not state.get('planning_pending'):
        if (state.get('plan_sha256') != _plan_digest(state)
                or type(state.get('cursor')) is not int
                or not 0 <= state['cursor'] <= len(state['scopes']) <= MAX_PLANNED_SCOPES):
            raise RuntimeError('Portfolio saved plan failed integrity checks')
        if state['cursor'] == len(state['scopes']):
            return _receipt(state, 'failed' if state['failed'] else 'ok')
    if now >= state['deadline']:
        state.update(continuation=False, terminal_reason='refresh_deadline_exceeded')
        checkpoint.save(state)
        return _receipt(state, 'failed')
    from app.background_job_guard import check_background_job_guard
    guard = check_background_job_guard('portfolio-simulation-refresh')
    if not guard.proceed:
        state.update(continuation=True, retry_after_seconds=60)
        checkpoint.save(state)
        return _receipt(state, 'yielded')

    from app import compute_replicated_portfolios as compute
    if state.get('planning_pending'):
        # Check the schema once for the fixed plan, not for every yielded scope.
        compute.Base.metadata.create_all(bind=compute.engine)
        state = _plan(state, compute)
        state['plan_sha256'] = _plan_digest(state)
        checkpoint.save(state)
    if state['methodology_version'] != compute.PORTFOLIO_METHODOLOGY_VERSION:
        raise RuntimeError('Portfolio methodology changed during the saved plan')

    processed = 0
    cpu_start, wall_start = cpu_clock(), wall_clock()
    while state['cursor'] < len(state['scopes']):
        scope = state['scopes'][state['cursor']]
        report = compute.run_all_congress_portfolio_batch(batch_size=1, max_batches=1, dry_run=False,
            lookback_days=scope['lookback_days'], benchmark=state['benchmark'],
            replace_existing=state['replace_existing'], _planned_entities=[scope['member']],
            _as_of_date=date.fromisoformat(state['as_of_date']), _schema_ready=True)
        results = report.get('results') or []
        if len(results) != 1 or results[0].get('status') not in {'created', 'replaced', 'skipped_existing'}:
            state['failed'].append({'entity_id': scope['member']['entity_id'], 'lookback_days': scope['lookback_days']})
        else:
            key = str(scope['lookback_days'])
            state['completed_by_lookback'][key] += 1
        state['cursor'] += 1
        processed += 1
        # Persist only after this scope's own transaction completed. An interrupted
        # active job remains a visible scheduler failure; it is never blindly replayed.
        state.update(continuation=state['cursor'] < len(state['scopes']), retry_after_seconds=1)
        checkpoint.save(state)
        if (processed >= BATCH_SCOPE_LIMIT or cpu_clock() - cpu_start >= BATCH_CPU_SECONDS
                or wall_clock() - wall_start >= BATCH_WALL_SECONDS):
            break
    if state['cursor'] < len(state['scopes']):
        return _receipt(state, 'yielded', processed)
    state['continuation'] = False
    checkpoint.save(state)
    return _receipt(state, 'failed' if state['failed'] else 'ok', processed)


if __name__ == '__main__':
    result = run()
    print(json.dumps(result, sort_keys=True), flush=True)
    raise SystemExit(75 if result['status'] == 'yielded' else 1 if result['status'] == 'failed' else 0)
