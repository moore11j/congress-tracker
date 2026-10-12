"""Cheap, read-only early exit for an already cooling-down reference job.

Uncertain, expired or unavailable state falls through to the existing worker.
No planner state, source attempt timestamp or scheduler request is changed.
"""
from datetime import datetime, timedelta, timezone
import os
import json
from app.jobs.reference_schedule import exhausted_plan


def read_state():
    dsn = os.getenv('DATABASE_URL', '')
    if dsn.startswith('postgresql+psycopg://'):
        dsn = dsn.replace('postgresql+psycopg://', 'postgresql://', 1)
    if not dsn.startswith(('postgres://', 'postgresql://')):
        return None
    # Import only the driver, not SQLAlchemy or the application model registry.
    import psycopg
    with psycopg.connect(dsn, connect_timeout=2,
            application_name=f'walnut:reference-preflight:{os.getpid()}',
            options='-c default_transaction_read_only=on -c statement_timeout=1500 -c lock_timeout=500') as conn:
        with conn.cursor() as cursor:
            cursor.execute("""SELECT payload_json::jsonb->>'cooldown_until',
                payload_json::jsonb->>'last_started_at',
                jsonb_build_object('universe',payload_json::jsonb->'universe',
                    'universe_prepared_at',payload_json::jsonb->'universe_prepared_at',
                    'attempted_at',payload_json::jsonb->'attempted_at')
                FROM insights_snapshots WHERE kind=%s""", ('institutional-reference:warming:v1',))
            return cursor.fetchone()


def early_result(*, now=None):
    if (os.getenv('INSTITUTIONAL_REFERENCE_WARMING_ENABLED', '0') != '1'
            or os.getenv('DIRECT_FEEDS_MODE', 'off') != 'shadow'):
        return {'status': 'disabled'}
    try:
        state = read_state()
        if state is None:
            return None
        now = now or datetime.now(timezone.utc)
        until, last = state[:2]
        if until and now < datetime.fromisoformat(until):
            return {'status': 'cooldown', 'until': until}
        if last and now - datetime.fromisoformat(last) < timedelta(seconds=60):
            return {'status': 'cooldown'}
        if len(state) == 3:
            plan = json.loads(state[2]) if isinstance(state[2], str) else state[2]
            return exhausted_plan(plan, now)
    except Exception:
        # Do not turn a failed preflight into lost work or log connection details.
        return None
    return None
