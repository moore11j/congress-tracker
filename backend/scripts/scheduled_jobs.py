"""Local durable scheduling queue. Only source-controlled manifest commands execute.

Supercronic retains calendar/timezone semantics and enqueues lightweight requests.
One worker per lane bounds Python imports and database pools. Repeated requests
coalesce while preserving the oldest waiting time, including across restarts.
"""
from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import signal
import sqlite3
import subprocess
import sys
import time
import uuid


PORTFOLIO_COMMAND = 'cd /app && python -m app.ingest_run --job portfolio-simulation-refresh'
OUTCOME_COMMAND = 'cd /app && python -m app.ingest_run --job outcome-ledger-hydrator'
CHECKPOINT_COMMANDS = {PORTFOLIO_COMMAND: 'portfolio_refresh_v1', OUTCOME_COMMAND: 'outcome_hydration_v1'}


DELIVERY_COMMANDS = (
    'run_email_digest_schedule.sh', 'run_email_intraday_alert_sweep.sh',
    'run_watchlist_price_alerts.sh', 'app.jobs.queue_strategy_event_deliveries',
    'app.jobs.deliver_strategy_event_emails', '--job monitoring-alert-refresh',
)


# Manual maintenance has no calendar trigger. An operator explicitly enqueues
# this fixed command, and the existing data lane prevents overlap with imports.
MANUAL_JOBS = {
    'maintenance-insider-lookup-index-v1': {
        'command': 'cd /app && python -m app.jobs.migrate_insider_lookup_index --apply',
        'environment': {}, 'lane': 'data',
    },
    'maintenance-institutional-capacity-v1': {
        'command': 'cd /app && python -m app.jobs.check_direct_13f_capacity',
        'environment': {}, 'lane': 'data',
    },
    'maintenance-price-lookup-index-v1': {
        'command': 'cd /app && python -m app.jobs.migrate_price_lookup_index --apply',
        'environment': {},
        'lane': 'data',
    },
}


# Existing GitHub triggers use these fixed commands through the same data lane.
EXTERNAL_INGEST_JOBS = (
    'core', 'recent-congress', 'government-contracts-daily',
    'government-contracts-weekly', 'daily-repair', 'market-data-refresh-daily',
    'portfolio-simulation-refresh', 'portfolio-methodology-guard', 'all',
)
for _job in EXTERNAL_INGEST_JOBS:
    MANUAL_JOBS['external-ingest-' + _job] = {
        'command': 'cd /app && python -m app.ingest_run --job ' + _job,
        'environment': {}, 'lane': 'data',
    }


def load_manifest(path):
    return {**json.loads(Path(path).read_text()), **MANUAL_JOBS}


def compile_schedule(source, script, manifest_path, queue_path):
    environment, manifest, lines = {}, {}, []
    for line in source.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith('#'):
            lines.append(line)
            continue
        if re.match(r'^[A-Za-z_][A-Za-z_0-9]*=', stripped):
            key, value = stripped.split('=', 1)
            environment[key] = value
            lines.append(line)
            continue
        fields = stripped.split(None, 5)
        if len(fields) != 6:
            raise ValueError('Only explicit five-field schedules are supported')
        command = fields[5]
        identity = json.dumps([command, environment], sort_keys=True)
        key = hashlib.sha256(identity.encode()).hexdigest()[:24]
        manifest[key] = {'command': command, 'environment': dict(environment),
                         'lane': 'delivery' if any(x in command for x in DELIVERY_COMMANDS) else 'data'}
        enqueue = shlex.join([sys.executable, str(script), '--queue', str(queue_path),
                              '--manifest', str(manifest_path), 'enqueue', key])
        lines.append(' '.join(fields[:5]) + ' ' + enqueue)
    return '\n'.join(lines) + '\n', manifest


@contextlib.contextmanager
def database(path):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path, timeout=10, isolation_level=None)
    db.row_factory = sqlite3.Row
    db.execute('PRAGMA journal_mode=WAL')
    db.execute('PRAGMA synchronous=FULL')
    db.execute('''CREATE TABLE IF NOT EXISTS jobs (
        key TEXT PRIMARY KEY, lane TEXT NOT NULL, pending_at REAL,
        started_at REAL, token TEXT, pid INTEGER, finished_at REAL,
        exit_code INTEGER, runs INTEGER NOT NULL DEFAULT 0)''')
    db.execute('''CREATE TABLE IF NOT EXISTS external_requests (
        request_id TEXT PRIMARY KEY, job_key TEXT NOT NULL, target_run INTEGER NOT NULL,
        created_at REAL NOT NULL, finished_at REAL, exit_code INTEGER)''')
    db.execute('CREATE TABLE IF NOT EXISTS release_hold (id INTEGER PRIMARY KEY CHECK(id=1), owner TEXT NOT NULL, expires_at REAL NOT NULL)')
    # Serialize additive migration with enqueuers starting at the same time.
    db.execute('BEGIN IMMEDIATE')
    try:
        columns = {row[1] for row in db.execute('PRAGMA table_info(jobs)')}
        for name, kind in [('cycle_id', 'TEXT'), ('continuation_at', 'REAL'), ('checkpoint_json', 'TEXT')]:
            if name not in columns:
                db.execute(f'ALTER TABLE jobs ADD COLUMN {name} {kind}')
        db.commit()
    except BaseException:
        db.rollback(); db.close()
        raise
    try:
        yield db
    finally:
        db.close()


RELEASE_PROTECTED_COMMANDS = {
    PORTFOLIO_COMMAND, OUTCOME_COMMAND, 'cd /app && python -m app.ingest_run --job all',
}


def reserve_release(db, owner, manifest, *, seconds=600, now=None):
    """Serialize a bounded release reservation against portfolio claims."""
    if not re.fullmatch(r'[0-9]{1,20}-[0-9]{1,6}', owner) or not 60 <= seconds <= 900:
        raise ValueError('Invalid release reservation identity or duration')
    now = time.time() if now is None else now
    db.execute('BEGIN IMMEDIATE')
    try:
        held = db.execute('SELECT owner,expires_at FROM release_hold WHERE id=1').fetchone()
        if held and held['expires_at'] > now:
            if held['owner'] != owner:
                db.commit()
                return {'status': 'blocked', 'reason': 'another_release_reserved'}
            # An idempotent transport retry cannot extend a hold indefinitely.
            db.commit()
            return {'status': 'ready', 'existing': True}
        busy = [row for row in db.execute("SELECT key,json_extract(checkpoint_json,'$.kind') AS kind FROM jobs WHERE started_at IS NOT NULL OR continuation_at IS NOT NULL")
                if manifest.get(row['key'], {}).get('command') in RELEASE_PROTECTED_COMMANDS
                or row['kind'] in CHECKPOINT_COMMANDS.values()]
        if busy:
            outcome_only = all(manifest.get(row['key'], {}).get('command') == OUTCOME_COMMAND
                               or row['kind'] == 'outcome_hydration_v1' for row in busy)
            db.commit()
            return {'status': 'blocked', 'reason': 'outcome_cycle_unfinished' if outcome_only else 'portfolio_cycle_unfinished'}
        db.execute('INSERT INTO release_hold(id,owner,expires_at) VALUES(1,?,?) '
                   'ON CONFLICT(id) DO UPDATE SET owner=excluded.owner,expires_at=excluded.expires_at',
                   (owner, now + seconds))
        db.commit()
        return {'status': 'ready', 'existing': False}
    except BaseException:
        db.rollback()
        raise


def release_reservation(db, owner):
    if not re.fullmatch(r'[0-9]{1,20}-[0-9]{1,6}', owner):
        raise ValueError('Invalid release reservation identity')
    released = db.execute('DELETE FROM release_hold WHERE id=1 AND owner=?', (owner,)).rowcount
    return {'status': 'released' if released else 'not_owned'}


def enqueue(db, key, definition, now=None):
    now = time.time() if now is None else now
    db.execute('''INSERT INTO jobs(key,lane,pending_at) VALUES (?,?,?)
        ON CONFLICT(key) DO UPDATE SET pending_at=COALESCE(jobs.pending_at,excluded.pending_at),
        lane=excluded.lane''', (key, definition['lane'], now))


def claim(db, lane, manifest, now=None):
    now = time.time() if now is None else now
    db.execute('BEGIN IMMEDIATE')
    try:
        # SQLite also protects against an accidentally duplicated lane worker.
        if db.execute('SELECT 1 FROM jobs WHERE lane=? AND started_at IS NOT NULL LIMIT 1', (lane,)).fetchone():
            db.commit()
            return None
        # A second refresh (or the legacy all-job) must not overwrite a newer
        # as-of window while an earlier portfolio cycle yields to unrelated jobs.
        release_reserved = db.execute('SELECT 1 FROM release_hold WHERE id=1 AND expires_at>?', (now,)).fetchone() is not None
        portfolio_owners = [row for row in db.execute(
            'SELECT key,cycle_id FROM jobs WHERE continuation_at IS NOT NULL')
            if manifest.get(row['key'], {}).get('command') == PORTFOLIO_COMMAND]
        portfolio_owner = min(portfolio_owners, key=lambda row: (row['cycle_id'] or '', row['key']))['key'] if portfolio_owners else None
        rows = db.execute('''SELECT * FROM jobs WHERE lane=? AND started_at IS NULL
            AND ((continuation_at IS NOT NULL AND continuation_at<=?)
                 OR (continuation_at IS NULL AND pending_at IS NOT NULL))
            ORDER BY COALESCE(continuation_at,pending_at),key''', (lane, now)).fetchall()
        for row in rows:
            if row['key'] not in manifest:
                db.execute('UPDATE jobs SET pending_at=NULL,continuation_at=NULL WHERE key=?', (row['key'],))
                continue
            command = manifest[row['key']].get('command')
            if release_reserved and command in RELEASE_PROTECTED_COMMANDS:
                continue
            if (portfolio_owner and portfolio_owner != row['key'] and command in {
                    PORTFOLIO_COMMAND, 'cd /app && python -m app.ingest_run --job all'}):
                continue
            token = uuid.uuid4().hex
            continuing = row['continuation_at'] is not None
            cycle_id = row['cycle_id'] if continuing else uuid.uuid4().hex
            if continuing and not cycle_id:
                raise RuntimeError('Continuation has no logical cycle identity')
            db.execute('''UPDATE jobs SET started_at=?,token=?,pid=NULL,cycle_id=?,continuation_at=NULL,
                pending_at=CASE WHEN ? THEN pending_at ELSE NULL END,
                checkpoint_json=CASE WHEN ? THEN checkpoint_json ELSE NULL END WHERE key=?''',
                (now, token, cycle_id, continuing, continuing, row['key']))
            db.commit()
            return {**dict(row), 'token': token, 'cycle_id': cycle_id, 'started_at': now,
                    'pending_at': row['continuation_at'] if continuing else row['pending_at']}
        db.commit()
        return None
    except BaseException:
        db.rollback()
        raise


def finish(db, job, code, now=None):
    now = time.time() if now is None else now
    db.execute('BEGIN IMMEDIATE')
    try:
        row = db.execute('SELECT runs FROM jobs WHERE key=? AND token=?',
                         (job['key'], job['token'])).fetchone()
        if row is not None:
            db.execute('UPDATE jobs SET started_at=NULL,token=NULL,pid=NULL,continuation_at=NULL,finished_at=?,exit_code=?,runs=runs+1 WHERE key=? AND token=?',
                       (now, code, job['key'], job['token']))
            db.execute('UPDATE external_requests SET finished_at=?,exit_code=? WHERE job_key=? AND target_run=? AND finished_at IS NULL',
                       (now, code, job['key'], row['runs'] + 1))
        db.commit()
    except BaseException:
        db.rollback()
        raise


def continue_portfolio(db, job, definition, code, now=None):
    # Compatibility entry point: never admit an unrelated command here.
    if definition.get('command') != PORTFOLIO_COMMAND:
        return False
    return continue_checkpointed_job(db, job, definition, code, now)


def continue_checkpointed_job(db, job, definition, code, now=None):
    """Only an explicitly approved, durably completed batch may yield."""
    kind = CHECKPOINT_COMMANDS.get(definition.get('command'))
    if code != 75 or kind is None or definition.get('lane') != 'data':
        return False
    now = time.time() if now is None else now
    db.execute('BEGIN IMMEDIATE')
    try:
        row = db.execute('SELECT checkpoint_json,cycle_id FROM jobs WHERE key=? AND token=?',
                         (job['key'], job['token'])).fetchone()
        state = json.loads(row['checkpoint_json'] or '{}') if row else {}
        if (not row or state.get('kind') != kind or not state.get('continuation')
                or state.get('cycle_id') != row['cycle_id'] or state.get('deadline', 0) <= now):
            db.commit()
            return False
        delay = min(60, max(1, float(state.get('retry_after_seconds', 1))))
        db.execute('UPDATE jobs SET started_at=NULL,token=NULL,pid=NULL,continuation_at=? '
                   'WHERE key=? AND token=?', (now + delay, job['key'], job['token']))
        db.commit()
        return True
    except (ValueError, TypeError):
        db.rollback()
        return False
    except BaseException:
        db.rollback()
        raise


def dispatch_ingest(db, job, request_id, manifest, now=None):
    """Atomic request identity makes an ambiguous SSH retry safe."""
    if job not in EXTERNAL_INGEST_JOBS or not re.fullmatch(r'[0-9]{1,20}-[0-9]{1,6}', request_id):
        raise ValueError('Invalid external ingestion request')
    key = 'external-ingest-' + job
    definition = manifest[key]
    if definition != MANUAL_JOBS[key]:
        raise ValueError('External ingestion command differs from the fixed manifest')
    now = time.time() if now is None else now
    db.execute('BEGIN IMMEDIATE')
    try:
        existing = db.execute('SELECT * FROM external_requests WHERE request_id=?', (request_id,)).fetchone()
        if existing is not None:
            if existing['job_key'] != key:
                raise ValueError('External request identity already belongs to another job')
        else:
            current = db.execute('SELECT runs,started_at,continuation_at FROM jobs WHERE key=?', (key,)).fetchone()
            # A request arriving during execution asks for the next run, just
            # like cron. Several requests for that next run can coalesce.
            target = (current['runs'] if current else 0) + (2 if current and (current['started_at'] is not None or current['continuation_at'] is not None) else 1)
            enqueue(db, key, definition, now=now)
            db.execute('INSERT INTO external_requests(request_id,job_key,target_run,created_at) VALUES (?,?,?,?)',
                       (request_id, key, target, now))
        db.commit()
    except BaseException:
        db.rollback()
        raise
    return dict(db.execute('SELECT * FROM external_requests WHERE request_id=?', (request_id,)).fetchone())


def wait_external_request(db, request_id, *, wait_seconds=1800):
    deadline = time.monotonic() + wait_seconds
    while True:
        row = dict(db.execute('SELECT * FROM external_requests WHERE request_id=?', (request_id,)).fetchone())
        if row['finished_at'] is not None:
            return {**row, 'status': 'succeeded' if row['exit_code'] == 0 else 'failed'}
        if time.monotonic() >= deadline:
            return {**row, 'status': 'pending'}
        time.sleep(min(2, max(0, deadline - time.monotonic())))


def owns_process(pid, token):
    from preloaded_jobs import owns_process as owns_preloaded_process
    if owns_preloaded_process(pid, token):
        return True
    try:
        environment = Path(f'/proc/{pid}/environ').read_bytes().split(b'\0')
        return ('WALNUT_JOB_TOKEN=' + token).encode() in environment
    except (FileNotFoundError, ProcessLookupError, PermissionError):
        return False


def owned_groups(token):
    groups = set()
    for entry in Path('/proc').iterdir():
        if entry.name.isdigit() and owns_process(int(entry.name), token):
            try:
                groups.add(os.getpgid(int(entry.name)))
            except ProcessLookupError:
                pass
    return groups


def stop_process(pid, token, *, grace_seconds=10):
    if pid is None:
        # Recover a crash between Popen and recording the new process ID.
        for group in owned_groups(token):
            stop_process(group, token, grace_seconds=grace_seconds)
        return
    if not pid or pid not in owned_groups(token):
        return
    try:
        os.killpg(pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    deadline = time.monotonic() + grace_seconds
    while time.monotonic() < deadline and pid in owned_groups(token):
        time.sleep(.1)
    # A shell may have exited while a grandchild ignored SIGTERM. Check the
    # identified group, not just its original leader, before escalating.
    if pid in owned_groups(token):
        try:
            os.killpg(pid, signal.SIGKILL)
        except ProcessLookupError:
            pass


def recover(db, lane):
    # Hold the OS lane lock before recovery. Never assume an expired timestamp
    # proves a child has stopped or blindly kill a reused process ID.
    for row in db.execute('SELECT * FROM jobs WHERE lane=? AND started_at IS NOT NULL', (lane,)).fetchall():
        stop_process(row['pid'], row['token'])
        # Preserve a subsequent scheduled request; an uncertain interrupted call
        # itself is not replayed automatically. Domain idempotency stays intact.
        finish(db, dict(row), -signal.SIGTERM)
        from preloaded_jobs import forget_identity
        forget_identity(row['token'])


def worker(queue, manifest, lane, *, idle=.5):
    import fcntl
    import resource
    stopped = False

    def shutdown(*_):
        nonlocal stopped
        stopped = True

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)
    lock_path = Path(str(queue) + '.' + lane + '.lock')
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open('a') as lock, database(queue) as db:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        recover(db, lane)
        while not stopped:
            job = claim(db, lane, manifest)
            if job is None:
                time.sleep(idle)
                continue
            definition = manifest[job['key']]
            env = {**os.environ, **definition['environment'], 'FLY_PROCESS_GROUP': 'cron',
                   'WALNUT_JOB_TOKEN': job['token'], 'WALNUT_JOB_CYCLE': job['cycle_id'],
                   'WALNUT_JOB_KEY': job['key'], 'WALNUT_JOB_QUEUE': str(queue),
                   'WALNUT_PORTFOLIO_CONTINUATION': '1' if definition['command'] == PORTFOLIO_COMMAND else '0',
                   'WALNUT_OUTCOME_CONTINUATION': '1' if definition['command'] == OUTCOME_COMMAND else '0',
                   'PYTHONUNBUFFERED': '1'}
            process = None
            usage_before = resource.getrusage(resource.RUSAGE_CHILDREN)
            try:
                # A group per job lets shutdown stop shells and descendants.
                from preloaded_jobs import launch
                process = launch(definition['command'], env, close_resources=(db, lock)) if lane == 'data' else None
                if process is None:
                    process = subprocess.Popen(['nice', '-n', '10', '/bin/sh', '-c', definition['command']],
                                               env=env, start_new_session=True)
                db.execute('UPDATE jobs SET pid=? WHERE key=? AND token=?',
                           (process.pid, job['key'], job['token']))
                print(json.dumps({'event': 'scheduled_job_started', 'key': job['key'], 'lane': lane, 'cycle_id': job['cycle_id'],
                                  'wait_seconds': round(job['started_at'] - job['pending_at'], 2)}), flush=True)
                deadline = time.monotonic() + int(os.getenv('SCHEDULED_JOB_MAX_SECONDS', '1200'))
                while process.poll() is None and not stopped and time.monotonic() < deadline:
                    time.sleep(.2)
                if process.poll() is None:
                    stop_process(process.pid, job['token'])
                code = process.wait(timeout=15)
                # CPU includes reaped descendants, unlike wall time spent on HTTP.
                # One child tree runs per lane, so the delta belongs to this job.
                usage_after = resource.getrusage(resource.RUSAGE_CHILDREN)
                cpu_seconds = ((usage_after.ru_utime - usage_before.ru_utime) +
                               (usage_after.ru_stime - usage_before.ru_stime))
                yielded = continue_checkpointed_job(db, job, definition, code)
                if not yielded:
                    finish(db, job, code)
                from preloaded_jobs import forget_identity
                forget_identity(job['token'])
                print(json.dumps({'event': 'scheduled_job_yielded' if yielded else 'scheduled_job_finished', 'key': job['key'], 'lane': lane,
                                  'cycle_id': job['cycle_id'], 'exit_code': code, 'seconds': round(time.time()-job['started_at'], 2),
                                  'cpu_seconds': round(cpu_seconds, 4),
                                  'preloaded': bool(getattr(process, 'preloaded', False))}), flush=True)
            finally:
                if process is not None and process.poll() is None:
                    stop_process(process.pid, job['token'])
                    process.wait(timeout=15)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--queue', default='/data/scheduled-jobs.sqlite3')
    parser.add_argument('--manifest', default='/tmp/walnut-schedule.json')
    sub = parser.add_subparsers(dest='action', required=True)
    sub.add_parser('enqueue').add_argument('key')
    sub.add_parser('worker').add_argument('lane', choices=['data', 'delivery'])
    sub.add_parser('status')
    dispatch = sub.add_parser('dispatch-ingest')
    dispatch.add_argument('job', choices=EXTERNAL_INGEST_JOBS)
    dispatch.add_argument('--request-id', required=True)
    dispatch.add_argument('--wait-seconds', type=int, default=1800)
    reserve = sub.add_parser('reserve-release')
    reserve.add_argument('--owner', required=True)
    reserve.add_argument('--seconds', type=int, default=600)
    release = sub.add_parser('release-reservation')
    release.add_argument('--owner', required=True)
    args = parser.parse_args()
    if args.action in {'dispatch-ingest', 'reserve-release', 'release-reservation'}:
        if (os.getenv('COMBINED_SCHEDULER_ENABLED', '').lower() != 'true'
                or not os.getenv('COMBINED_SCHEDULER_MACHINE_ID')
                or os.getenv('COMBINED_SCHEDULER_MACHINE_ID') != os.getenv('FLY_MACHINE_ID')
                or not Path(args.queue).is_file()
                or (args.action == 'dispatch-ingest' and not 0 <= args.wait_seconds <= 3600)):
            raise SystemExit('Queue operations require the active scheduler owner and existing queue')
    manifest = load_manifest(args.manifest)
    if args.action == 'worker':
        worker(args.queue, manifest, args.lane)
    else:
        with database(args.queue) as db:
            if args.action == 'dispatch-ingest':
                dispatch_ingest(db, args.job, args.request_id, manifest)
                result = wait_external_request(db, args.request_id, wait_seconds=args.wait_seconds)
                print(json.dumps(result), flush=True)
                raise SystemExit(0 if result['status'] == 'succeeded' else 75 if result['status'] == 'pending' else 1)
            elif args.action == 'reserve-release':
                result = reserve_release(db, args.owner, manifest, seconds=args.seconds)
                print(json.dumps(result), flush=True)
                raise SystemExit(0 if result['status'] == 'ready' else 75)
            elif args.action == 'release-reservation':
                print(json.dumps(release_reservation(db, args.owner)), flush=True)
            elif args.action == 'enqueue':
                enqueue(db, args.key, manifest[args.key])
            else:
                print(json.dumps([dict(r) for r in db.execute('SELECT * FROM jobs ORDER BY lane,key')]))


if __name__ == '__main__':
    main()
