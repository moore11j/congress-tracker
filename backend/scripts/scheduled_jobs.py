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


DELIVERY_COMMANDS = (
    'run_email_digest_schedule.sh', 'run_email_intraday_alert_sweep.sh',
    'run_watchlist_price_alerts.sh', 'app.jobs.queue_strategy_event_deliveries',
    'app.jobs.deliver_strategy_event_emails', '--job monitoring-alert-refresh',
)


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
    try:
        yield db
    finally:
        db.close()


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
        rows = db.execute('''SELECT * FROM jobs WHERE lane=? AND pending_at IS NOT NULL
            AND started_at IS NULL ORDER BY pending_at,key''', (lane,)).fetchall()
        for row in rows:
            if row['key'] not in manifest:
                db.execute('UPDATE jobs SET pending_at=NULL WHERE key=?', (row['key'],))
                continue
            token = uuid.uuid4().hex
            db.execute('UPDATE jobs SET started_at=?,pending_at=NULL,token=?,pid=NULL WHERE key=?',
                       (now, token, row['key']))
            db.commit()
            return {**dict(row), 'token': token, 'started_at': now}
        db.commit()
        return None
    except BaseException:
        db.rollback()
        raise


def finish(db, job, code, now=None):
    db.execute('''UPDATE jobs SET started_at=NULL,token=NULL,pid=NULL,
        finished_at=?,exit_code=?,runs=runs+1 WHERE key=? AND token=?''',
        (time.time() if now is None else now, code, job['key'], job['token']))


def owns_process(pid, token):
    try:
        environment = Path(f'/proc/{pid}/environ').read_bytes().split(b'\0')
        return ('WALNUT_JOB_TOKEN=' + token).encode() in environment
    except (FileNotFoundError, ProcessLookupError):
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


def worker(queue, manifest, lane, *, idle=.5):
    import fcntl
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
                   'WALNUT_JOB_TOKEN': job['token'], 'PYTHONUNBUFFERED': '1'}
            process = None
            try:
                # A group per job lets shutdown stop shells and descendants.
                process = subprocess.Popen(['nice', '-n', '10', '/bin/sh', '-c', definition['command']],
                                           env=env, start_new_session=True)
                db.execute('UPDATE jobs SET pid=? WHERE key=? AND token=?',
                           (process.pid, job['key'], job['token']))
                print(json.dumps({'event': 'scheduled_job_started', 'key': job['key'], 'lane': lane,
                                  'wait_seconds': round(job['started_at'] - job['pending_at'], 2)}), flush=True)
                deadline = time.monotonic() + int(os.getenv('SCHEDULED_JOB_MAX_SECONDS', '1200'))
                while process.poll() is None and not stopped and time.monotonic() < deadline:
                    time.sleep(.2)
                if process.poll() is None:
                    stop_process(process.pid, job['token'])
                code = process.wait(timeout=15)
                finish(db, job, code)
                print(json.dumps({'event': 'scheduled_job_finished', 'key': job['key'], 'lane': lane,
                                  'exit_code': code, 'seconds': round(time.time()-job['started_at'], 2)}), flush=True)
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
    args = parser.parse_args()
    manifest = json.loads(Path(args.manifest).read_text())
    if args.action == 'worker':
        worker(args.queue, manifest, args.lane)
    else:
        with database(args.queue) as db:
            if args.action == 'enqueue':
                enqueue(db, args.key, manifest[args.key])
            else:
                print(json.dumps([dict(r) for r in db.execute('SELECT * FROM jobs ORDER BY lane,key')]))


if __name__ == '__main__':
    main()
