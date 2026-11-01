"""Explicitly approved, one-time, queued read-only canonical institutional peer capture.

Installing this command does not authorize a capture. An operator must separately
create the short-lived, package-bound request after owner approval of the payload
and its local destination. No recurring trigger or transfer is provided here.
"""
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import time

JOB_NAME = 'maintenance-institutional-peers-v1'
QUEUE = '/data/scheduled-jobs.sqlite3'
REQUEST = '/data/institutional-peer-capture-request.json'
OUTPUT = '/tmp/walnut-institutional-peers'
SCOPE = '/data/institutional-peer-scope.json'
MAX_WALL_SECONDS = 480
MAX_CPU_SECONDS = 85


def package_hash():
    root = Path(__file__).resolve().parents[1]
    files = [Path(__file__), *(root / 'services' / name for name in (
        'institutional_capture_transport.py', 'institutional_capture_file.py',
        'institutional_peer_capture.py', 'direct_feed_store.py', 'institutional_activity.py')), root / 'models.py']
    digest = hashlib.sha256()
    for path in files:
        digest.update(path.name.encode() + b'\0' + path.read_bytes() + b'\0')
    return digest.hexdigest()


def read_request(path, *, now=None):
    now = time.time() if now is None else now
    path = Path(path)
    if path.is_symlink() or path.stat().st_size > 4096:
        raise ValueError('invalid_capture_request_file')
    # A request contains no credentials or customer records. It authorizes only
    # this exact installed package and preserves an explicit operator audit ID.
    from app.services.institutional_capture_transport import decode
    request = decode(path.read_bytes())
    if (not isinstance(request, dict) or type(request.get('version')) is not int or request['version'] != 1 or
            not isinstance(request.get('request_id'), str) or not re.fullmatch(r'[a-f0-9]{32}', request['request_id']) or
            request.get('package_sha256') != package_hash() or
            type(request.get('approved_at')) not in (int, float) or
            type(request.get('expires_at')) not in (int, float) or
            not request['approved_at'] <= now < request['expires_at'] or
            not 0 < request['expires_at'] - request['approved_at'] <= 3600 or
            not isinstance(request.get('scope_sha256'), str) or
            not re.fullmatch(r'[a-f0-9]{64}', request['scope_sha256']) or
            request.get('payload') != 'institutional_canonical_peer_supplement' or
            request.get('destination') != 'owner_local_onedrive_migration_validation'):
        raise ValueError('missing_or_expired_exact_capture_approval')
    return request


def require_queue_ownership(queue, token, cycle, process_group):
    if not all(isinstance(value, str) and re.fullmatch(r'[a-f0-9]{32}', value) for value in (token, cycle)):
        raise RuntimeError('missing_scheduler_identity')
    with sqlite3.connect(Path(queue).resolve().as_uri() + '?mode=ro', uri=True, timeout=.2) as db:
        rows = db.execute("SELECT key,token,cycle_id,pid FROM jobs WHERE lane='data' AND started_at IS NOT NULL").fetchall()
    if rows != [(JOB_NAME, token, cycle, process_group)]:
        raise RuntimeError('not_exclusive_data_lane_owner')


def export_snapshot(output, scope):
    from sqlalchemy import text
    from sqlalchemy.orm import Session
    from app.db import engine
    from app.services.institutional_peer_capture import export_peers

    if engine.dialect.name != 'postgresql':
        raise RuntimeError('postgresql_required')
    with engine.connect().execution_options(isolation_level='REPEATABLE READ') as connection:
        with connection.begin():
            connection.execute(text('SET TRANSACTION READ ONLY'))
            connection.execute(text("SET LOCAL statement_timeout='3s'"))
            connection.execute(text("SET LOCAL lock_timeout='500ms'"))
            connection.execute(text("SET LOCAL idle_in_transaction_session_timeout='60s'"))
            if connection.scalar(text('SHOW transaction_read_only')) != 'on':
                raise RuntimeError('readonly_transaction_required')
            with Session(bind=connection) as db:
                stream = export_peers(db, output, scope)
    # A failure while closing the transaction must not earn a terminal frame.
    return stream.finish(transaction_read_only=True, production_writes=0)


def main():
    import resource
    import signal
    started, cpu = time.monotonic(), time.process_time()
    token, cycle = os.getenv('WALNUT_JOB_TOKEN'), os.getenv('WALNUT_JOB_CYCLE')
    result = dict(event='institutional_peer_capture', status='held', phase='approval',
                  production_writes=0, provider_requests=0, emails=0)
    last_check = [0.0]

    def guard(force=False):
        elapsed = time.monotonic()
        if elapsed-started > MAX_WALL_SECONDS or time.process_time()-cpu > MAX_CPU_SECONDS:
            raise RuntimeError('capture_work_budget')
        if force or elapsed-last_check[0] >= 1:
            require_queue_ownership(QUEUE, token, cycle, os.getpgrp())
            if time.time() >= request['expires_at']:
                raise RuntimeError('peer_capture_approval_expired')
            available = next(int(line.split()[1]) for line in Path('/proc/meminfo').read_text().splitlines()
                             if line.startswith('MemAvailable:'))
            if available < 750 * 1024:
                raise RuntimeError('capture_memory_headroom')
            last_check[0] = elapsed

    def interrupted(*_):
        raise RuntimeError('capture_interrupted_or_work_budget')

    try:
        if (os.getenv('COMBINED_SCHEDULER_ENABLED', '').lower() != 'true' or
                not os.getenv('FLY_MACHINE_ID') or
                os.getenv('COMBINED_SCHEDULER_MACHINE_ID') != os.getenv('FLY_MACHINE_ID') or
                os.getenv('WALNUT_JOB_KEY') != JOB_NAME or
                os.getenv('WALNUT_JOB_QUEUE') != QUEUE):
            raise RuntimeError('not_designated_scheduler_job')
        request = read_request(REQUEST)
        result['phase'] = 'queue_ownership'
        # The worker records the child's process group immediately after Popen.
        for attempt in range(10):
            try:
                guard(force=True)
                break
            except RuntimeError as exc:
                if str(exc) != 'not_exclusive_data_lane_owner' or attempt == 9:
                    raise
                time.sleep(.05)
        os.nice(9)  # Queue starts at nice10; yield remaining CPU to API/delivery.
        signal.signal(signal.SIGALRM, interrupted)
        signal.signal(signal.SIGXCPU, interrupted)
        signal.signal(signal.SIGTERM, interrupted)
        signal.alarm(MAX_WALL_SECONDS)
        resource.setrlimit(resource.RLIMIT_CPU, (85, 90))
        resource.setrlimit(resource.RLIMIT_AS, (1_000_000_000, 1_000_000_000))
        result['phase'] = 'pressure_and_collection_guard'
        from app.services.institutional_capture_file import prepare_file
        from app.background_job_guard import check_background_job_guard
        from app.jobs.collect_direct_feeds import collector_lock
        if not check_background_job_guard('institutional-peer-capture').proceed:
            raise RuntimeError('background_guard')
        from app.services.institutional_peer_capture import read_scope
        scope = read_scope(SCOPE, request['scope_sha256'])
        root = Path(OUTPUT)
        root.mkdir(mode=0o700, exist_ok=True)
        with collector_lock(recover_orphaned=False) as acquired:
            if not acquired:
                raise RuntimeError('source_collection_active')
            guard(force=True)
            result['phase'] = 'capture'
            captured = prepare_file(root, request['request_id'],
                                    lambda output: export_snapshot(output, scope), guard)
        result.update(status='captured', phase='complete', request_id=request['request_id'],
                      compressed_bytes=captured['compressed_bytes'],
                      compressed_sha256=captured['compressed_sha256'],
                      scope_sha256=request['scope_sha256'],
                      acceptance='transport_only_requires_canonical_peer_scope_verification')
    except Exception as exc:
        # Do not leak database exception text or original-record contents to logs.
        result.update(error_type=type(exc).__name__)
    finally:
        signal.alarm(0)
    result.update(cpu_seconds=round(time.process_time()-cpu, 4),
                  wall_seconds=round(time.monotonic()-started, 4))
    print(json.dumps(result, sort_keys=True), flush=True)
    if result['status'] != 'captured':
        raise SystemExit(75)


if __name__ == '__main__':
    main()
