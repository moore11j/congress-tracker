"""Opt-in bounded institutional preparation; at most five committed chunks, no publication."""
import json
import os
import time
from pathlib import Path
import sqlite3

JOB_NAME = 'maintenance-institutional-preparation-v1'
QUEUE = '/data/scheduled-jobs.sqlite3'


class PreparationMemoryHold(RuntimeError):
    """The existing headroom guard stopped further work."""


class PreparationWorkBudgetHold(RuntimeError):
    """End this worker without discarding previously committed chunks."""


def _stop_for_work_budget(signum=None, _frame=None):
    import signal
    # A hard CPU-limit signal remains a failure, including after prior commits.
    if signum is not None and signum == getattr(signal, 'SIGXCPU', None):
        raise RuntimeError('Institutional preparation hard CPU budget')
    raise PreparationWorkBudgetHold('Institutional preparation work budget')


def require_preparation_queue(queue, token, process_group):
    if not token:
        raise RuntimeError('missing_scheduler_token')
    with sqlite3.connect(Path(queue).resolve().as_uri()+'?mode=ro', uri=True, timeout=.2) as db:
        rows = db.execute("SELECT key,token,pid FROM jobs WHERE lane='data' AND started_at IS NOT NULL").fetchall()
    if rows != [(JOB_NAME, token, process_group)]:
        raise RuntimeError('not_exclusive_data_lane_owner')


def prepare_chunks(session_factory, *, checkpoint, max_chunks=5):
    """Amortize process startup while committing each bounded chunk separately."""
    if not 1 <= max_chunks <= 5:
        raise ValueError('Invalid preparation run limit')
    from sqlalchemy import text
    from app.services.direct_13f_preparation import advance_preparation
    chunks = []
    deferred = None
    for _ in range(max_chunks):
        try:
            checkpoint()
        except (PreparationMemoryHold, PreparationWorkBudgetHold) as exc:
            if not chunks:
                raise
            # Completed transactions are durable. End this worker so its
            # memory is released before any separately guarded next request.
            deferred = 'memory_deferred' if isinstance(exc, PreparationMemoryHold) else 'budget_deferred'
            break
        with session_factory() as db:
            if db.bind.dialect.name != 'postgresql':
                raise RuntimeError('Preparation job requires PostgreSQL')
            db.execute(text("SET LOCAL statement_timeout='3s'"))
            db.execute(text("SET LOCAL lock_timeout='500ms'"))
            db.execute(text("SET LOCAL idle_in_transaction_session_timeout='75s'"))
            try:
                result = advance_preparation(db, checkpoint=checkpoint)
            except PreparationWorkBudgetHold:
                # An alarm may interrupt a DBAPI query. Sending ROLLBACK on
                # that connection can fail while its query is still pending.
                # Closing it aborts the uncommitted transaction. Close first
                # so a driver close error propagates instead of being logged
                # and suppressed by pool invalidation, then discard it.
                try:
                    db.connection().connection.driver_connection.close()
                finally:
                    db.invalidate()
                if not chunks:
                    raise
                # Report only earlier durable progress. Commit interruption
                # remains outside this handler because its outcome is unknown.
                deferred = 'budget_deferred'
                break
            db.commit()
        chunks.append(result)
        if result['status'] in {'sweep_complete', 'busy'}:
            break
    return dict(status=deferred or chunks[-1]['status'], cursor=chunks[-1].get('cursor'), chunks=len(chunks),
                processed=sum(c['processed'] for c in chunks), parsed=sum(c.get('parsed', 0) for c in chunks),
                source_bytes=sum(c.get('source_bytes', 0) for c in chunks), publication_writes=0)


def main():
    if os.getenv('DIRECT_13F_PEER_PREPARATION_ENABLED', 'false').strip().lower() != 'true':
        print(json.dumps({'status': 'disabled'}))
        return
    import resource
    import signal
    for attempt in range(10):
        try:
            require_preparation_queue(QUEUE, os.getenv('WALNUT_JOB_TOKEN'), os.getpgrp())
            break
        except RuntimeError:
            if attempt == 9:
                raise
            time.sleep(.05)
    from app.background_job_guard import check_background_job_guard
    from app.db import SessionLocal
    from app.jobs.collect_direct_feeds import collector_lock

    guard = check_background_job_guard('direct-13f-peer-preparation')
    if not guard.proceed:
        print(json.dumps(guard.to_dict()))
        return
    started, cpu = time.monotonic(), time.process_time()
    def checkpoint():
        require_preparation_queue(QUEUE, os.getenv('WALNUT_JOB_TOKEN'), os.getpgrp())
        if time.monotonic() - started > 60 or time.process_time() - cpu > 45:
            _stop_for_work_budget()
        available = next(int(line.split()[1]) for line in Path('/proc/meminfo').read_text().splitlines()
                         if line.startswith('MemAvailable:'))
        if available < 750*1024:
            raise PreparationMemoryHold('memory_headroom')
    signal.signal(signal.SIGALRM, _stop_for_work_budget)
    signal.signal(signal.SIGXCPU, _stop_for_work_budget)
    signal.alarm(70)
    resource.setrlimit(resource.RLIMIT_CPU, (60, 65))
    try:
        checkpoint()
        with collector_lock(recover_orphaned=False) as acquired:
            if not acquired:
                print(json.dumps({'status': 'busy'}))
                return
            result = prepare_chunks(SessionLocal, checkpoint=checkpoint)
        print(json.dumps(dict(result, cpu_seconds=time.process_time()-cpu,
                              wall_seconds=time.monotonic()-started, emails=0)))
    finally:
        signal.alarm(0)


if __name__ == '__main__':
    main()
