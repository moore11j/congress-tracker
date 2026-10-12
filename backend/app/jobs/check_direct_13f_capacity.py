"""One-time, queued, read-only institutional preparation capacity check."""
import json
import os
from pathlib import Path
import sqlite3
import time


JOB_KEY = 'maintenance-institutional-capacity-v1'
QUEUE = '/data/scheduled-jobs.sqlite3'


def require_queue_ownership(queue, token, process_group):
    """An environment token alone is insufficient: require the live queue claim."""
    if not token:
        raise RuntimeError('missing_scheduler_token')
    with sqlite3.connect(Path(queue).resolve().as_uri() + '?mode=ro', uri=True, timeout=.2) as db:
        rows = db.execute("SELECT key, token, pid FROM jobs WHERE lane='data' AND started_at IS NOT NULL").fetchall()
    if rows != [(JOB_KEY, token, process_group)]:
        raise RuntimeError('not_exclusive_data_lane_owner')


def prepare_readonly(db, *, checkpoint, identifier_manifest):
    from sqlalchemy import text
    from app.services.direct_13f_batch import select_13f_batch, prepare_13f_batch
    from app.services.direct_13f_identifiers import load_staged_identifiers
    from app.services.feed_source_control import FeedSourceControl

    if db.get_bind().dialect.name != 'postgresql' or db.scalar(text('SHOW transaction_read_only')) != 'on':
        raise RuntimeError('read_only_postgresql_required')
    control = db.get(FeedSourceControl, 'sec_13f')
    generation = control.generation if control else 0
    ids, retries, _ = select_13f_batch(db, generation=generation, limit=20, include_waiting=True)
    if not ids:
        raise RuntimeError('no_eligible_targets')
    checkpoint('identifiers')
    identifiers = load_staged_identifiers(db, identifier_manifest)
    prepared = prepare_13f_batch(db, ids=ids, identifier_documents=identifiers,
                                use_staged_references=True, checkpoint=checkpoint)
    return dict(status='prepared', targets=ids, retry_targets=retries,
                identifier_documents=len(identifiers), source_provider=control.provider if control else 'fmp',
                source_generation=generation, **prepared['summary'])


def main():
    # Linux-only production diagnostic; no ORM imports before queue ownership.
    import resource
    import signal
    from datetime import datetime, timezone

    started, cpu = time.monotonic(), time.process_time()
    result = dict(event='institutional_capacity_check', status='held', phase='preflight',
                  observed_at=datetime.now(timezone.utc).isoformat(),
                  scope='preparation_only_publication_cost_not_measured',
                  source_calls=0, canonical_writes=0, email_deliveries=0)
    def bounded_signal(*_):
        raise RuntimeError('diagnostic_work_budget')
    phase_cpu = [cpu]
    phase_costs = {}
    def checkpoint(phase):
        now = time.process_time()
        previous = result['phase']
        phase_costs[previous] = phase_costs.get(previous, 0) + now - phase_cpu[0]
        phase_cpu[0] = now
        result['phase'] = phase
        require_queue_ownership(QUEUE, os.getenv('WALNUT_JOB_TOKEN'), os.getpgrp())
        if time.monotonic()-started > 150 or time.process_time()-cpu > 85:
            raise RuntimeError('diagnostic_work_budget')
        available = next(int(line.split()[1]) for line in Path('/proc/meminfo').read_text().splitlines()
                         if line.startswith('MemAvailable:'))
        if available < 750 * 1024:
            raise RuntimeError('memory_headroom')
    try:
        if (os.getenv('COMBINED_SCHEDULER_ENABLED', '').lower() != 'true'
                or not os.getenv('FLY_MACHINE_ID')
                or os.getenv('COMBINED_SCHEDULER_MACHINE_ID') != os.getenv('FLY_MACHINE_ID')):
            raise RuntimeError('not_scheduler_machine')
        # Worker persists child PID immediately after Popen. Allow that small race.
        for attempt in range(10):
            try:
                require_queue_ownership(QUEUE, os.getenv('WALNUT_JOB_TOKEN'), os.getpgrp())
                break
            except RuntimeError:
                if attempt == 9:
                    raise
                time.sleep(.05)
        signal.signal(signal.SIGALRM, bounded_signal)
        signal.signal(signal.SIGXCPU, bounded_signal)
        signal.alarm(150)
        resource.setrlimit(resource.RLIMIT_CPU, (90, 95))
        checkpoint('imports')
        from sqlalchemy import text
        from sqlalchemy.orm import Session
        from app.db import engine
        from app.background_job_guard import check_background_job_guard
        from app.jobs.collect_direct_feeds import collector_lock
        guard = check_background_job_guard('institutional-capacity-readonly')
        if not guard.proceed:
            raise RuntimeError('background_guard:' + guard.reason)
        with collector_lock(recover_orphaned=False) as acquired:
            if not acquired:
                raise RuntimeError('source_collection_active')
            with engine.connect().execution_options(isolation_level='REPEATABLE READ') as connection:
                with connection.begin():
                    connection.execute(text('SET TRANSACTION READ ONLY'))
                    connection.execute(text("SET LOCAL statement_timeout='3s'"))
                    connection.execute(text("SET LOCAL idle_in_transaction_session_timeout='155s'"))
                    with Session(bind=connection) as db:
                        result.update(prepare_readonly(db, checkpoint=checkpoint,
                            identifier_manifest='/app/config/sec_13f_identifiers.json'))
                        result['readonly'] = connection.scalar(text('SHOW transaction_read_only'))
    except Exception as exc:
        result.update(status='held', reason=str(exc)[:300], error_type=type(exc).__name__)
    finally:
        signal.alarm(0)
    phase_costs[result['phase']] = phase_costs.get(result['phase'], 0) + time.process_time() - phase_cpu[0]
    result['phase_cpu_seconds'] = {k: round(v, 4) for k, v in phase_costs.items()}
    result.update(cpu_seconds=round(time.process_time()-cpu, 4),
                  wall_seconds=round(time.monotonic()-started, 4),
                  peak_rss_mb=round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss/1024, 2))
    print(json.dumps(result, sort_keys=True), flush=True)
    if result['status'] != 'prepared':
        raise SystemExit(75)


if __name__ == '__main__':
    main()
