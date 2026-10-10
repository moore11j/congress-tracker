"""Supervise the API and bounded scheduled workers on one Fly machine."""
from __future__ import annotations

import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

from scheduled_jobs import compile_schedule, database, recover


def scheduler_enabled(environment):
    enabled = environment.get('COMBINED_SCHEDULER_ENABLED', '').lower() == 'true'
    owner = environment.get('COMBINED_SCHEDULER_MACHINE_ID', '')
    # During rollout, only the explicitly selected API may run scheduled work.
    return enabled and bool(owner) and owner == environment.get('FLY_MACHINE_ID')


def run(*, app_command=None, root=Path('/app'), runtime_dir=Path('/tmp')):
    stopped = False
    processes = []
    queue = os.getenv('SCHEDULED_QUEUE_PATH', '/data/scheduled-jobs.sqlite3')
    enabled = scheduler_enabled(os.environ)

    def shutdown(*_):
        nonlocal stopped
        stopped = True

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)
    try:
        processes.append(subprocess.Popen(app_command or [sys.executable, '-m', 'uvicorn', 'app.main:app',
                                          '--host', '0.0.0.0', '--port', '8080'], start_new_session=True))
        if enabled:
            script = Path(__file__).with_name('scheduled_jobs.py')
            manifest_path = runtime_dir / 'walnut-schedule.json'
            cron_path = runtime_dir / 'walnut-queued-crontab'
            cron, manifest = compile_schedule((root/'crontab').read_text(), script, manifest_path, queue)
            manifest_path.write_text(json.dumps(manifest))
            cron_path.write_text(cron)
            # Validate before starting any schedule; keep native timezone parsing.
            subprocess.run(['supercronic', '-test', str(cron_path)], check=True)
            for lane in ('data', 'delivery'):
                processes.append(subprocess.Popen([sys.executable, str(script), '--queue', queue,
                    '--manifest', str(manifest_path), 'worker', lane], start_new_session=True))
            processes.append(subprocess.Popen(['supercronic', str(cron_path)], start_new_session=True))
        print(json.dumps({'event': 'combined_service_started', 'scheduler_enabled': enabled}), flush=True)
        while not stopped:
            for process in processes:
                if process.poll() is not None:
                    raise RuntimeError('Supervised process exited; restarting the whole service')
            time.sleep(.5)
    finally:
        # Stop scheduling before workers, then let the API drain its requests.
        for process in reversed(processes):
            if process.poll() is None:
                try:
                    os.killpg(process.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
        deadline = time.monotonic() + 25
        for process in reversed(processes):
            try:
                process.wait(timeout=max(.1, deadline-time.monotonic()))
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
        # If a worker crashed, stop its identified job group before Fly restarts.
        if enabled:
            with database(queue) as db:
                for lane in ('data', 'delivery'):
                    recover(db, lane)


if __name__ == '__main__':
    run()
