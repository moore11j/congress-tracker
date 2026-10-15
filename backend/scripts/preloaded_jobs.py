"""Fork isolated data jobs from a read-only, single-threaded import template.

Only fixed warmers and the exact portfolio-refresh alias use this path.
No task executes in the parent; unsupported commands retain the original
subprocess launcher.
"""
from __future__ import annotations

import importlib
import json
import os
from pathlib import Path
import re
import runpy
import select
import signal
import subprocess
import sys
import threading
import time
import traceback

MODULES = frozenset('app.jobs.' + name for name in (
    'warm_institutional_reference', 'warm_finnhub_news', 'warm_sec_research',
    'warm_sec_earnings', 'warm_sec_metadata', 'warm_sec_fundamentals',
))
_template_environment = None
_preload_failed = False


def selected_module(command):
    if command == 'cd /app && python -m app.ingest_run --job portfolio-simulation-refresh':
        return 'app.jobs.refresh_portfolio_batch'
    prefix = 'cd /app && python -m '
    module = command[len(prefix):] if command.startswith(prefix) else ''
    return module if module in MODULES else None


def _identity_path(token):
    if not re.fullmatch(r'[0-9a-f]{32}', token or ''):
        raise ValueError('Invalid scheduled job identity')
    return Path(os.getenv('SCHEDULED_JOB_IDENTITY_DIRECTORY', '/tmp/walnut-job-identities')) / (token + '.json')


def _start_tick(pid):
    # comm can contain spaces and parentheses; starttime is field 22.
    fields = Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()
    if fields[0] == 'Z':
        raise ProcessLookupError(pid)
    return fields[19]


def owns_process(pid, token):
    try:
        record = json.loads(_identity_path(token).read_text())
        return (record['pid'] == pid and record['start_tick'] == _start_tick(pid)
                and record['boot_id'] == Path('/proc/sys/kernel/random/boot_id').read_text().strip())
    except (OSError, ValueError, KeyError, IndexError, TypeError):
        return False


def forget_identity(token):
    try:
        _identity_path(token).unlink(missing_ok=True)
    except (OSError, ValueError):
        pass  # PID/starttime validation makes an old receipt harmless.


def _record_identity(pid, token):
    path = _identity_path(token)
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    temporary = path.with_suffix('.pending')
    with temporary.open('x', encoding='utf-8') as stream:
        json.dump({'pid': pid, 'start_tick': _start_tick(pid),
                   'boot_id': Path('/proc/sys/kernel/random/boot_id').read_text().strip()}, stream)
    os.replace(temporary, path)


def preload(environment):
    """Import definitions only. Never share task state or an open DB connection."""
    global _template_environment, _preload_failed
    fingerprint = {k: v for k, v in environment.items() if k not in {'WALNUT_JOB_TOKEN', 'WALNUT_JOB_KEY', 'WALNUT_JOB_CYCLE', 'WALNUT_PORTFOLIO_CONTINUATION'}}
    if _preload_failed:
        return False
    if _template_environment is not None:
        return fingerprint == _template_environment
    if threading.active_count() != 1:
        return False
    cpu = time.process_time()
    original_environment = dict(os.environ)
    try:
        os.environ.clear()
        os.environ.update(fingerprint)
        # These imports only define models/transports. Job modules are deliberately
        # absent, and every child gets a fresh copy of subsequent mutable state.
        importlib.import_module('app.models')
        importlib.import_module('app.services.direct_feed_store')
        importlib.import_module('requests')
        importlib.import_module('app.compute_replicated_portfolios')
        from sqlalchemy.orm import configure_mappers
        from app.db import engine
        configure_mappers()
        checked_out = getattr(engine.pool, 'checkedout', lambda: 0)()
        checked_in = getattr(engine.pool, 'checkedin', lambda: 0)()
        if checked_out or checked_in or threading.active_count() != 1:
            raise RuntimeError('Import template contains live resources')
    except Exception as exc:
        _preload_failed = True
        print(json.dumps({'event': 'scheduled_preload_disabled', 'reason': type(exc).__name__}), flush=True)
        return False
    finally:
        os.environ.clear()
        os.environ.update(original_environment)
    _template_environment = fingerprint
    print(json.dumps({'event': 'scheduled_import_template_ready',
                      'cpu_seconds': round(time.process_time() - cpu, 4)}), flush=True)
    return True


def _reset_database():
    from app.db import engine
    from sqlalchemy import event
    # Even the empty template pool is replaced. Future import changes cannot
    # cause child sessions to reuse a parent connection.
    engine.dispose(close=False)
    if engine.dialect.name == 'postgresql':
        @event.listens_for(engine, 'do_connect')
        def identify_child(_dialect, _record, _args, params):
            params['application_name'] = (
                f"walnut:cron:{os.getenv('FLY_MACHINE_ID', 'unknown')}:{os.getpid()}"[:63])


class ForkedJob:
    preloaded = True

    def __init__(self, pid):
        self.pid, self.returncode = pid, None

    def poll(self):
        if self.returncode is None:
            pid, status = os.waitpid(self.pid, os.WNOHANG)
            if pid:
                self.returncode = os.waitstatus_to_exitcode(status)
        return self.returncode

    def wait(self, timeout=None):
        deadline = None if timeout is None else time.monotonic() + timeout
        while self.poll() is None:
            if deadline is not None and time.monotonic() >= deadline:
                raise subprocess.TimeoutExpired('preloaded scheduled job', timeout)
            time.sleep(.02)
        return self.returncode


def _fork_job(module, environment, *, close_resources=(), working_directory='/app',
              reset_database=_reset_database, startup_timeout=10):
    # Two pipes establish a process group and identity before allowing any job
    # code to run. Parent death before acknowledgement means no job executes.
    ready_r, ready_w = os.pipe()
    go_r, go_w = os.pipe()
    sys.stdout.flush(); sys.stderr.flush()
    try:
        pid = os.fork()
    except BaseException:
        for fd in (ready_r, ready_w, go_r, go_w):
            os.close(fd)
        raise
    if pid == 0:
        code = 1
        try:
            os.close(ready_r); os.close(go_w)
            os.setsid()
            signal.signal(signal.SIGTERM, signal.SIG_DFL)
            signal.signal(signal.SIGINT, signal.SIG_DFL)
            for resource in close_resources:
                resource.close()
            os.environ.clear(); os.environ.update(environment)
            os.nice(10)
            os.write(ready_w, b'R'); os.close(ready_w)
            if os.read(go_r, 1) != b'G':
                os._exit(1)
            os.close(go_r)
            os.chdir(working_directory)
            reset_database()
            sys.argv = [module]
            runpy.run_module(module, run_name='__main__', alter_sys=True)
            code = 0
        except SystemExit as exc:
            code = exc.code if isinstance(exc.code, int) else (0 if exc.code is None else 1)
            if exc.code is not None and not isinstance(exc.code, int):
                print(exc.code, file=sys.stderr)
        except BaseException:
            traceback.print_exc()
        finally:
            sys.stdout.flush(); sys.stderr.flush()
            os._exit(code & 255)
    os.close(ready_w); os.close(go_r)
    try:
        if not select.select([ready_r], [], [], startup_timeout)[0]:
            raise TimeoutError('Scheduled child startup timed out')
        if os.read(ready_r, 1) != b'R':
            raise RuntimeError('Scheduled child failed before identity registration')
        _record_identity(pid, environment['WALNUT_JOB_TOKEN'])
        os.write(go_w, b'G')
        return ForkedJob(pid)
    except BaseException:
        # No task was acknowledged if registration failed. Reap this child;
        # do not silently fall back and risk executing a started job twice.
        os.close(go_w); go_w = None
        # This PID remains our unreaped child, so it cannot have been reused.
        # Kill directly even if setsid/readiness has not completed yet.
        try:
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        os.waitpid(pid, 0)
        forget_identity(environment['WALNUT_JOB_TOKEN'])
        raise
    finally:
        os.close(ready_r)
        if go_w is not None:
            os.close(go_w)


def launch(command, environment, *, close_resources=()):
    module = selected_module(command)
    if (module is None or sys.platform != 'linux'
            or environment.get('SCHEDULED_JOB_PRELOAD_ENABLED', '0') != '1'
            or threading.active_count() != 1 or not preload(environment)):
        return None
    return _fork_job(module, environment, close_resources=close_resources)
