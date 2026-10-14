"""Cold ingestion startup must not initialize the public web application."""
import os
from pathlib import Path
import subprocess
import sys


def test_cold_job_import_and_disabled_profile_do_not_load_web_app():
    code = '''
import importlib.abc, socket, sys, types
def blocked_network(*args, **kwargs):
    raise AssertionError('No network during startup verification')
socket.socket.connect = blocked_network
class RefuseWebApplication(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname in {'app.main', 'app.routers.events'}:
            raise AssertionError('Ingestion imported the web application')
blocker = RefuseWebApplication()
sys.meta_path.insert(0, blocker)
from app import ingest_run
assert 'app.main' not in sys.modules
assert ingest_run._build_parser().parse_args(['--job', 'monitoring-alert-refresh']).job == 'monitoring-alert-refresh'
assert ingest_run._run_profile_overview_prewarm_job() == {
    'job': 'profile-overview-prewarm', 'status': 'skipped',
    'reason': 'profile_overview_prewarm_disabled', 'cache_entries_refreshed': 0}
assert 'app.main' not in sys.modules
# On explicit use, call the same application's warmer with its exact keyword.
sys.meta_path.remove(blocker)
calls = []
application = types.ModuleType('app.main')
sentinel = {'status': 'ok', 'cache_entries_refreshed': 3}
def warm(*, force_refresh=False):
    calls.append(force_refresh)
    return sentinel
application._run_profile_overview_prewarm = warm
sys.modules['app.main'] = application
assert ingest_run._run_profile_overview_prewarm() is sentinel
assert ingest_run._run_profile_overview_prewarm(force_refresh=True) is sentinel
assert calls == [False, True]
def failing(*, force_refresh=False):
    raise ValueError('warmer failed')
application._run_profile_overview_prewarm = failing
try:
    ingest_run._run_profile_overview_prewarm(force_refresh=True)
except ValueError as exc:
    assert str(exc) == 'warmer failed'
else:
    raise AssertionError('Warm failure was swallowed')
'''
    env = {**os.environ, 'DATABASE_URL': 'sqlite:///:memory:',
           'PROFILE_OVERVIEW_PREWARM_ENABLED': 'false'}
    result = subprocess.run([sys.executable, '-c', code],
        cwd=Path(__file__).resolve().parents[1], env=env,
        capture_output=True, text=True, timeout=45)
    assert result.returncode == 0, result.stderr[-4000:]


def test_explicit_backfill_verification_retains_filter_checks(monkeypatch):
    import types
    import pytest
    from app.backfill_events_from_trades import verify_event_filters
    from app.routers import events

    session = object()
    calls = []
    lengths = iter([0, 0, 1, 2])

    def reader(**kwargs):
        assert kwargs['db'] is session
        assert kwargs['limit'] == 50
        calls.append(kwargs)
        return types.SimpleNamespace(items=list(range(next(lengths))))

    monkeypatch.setattr(events, 'list_events', reader)
    verify_event_filters(session)
    assert [c.get('symbol') for c in calls] == ['ZZZZZZ', None, None, None]
    assert [c.get('min_amount') for c in calls] == [None, 999_999_999, None, None]
    assert [c.get('recent_days') for c in calls] == [None, None, 1, 30]

    for results, message in [([1], 'symbol=ZZZZZZ'), ([0, 1], 'min_amount=999999999'),
                             ([0, 0, 2, 1], 'recent_days=1')]:
        lengths = iter(results)
        with pytest.raises(RuntimeError, match=message):
            verify_event_filters(session)

    def unavailable(**_kwargs):
        raise ValueError('reader unavailable')

    monkeypatch.setattr(events, 'list_events', unavailable)
    with pytest.raises(ValueError, match='reader unavailable'):
        verify_event_filters(session)
