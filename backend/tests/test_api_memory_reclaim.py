import copy
import json
import sys
import threading
import time
import os

import pytest

from app.services import api_memory_reclaim as reclaim
from app.services import api_memory_inventory as inventory


@pytest.fixture(autouse=True)
def lease_isolation(monkeypatch):
    reclaim._lease_until.cache_clear()
    monkeypatch.delenv('API_MEMORY_RECLAIM_MODE', raising=False)
    monkeypatch.delenv('API_MEMORY_RECLAIM_UNTIL', raising=False)
    yield
    reclaim._lease_until.cache_clear()


def sample():
    return {'process_status': {'VmRSS': reclaim.MIN_RSS_KIB},
            'native_allocator': {'state': 'observed', 'api': 'glibc_mallinfo2',
                                 'counters': {'fordblks': reclaim.MIN_FREE_BYTES}}}


def trial(trim=lambda pad: 1):
    now = [0]
    instance = reclaim.ReclaimTrial(clock=lambda: now[0], resolve=lambda: trim, rss=lambda: 12345)
    return instance, now


@pytest.mark.parametrize('mode', [None, '', 'off', 'true', '1', 'ONCE', ' once '])
def test_default_and_non_exact_opt_in_never_enter_trial(monkeypatch, mode):
    class Trap:
        def observe(self, value):
            raise AssertionError('Disabled trial entered')
    monkeypatch.setattr(reclaim, '_trial', Trap())
    if mode is None:
        monkeypatch.delenv('API_MEMORY_RECLAIM_MODE', raising=False)
    else:
        monkeypatch.setenv('API_MEMORY_RECLAIM_MODE', mode)
    assert reclaim.maybe_reclaim(sample()) is None


def test_two_qualifying_observations_and_no_second_attempt(monkeypatch):
    calls = []
    instance, now = trial(lambda pad: calls.append(pad) or 1)
    monkeypatch.setenv('API_MEMORY_RECLAIM_MODE', 'once')
    monkeypatch.setenv('API_MEMORY_RECLAIM_UNTIL', str(time.time() + 3500))
    monkeypatch.setattr(reclaim, '_trial', instance)
    original = sample()
    saved = copy.deepcopy(original)
    assert reclaim.maybe_reclaim(original) is None
    now[0] = 299
    assert reclaim.maybe_reclaim(original) is None
    now[0] = 300
    result = reclaim.maybe_reclaim(original)
    assert result['state'] == 'attempted' and result['released_pages'] is True
    assert result['after_rss_kib'] == 12345
    assert result['pid'] == os.getpid() and result['duration_ms'] >= 0
    assert calls == [8 * 1024**2] and original == saved
    now[0] = 999999
    assert reclaim.maybe_reclaim(original) is None
    assert len(calls) == 1


@pytest.mark.parametrize('field,value', [('rss', 0), ('rss', True), ('rss', None),
                                      ('free', 0), ('free', True), ('free', '999999999'),
                                      ('state', 'unavailable'), ('api', 'other')])
def test_invalid_or_low_measurement_resets_qualification(field, value):
    instance, now = trial()
    instance.observe(sample())
    changed = sample()
    if field == 'rss':
        changed['process_status']['VmRSS'] = value
    elif field == 'free':
        changed['native_allocator']['counters']['fordblks'] = value
    else:
        changed['native_allocator'][field] = value
    now[0] = 300
    assert instance.observe(changed) is None
    assert instance.observe(sample()) is None
    now[0] = 600
    assert instance.observe(sample())['state'] == 'attempted'


@pytest.mark.parametrize('outcome', ['unsupported', 'failed', 'no_pages'])
def test_unsupported_failure_and_no_release_are_terminal_without_private_details(outcome):
    calls = []
    def resolve():
        calls.append('resolve')
        if outcome == 'unsupported':
            return None
        def trim(pad):
            if outcome == 'failed':
                raise RuntimeError('PRIVATE-HEAP-CONTENT')
            return 0
        return trim
    now = [0]
    instance = reclaim.ReclaimTrial(clock=lambda: now[0], resolve=resolve)
    instance.observe(sample())
    now[0] = 300
    result = instance.observe(sample())
    assert 'PRIVATE' not in json.dumps(result)
    assert result['state'] == {'unsupported': 'unsupported', 'failed': 'unavailable', 'no_pages': 'attempted'}[outcome]
    if outcome == 'no_pages':
        assert result['released_pages'] is False
    assert instance.observe(sample()) is None and calls == ['resolve']


def test_concurrent_caller_does_not_wait_or_repeat_native_call():
    entered, release = threading.Event(), threading.Event()
    calls = []
    def trim(pad):
        calls.append(pad)
        entered.set()
        assert release.wait(2)
        return 1
    instance, now = trial(trim)
    instance.observe(sample())
    now[0] = 300
    worker = threading.Thread(target=lambda: instance.observe(sample()))
    worker.start()
    try:
        assert entered.wait(2)
        assert instance.observe(sample()) is None
    finally:
        release.set()
        worker.join(2)
    assert not worker.is_alive() and len(calls) == 1


@pytest.mark.parametrize('until', ['', 'nan', 'inf', '-inf', 'private', '999999999999', '0'])
def test_invalid_expired_or_unbounded_lease_never_enters_trial(monkeypatch, until):
    class Trap:
        def observe(self, value):
            raise AssertionError('Invalid lease entered trial')
    monkeypatch.setenv('API_MEMORY_RECLAIM_MODE', 'once')
    monkeypatch.setenv('API_MEMORY_RECLAIM_UNTIL', until)
    monkeypatch.setattr(reclaim, '_trial', Trap())
    assert reclaim.maybe_reclaim(sample()) is None


def test_lease_expiry_stops_between_qualifying_samples(monkeypatch):
    wall = [1000]
    monkeypatch.setattr(reclaim.time, 'time', lambda: wall[0])
    monkeypatch.setenv('API_MEMORY_RECLAIM_MODE', 'once')
    monkeypatch.setenv('API_MEMORY_RECLAIM_UNTIL', '1300')
    instance, now = trial(lambda pad: pytest.fail('Expired lease trimmed'))
    monkeypatch.setattr(reclaim, '_trial', instance)
    assert reclaim.maybe_reclaim(sample()) is None
    wall[0], now[0] = 1300, 300
    assert reclaim.maybe_reclaim(sample()) is None


def test_monotonic_deadline_also_limits_trial_lifetime():
    instance, now = trial(lambda pad: pytest.fail('Expired trial trimmed'))
    instance.observe(sample())
    now[0] = reclaim.MAX_LEASE_SECONDS
    assert instance.observe(sample()) is None


def test_shutdown_after_sample_prevents_native_action(monkeypatch):
    stop = threading.Event()
    class NoInitialWait:
        def wait(self, seconds):
            return stop.is_set()
        def is_set(self):
            return stop.is_set()
    records = []
    def collect():
        stop.set()
        return {}
    monkeypatch.setattr(inventory, 'maybe_reclaim', lambda value: pytest.fail('Shutdown reclamation'))
    inventory.observe(NoInitialWait(), records.append, collect=collect, summary=sample)
    assert len(records) == 1 and records[0]['event'] == 'api_memory_inventory'


def test_observer_emits_numeric_reclamation_receipt_separately(monkeypatch):
    class Stop:
        calls = 0
        def wait(self, seconds):
            self.calls += 1
            return self.calls > 1
        def is_set(self):
            return False
    result = {'event': 'api_memory_reclaim', 'state': 'attempted', 'released_pages': False}
    monkeypatch.setattr(inventory, 'maybe_reclaim', lambda value: result)
    records = []
    inventory.observe(Stop(), records.append, collect=lambda: {}, summary=sample)
    assert len(records) == 2 and records[-1] == result


@pytest.mark.skipif(sys.platform != 'linux', reason='GNU/Linux ABI check')
def test_real_gnu_trim_preserves_live_python_values():
    trim = reclaim._native_trim()
    if trim is None:
        pytest.skip('GNU malloc unavailable')
    assert trim.argtypes == [reclaim.ctypes.c_size_t]
    live = [bytes(str(i), 'ascii') * 8192 for i in range(128)]
    saved = list(live)
    assert trim(reclaim.PAD_BYTES) in (0, 1)
    assert live == saved
