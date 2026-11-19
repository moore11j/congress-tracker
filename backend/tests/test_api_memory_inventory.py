import json
import sys
import threading
from types import ModuleType

from app.services import api_memory_inventory as inventory
from app.services.response_memory_cache import ResponseMemoryCache


def module(cache, lock=None):
    m = ModuleType('fixture')
    m.cache, m.lock = cache, lock
    return m


def test_shared_and_cyclic_objects_count_once_without_values_in_report():
    secret = 'PRIVATE-CUSTOMER-VALUE'
    payload = {secret: [secret]}
    payload['cycle'] = payload
    report = inventory.collect_inventory(modules={'fixture': module(payload)},
        registry=(('fixture', 'cache', None),), clock=lambda: 0)
    row = report['caches'][0]
    assert row['complete'] is True
    assert row['object_bytes_lower_bound'] == sum(sys.getsizeof(v) for v in (payload, secret, payload[secret], 'cycle'))
    assert secret not in json.dumps(report)
    assert payload['cycle'] is payload


def test_unsupported_objects_do_not_execute_user_code():
    class Trap:
        def __sizeof__(self):
            raise AssertionError('private object inspected')
        def __repr__(self):
            raise AssertionError('private object printed')
    report = inventory.measure_builtin_graph({'private': Trap()}, deadline=1, clock=lambda: 0)
    assert report['complete'] is False
    assert 'private' not in json.dumps(report)


def test_node_and_time_budgets_are_explicit_lower_bounds():
    payload = {str(i): [i] for i in range(1000)}
    bounded = inventory.measure_builtin_graph(payload, deadline=1, clock=lambda: 0, max_nodes=10)
    assert bounded['sampled_objects'] <= 10
    assert bounded['complete'] is False
    expired = inventory.measure_builtin_graph(payload, deadline=0, clock=lambda: 1)
    assert expired['sampled_objects'] == 0 and expired['complete'] is False


def test_busy_lock_is_skipped_without_waiting_or_mutating():
    lock = threading.Lock()
    lock.acquire()
    try:
        payload = {'key': 'value'}
        report = inventory.collect_inventory(modules={'fixture': module(payload, lock)},
            registry=(('fixture', 'cache', 'lock'),))
        assert report['caches'] == [{'cache': 'fixture.cache', 'state': 'busy'}]
        assert payload == {'key': 'value'}
    finally:
        lock.release()


def test_missing_module_or_lock_does_not_import_or_assume_safety():
    report = inventory.collect_inventory(modules={'fixture': module({})},
        registry=(('absent.secret', 'cache', None), ('fixture', 'cache', 'missing')))
    assert report['caches'] == [{'cache': 'fixture.cache', 'state': 'missing_lock'}]
    assert 'absent.secret' not in sys.modules


def test_known_bounded_cache_reports_accounting_without_traversal():
    cache = ResponseMemoryCache(max_bytes=4096, clock=lambda: 0)
    cache['PRIVATE-KEY'] = (1, {'secret': 'PRIVATE-VALUE'})
    report = inventory.collect_inventory(modules={'fixture': module(cache)},
        registry=(('fixture', 'cache', None),))
    row = report['caches'][0]
    assert row['state'] == 'accounted' and row['entries'] == 1
    assert row['charged_bytes'] == cache.charged_bytes
    assert 'PRIVATE' not in json.dumps(report)


def test_global_budget_skips_traversal_but_preserves_entry_counts():
    ticks = iter([0, 1, 1])
    report = inventory.collect_inventory(modules={'fixture': module({'x': [1]})},
        registry=(('fixture', 'cache', None),), clock=lambda: next(ticks))
    assert report['caches'] == [{'cache': 'fixture.cache', 'entries': 1, 'state': 'budget'}]


def test_finite_observer_stops_after_288_samples_and_never_logs_error_content():
    class Stop:
        def __init__(self): self.waits = []
        def wait(self, seconds): self.waits.append(seconds); return False
    stop, records = Stop(), []
    def fail(): raise ValueError('SECRET-EXCEPTION')
    inventory.observe(stop, records.append, collect=fail, summary=lambda: {})
    assert len(records) == 288
    assert stop.waits == [60] + [300] * 287
    assert 'SECRET' not in json.dumps(records)
    assert all(r['state'] == 'unavailable' for r in records)


def test_shutdown_before_first_sample_does_no_work():
    stop = threading.Event()
    stop.set()
    records = []
    inventory.observe(stop, records.append, collect=lambda: (_ for _ in ()).throw(AssertionError()))
    assert records == []


def test_wall_deadline_stops_observer_even_when_samples_are_delayed():
    now, records = [0], []
    class Stop:
        def wait(self, seconds): return False
    def delayed():
        now[0] += 86401
        return {}
    inventory.observe(Stop(), records.append, collect=delayed, summary=lambda: {}, clock=lambda: now[0])
    assert len(records) == 1


def test_startup_is_idempotent_and_shutdown_interrupts_initial_wait(monkeypatch):
    monkeypatch.setenv('API_MEMORY_DIAGNOSTICS_ENABLED', 'true')
    monkeypatch.setattr(inventory, '_thread', None)
    monkeypatch.setattr(inventory, '_stop', None)
    inventory.start_observer()
    first = inventory._thread
    try:
        inventory.start_observer()
        assert inventory._thread is first and first.daemon
    finally:
        inventory.stop_observer()
        first.join(timeout=1)
    assert not first.is_alive()


def test_disabled_observer_does_not_spawn(monkeypatch):
    monkeypatch.setenv('API_MEMORY_DIAGNOSTICS_ENABLED', 'false')
    monkeypatch.setattr(inventory, '_thread', None)
    inventory.start_observer()
    assert inventory._thread is None


def test_registered_existing_cache_locks_are_real_and_nonblocking():
    import app.main
    for module_name, attribute, lock_name in inventory.REGISTRY:
        mod = sys.modules.get(module_name)
        if mod is None or not lock_name:
            continue
        assert attribute in vars(mod)
        lock = vars(mod)[lock_name]
        assert lock.acquire(blocking=False)
        lock.release()
