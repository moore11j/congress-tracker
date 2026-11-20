"""Finite, bounded API memory observations; never emit cache keys or values.

This measures only named caches in already-loaded modules. Partial traversals
are explicitly lower bounds; totals cannot be equated with process RSS.
"""
import itertools
import ctypes
from functools import lru_cache
import json
import logging
import os
from pathlib import Path
import sys
import threading
import time

from app.services.response_memory_cache import ResponseMemoryCache
from app.services.api_memory_reclaim import maybe_reclaim


_MAIN = (
    ('_PUBLIC_GET_RESPONSE_CACHE', '_PUBLIC_GET_RESPONSE_CACHE_LOCK'),
    ('_MEMBER_ANALYTICS_CACHE', '_MEMBER_ANALYTICS_CACHE_LOCK'),
    ('_PUBLIC_OUTCOME_LEDGER_RESPONSE_CACHE', '_PUBLIC_OUTCOME_LEDGER_RESPONSE_CACHE_LOCK'),
    ('_PROFILE_OVERVIEW_RESPONSE_CACHE', '_PROFILE_OVERVIEW_RESPONSE_CACHE_LOCK'),
    ('_TICKER_PROFILE_RESPONSE_CACHE', '_TICKER_RESPONSE_CACHE_LOCK'),
    ('_TICKER_SIGNALS_SUMMARY_CACHE', '_TICKER_RESPONSE_CACHE_LOCK'),
    ('_TICKER_CHART_BUNDLE_CACHE', '_TICKER_RESPONSE_CACHE_LOCK'),
    ('_TICKER_CONTEXT_BUNDLE_MEMORY_CACHE', '_TICKER_CONTEXT_BUNDLE_MEMORY_CACHE_LOCK'),
    ('_MARKET_QUOTES_EOD_CACHE', '_MARKET_QUOTES_EOD_CACHE_LOCK'),
    ('_MARKET_QUOTES_RESPONSE_CACHE', '_MARKET_QUOTES_RESPONSE_CACHE_LOCK'),
    *((name, None) for name in ('_CONGRESS_IDENTITY_CACHE', '_TICKER_QUOTE_SNAPSHOT_CACHE',
        '_TICKER_INTRADAY_PRICE_VOLUME_CACHE', '_TICKER_RATIOS_TTM_CACHE',
        '_TICKER_PROFILE_SNAPSHOT_CACHE', '_TICKER_MARKET_CAP_SNAPSHOT_CACHE')),
)
REGISTRY = tuple(('app.main', name, lock) for name, lock in _MAIN) + (
    ('app.services.cache_store', '_MEMORY', '_LOCK'),
    ('app.routers.events', '_EVENTS_RESPONSE_CACHE', '_EVENTS_RESPONSE_CACHE_LOCK'),
    ('app.routers.events', '_INSIDER_SUMMARY_CACHE', '_INSIDER_SUMMARY_CACHE_LOCK'),
    ('app.routers.events', '_INSIDER_ANALYTICS_CACHE', '_INSIDER_ANALYTICS_CACHE_LOCK'),
    ('app.routers.screener', '_SCREENER_RESPONSE_CACHE', '_SCREENER_RESPONSE_LOCK'),
    ('app.services.research_briefs', '_SEC_COMPANY_FACTS_CACHE', '_SEC_CACHE_LOCK'),
    ('app.services.research_briefs', '_SEC_COMPANY_RECORD_CACHE', '_SEC_CACHE_LOCK'),
    ('app.services.ticker_financials', '_CACHE', '_CACHE_LOCK'),
    ('app.services.fmp_news', '_CACHE', '_CACHE_LOCK'),
    ('app.services.fmp_market_snapshot', '_CACHE', '_CACHE_LOCK'),
    ('app.services.price_lookup', '_PROVIDER_EOD_SERIES_CACHE', None),
    ('app.services.price_lookup', '_NEGATIVE_EOD_CACHE', None),
    ('app.services.quote_lookup', '_QUOTE_CACHE', None),
    ('app.services.quote_lookup', '_QUOTE_META_CACHE', None),
    ('app.services.quote_lookup', '_MISS_CACHE', None),
)


def measure_builtin_graph(value, *, deadline, clock=time.monotonic, max_nodes=5000):
    """Inspect built-in containers only, using at most max_nodes identity slots."""
    seen, stack = set(), [iter((value,))]
    size, complete = 0, True
    while stack:
        if len(seen) >= max_nodes or clock() >= deadline:
            complete = False
            break
        try:
            obj = next(stack[-1])
        except StopIteration:
            stack.pop()
            continue
        kind = type(obj)
        if kind not in (dict, list, tuple, set, frozenset, str, bytes, int, float, bool, type(None)):
            complete = False
            continue  # No user-defined traversal, repr, or __sizeof__ calls.
        identity = id(obj)
        if identity in seen:
            continue
        seen.add(identity)
        size += sys.getsizeof(obj)
        if kind is dict:
            stack.append(itertools.chain.from_iterable(obj.items()))
        elif kind in (list, tuple, set, frozenset):
            stack.append(iter(obj))
    return dict(object_bytes_lower_bound=size, sampled_objects=len(seen), complete=complete)


def collect_inventory(*, modules=None, registry=REGISTRY, clock=time.monotonic):
    modules = sys.modules if modules is None else modules
    start = clock()
    deadline = start + .020
    result = []
    for module_name, attribute, lock_name in registry:
        module = modules.get(module_name)
        if module is None:
            continue  # Never import code or create another API instance.
        values = vars(module)
        cache = values.get(attribute)
        if type(cache) not in (dict, ResponseMemoryCache):
            continue
        row = dict(cache=module_name + '.' + attribute)
        lock = values.get(lock_name) if lock_name else None
        if lock_name and lock is None:
            row['state'] = 'missing_lock'
            result.append(row)
            continue
        acquired = lock.acquire(blocking=False) if lock is not None else False
        if lock is not None and not acquired:
            row['state'] = 'busy'
            result.append(row)
            continue
        try:
            row['entries'] = len(cache)
            if type(cache) is ResponseMemoryCache:
                row.update(state='accounted', charged_bytes=cache.charged_bytes,
                           max_bytes=cache.max_bytes, max_entries=cache.max_entries,
                           evictions=cache.evictions, bypasses=cache.bypasses)
            elif clock() >= deadline:
                row['state'] = 'budget'
            else:
                row.update(measure_builtin_graph(cache, deadline=min(deadline, clock() + .002), clock=clock))
                row['state'] = 'measured'
        except RuntimeError:
            row.update(state='changed_during_sample', complete=False)
        finally:
            if acquired:
                lock.release()
        result.append(row)
    return dict(caches=result, measurement_ms=round((clock()-start)*1000, 3))


def process_summary():
    fields = {'VmRSS', 'VmData', 'RssAnon', 'RssFile', 'Threads'}
    try:
        status = {key: int(value.split()[0]) for line in Path('/proc/self/status').read_text().splitlines()
                  if ':' in line for key, value in (line.split(':', 1),) if key in fields}
    except (OSError, ValueError):
        status = {}
    return dict(pid=os.getpid(), process_status=status, python_allocated_blocks=sys.getallocatedblocks(),
                python_threads=threading.active_count(), native_allocator=allocator_summary())


_MALLINFO_FIELDS = ('arena', 'ordblks', 'smblks', 'hblks', 'hblkhd', 'usmblks',
                    'fsmblks', 'uordblks', 'fordblks', 'keepcost')


class _Mallinfo2(ctypes.Structure):
    # GNU malloc.h ABI: ten size_t fields, not deprecated mallinfo's int fields.
    _fields_ = [(name, ctypes.c_size_t) for name in _MALLINFO_FIELDS]


@lru_cache(maxsize=1)
def _allocator_reader():
    if not sys.platform.startswith('linux'):
        return None
    try:
        library = ctypes.CDLL(None)
        # Do not assume a similarly named API on another libc has the GNU ABI.
        getattr(library, 'gnu_get_libc_version')
        reader = library.mallinfo2
        reader.argtypes = []
        reader.restype = _Mallinfo2
        return reader
    except (AttributeError, OSError):
        return None


def allocator_summary(*, read=None):
    """Allocator-reported counters only, not a full heap census or an RSS total.

    No heap traversal, pointers, allocation contents, trim, GC or malloc tuning.
    Native allocator locking has no Python-enforceable wall-time ceiling; report
    elapsed time separately from the existing cooperative cache traversal budget.
    """
    started = time.monotonic()
    try:
        reader = read if read is not None else _allocator_reader()
        if reader is None:
            return {'state': 'unsupported'}
        measured = reader()
        counters = {name: getattr(measured, name) for name in _MALLINFO_FIELDS}
        if any(type(value) is not int or value < 0 for value in counters.values()):
            return {'state': 'unavailable'}
        return dict(state='observed', api='glibc_mallinfo2', counters=counters,
                    measurement_ms=round((time.monotonic()-started)*1000, 3))
    except Exception:
        return {'state': 'unavailable'}  # Never emit exception text or native values beyond the counters.


def observe(stop, emit, *, collect=collect_inventory, summary=process_summary, clock=time.monotonic):
    # One finite day per API startup; no scheduler/database/provider work.
    deadline = clock() + 86400
    if stop.wait(60):
        return
    for index in range(288):
        if clock() >= deadline:
            return
        try:
            sample = dict(event='api_memory_inventory', sequence=index + 1, **summary(), **collect())
            emit(sample)
            if not stop.is_set():
                reclaimed = maybe_reclaim(sample)
                if reclaimed is not None:
                    emit(reclaimed)
        except Exception:
            emit(dict(event='api_memory_inventory', sequence=index + 1, state='unavailable'))
        remaining = deadline - clock()
        if index == 287 or remaining <= 0 or stop.wait(min(300, remaining)):
            return


_guard = threading.Lock()
_thread = None
_stop = None


def start_observer():
    global _thread, _stop
    if os.getenv('API_MEMORY_DIAGNOSTICS_ENABLED', 'true').strip().lower() not in {'true', '1', 'yes'}:
        return
    with _guard:
        if _thread is not None and _thread.is_alive():
            return
        _stop = threading.Event()
        def emit(record):
            logging.getLogger('uvicorn.error').info('api_memory_inventory %s', json.dumps(record, separators=(',', ':')))
        _thread = threading.Thread(target=observe, args=(_stop, emit), daemon=True, name='api-memory-inventory')
        _thread.start()


def stop_observer():
    with _guard:
        if _stop is not None:
            _stop.set()
