"""Default-off, one-attempt-per-process GNU allocator reclamation trial.

This releases allocator-owned free pages, not live objects or cache entries.
Native allocator locks have no enforceable Python wall-time limit. A production
trial requires explicit review; it is not evidence of sustained capacity.
"""
import ctypes
from functools import lru_cache
import os
import math
from pathlib import Path
import sys
import threading
import time

MIN_FREE_BYTES = 128 * 1024**2
MIN_RSS_KIB = 512 * 1024
QUALIFY_SECONDS = 300
PAD_BYTES = 8 * 1024**2
MAX_LEASE_SECONDS = 3600


@lru_cache(maxsize=1)
def _native_trim():
    if sys.platform != 'linux':
        return None
    try:
        libc = ctypes.CDLL(None)
        getattr(libc, 'gnu_get_libc_version')
        trim = libc.malloc_trim
        trim.argtypes = [ctypes.c_size_t]
        trim.restype = ctypes.c_int
        return trim
    except (AttributeError, OSError):
        return None


def _rss_kib():
    try:
        return next(int(line.split()[1]) for line in Path('/proc/self/status').read_text().splitlines()
                    if line.startswith('VmRSS:'))
    except (OSError, ValueError, StopIteration):
        return None


class ReclaimTrial:
    def __init__(self, *, clock=time.monotonic, resolve=_native_trim, rss=_rss_kib):
        self._clock, self._resolve, self._rss = clock, resolve, rss
        self._deadline = clock() + MAX_LEASE_SECONDS
        self._eligible_at = None
        self._consumed = False
        self._lock = threading.Lock()

    def observe(self, sample):
        # Serialize trial decisions; never wait on another caller or retry a call.
        if not self._lock.acquire(blocking=False):
            return None
        try:
            if self._consumed or self._clock() >= self._deadline:
                return None
            native = sample.get('native_allocator', {})
            free = native.get('counters', {}).get('fordblks')
            rss = sample.get('process_status', {}).get('VmRSS')
            qualifies = (native.get('state') == 'observed' and native.get('api') == 'glibc_mallinfo2'
                         and type(free) is int and free >= MIN_FREE_BYTES
                         and type(rss) is int and rss >= MIN_RSS_KIB)
            now = self._clock()
            if not qualifies:
                self._eligible_at = None
                return None
            if self._eligible_at is None or now < self._eligible_at:
                self._eligible_at = now
                return None
            if now - self._eligible_at < QUALIFY_SECONDS:
                return None
            # Consume before symbol resolution or native invocation, including failure.
            self._consumed = True
            result = dict(event='api_memory_reclaim', mode='once', observed_rss_kib=rss,
                          observed_allocator_free_bytes=free, pad_bytes=PAD_BYTES)
            started = self._clock()
            try:
                trim = self._resolve()
                if trim is None:
                    result['state'] = 'unsupported'
                    return result
                result['released_pages'] = trim(PAD_BYTES) == 1
                result['state'] = 'attempted'
            except Exception:
                result['state'] = 'unavailable'  # No exception text or heap values.
            finally:
                result['duration_ms'] = round((self._clock() - started) * 1000, 3)
            try:
                after = self._rss()
                if type(after) is int and after >= 0:
                    result['after_rss_kib'] = after
            except Exception:
                pass
            return result
        finally:
            self._lock.release()


_trial = ReclaimTrial()


@lru_cache(maxsize=1)
def _lease_until():
    # An absolute expiry survives restarts. Reject unbounded/future leases once,
    # rather than allowing an invalid far-future setting to become eligible later.
    try:
        until = float(os.getenv('API_MEMORY_RECLAIM_UNTIL', ''))
        remaining = until - time.time()
        if math.isfinite(until) and 0 < remaining <= MAX_LEASE_SECONDS:
            return until
    except ValueError:
        pass
    return None


def maybe_reclaim(sample):
    # Only this exact opt-in value enables the trial; no implicit true/default.
    if os.getenv('API_MEMORY_RECLAIM_MODE', 'off') != 'once':
        return None
    until = _lease_until()
    if until is None or time.time() >= until:
        return None
    return _trial.observe(sample)
