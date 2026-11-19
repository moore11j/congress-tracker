"""Bound retained response copies. Callers keep their existing cache locks.

The charge covers keys and recursively measured built-in payload objects plus
entry overhead. It is a retention budget, not a bound on process RSS or response
construction. Unsupported or excessively complex values simply bypass caching.
"""
from collections.abc import MutableMapping
from datetime import date, datetime
from decimal import Decimal
import sys
import time


def _charge(key, value, limit):
    total, nodes, ancestors = 512, 0, set()

    def visit(obj, depth=0):
        nonlocal total, nodes
        nodes += 1
        if depth > 64 or nodes > 100_000:
            raise ValueError('cache complexity budget')
        total += sys.getsizeof(obj)
        if total > limit:
            raise ValueError('cache byte budget')
        if type(obj) in (str, bytes, int, float, bool, type(None), date, datetime, Decimal):
            return
        if type(obj) not in (tuple, list, dict) or id(obj) in ancestors:
            raise ValueError('unsupported cache object')
        ancestors.add(id(obj))
        try:
            if type(obj) is dict:
                for item_key, item_value in obj.items():
                    visit(item_key, depth + 1)
                    visit(item_value, depth + 1)
            else:
                for item in obj:
                    visit(item, depth + 1)
        finally:
            ancestors.remove(id(obj))

    visit(key)
    visit(value)
    return total


class ResponseMemoryCache(MutableMapping):
    """FIFO admission with expiry cleanup and a fixed entry and byte budget.

    Stored objects must not be mutated. Freshness and any allowed stale fallback
    remain decisions of the reader; retain_until only controls reclamation when
    another response is stored. A cache miss never changes the response itself.
    """
    def __init__(self, *, max_bytes, max_entries=512, retain_until=lambda value: value[0], clock=None):
        if max_bytes <= 0 or max_entries <= 0:
            raise ValueError('Invalid response cache budget')
        self.max_bytes, self.max_entries = max_bytes, max_entries
        self.retain_until, self.clock = retain_until, clock or (lambda: time.time())
        self._entries = {}
        self.charged_bytes = 0
        self.evictions = 0
        self.bypasses = 0

    def __getitem__(self, key):
        return self._entries[key][0]

    def __iter__(self):
        return iter(self._entries)

    def __len__(self):
        return len(self._entries)

    def __delitem__(self, key):
        _, size = self._entries.pop(key)
        self.charged_bytes -= size

    def clear(self):
        self._entries.clear()
        self.charged_bytes = 0

    def __setitem__(self, key, value):
        now = self.clock()
        for old_key, (old_value, _) in list(self._entries.items()):
            if self.retain_until(old_value) <= now:
                del self[old_key]
        # A rejected replacement must not leave the previous response cached.
        self.pop(key, None)
        try:
            size = _charge(key, value, self.max_bytes)
        except ValueError:
            self.bypasses += 1
            return
        if self.retain_until(value) <= now:
            return
        while self._entries and (len(self) >= self.max_entries or self.charged_bytes + size > self.max_bytes):
            del self[next(iter(self))]
            self.evictions += 1
        self._entries[key] = (value, size)
        self.charged_bytes += size
