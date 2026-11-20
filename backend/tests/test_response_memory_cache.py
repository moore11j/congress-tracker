import asyncio
import json
import time

import pytest
from starlette.responses import Response

from app.services.response_memory_cache import ResponseMemoryCache


def test_expired_unique_keys_are_reclaimed_without_another_read():
    now = [0]
    cache = ResponseMemoryCache(max_bytes=100_000, clock=lambda: now[0])
    for i in range(1000):
        cache[str(i)] = (now[0] + 1, {'data': 'x' * 1000})
        now[0] += 2
    assert list(cache) == ['999']
    assert cache.charged_bytes < 2000


def test_active_payloads_respect_bytes_and_entry_limit_and_account_for_pop_clear():
    cache = ResponseMemoryCache(max_bytes=4000, max_entries=2, clock=lambda: 0)
    for i in range(20):
        cache[str(i)] = (100, {'data': str(i) * 400})
        assert len(cache) <= 2
        assert cache.charged_bytes <= 4000
    assert cache.evictions > 0
    before = cache.charged_bytes
    cache.pop('19')
    assert cache.charged_bytes < before
    cache.clear()
    assert cache.charged_bytes == 0
    assert not cache


def test_oversized_replacement_removes_old_value_without_evicting_unrelated_hit():
    cache = ResponseMemoryCache(max_bytes=4000, clock=lambda: 0)
    cache['a'] = (100, {'data': 'old'})
    cache['b'] = (100, {'data': 'keep'})
    cache['a'] = (100, {'data': 'x' * 4000})
    assert 'a' not in cache
    assert cache['b'] == (100, {'data': 'keep'})


def test_shared_objects_are_charged_conservatively_and_cycles_bypass():
    cache = ResponseMemoryCache(max_bytes=4000, clock=lambda: 0)
    shared = ['x' * 500]
    cache['a'] = (100, [shared] * 20)
    assert not cache
    cyclic = []
    cyclic.append(cyclic)
    cache['b'] = (100, cyclic)
    assert not cache


def test_stale_retention_and_monotonic_profile_clock():
    now = [10]
    cache = ResponseMemoryCache(max_bytes=4000, retain_until=lambda v: v[0] + 20,
                                clock=lambda: now[0])
    cache['stale'] = (0, b'old')
    cache['new'] = (10, b'new')
    assert 'stale' in cache
    now[0] = 20
    cache['third'] = (20, b'new')
    assert 'stale' not in cache
    assert 'new' in cache


@pytest.mark.parametrize('cache_name,setter,getter', [
    ('_MEMBER_ANALYTICS_CACHE', '_member_analytics_cache_set', '_member_analytics_cache_get'),
    ('_PUBLIC_OUTCOME_LEDGER_RESPONSE_CACHE', '_public_outcome_ledger_cache_set', '_public_outcome_ledger_cache_get'),
    ('_TICKER_PROFILE_RESPONSE_CACHE', None, None),
    ('_TICKER_SIGNALS_SUMMARY_CACHE', None, None),
    ('_TICKER_CHART_BUNDLE_CACHE', None, None),
])
def test_real_payload_helpers_keep_copy_isolation_and_return_uncached_large_values(monkeypatch, cache_name, setter, getter):
    import app.main as main
    cache = ResponseMemoryCache(max_bytes=4000)
    monkeypatch.setattr(main, cache_name, cache)
    put = getattr(main, setter) if setter else lambda k, v: main._ticker_response_cache_set(cache, k, v)
    get = getattr(main, getter) if getter else lambda k: main._ticker_response_cache_get(cache, k)
    payload = {'items': [{'value': 123, 'source': 'original'}]}
    assert put('a', payload) == payload
    payload['items'][0]['value'] = 999
    hit = get('a')
    assert hit['items'][0]['value'] == 123
    hit['items'][0]['value'] = 888
    assert get('a')['items'][0]['value'] == 123
    large = {'data': 'x' * 5000}
    assert put('a', large) == large
    assert get('a') is None


def test_public_middleware_capacity_bypass_preserves_body_and_unblocks_waiter(monkeypatch):
    import app.main as main
    from test_public_get_response_cache import _request
    cache = ResponseMemoryCache(max_bytes=4000)
    monkeypatch.setattr(main, '_PUBLIC_GET_RESPONSE_CACHE', cache)
    monkeypatch.setattr(main, '_PUBLIC_GET_RESPONSE_INFLIGHT', {})
    body = json.dumps({'items': ['x' * 6000]}).encode()
    calls = []

    async def run():
        first_started, release = asyncio.Event(), asyncio.Event()
        async def downstream(request):
            calls.append(1)
            first_started.set()
            await release.wait()
            return Response(body, media_type='application/json', headers={'x-source': 'retained'})
        request = _request('/api/tickers/TEST/chart-bundle')
        first = asyncio.create_task(main.public_get_response_cache(request, downstream))
        await first_started.wait()
        second = asyncio.create_task(main.public_get_response_cache(request, downstream))
        await asyncio.sleep(0)
        release.set()
        return await asyncio.wait_for(asyncio.gather(first, second), timeout=2)

    for result in asyncio.run(run()):
        assert result.status_code == 200
        assert result.body == body
        assert result.headers['x-source'] == 'retained'
        assert result.headers['x-walnut-public-cache'] == 'bypass'
    assert len(calls) == 2
    assert not cache
    assert not main._PUBLIC_GET_RESPONSE_INFLIGHT


def test_profile_database_hits_also_obey_retention_limit(monkeypatch):
    import app.main as main
    cache = ResponseMemoryCache(max_bytes=4000, max_entries=2,
        retain_until=lambda v: v[0] + main.PROFILE_OVERVIEW_CACHE_TTL_SECONDS, clock=time.monotonic)
    monkeypatch.setattr(main, '_PROFILE_OVERVIEW_RESPONSE_CACHE', cache)
    monkeypatch.setattr(main, '_profile_overview_database_cache_get', lambda *a, **kw: {'data': 'x' * 300})
    for i in range(20):
        result = main._cached_profile_overview_response(None, ('test', i), lambda: pytest.fail('unexpected rebuild'))
        assert result == {'data': 'x' * 300}
    assert len(cache) <= 2
    assert cache.charged_bytes <= 4000
