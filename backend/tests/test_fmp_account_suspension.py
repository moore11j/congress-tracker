"""Account refusal backoff preserves caller errors and makes no live requests."""
import json
import requests
import pytest

from app.services import provider_usage as usage, fundamentals_cache, fmp_client, price_lookup


@pytest.fixture(autouse=True)
def isolated(monkeypatch):
    monkeypatch.setenv('FMP_API_KEY', 'offline-key-a')
    monkeypatch.setenv('FMP_PROVIDER_ENABLED', 'true')
    monkeypatch.setenv('FMP_PROVIDER_DISABLED', 'false')
    monkeypatch.setenv('FMP_PERSIST_USAGE_EVENTS', 'false')
    monkeypatch.setattr(usage, '_STATE', usage._UsageState())
    monkeypatch.setattr(usage, '_is_user_request', lambda: False)
    clock = [100.0]
    monkeypatch.setattr(usage.time, 'monotonic', lambda: clock[0])
    price_lookup._PROVIDER_EOD_SERIES_CACHE.clear()
    yield clock
    price_lookup._PROVIDER_EOD_SERIES_CACHE.clear()


def response(body=None, status=403):
    value = requests.Response()
    value.status_code = status
    value.url = 'https://financialmodelingprep.com/stable/profile'
    value._content = body if isinstance(body, bytes) else json.dumps(
        body if body is not None else {'Error Message': 'Account suspended. Please contact support@example.test more details.'}).encode()
    return value


def record(value, key='offline-key-a'):
    usage.record_provider_response(category='fundamentals:profile', status_code=value.status_code,
                                  response=value, request_api_key=key)


def test_thousand_symbol_refusal_only_makes_one_request_and_preserves_error_contract(monkeypatch):
    calls=[]
    def get(*args, **kwargs):
        calls.append(kwargs['params']['symbol'])
        return response()
    monkeypatch.setattr(requests, 'get', get)
    with pytest.raises(fundamentals_cache.FMPClientError, match='auth failed'):
        fundamentals_cache._request_rows('profile', params={'symbol': 'S0'})
    for index in range(1, 1000):
        with pytest.raises(usage.ProviderAccountSuspended):
            fundamentals_cache._request_rows('profile', params={'symbol': f'S{index}'})
    assert calls == ['S0']
    assert usage._STATE.counters['provider_call'] == 1
    assert usage._STATE.counters['provider_error'] == 1
    events=json.dumps(list(usage._STATE.events))
    assert 'offline-key-a' not in events and 'support@example.test' not in events
    assert usage._STATE.account_suspension_key_hash not in events


@pytest.mark.parametrize('body,status', [
    ({'Error Message': 'Endpoint requires premium subscription'},403),
    ({'error': 'Invalid API key'},403),
    ({'message': 'Account suspended.'},200),
    ({'message': 'Account suspended.'},429),
    ({'message': 'Account suspended.'},503),
    ({'message': 'Account suspended.'},401),
    ([{'message': 'Account suspended.'}],403),
    ({'message': 1},403),
    ({'message': 'Account is not suspended.'},403),
    (b'{invalid',403),
    (b'\xff',403),
    (b'X'*8193,403),
    (b'['*1500+b']'*1500,403),
])
def test_other_denials_or_untrusted_bodies_do_not_block_other_endpoints(body,status):
    record(response(body,status))
    usage.ensure_fmp_live_allowed(category='price:eod')
    assert usage._STATE.counters['provider_call'] == 1


@pytest.mark.parametrize('field',['Error Message','error','message'])
def test_explicit_account_refusal_blocks_other_categories_and_user_override(field):
    record(response({field:'Account suspended. Please contact support.'}))
    for category in ('quote','price:eod','fundamentals:profile'):
        with pytest.raises(usage.ProviderAccountSuspended):
            usage.ensure_fmp_live_allowed(category=category,allow_user_request=True)
    assert usage._STATE.counters['provider_call'] == 0


def test_backoff_expires_and_key_rotation_can_recover_without_restart(isolated,monkeypatch):
    record(response())
    isolated[0]=699.99
    with pytest.raises(usage.ProviderAccountSuspended):
        usage.ensure_fmp_live_allowed(category='profile')
    monkeypatch.setenv('FMP_API_KEY','offline-key-b')
    usage.ensure_fmp_live_allowed(category='profile')
    monkeypatch.setenv('FMP_API_KEY','offline-key-a')
    isolated[0]=700.0
    usage.ensure_fmp_live_allowed(category='profile')
    assert usage._STATE.counters['provider_call']==2


def test_late_response_for_old_credential_does_not_block_current_credential(monkeypatch):
    monkeypatch.setenv('FMP_API_KEY','offline-key-b')
    record(response(),key='offline-key-a')
    usage.ensure_fmp_live_allowed(category='profile')


def test_missing_response_or_credential_does_not_arm_circuit():
    usage.record_provider_response(category='profile',status_code=403)
    record(response(),key=None)
    usage.ensure_fmp_live_allowed(category='profile')


def test_controlled_client_preserves_error_reason_on_backoff(monkeypatch):
    calls=[]
    monkeypatch.setattr(requests,'get',lambda *a,**k:(calls.append(1),response())[1])
    with pytest.raises(fmp_client.FMPControlledError) as first:
        fmp_client.request_fmp_json('profile',category='fundamentals:profile',source='scheduled_job')
    assert first.value.reason=='provider_entitlement'
    with pytest.raises(fmp_client.FMPControlledError) as second:
        fmp_client.request_fmp_json('profile',category='fundamentals:profile',source='scheduled_job')
    assert second.value.reason=='provider_account_suspended' and len(calls)==1


def test_shutdown_retains_priority_over_temporary_backoff(monkeypatch):
    record(response())
    monkeypatch.setenv('FMP_PROVIDER_DISABLED','true')
    with pytest.raises(usage.ProviderDisabled):
        usage.ensure_fmp_live_allowed(category='profile',allow_user_request=True)


def test_provider_reset_clears_process_local_suspension():
    record(response());usage.reset_provider_usage()
    usage.ensure_fmp_live_allowed(category='profile')


def test_price_series_preserves_cached_values_and_never_caches_a_denial(monkeypatch,isolated):
    calls=[]
    def get(*args,**kwargs):
        calls.append(1)
        return response() if len(calls)==1 else response([{'date':'2026-11-05','close':100}],200)
    monkeypatch.setattr(price_lookup,'_fetch_with_backoff',get)
    cache_key=('historical-price-eod/full','CACHED','2026-11-01','2026-11-05')
    existing=[{'date':'2026-11-05','close':99}]
    price_lookup._PROVIDER_EOD_SERIES_CACHE[cache_key]=(usage.time.time()+60,existing)
    def fetch(symbol):
        return price_lookup._fetch_provider_eod_payload('historical-price-eod/full',symbol,
            '2026-11-01','2026-11-05','offline-key-a')
    assert fetch('AAPL').status_code==403
    for symbol in ('AAPL','NVDA','MSFT'):
        assert fetch(symbol) is None
    assert fetch('CACHED') is existing
    assert set(price_lookup._PROVIDER_EOD_SERIES_CACHE)=={cache_key}
    assert len(calls)==1
    isolated[0]=700.0
    assert fetch('AAPL')==[{'date':'2026-11-05','close':100}]
    assert len(calls)==2 and fetch('CACHED') is existing
