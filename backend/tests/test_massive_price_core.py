"""Price migration checks against real service/cache paths, with FMP network forbidden."""
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.clients import massive_stocks as client
from app.db import Base
from app.models import PriceCache, QuoteCache
from app.services import price_lookup as prices, quote_lookup as quotes
from app.services.price_alert_reference import daily_price_observation


@pytest.fixture(autouse=True)
def isolated(monkeypatch):
    monkeypatch.setenv('STOCK_PRICE_PROVIDER', 'massive')
    monkeypatch.setenv('MASSIVE_API_KEY', 'test-only')
    monkeypatch.setattr(prices, '_is_foreground_request_context', lambda: False)
    monkeypatch.setattr(quotes, 'get_request_context', lambda: {})
    monkeypatch.setattr(quotes, '_enqueue_quote_refreshes', lambda *a, **k: None)
    quotes._MASSIVE_META_CACHE.clear()
    def forbidden(*args, **kwargs):
        pytest.fail('Unexpected network request')
    monkeypatch.setattr(client.requests, 'get', forbidden)
    monkeypatch.setattr(prices, '_fetch_with_backoff', forbidden)
    yield
    quotes._MASSIVE_META_CACHE.clear()


@pytest.fixture
def db():
    engine = create_engine('sqlite://')
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        yield session
    engine.dispose()


def response(payload, status=200):
    return SimpleNamespace(status_code=status, json=lambda: payload)


def network(monkeypatch, handler):
    def get(url, **kwargs):
        assert url.startswith('https://api.massive.com/')
        assert kwargs['headers'] == {'Authorization': 'Bearer test-only'}
        assert kwargs['allow_redirects'] is False
        assert 'apiKey' not in (kwargs.get('params') or {})
        params = kwargs.get('params') or {}
        result = handler(url, params)
        if '/aggs/ticker/' in url and result.status_code == 200:
            from urllib.parse import unquote
            payload = result.json()
            payload.setdefault('ticker', unquote(url.split('/aggs/ticker/')[1].split('/')[0]))
            payload.setdefault('adjusted', params.get('adjusted', 'true') == 'true')
            return response(payload, result.status_code)
        return result
    monkeypatch.setattr(client.requests, 'get', get)


def stamp(day):
    return int(datetime.fromisoformat(day + 'T04:00:00+00:00').timestamp() * 1000)


def test_benchmark_callers_use_selected_provider_without_fmp(monkeypatch):
    observed = datetime.now(timezone.utc) - timedelta(minutes=16)
    def handler(url, params):
        if '/snapshot/' in url:
            return response({'status': 'DELAYED', 'tickers': [
                {'ticker': 'SPY', 'min': {'t': observed.timestamp() * 1000, 'c': 110}}]})
        assert '/aggs/ticker/SPY/' in url
        return response({'status': 'OK', 'results': [{'t': stamp('2026-10-06'), 'c': 100}]})
    network(monkeypatch, handler)
    assert quotes.get_index_quote('SPY') == 110
    assert prices.get_index_eod_map('SPY', '2026-10-06', '2026-10-06') == {'2026-10-06': 100}


def test_benchmark_entitlement_failure_does_not_fall_back(monkeypatch):
    network(monkeypatch, lambda u, p: response({}, status=403))
    with pytest.raises(client.MassiveStocksError, match='benchmark_quote_unavailable'):
        quotes.get_index_quote('SPY')


def test_market_pressure_delayed_quotes_preserve_daily_history_and_metadata(monkeypatch, db):
    from app.models import Security, MarketPressureSnapshot
    from app.services import market_pressure_ingest as ingest, market_pressure
    import json
    now = datetime.now(timezone.utc)
    # Keep the test inside the same New York session even at midnight.
    now = now.replace(hour=19, minute=0, second=0, microsecond=0) - timedelta(days=1)
    observed = now - timedelta(minutes=16)
    db.add(Security(symbol='SPY', name='SPY', asset_class='etf'))
    db.add(PriceCache(symbol='SPY', date=now.date().isoformat(), close=99))
    db.commit()
    monkeypatch.setattr(market_pressure, '_resolve_universe_symbols', lambda *a, **k: ['SPY'])
    monkeypatch.setattr(market_pressure, '_default_confirmation_loader', lambda *a, **k: {})
    network(monkeypatch, lambda u, p: response({'status': 'DELAYED', 'tickers': [
        {'ticker': 'SPY', 'min': {'t': observed.timestamp() * 1000, 'c': 110}, 'prevDay': {'c': 100}}]}))
    # Frozen service clock avoids a synthetic prior-session quote being treated as stale.
    real_snapshots = client.MassiveStocksClient.snapshots
    def snapshots(self, symbols):
        rows = real_snapshots(self, symbols)
        for row in rows.values():
            row['is_stale'] = False
        return rows
    monkeypatch.setattr(client.MassiveStocksClient, 'snapshots', snapshots)
    result = ingest.refresh_market_pressure_universe_snapshot(db, universe='etf', generated_at=now)
    assert result.fetched_count == 1
    quote = db.get(QuoteCache, 'SPY')
    assert quote.asof_ts == observed.replace(tzinfo=None)
    assert quotes.persisted_quote_metadata(quote)['source'] == 'massive_minute_snapshot'
    assert db.query(PriceCache).one().close == 99
    snapshot = db.query(MarketPressureSnapshot).one()
    assert snapshot.price_change_pct == 10
    tile = json.loads(snapshot.tile_json)
    assert tile['priceDelayMinutes'] == 15
    assert tile['priceStartAt'] is None
    # Unverified legacy cache cannot become a valid selected-provider reading.
    monkeypatch.setattr(ingest, 'get_current_prices_meta_db', lambda *a, **k: {
        'SPY': {'price': 111, 'asof_ts': now, 'source': 'cache_unverified_provider'}})
    result = ingest.refresh_market_pressure_universe_snapshot(db, universe='etf', generated_at=now)
    assert result.skipped_count == 1
    assert result.status == 'unavailable'
    db.expire_all()
    assert db.query(MarketPressureSnapshot).one().price is None


def test_snapshot_uses_minute_time_not_day_or_updated_and_preserves_cache_age(monkeypatch, db):
    now = datetime.now(timezone.utc)
    observed = (now - timedelta(minutes=16)).replace(microsecond=0)
    payload = {'status': 'DELAYED', 'tickers': [
        {'ticker': 'AAPL', 'min': {'t': observed.timestamp() * 1000, 'c': 110},
         'day': {'c': 200}, 'updated': now.timestamp() * 1e9},
        {'ticker': 'MSFT', 'day': {'c': 999}, 'updated': now.timestamp() * 1e9}]}
    network(monkeypatch, lambda u, p: response(payload))
    result = quotes.get_current_prices_meta_db(db, ['AAPL', 'MSFT'])
    assert set(result) == {'AAPL'}
    assert result['AAPL']['is_delayed'] is True
    assert result['AAPL']['delay_minutes'] == 15
    assert result['AAPL']['price'] == 110
    assert result['AAPL']['asof_ts'] == observed.replace(tzinfo=None)
    assert db.get(QuoteCache, 'AAPL').asof_ts == observed.replace(tzinfo=None)
    monkeypatch.setattr(client.requests, 'get', lambda *a, **k: pytest.fail('cache hit fetched'))
    cached = quotes.get_current_prices_meta_db(db, ['AAPL'])['AAPL']
    assert cached['asof_ts'] == result['AAPL']['asof_ts']


def test_older_snapshot_cannot_replace_a_newer_quote(monkeypatch, db):
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    quotes.quote_cache_upsert_many(db, {'AAPL': 110}, asof_by_symbol={'AAPL': now})
    quotes.quote_cache_upsert_many(db, {'AAPL': 90}, asof_by_symbol={'AAPL': now - timedelta(minutes=20)})
    db.expire_all()
    assert db.get(QuoteCache, 'AAPL').price == 110
    assert db.get(QuoteCache, 'AAPL').asof_ts == now


def test_delayed_quote_alert_accepts_observation_then_rejects_it_after_30_minutes(monkeypatch, db):
    now = datetime(2026, 10, 6, 15, tzinfo=timezone.utc)
    observed = now - timedelta(minutes=16)
    network(monkeypatch, lambda u, p: response({'status': 'OK', 'tickers': [
        {'ticker': 'AAPL', 'min': {'c': 110, 't': observed.timestamp() * 1000}}]}))
    db.add(PriceCache(symbol='AAPL', date='2026-10-05', close=100, price_source='massive', adjustment_status='split_adjusted_price_return'))
    db.commit()
    quotes.get_current_prices_meta_db(db, ['AAPL'])
    observation = daily_price_observation(db, 'AAPL', now)
    assert observation['change_pct'] == pytest.approx(10)
    assert observation['observed_at'] == observed.isoformat()
    assert daily_price_observation(db, 'AAPL', now + timedelta(minutes=15))['status'] == 'missing_current_price'


@pytest.mark.parametrize('status', [403, 429, 500])
def test_snapshot_failure_never_calls_fmp_or_writes_fresh_cache(monkeypatch, db, status):
    network(monkeypatch, lambda u, p: response({}, status))
    assert quotes.get_current_prices_meta_db(db, ['AAPL']) == {}
    assert db.get(QuoteCache, 'AAPL') is None


@pytest.mark.parametrize('mode', ['cache_only', 'foreground'])
def test_read_paths_do_not_fetch_or_relabel_legacy_cache(monkeypatch, db, mode):
    db.add(QuoteCache(symbol='AAPL', price=100, asof_ts=datetime(2026, 10, 6, 15)))
    db.commit()
    if mode == 'foreground':
        monkeypatch.setattr(quotes, 'get_request_context', lambda: {'path': '/ticker/AAPL'})
    result = quotes.get_current_prices_meta_db(db, ['AAPL'], cache_only=mode == 'cache_only')['AAPL']
    assert result['source'] == 'cache_unverified_provider'
    assert result['is_stale'] is True
    assert 'is_delayed' not in result


def test_split_hydration_uses_raw_bars_and_adjusts_once(monkeypatch, db):
    def handler(url, params):
        if '/aggs/' in url:
            assert params['adjusted'] == 'false'
            return response({'status': 'OK', 'results': [
                {'t': stamp('2026-10-05'), 'c': 100, 'o': 90, 'h': 110, 'l': 80, 'v': 1000},
                {'t': stamp('2026-10-06'), 'c': 11, 'o': 10, 'h': 12, 'l': 9, 'v': 2000}]})
        assert url.endswith('/stocks/v1/splits')
        return response({'status': 'OK', 'results': [
            {'id': 'split', 'ticker': 'AAPL', 'execution_date': '2026-10-06', 'split_from': 1, 'split_to': 10}]})
    network(monkeypatch, handler)
    assert prices.hydrate_split_adjusted_ohlc(db, 'AAPL', '2026-10-05', '2026-10-06') == 2
    first = db.get(PriceCache, ('AAPL', '2026-10-05'))
    assert first.raw_close == 100
    assert first.adjusted_close == first.close == 10
    assert first.open_price == 9
    assert first.adjustment_status == 'split_adjusted_price_return'
    assert first.price_source == 'massive+corporate_actions'
    assert db.get(PriceCache, ('AAPL', '2026-10-06')).close / first.close - 1 == pytest.approx(.1)


def test_split_entitlement_failure_does_not_publish_unadjusted_chart(monkeypatch, db):
    network(monkeypatch, lambda u, p: response({'status': 'OK', 'results': [
        {'t': stamp('2026-10-05'), 'c': 100}]}) if '/aggs/' in u else response({}, 403))
    with pytest.raises(client.MassiveStocksError, match='provider_403'):
        prices.hydrate_split_adjusted_ohlc(db, 'AAPL', '2026-10-05', '2026-10-06')
    assert db.query(PriceCache).count() == 0


def test_prior_session_is_never_cached_under_requested_missing_date(monkeypatch, db):
    network(monkeypatch, lambda u, p: response({'status': 'OK', 'results': [
        {'t': stamp('2026-10-05'), 'c': 100, 'v': 1000}]}))
    result = prices.get_eod_close_with_meta(db, 'AAPL', '2026-10-06')
    assert result['price_date'] == '2026-10-05'
    assert result['fallback_days'] == 1
    assert db.get(PriceCache, ('AAPL', '2026-10-06')) is None
    assert db.get(PriceCache, ('AAPL', '2026-10-05')).price_source == 'massive'


def test_paginated_actions_are_not_double_counted(monkeypatch):
    split = {'id': 's', 'ticker': 'AAPL', 'execution_date': '2026-10-06', 'split_from': 1, 'split_to': 10}
    dividend = {'id': 'd', 'ticker': 'AAPL', 'ex_dividend_date': '2026-10-06', 'cash_amount': .5, 'currency': 'USD'}
    def handler(url, params):
        if 'dividends' in url:
            return response({'status': 'OK', 'results': [dividend, dividend]})
        if 'cursor=' in url:
            return response({'status': 'OK', 'results': [split]})
        return response({'status': 'OK', 'results': [split], 'next_url': 'https://api.massive.com/stocks/v1/splits?cursor=2'})
    network(monkeypatch, handler)
    dividends, splits = prices._fetch_provider_corporate_actions('AAPL', '2026-10-05', '2026-10-06')
    assert dividends == {'2026-10-06': .5}
    assert splits == {'2026-10-06': .1}


def test_pagination_host_is_checked_before_credentials_are_sent(monkeypatch):
    network(monkeypatch, lambda u, p: response({'status': 'OK', 'results': [], 'next_url': 'https://evil.test/data'}))
    with pytest.raises(client.MassiveStocksError, match='invalid_pagination_host'):
        client.MassiveStocksClient().bars('AAPL', '2026-10-05', '2026-10-06')


def test_paginated_partial_history_failure_is_not_published(monkeypatch, db):
    network(monkeypatch, lambda u, p: response({}, 429) if 'cursor=' in u else response({
        'status': 'OK', 'results': [{'t': stamp('2026-10-05'), 'c': 100}],
        'next_url': 'https://api.massive.com/v2/aggs/ticker/AAPL?cursor=2'}))
    assert prices.get_eod_close_with_meta(db, 'AAPL', '2026-10-06')['status'] == 'provider_unavailable'
    assert db.query(PriceCache).count() == 0


def test_share_class_mapping_and_adjusted_daily_volume(monkeypatch):
    def handler(url, params):
        assert '/BRK.B/' in url
        assert params['adjusted'] == 'true'
        return response({'status': 'OK', 'results': [{'t': stamp('2026-10-05'), 'c': 100, 'v': 1234}]})
    network(monkeypatch, handler)
    series, volumes, symbol = prices._fetch_provider_eod_price_volume_series('BRK-B', '2026-10-05', '2026-10-06')
    assert series == {'2026-10-05': 100}
    assert volumes == {'2026-10-05': 1234}
    assert symbol == 'BRK.B'


def test_intraday_uses_market_time_deduplicates_and_limits_sessions(monkeypatch):
    def handler(url, params):
        assert '/range/1/minute/' in url
        rows = [{'t': datetime.fromisoformat(day + 'T15:00:00+00:00').timestamp() * 1000,
                 'c': 100, 'o': 99, 'h': 101, 'l': 98, 'v': 10} for day in ['2026-10-05', '2026-10-06']]
        return response({'status': 'OK', 'results': rows + rows})
    network(monkeypatch, handler)
    result = prices.get_intraday_chart_series_with_fallback('AAPL', '2026-10-05', '2026-10-06', limit_days=1)
    assert result == [{'date': '2026-10-06T15:00:00Z', 'close': 100, 'open': 99, 'high': 101, 'low': 98, 'volume': 10}]


def test_recent_refresh_stores_canonical_symbol_and_source_metadata(monkeypatch, db):
    network(monkeypatch, lambda u, p: response({'status': 'OK', 'results': [
        {'t': stamp('2026-10-06'), 'c': 100, 'v': 1000}]}))
    result = prices.refresh_recent_price_history(db, 'BRK-B', lookback_days=1, end_date='2026-10-06')
    assert result['symbol'] == 'BRK-B'
    cached = db.get(PriceCache, ('BRK-B', '2026-10-06'))
    assert cached.price_source == 'massive'
    assert cached.provider_symbol == 'BRK.B'
    assert cached.adjusted_close == cached.close == 100
    assert db.get(PriceCache, ('BRK.B', '2026-10-06')) is None


def test_conflicting_duplicate_action_is_rejected(monkeypatch):
    split = {'id': 's', 'ticker': 'AAPL', 'execution_date': '2026-10-06', 'split_from': 1, 'split_to': 10}
    network(monkeypatch, lambda u, p: response({'status': 'OK', 'results': [split, {**split, 'split_to': 20}]}))
    with pytest.raises(client.MassiveStocksError, match='conflicting_action_identity'):
        client.MassiveStocksClient().actions('AAPL', '2026-10-05', '2026-10-06', splits_only=True)


def test_snapshot_memory_recomputes_staleness_even_before_cache_expiry():
    import time
    observed = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(minutes=40)
    quotes._MASSIVE_META_CACHE['AAPL'] = ({'symbol': 'AAPL', 'price': 100, 'asof_ts': observed,
        'source': 'massive_minute_snapshot', 'is_stale': False}, time.monotonic() + 60)
    result = quotes.get_current_prices_meta_db(None, ['AAPL'])['AAPL']
    assert result['is_stale'] is True
    assert result['asof_ts'] == observed


def test_restart_reuses_persisted_provenance_without_refetch(monkeypatch, db):
    observed = datetime.now(timezone.utc) - timedelta(minutes=16)
    network(monkeypatch, lambda u, p: response({'status': 'OK', 'tickers': [{
        'ticker': 'AAPL', 'min': {'c': 110, 't': observed.timestamp() * 1000},
        'prevDay': {'c': 100}, 'todaysChangePerc': 999}]}))
    first = quotes.get_current_prices_meta_db(db, ['AAPL'])['AAPL']
    assert first['change_percent'] == pytest.approx(10)
    quotes._MASSIVE_META_CACHE.clear()
    db.expire_all()
    monkeypatch.setattr(client.requests, 'get', lambda *a, **k: pytest.fail('Persisted hit fetched'))
    second = quotes.get_current_prices_meta_db(db, ['AAPL'])['AAPL']
    assert second['asof_ts'] == first['asof_ts']
    assert second['source'] == 'massive_minute_snapshot'
    assert second['is_delayed'] is True and second['delay_minutes'] == 15
    assert second['change_percent'] == pytest.approx(10)


def test_legacy_overwrite_clears_old_provider_metadata(monkeypatch, db):
    observed = datetime.now(timezone.utc).replace(tzinfo=None)
    meta = {'source': 'massive_minute_snapshot', 'is_delayed': True, 'cached_at': observed}
    quotes.quote_cache_upsert_many(db, {'AAPL': 100}, asof_by_symbol={'AAPL': observed}, metadata_by_symbol={'AAPL': meta})
    quotes.quote_cache_upsert_many(db, {'AAPL': 101})
    db.expire_all()
    assert quotes.persisted_quote_metadata(db.get(QuoteCache, 'AAPL')) == {}


def test_metadata_is_rejected_if_observation_changes(db):
    import json
    observed = datetime(2026, 10, 6, 15)
    db.add(QuoteCache(symbol='AAPL', price=101, asof_ts=observed, quote_metadata_json=json.dumps({
        'price': 100, 'asof_ts': observed.isoformat(), 'source': 'massive_minute_snapshot'})))
    db.commit()
    assert quotes.get_current_prices_meta_db(db, ['AAPL'], cache_only=True)['AAPL']['source'] == 'cache_unverified_provider'


def test_legacy_schema_upgrade_is_repeatable_and_preserves_observation():
    from sqlalchemy import text
    from app.db import ensure_quote_cache_market_cap_schema
    engine = create_engine('sqlite://')
    with engine.begin() as connection:
        connection.execute(text('CREATE TABLE quotes_cache (symbol TEXT PRIMARY KEY, price FLOAT, asof_ts DATETIME)'))
        connection.execute(text("INSERT INTO quotes_cache VALUES ('AAPL', 100, '2026-10-06 15:00:00')"))
    ensure_quote_cache_market_cap_schema(engine)
    ensure_quote_cache_market_cap_schema(engine)
    with Session(engine) as db:
        row = db.get(QuoteCache, 'AAPL')
        assert row.price == 100 and row.asof_ts == datetime(2026, 10, 6, 15)
        assert row.quote_metadata_json is None
    engine.dispose()












def test_expired_memory_yields_to_newer_durable_quote_without_provider_call(monkeypatch, db):
    import time
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    old = now - timedelta(minutes=20)
    new = now - timedelta(minutes=16)
    quotes._MASSIVE_META_CACHE['AAPL'] = ({'symbol': 'AAPL', 'price': 100, 'asof_ts': old,
        'source': 'massive_minute_snapshot'}, time.monotonic() - 1)
    quotes.quote_cache_upsert_many(db, {'AAPL': 110}, asof_by_symbol={'AAPL': new}, metadata_by_symbol={
        'AAPL': {'source': 'massive_minute_snapshot', 'cached_at': now, 'price_kind': 'minute_close',
                 'is_delayed': True, 'delay_minutes': 15}})
    result = quotes.get_current_prices_meta_db(db, ['AAPL'])['AAPL']
    assert result['price'] == 110 and result['asof_ts'] == new and not result['is_stale']


def test_aware_write_preserves_utc_binding_and_older_write_cannot_replace(db):
    east = timezone(timedelta(hours=-4))
    observed = datetime(2026, 10, 6, 11, tzinfo=east)
    quotes.quote_cache_upsert_many(db, {'AAPL': 100}, asof_by_symbol={'AAPL': observed},
        metadata_by_symbol={'AAPL': {'source': 'massive_minute_snapshot', 'cached_at': observed}})
    quotes.quote_cache_upsert_many(db, {'AAPL': 99}, asof_by_symbol={'AAPL': observed - timedelta(minutes=1)})
    db.expire_all()
    row = db.get(QuoteCache, 'AAPL')
    assert row.price == 100 and row.asof_ts == datetime(2026, 10, 6, 15)
    assert quotes.persisted_quote_metadata(row)['asof_ts'] == row.asof_ts


def test_future_cached_observation_is_stale(db):
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    quotes.quote_cache_upsert_many(db, {'AAPL': 100}, asof_by_symbol={'AAPL': now + timedelta(minutes=1)},
        metadata_by_symbol={'AAPL': {'source': 'massive_minute_snapshot', 'cached_at': now}})
    assert quotes.get_current_prices_meta_db(db, ['AAPL'], cache_only=True)['AAPL']['is_stale']


def test_foreground_missing_quote_queues_without_transport(monkeypatch, db):
    queued = []
    monkeypatch.setattr(quotes, 'get_request_context', lambda: {'path': '/ticker/AAPL'})
    monkeypatch.setattr(quotes, '_enqueue_quote_refreshes', lambda symbols, **kw: queued.extend(symbols))
    assert quotes.get_current_prices_meta_db(db, ['AAPL']) == {}
    assert queued == ['AAPL']
    queued.clear()
    assert quotes.get_current_prices_meta_db(db, ['AAPL'], cache_only=True) == {}
    assert queued == []


@pytest.mark.parametrize('change', [
    {'ticker': 'MSFT'}, {'adjusted': False}, {'adjusted': 'true'}, {'ticker': None}, {'adjusted': None},
])
def test_aggregate_identity_and_split_basis_mismatch_never_writes(monkeypatch, db, change):
    network(monkeypatch, lambda u, p: response({'status': 'OK', 'ticker': 'AAPL', 'adjusted': True,
        'results': [{'t': stamp('2026-10-06'), 'c': 100}], **change}))
    result = prices.get_eod_close_with_meta(db, 'AAPL', '2026-10-06')
    assert result['status'] == 'provider_unavailable'
    assert db.query(PriceCache).count() == 0


def test_each_aggregate_page_must_match_requested_identity(monkeypatch):
    def handler(url, params):
        return response({'status': 'OK', 'ticker': 'MSFT' if 'cursor' in url else 'AAPL',
            'adjusted': True, 'results': [{'t': stamp('2026-10-06'), 'c': 100}],
            **({} if 'cursor' in url else {'next_url': 'https://api.massive.com/v2/aggs/ticker/AAPL?cursor=2'})})
    network(monkeypatch, handler)
    with pytest.raises(client.MassiveStocksError, match='identity_or_adjustment'):
        client.MassiveStocksClient().bars('AAPL', '2026-10-05', '2026-10-06')


@pytest.mark.parametrize('conflict', [False, True])
def test_duplicate_aggregate_identity_is_deduplicated_or_rejected(monkeypatch, db, conflict):
    row = {'t': stamp('2026-10-06'), 'c': 100}
    network(monkeypatch, lambda u, p: response({'status': 'OK', 'results': [row, {**row, 'c': 101 if conflict else 100}]}))
    result = prices.get_eod_close_with_meta(db, 'AAPL', '2026-10-06')
    assert result['close'] == (None if conflict else 100)
    assert db.query(PriceCache).count() == (0 if conflict else 1)


def test_conflicting_snapshot_identity_never_writes_or_returns_partial_batch(monkeypatch, db):
    observed = datetime.now(timezone.utc) - timedelta(minutes=16)
    row = {'ticker': 'AAPL', 'min': {'t': observed.timestamp() * 1000, 'c': 100}}
    network(monkeypatch, lambda u, p: response({'status': 'OK', 'tickers': [row,
        {**row, 'min': {**row['min'], 'c': 101}}]}))
    assert quotes.get_current_prices_meta_db(db, ['AAPL']) == {}
    assert db.query(QuoteCache).count() == 0


def test_daily_history_rejects_unfinished_session_and_multiple_daily_windows(monkeypatch, db):
    from datetime import date
    monkeypatch.setattr(prices, 'get_expected_latest_market_date', lambda: date(2026, 10, 5))
    network(monkeypatch, lambda u, p: response({'status': 'OK', 'results': [
        {'t': stamp('2026-10-05'), 'c': 100}, {'t': stamp('2026-10-06'), 'c': 101}]}))
    result = prices.get_eod_close_with_meta(db, 'AAPL', '2026-10-06')
    assert result['price_date'] == '2026-10-05'
    assert db.get(PriceCache, ('AAPL', '2026-10-06')) is None
    network(monkeypatch, lambda u, p: response({'status': 'OK', 'results': [
        {'t': stamp('2026-10-05'), 'c': 100}, {'t': stamp('2026-10-05') + 60000, 'c': 101}]}))
    assert prices._selected_massive_bars('AAPL', '2026-10-05', '2026-10-06', adjusted=True) == ({}, None)


def test_legacy_market_pressure_preserves_existing_daily_close_and_quote_time(monkeypatch, db):
    from app.services import market_pressure_ingest as ingest
    monkeypatch.setenv('STOCK_PRICE_PROVIDER', 'fmp')
    observed = datetime(2026, 10, 6, 15, tzinfo=timezone.utc)
    db.add(PriceCache(symbol='AAPL', date='2026-10-06', close=90))
    db.commit()
    ingest._write_price_caches(db, {'AAPL': 100}, {'AAPL': observed}, observed.date())
    assert db.get(PriceCache, ('AAPL', '2026-10-06')).close == 90
    assert db.get(QuoteCache, 'AAPL').asof_ts == observed.replace(tzinfo=None)
    assert db.get(QuoteCache, 'AAPL').quote_metadata_json is None


@pytest.mark.parametrize('legacy', ['reference', 'quote', 'current_daily'])
def test_selected_price_alerts_do_not_use_unverified_cache(monkeypatch, db, legacy):
    now = datetime(2026, 10, 6, 15, tzinfo=timezone.utc)
    observed = now - timedelta(minutes=16)
    source = {} if legacy == 'reference' else {'price_source': 'massive', 'adjustment_status': 'split_adjusted_price_return'}
    db.add(PriceCache(symbol='AAPL', date='2026-10-05', close=100, **source))
    if legacy == 'current_daily':
        db.add(PriceCache(symbol='AAPL', date='2026-10-06', close=110))
    else:
        db.add(QuoteCache(symbol='AAPL', price=110, asof_ts=observed.replace(tzinfo=None)))
    db.commit()
    evidence = daily_price_observation(db, 'AAPL', now)
    assert evidence['change_pct'] is None
    assert evidence['status'] == ('missing_reference_close' if legacy == 'reference' else 'missing_current_price')


def test_selected_reference_warming_replaces_unverified_prior_without_missing_as_zero(monkeypatch, db):
    from app.services import price_alert_reference as refs
    from sqlalchemy.orm import sessionmaker
    now = datetime(2026, 10, 6, 15, tzinfo=timezone.utc)
    db.add(PriceCache(symbol='AAPL', date='2026-10-05', close=90))
    db.commit()
    calls = []
    def refresh(session, ticker, **kwargs):
        calls.append((ticker, kwargs))
        row = session.get(PriceCache, (ticker, '2026-10-05'))
        row.close=100;row.adjusted_close=100;row.price_source='massive';row.adjustment_status='split_adjusted_price_return'
        session.commit()
    monkeypatch.setattr(refs, 'refresh_recent_price_history', refresh)
    factory = sessionmaker(bind=db.get_bind())
    result = refs.refresh_daily_price_references(factory, now=now, symbols=['AAPL'])
    assert result['fetched'] == result['available'] == 1 and result['unavailable'] == 0
    assert calls == [('AAPL', {'lookback_days': 1, 'end_date': '2026-10-05'})]
    repeat = refs.refresh_daily_price_references(factory, now=now, symbols=['AAPL'])
    assert repeat['fetched'] == 0 and repeat['available'] == 1 and len(calls) == 1


@pytest.mark.parametrize('compatible', [False, True])
def test_partial_daily_write_only_preserves_compatible_metadata(db, compatible):
    db.add(PriceCache(symbol='AAPL', date='2026-10-05', close=100, adjusted_close=100,
        raw_close=1000, open_price=90, high_price=110, low_price=80, volume=10, day_volume=10,
        dollar_volume=1000, split_coefficient=.1, dividend_amount=0,
        price_source='massive+corporate_actions' if compatible else 'fmp', provider_symbol='AAPL',
        adjustment_status='split_adjusted_price_return'))
    db.commit()
    prices._safe_cache_upsert(db, 'AAPL', '2026-10-05', 100, adjusted_close_value=100,
        price_source='massive', provider_symbol='AAPL', adjustment_status='split_adjusted_price_return')
    db.commit();db.expire_all()
    row = db.get(PriceCache, ('AAPL', '2026-10-05'))
    assert row.raw_close == (1000 if compatible else None)
    assert row.open_price == (90 if compatible else None)
    assert row.volume == (10 if compatible else None)
    assert row.price_source == 'massive'
    # A later adjusted close changes the basis even with the same provider.
    prices._safe_cache_upsert(db, 'AAPL', '2026-10-05', 10, 20, adjusted_close_value=10,
        price_source='massive', provider_symbol='AAPL', adjustment_status='split_adjusted_price_return')
    db.commit();db.expire_all()
    row = db.get(PriceCache, ('AAPL', '2026-10-05'))
    assert row.raw_close is None and row.open_price is None and row.split_coefficient is None
    assert row.volume == 20 and row.dollar_volume == 200


def test_selected_market_pressure_missing_previous_close_reports_partial(monkeypatch, db):
    from app.models import Security
    from app.services import market_pressure_ingest as ingest, market_pressure
    now = datetime.now(timezone.utc)
    db.add(Security(symbol='SPY', name='SPY', asset_class='etf'));db.commit()
    monkeypatch.setattr(market_pressure, '_resolve_universe_symbols', lambda *a, **k: ['SPY'])
    monkeypatch.setattr(market_pressure, '_default_confirmation_loader', lambda *a, **k: {})
    monkeypatch.setattr(ingest, 'get_current_prices_meta_db', lambda *a, **k: {
        'SPY': {'price': 100, 'asof_ts': now, 'source': 'massive_minute_snapshot', 'is_stale': False}})
    result = ingest.refresh_market_pressure_universe_snapshot(db, universe='etf', generated_at=now)
    assert result.status == 'partial' and result.fetched_count == 1
