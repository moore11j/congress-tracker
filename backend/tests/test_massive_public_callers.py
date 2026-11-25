from datetime import datetime, timedelta, timezone
import pytest
from sqlalchemy.orm import Session
from test_massive_price_core import db, isolated, network, response
from app.services import quote_lookup as quotes
from app.clients import massive_stocks as client
from app.models import PriceCache
from zoneinfo import ZoneInfo
import time


def test_market_api_does_not_fetch_fmp_or_replace_minute_close(monkeypatch, db):
    from app import main
    main._MARKET_QUOTES_RESPONSE_CACHE.clear()
    network(monkeypatch, lambda u, p: response({'status': 'OK', 'tickers': [{
        'ticker': 'AAPL', 'min': {'c': 110, 't': (datetime.now(timezone.utc) - timedelta(minutes=16)).timestamp() * 1000},
        'prevDay': {'c': 100}}]}))
    monkeypatch.setattr(main, '_ticker_meta_with_security_names', lambda *a: {})
    monkeypatch.setattr(main, '_latest_cached_closes_by_symbol', lambda *a: {'AAPL': [{'date': '2026-10-06', 'close': 1}]})
    monkeypatch.setattr(main, '_fetch_eod_light_closes_for_symbol', lambda *a, **k: pytest.fail('FMP close fetch'))
    result = main._build_market_quotes_response('AAPL', db)['items'][0]
    assert result['current_price'] == 110
    assert result['day_change_pct'] == pytest.approx(10)
    assert result['is_delayed'] is True and result['delay_minutes'] == 15
    main._MARKET_QUOTES_RESPONSE_CACHE.clear()


def test_ticker_chart_selected_mode_uses_cached_metrics_and_reports_daily_close(monkeypatch, db):
    from app import main
    for name in ('_quote_snapshot_from_fmp', '_ratios_ttm_from_fmp', '_company_profile_snapshot_from_fmp',
                 '_market_cap_snapshot_from_fmp_for_user_request', '_company_profile_snapshot_from_fmp_for_user_request'):
        monkeypatch.setattr(main, name, lambda *a, **k: pytest.fail('FMP chart dependency'))
    monkeypatch.setattr(main, 'get_current_prices_meta_db', lambda *a, **k: {})
    monkeypatch.setattr(main, 'enqueue_data_enrichment_job', lambda *a, **k: None)
    result = main._build_ticker_chart_quote(db, 'AAPL', [
        {'date': '2026-10-05', 'close': 100, 'volume': 10}, {'date': '2026-10-06', 'close': 110, 'volume': 10}])
    assert result['current_price'] == 110
    assert result['price_kind'] == 'daily_close' and result['is_delayed'] is None
    assert result['market_cap'] is None


def test_financial_valuation_price_uses_selected_provider(monkeypatch, db):
    from app.services import ticker_financials
    monkeypatch.setattr(ticker_financials, "SessionLocal", lambda: Session(db.get_bind()))
    observed = datetime.now(timezone.utc) - timedelta(minutes=16)
    network(monkeypatch, lambda u, p: response({'status': 'OK', 'tickers': [{
        'ticker': 'AAPL', 'min': {'c': 110, 't': observed.timestamp() * 1000}}]}))
    monkeypatch.setattr(ticker_financials, 'ensure_fmp_live_allowed', lambda *a, **k: pytest.fail('FMP price dependency'))
    result = ticker_financials._request_rows('historical-price-eod/light', params={'symbol': 'AAPL'}, symbol='AAPL')
    assert result[0]['price'] == 110 and result[0]['is_delayed'] is True


def test_context_bundle_preserves_delay_and_utc_timestamp(monkeypatch, db):
    from app import main
    observed = datetime.now(timezone.utc) - timedelta(minutes=16)
    network(monkeypatch, lambda u, p: response({'status': 'OK', 'tickers': [{
        'ticker': 'AAPL', 'min': {'c': 110, 't': observed.timestamp() * 1000}}]}))
    quote = main._ticker_context_bundle_quote(db, 'AAPL', {'name': 'Apple'})
    public = main._ticker_context_bundle_public_payload(quote)
    assert public['is_delayed'] is True and public['delay_minutes'] == 15
    assert public['as_of'].endswith('+00:00')
    db.expire_all()
    shell = main._ticker_shell_quote_snapshot(db, 'AAPL', None)
    assert shell['is_delayed'] is True and shell['delay_minutes'] == 15


def test_chart_quote_fallback_preserves_intraday_observation_time(monkeypatch, db):
    from app import main
    observed = (datetime.now(timezone.utc) - timedelta(minutes=16)).replace(microsecond=0)
    network(monkeypatch, lambda u, p: response({'status': 'OK', 'tickers': [{
        'ticker': 'AAPL', 'min': {'c': 110, 't': observed.timestamp() * 1000}}]}))
    monkeypatch.setattr(main, 'enqueue_data_enrichment_job', lambda *a, **k: None)
    result = main._build_ticker_chart_quote(db, 'AAPL', [])
    assert result['current_price'] == 110 and result['is_delayed'] is True
    assert result['asof'] == observed.isoformat()


@pytest.mark.parametrize('surface', ['market', 'context', 'chart'])
def test_selected_public_cache_miss_never_waits_on_provider_http(monkeypatch, db, surface):
    from app import main
    main._MARKET_QUOTES_RESPONSE_CACHE.clear()
    monkeypatch.setattr(quotes, 'get_request_context', lambda: {'path': '/api/tickers/AAPL'})
    monkeypatch.setattr(main, 'get_request_context', lambda: {'path': '/api/tickers/AAPL'})
    queued = []
    monkeypatch.setattr(quotes, '_enqueue_quote_refreshes', lambda symbols, **kwargs: queued.extend(symbols))
    monkeypatch.setattr(main, 'enqueue_data_enrichment_job', lambda *a, **k: None)
    monkeypatch.setattr(client.requests, 'get', lambda *a, **k: pytest.fail('Foreground provider HTTP'))
    if surface == 'market':
        monkeypatch.setattr(main, '_ticker_meta_with_security_names', lambda *a: {})
        monkeypatch.setattr(main, '_latest_cached_closes_by_symbol', lambda *a: {})
        result = main._build_market_quotes_response('AAPL', db)['items'][0]
        assert result['is_stale'] is True
    elif surface == 'context':
        result = main._ticker_context_bundle_quote(db, 'AAPL', {})
        assert result['stale'] is True
    else:
        result = main._build_ticker_chart_quote(db, 'AAPL', [])
    assert result['current_price'] is None
    assert queued == ['AAPL']
    main._MARKET_QUOTES_RESPONSE_CACHE.clear()


def test_selected_quote_fallback_never_declares_unknown_profile_price_fresh(monkeypatch, db):
    from app import main
    monkeypatch.setattr(main, 'get_current_prices_meta_db', lambda *a, **k: {})
    result = main._ticker_context_bundle_quote(db, 'AAPL', {'price': 95, 'quote_as_of': datetime(2026, 10, 1, tzinfo=timezone.utc)})
    assert result['current_price'] == 95 and result['stale'] is True
    assert result['is_delayed'] is None


def test_selected_chart_does_not_acquire_incomplete_financial_metrics_on_request(monkeypatch, db):
    from app import main
    from app.models import FundamentalsCache
    db.add(FundamentalsCache(symbol='AAPL', provider='fmp', status='ok', fetched_at=datetime.now(timezone.utc), price=99))
    db.commit()
    monkeypatch.setattr(main, 'enqueue_data_enrichment_job', lambda *a, **k: None)
    monkeypatch.setattr(main, '_fetch_and_cache_ticker_fundamentals_row', lambda *a: pytest.fail('Synchronous financial HTTP'))
    result = main._build_ticker_chart_quote(db, 'AAPL', [{'date':'2026-11-06','close':110}])
    assert result['current_price'] == 110 and result['price_kind'] == 'daily_close'


@pytest.mark.parametrize('volume', [250, None, 0])
def test_price_volume_uses_prepared_massive_quote_without_foreground_http(monkeypatch, db, volume):
    from app import main
    observed = datetime.now(timezone.utc) - timedelta(minutes=16)
    session = observed.astimezone(ZoneInfo('America/New_York')).date()
    db.add(PriceCache(symbol='AAPL', date=(session-timedelta(days=1)).isoformat(), close=100, volume=1000))
    db.commit()
    quotes.quote_cache_upsert_many(db, {'AAPL': 110}, asof_by_symbol={'AAPL': observed}, metadata_by_symbol={'AAPL': {
        'price': 110, 'asof_ts': observed, 'cached_at': datetime.now(timezone.utc),
        'source': 'massive_minute_snapshot', 'volume': volume, 'is_delayed': True, 'delay_minutes': 15}})
    monkeypatch.setattr(main, '_is_public_api_request_context', lambda: True)
    monkeypatch.setattr(main, 'enqueue_data_enrichment_job', lambda *a, **k: pytest.fail('Fresh quote requested refresh'))
    # A fresh legacy cache must not win after selecting Massive.
    main._TICKER_INTRADAY_PRICE_VOLUME_CACHE['AAPL'] = (time.time()+60, {'price': 999, 'source': 'historical-chart/1min'})
    try:
        result = main._ticker_cached_price_volume_inputs(db, 'AAPL')
        assert result['latest_close'] == 110 and result['previous_close'] == 100
        assert result['latest_date'] == session.isoformat() and result['latest_volume'] == volume
        assert result['latest_source'] == 'massive_minute_snapshot'
        assert result['latest_asof'] == observed.isoformat() and result['delay_minutes'] == 15
        summary = main._ticker_price_volume_summary(db, 'AAPL')
        assert summary['is_delayed'] is True and summary['delay_minutes'] == 15
        assert 'Latest price (delayed 15 minutes): 110.00' in summary['lines']
        assert main._ticker_intraday_price_volume_snapshot('AAPL') == {}
    finally:
        main._TICKER_INTRADAY_PRICE_VOLUME_CACHE.clear()


@pytest.mark.parametrize('kind', ['missing', 'stale', 'future', 'unverified', 'nonfinite'])
def test_price_volume_keeps_daily_evidence_when_selected_quote_is_unusable(monkeypatch, db, kind):
    from app import main
    day = (datetime.now(timezone.utc)-timedelta(days=2)).date().isoformat()
    db.add(PriceCache(symbol='AAPL', date=day, close=100, volume=1000));db.commit()
    observed = datetime.now(timezone.utc)-timedelta(minutes=16)
    meta = {'price': 110, 'asof_ts': observed, 'source': 'massive_minute_snapshot', 'volume': 250, 'is_stale': False}
    if kind=='missing': meta={}
    if kind=='stale': meta['asof_ts']=observed-timedelta(hours=1)
    if kind=='future': meta['asof_ts']=observed+timedelta(hours=1)
    if kind=='unverified': meta['source']='cache_unverified_provider'
    if kind=='nonfinite': meta['price']=float('inf')
    queued=[]
    def prepared(db, symbols, **kwargs):
        assert kwargs['cache_only'] is True and kwargs['allow_cache_write'] is False
        return {'AAPL': meta}
    monkeypatch.setattr(main, 'get_current_prices_meta_db', prepared)
    monkeypatch.setattr(main, '_is_public_api_request_context', lambda: True)
    monkeypatch.setattr(main, 'enqueue_data_enrichment_job', lambda **kwargs: queued.append(kwargs))
    result=main._ticker_cached_price_volume_inputs(db,'AAPL')
    assert result['latest_close']==100 and result['latest_date']==day
    assert result['latest_source']=='daily_cache' and 'is_delayed' not in result
    assert len(queued)==1 and queued[0]['job_type']=='quote'


def test_price_volume_uses_new_york_session_and_cannot_relabel_newer_daily_data(monkeypatch, db):
    from app import main
    fixed=datetime(2026,11,25,0,10,tzinfo=timezone.utc)
    class Clock(datetime):
        @classmethod
        def now(cls,tz=None):return cls.fromtimestamp(fixed.timestamp(),tz=tz)
    observed=Clock(2026,11,24,23,54,tzinfo=timezone.utc)
    monkeypatch.setattr(main,'datetime',Clock)
    monkeypatch.setattr(main,'get_current_prices_meta_db',lambda *a,**k:{'AAPL':{
        'price':110,'asof_ts':observed,'source':'massive_minute_snapshot','is_stale':False,'volume':250}})
    monkeypatch.setattr(main,'_is_public_api_request_context',lambda:True)
    quote=main._ticker_massive_price_volume_snapshot(db,'AAPL')
    assert quote['date']=='2026-11-24' and quote['asof_ts']==observed.isoformat()
    db.add(PriceCache(symbol='AAPL',date='2026-11-25',close=120,volume=1000));db.commit()
    result=main._ticker_cached_price_volume_inputs(db,'AAPL')
    assert result['latest_close']==120 and result['latest_source']=='daily_cache'
    assert result['latest_volume']==1000 and 'is_delayed' not in result
