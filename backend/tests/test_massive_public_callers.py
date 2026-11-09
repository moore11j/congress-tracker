from datetime import datetime, timedelta, timezone
import pytest
from sqlalchemy.orm import Session
from test_massive_price_core import db, isolated, network, response
from app.services import quote_lookup as quotes
from app.clients import massive_stocks as client


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
