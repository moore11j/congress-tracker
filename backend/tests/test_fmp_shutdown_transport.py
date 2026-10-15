"""Exercise real retained callers with the final FMP switch off, without HTTP."""
from datetime import date
import time
from types import SimpleNamespace

import pytest
import requests

from app import ingest_house, ingest_senate, ingest_run
from app.clients import fmp
import app.main as main
from app.services import (ai_marketing, fmp_client, fmp_market_snapshot, fmp_news,
                          fundamentals_cache, market_pressure_ingest,
                          operational_intelligence, price_lookup, provider_usage,
                          quote_lookup, ticker_financials, ticker_meta)


@pytest.fixture(autouse=True)
def isolated_provider(monkeypatch):
    monkeypatch.setenv('FMP_API_KEY', 'offline-placeholder-not-a-secret')
    monkeypatch.setenv('FMP_PROVIDER_DISABLED', 'true')
    monkeypatch.setenv('FMP_PROVIDER_ENABLED', 'true')
    monkeypatch.setenv('FMP_PERSIST_USAGE_EVENTS', 'false')
    monkeypatch.setattr(provider_usage, '_STATE', provider_usage._UsageState())
    main._MARKET_QUOTES_EOD_CACHE.clear()
    def forbidden(*args, **kwargs):
        pytest.fail('FMP off switch allowed an outbound HTTP request')
    monkeypatch.setattr(requests.sessions.Session, 'request', forbidden)
    yield
    main._MARKET_QUOTES_EOD_CACHE.clear()


def previous_close():
    return main._fetch_eod_light_closes_for_symbol('AAPL', api_key='offline-placeholder',
        start_day=date(2026, 10, 1), end_day=date(2026, 10, 6), timeout_seconds=1)


def test_previous_close_off_has_no_transport():
    assert previous_close() == []


def test_threaded_previous_close_off_has_no_transport_or_cache_write():
    assert main._latest_eod_light_closes_by_symbol(['AAPL', 'NVDA']) == {}
    assert main._MARKET_QUOTES_EOD_CACHE == {}


def test_existing_previous_close_cache_remains_readable_when_off():
    rows = [{'date': '2026-10-06', 'close': 123.0}]
    main._MARKET_QUOTES_EOD_CACHE['AAPL'] = (rows, time.time() + 60)
    assert main._latest_eod_light_closes_by_symbol(['AAPL', 'NVDA']) == {'AAPL': rows}
    assert set(main._MARKET_QUOTES_EOD_CACHE) == {'AAPL'}


@pytest.mark.parametrize('status', [200, 403, 429, 503])
def test_enabled_previous_close_accounts_actual_response_and_keeps_contract(monkeypatch, status):
    monkeypatch.setenv('FMP_PROVIDER_DISABLED', 'false')
    calls = []
    def response(_session, method, url, **kwargs):
        calls.append((method, url))
        return SimpleNamespace(status_code=status, json=lambda: [
            {'date': '2026-10-05', 'price': 101}, {'date': '2026-10-06', 'close': 102}])
    monkeypatch.setattr(requests.sessions.Session, 'request', response)
    result = previous_close()
    assert len(calls) == 1
    assert result == ([{'date': '2026-10-06', 'close': 102.0}, {'date': '2026-10-05', 'close': 101.0}] if status == 200 else [])
    events = list(provider_usage._STATE.events)
    assert sum(event.get('category') == 'market:previous-close' and event.get('kind') == 'provider_call' for event in events) == 1
    if status >= 400:
        assert any(event.get('category') == 'market:previous-close' and event.get('status_code') == status for event in events)


def test_previous_close_budget_exhaustion_has_no_transport(monkeypatch):
    monkeypatch.setenv('FMP_PROVIDER_DISABLED', 'false')
    def deny(**kwargs):
        raise provider_usage.ProviderBudgetExceeded('provider_budget_exceeded')
    monkeypatch.setattr(main, 'ensure_fmp_live_allowed', deny)
    assert previous_close() == []


@pytest.mark.parametrize('call', [
    lambda: ingest_house._fetch_page(0, 5),
    lambda: ingest_senate._fetch_page(0, 5),
    lambda: ai_marketing.fetch_fmp_articles(),
    lambda: fundamentals_cache._request_rows('ratios-ttm', params={'symbol': 'AAPL'}),
    lambda: fmp._request_stable_rows('profile', category='shutdown:test', symbol='AAPL'),
    lambda: fmp.fetch_insider_trades(symbol='AAPL'),
    lambda: fmp.fetch_institutional_buys(),
    lambda: fmp.fetch_company_screener(),
    lambda: fmp_market_snapshot._request_payload_with_status('market-risk-premium'),
    lambda: operational_intelligence._fmp_rows('earning-call-transcript', {'symbol': 'AAPL'}),
    lambda: price_lookup.get_index_eod_map('SPY', '2026-10-01', '2026-10-06'),
    lambda: quote_lookup.get_index_quote('SPY'),
    lambda: market_pressure_ingest._fetch_latest_intraday_minute('AAPL'),
    lambda: ticker_meta._fetch_symbol_meta('AAPL'),
    lambda: ticker_meta._fetch_cik_meta('0000320193'),
])
def test_retained_fmp_entry_points_stop_before_http(call):
    with pytest.raises(provider_usage.ProviderDisabled):
        call()


def test_controlled_client_explicit_live_override_cannot_override_shutdown():
    with pytest.raises(fmp_client.FMPControlledError, match='provider_disabled'):
        fmp_client.request_fmp_json('profile', category='shutdown:test', symbol='AAPL',
            source='explicit_user_refresh', allow_live_fetch=True, allow_user_request=True)


def test_financial_reader_preserves_unavailable_contract():
    with pytest.raises(ticker_financials.TickerFinancialsUnavailable):
        ticker_financials._request_rows('income-statement', params={'symbol': 'AAPL'}, symbol='AAPL')


def test_news_readers_preserve_unavailable_contracts():
    with pytest.raises(fmp_news.FMPNewsUnavailable):
        fmp_news._request_rows('sec-filings-search/symbol', params={'symbol': 'AAPL'})
    for reader in (fmp_news._request_ticker_news_rows, fmp_news._request_ticker_press_rows):
        rows, unavailable = reader(symbol='AAPL', page=0, limit=10)
        assert rows == [] and unavailable is not None


def test_freshness_and_diagnostics_cannot_escape_shutdown(monkeypatch):
    monkeypatch.setenv('COMPANY_METADATA_PROVIDER', 'fmp')
    assert ingest_run._check_insider_freshness_from_fmp() is None
    result = fundamentals_cache._request_fundamentals_diagnostic('ratios-ttm')
    assert result['status_code'] is None and 'disabled' in result['error']
    assert 'error' in ticker_meta.debug_stable_search_row('AAPL')
