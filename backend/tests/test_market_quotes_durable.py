from datetime import datetime, timedelta, timezone
import time

import pytest

from test_massive_price_core import db, isolated
from app import main
from app.clients import massive_stocks
from app.services import quote_lookup as quotes


@pytest.mark.parametrize('symbol', sorted(main._MARKET_QUOTES_STATIC_META))
@pytest.mark.parametrize('memory_present', [False, True])
def test_selected_hot_quote_reads_newer_durable_observation(monkeypatch, db, symbol, memory_present):
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    observed = now - timedelta(minutes=16)
    quotes.quote_cache_upsert_many(db, {symbol: 129.6462}, asof_by_symbol={symbol: observed},
        metadata_by_symbol={symbol: {'source': 'massive_minute_snapshot', 'cached_at': now,
            'previous_close': 145.98, 'price_kind': 'minute_close', 'is_delayed': True, 'delay_minutes': 15}})
    if memory_present:
        quotes._MASSIVE_META_CACHE[symbol] = ({'symbol': symbol, 'price': 129.16,
            'asof_ts': now - timedelta(hours=1), 'source': 'massive_minute_snapshot'}, time.monotonic() - 1)
    main._MARKET_QUOTES_RESPONSE_CACHE.clear()
    main._MARKET_QUOTES_RESPONSE_INFLIGHT.clear()
    opened, closed = [], []
    def session():
        opened.append(True)
        return db
    monkeypatch.setattr(main, 'SessionLocal', session)
    monkeypatch.setattr(db, 'close', lambda: closed.append(True))
    monkeypatch.setattr(quotes, 'get_request_context', lambda: {'path': '/api/market/quotes'})
    monkeypatch.setattr(massive_stocks.requests, 'get', lambda *a, **k: pytest.fail('Foreground provider call'))
    monkeypatch.setattr(quotes, '_enqueue_quote_refreshes', lambda *a, **k: pytest.fail('Fresh durable quote queued'))
    monkeypatch.setattr(quotes, 'quote_cache_upsert_many', lambda *a, **k: pytest.fail('Reader wrote quote cache'))
    try:
        result = main._build_market_quotes_response(symbol)
        quote = result['items'][0]
        assert quote['current_price'] == 129.6462
        assert quote['as_of'] == observed.replace(tzinfo=timezone.utc).isoformat()
        assert quote['is_stale'] is False
        assert quote['is_delayed'] is True and quote['delay_minutes'] == 15
        assert quote['day_change_pct'] == pytest.approx((129.6462 - 145.98) / 145.98 * 100)
        assert opened == closed == [True]
        # Response-cache hits keep the existing inexpensive path.
        assert main._build_market_quotes_response(symbol) == result
        assert opened == closed == [True]
    finally:
        main._MARKET_QUOTES_RESPONSE_CACHE.clear()
        main._MARKET_QUOTES_RESPONSE_INFLIGHT.clear()
