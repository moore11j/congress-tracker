"""Owned CI database regression for the production quote column type."""
from datetime import datetime, timedelta, timezone
import os

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session

from app.db import ensure_quote_cache_market_cap_schema
from app.models import QuoteCache
from app.services import quote_lookup as quotes


@pytest.mark.parametrize('column_type', ['TIMESTAMP', 'TIMESTAMPTZ'])
def test_additive_quote_schema_and_provenance_across_workers(monkeypatch, column_type):
    url = os.getenv('MASSIVE_TEST_POSTGRES_URL')
    if not url:
        pytest.skip('Requires the dedicated CI PostgreSQL service')
    parsed = make_url(url)
    assert os.getenv('CI') == 'true' and parsed.host in {'localhost', '127.0.0.1'}
    assert parsed.database == 'massive_quote_contract_test'
    monkeypatch.setenv('STOCK_PRICE_PROVIDER', 'massive')
    def forbidden(*a, **k):
        pytest.fail('Owned database rehearsal must not request providers or enqueue work')
    monkeypatch.setattr('requests.sessions.Session.request', forbidden)
    monkeypatch.setattr(quotes, '_enqueue_quote_refreshes', forbidden)
    engine = create_engine(url, connect_args={'options': '-c timezone=UTC -c statement_timeout=5000'})
    created = False
    quotes._MASSIVE_META_CACHE.clear()
    try:
        with engine.begin() as connection:
            assert connection.scalar(text("SELECT to_regclass('public.quotes_cache')")) is None
            connection.execute(text(f'CREATE TABLE quotes_cache (symbol TEXT PRIMARY KEY, price DOUBLE PRECISION NOT NULL, asof_ts {column_type} NOT NULL)'))
            connection.execute(text("INSERT INTO quotes_cache VALUES ('LEGACY',100,'2026-11-06 15:00:00')"))
        created = True
        ensure_quote_cache_market_cap_schema(engine)
        ensure_quote_cache_market_cap_schema(engine)
        with Session(engine) as db:
            legacy = db.get(QuoteCache, 'LEGACY')
            assert legacy.price == 100 and legacy.quote_metadata_json is None
            assert quotes._freshness_datetime(legacy.asof_ts) == datetime(2026, 11, 6, 15)
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        observed = now - timedelta(minutes=16)
        metadata = dict(source='massive_minute_snapshot', price_kind='minute_close',
                        cached_at=now, is_delayed=True, delay_minutes=15, previous_close=100)
        with Session(engine) as db:
            quotes.quote_cache_upsert_many(db, {'TEST': 110}, asof_by_symbol={'TEST': observed},
                                           metadata_by_symbol={'TEST': metadata})
        quotes._MASSIVE_META_CACHE.clear()
        with Session(engine) as other_worker:
            value = quotes.get_current_prices_meta_db(other_worker, ['TEST'], cache_only=True)['TEST']
            assert value['price'] == 110 and value['previous_close'] == 100
            assert value['asof_ts'] == observed and value['delay_minutes'] == 15 and not value['is_stale']
        with Session(engine) as late_writer:
            quotes.quote_cache_upsert_many(late_writer, {'TEST': 90}, asof_by_symbol={'TEST': observed-timedelta(seconds=1)})
        with Session(engine) as db:
            row = db.get(QuoteCache, 'TEST')
            assert row.price == 110 and quotes.persisted_quote_metadata(row)['asof_ts'] == observed
        # Old application writes remain possible, but cannot leave trusted
        # metadata attached to a different price after code rollback.
        with engine.begin() as connection:
            connection.execute(text("UPDATE quotes_cache SET price=111 WHERE symbol='TEST'"))
        with Session(engine) as db:
            assert quotes.persisted_quote_metadata(db.get(QuoteCache, 'TEST')) == {}
    finally:
        if created:
            with engine.begin() as connection:
                connection.execute(text('DROP TABLE public.quotes_cache'))
        engine.dispose()
        quotes._MASSIVE_META_CACHE.clear()
