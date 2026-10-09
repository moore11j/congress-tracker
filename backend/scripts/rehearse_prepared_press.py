"""Replay public production press caches through the exact release, without IO."""
import argparse
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import sys
from types import SimpleNamespace
from unittest.mock import patch

p = argparse.ArgumentParser()
p.add_argument('--backend', type=Path, required=True)
p.add_argument('--receipt', type=Path, required=True)
p.add_argument('--output', type=Path, required=True)
a = p.parse_args()
assert not a.output.exists()
os.environ.update(DATABASE_URL='sqlite:///:memory:', FMP_PROVIDER_DISABLED='1', PRESS_RELEASE_PROVIDER='sec_edgar')
sys.path.insert(0, str(a.backend.resolve()))
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import sessionmaker
from app.db import Base
from app.models import Event, EmailDelivery, ResearchSourceDocument, Security, TickerContentCache
from app.services import sec_press_releases as press, fmp_news as news, email_digests as digests
from app.main import ticker_press_releases

receipt = json.loads(a.receipt.read_text(encoding='utf-8'))
rows = receipt['public_caches']
assert rows and not receipt['caches_truncated']
engine = create_engine('sqlite:///:memory:')
Base.metadata.create_all(engine)
factory = sessionmaker(bind=engine)
press.SessionLocal = factory
with factory() as db:
    for i, row in enumerate(rows, 1):
        db.add(Security(id=i, symbol=row['symbol'], name=row['symbol'], asset_class='stock'))
        db.add(TickerContentCache(content_type=press.CONTENT_TYPE, symbol=row['symbol'],
            window_key='latest', cache_key=f'sec-replay:{row["symbol"]}', source=press.PROVIDER,
            status='ok', item_count=len(row['payload']['items']), payload_json=json.dumps(row['payload']),
            fetched_at=datetime.fromisoformat(row['fetched_at'])))
    db.commit()

def forbidden(*args, **kwargs):
    raise AssertionError('Unexpected source, queue or delivery call')

panels = []
with patch('requests.sessions.Session.request', forbidden), patch('httpx.Client.send', forbidden), \
     patch.object(news, '_enqueue_news_refresh', forbidden), patch.object(digests, '_send_digest', forbidden), \
     patch.object(digests, '_subscription_payload', lambda _: {'watchlist_news_enabled': True}), \
     patch.object(digests, '_watchlist_market_news_enabled', lambda _: True), \
     patch.object(digests, '_watchlist_symbols', lambda *a: [r['symbol'] for r in rows]), \
     patch.object(digests, 'get_stock_news', lambda **kw: {'items': []}):
    for row in rows:
        symbol, saved = row['symbol'], row['payload']
        public = ticker_press_releases(symbol, page=0, limit=20)
        assert public['items'] == saved['items'] and public['provider'] == press.PROVIDER
        assert public['message'] == saved['message']
        assert news.get_press_releases(symbol=symbol)['items'] == saved['items']
        assert all(item['published_at'] is None and item['filing_date'] for item in public['items'])
        assert news.get_press_releases(symbol=symbol, page=1, limit=20)['items'] == []
        panels.append({'symbol': symbol, 'items': len(public['items'])})
    # The legacy digest path cannot invent publication times from prepared links.
    assert not digests._watchlist_market_news_items(None, SimpleNamespace(id=1),
        since=datetime.now(timezone.utc)-timedelta(days=7), subscription=SimpleNamespace(active=True))
with factory() as db:
    for model in (Event, EmailDelivery, ResearchSourceDocument):
        assert db.scalar(select(func.count()).select_from(model)) == 0
report = {'status': 'passed', 'observed_cache_receipt': receipt['observed_at'], 'panels': panels,
    'source_calls': 0, 'queue_calls': 0, 'emails': 0, 'canonical_writes': 0,
    'legacy_digest_backfill': 0, 'limitation': 'Isolated replay of public prepared caches; canonical publication remains separate.'}
a.output.write_text(json.dumps(report, indent=2), encoding='utf-8')
print(json.dumps(report))
engine.dispose()
