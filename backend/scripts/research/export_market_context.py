"""Read-only historical benchmark cache export for offline regime research."""
import base64
import json
import zlib
from sqlalchemy import select
from app.db import SessionLocal
from app.models import PriceCache

with SessionLocal() as db:
    connection = db.connection()
    if connection.dialect.name == 'sqlite':
        connection.exec_driver_sql('PRAGMA query_only=ON')
    elif connection.dialect.name == 'postgresql':
        connection.exec_driver_sql('SET TRANSACTION READ ONLY')
    else:
        raise RuntimeError('Unsupported read-only transaction policy')
    prices = db.scalars(select(PriceCache).where(
        PriceCache.symbol.in_(['SPY', 'QQQ']),
        PriceCache.date >= '2026-07-01', PriceCache.date <= '2026-09-11',
    )).all()
    payload = [{'symbol': p.symbol, 'date': p.date, 'close': p.close,
                'raw_close': p.raw_close, 'adjusted_close': p.adjusted_close,
                'source': p.price_source, 'adjustment_status': p.adjustment_status,
                'updated_at': str(p.updated_at)} for p in prices]
    print('MARKET_BASE64=' + base64.b64encode(zlib.compress(json.dumps(payload).encode())).decode())
    db.rollback()
