"""Read-only export of the next fixed symbol sample; no provider or cache writes."""
import base64
import hashlib
import json
import zlib
from sqlalchemy import bindparam, text
from app.db import SessionLocal

with SessionLocal() as db:
    db.connection().exec_driver_sql('SET TRANSACTION READ ONLY')
    db.connection().exec_driver_sql("SET LOCAL statement_timeout='60s'")
    eligible = list(db.execute(text("SELECT symbol FROM price_cache WHERE date BETWEEN '2023-01-01' AND '2023-12-31' AND raw_close>0 AND open_price>0 AND price_source='fmp:historical-price-eod/full+corporate_actions' GROUP BY symbol HAVING count(*)>=180")).scalars())
    ordered = sorted(eligible, key=lambda s: hashlib.sha256(('expanded-research-v1|'+s).encode()).hexdigest())
    symbols = sorted(set(ordered[256:512]+['SPY', 'QQQ']))
    q = text("SELECT symbol,date,raw_close,adjusted_close,open_price,volume,price_source,adjustment_status,split_coefficient FROM price_cache WHERE symbol IN :symbols AND date BETWEEN '2013-01-01' AND '2026-09-11' AND raw_close>0 ORDER BY symbol,date").bindparams(bindparam('symbols', expanding=True))
    payload = {'eligibility': 'Next 256 symbols, same expanded-research-v1 SHA256 ordering; no outcomes used', 'eligible_count': len(eligible), 'symbols': symbols, 'columns': ['symbol','date','raw_close','adjusted_close','open','volume','source','status','split'], 'rows': [list(r) for r in db.execute(q, {'symbols': symbols})]}
    raw = json.dumps(payload, default=str, separators=(',', ':')).encode()
    print('RESEARCH_BASE64='+base64.b64encode(zlib.compress(raw)).decode(), flush=True)
    db.rollback()
