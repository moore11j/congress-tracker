"""Bounded read-only price research export. No cache/provider mutations."""
import base64
import hashlib
import json
import zlib
from sqlalchemy import text, bindparam
from app.db import SessionLocal

with SessionLocal() as db:
    db.connection().exec_driver_sql('SET TRANSACTION READ ONLY')
    db.connection().exec_driver_sql("SET LOCAL statement_timeout='60s'")
    eligible = list(db.execute(text("SELECT symbol FROM price_cache WHERE date BETWEEN '2023-01-01' AND '2023-12-31' AND raw_close>0 AND open_price>0 AND price_source='fmp:historical-price-eod/full+corporate_actions' GROUP BY symbol HAVING count(*)>=180")).scalars())
    symbols = sorted(set(sorted(eligible,key=lambda s:hashlib.sha256(('expanded-research-v1|'+s).encode()).hexdigest())[:256]+['SPY','QQQ']))
    q=text("SELECT symbol,date,raw_close,adjusted_close,open_price,volume,price_source,adjustment_status,split_coefficient FROM price_cache WHERE symbol IN :symbols AND date BETWEEN '2013-01-01' AND '2026-09-11' AND raw_close>0 ORDER BY symbol,date").bindparams(bindparam('symbols',expanding=True))
    columns=['symbol','date','raw_close','adjusted_close','open','volume','source','status','split']
    rows=[list(r) for r in db.execute(q,{'symbols':symbols})]
    payload={'eligibility':'180 attributed raw-close/open observations during 2023; fixed SHA256 sample, not outcomes-selected','eligible_count':len(eligible),'symbols':symbols,'columns':columns,'rows':rows}
    raw=json.dumps(payload,default=str,separators=(',',':')).encode()
    print('RESEARCH_BASE64='+base64.b64encode(zlib.compress(raw)).decode(),flush=True)
    db.rollback()
