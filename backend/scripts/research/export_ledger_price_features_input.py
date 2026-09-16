"""Read-only trailing-price export for confirmation research, no outcome writes."""
import base64
import json
import zlib
from sqlalchemy import text
from app.db import SessionLocal
with SessionLocal() as db:
    db.connection().exec_driver_sql('SET TRANSACTION READ ONLY')
    db.connection().exec_driver_sql("SET LOCAL statement_timeout='60s'")
    q=text("SELECT symbol,date,raw_close,adjusted_close,open_price,volume,price_source,adjustment_status,split_coefficient FROM price_cache WHERE date BETWEEN '2025-01-01' AND '2026-09-11' AND raw_close>0 AND price_source='fmp:historical-price-eod/full+corporate_actions' AND (symbol IN (SELECT DISTINCT ticker_at_time FROM confirmation_score_snapshots) OR symbol IN ('SPY','QQQ')) ORDER BY symbol,date")
    rows=[list(r) for r in db.execute(q)]
    payload={'symbols':sorted(set(r[0] for r in rows)),'rows':rows}
    print('RESEARCH_BASE64='+base64.b64encode(zlib.compress(json.dumps(payload,default=str,separators=(',',':')).encode())).decode(),flush=True)
    db.rollback()

