"""Read-only alternate-provider rows to complete trailing price histories."""
import base64
import json
import zlib
from sqlalchemy import text
from app.db import SessionLocal
with SessionLocal() as db:
    db.connection().exec_driver_sql('SET TRANSACTION READ ONLY')
    db.connection().exec_driver_sql("SET LOCAL statement_timeout='45s'")
    rows=[list(r) for r in db.execute(text("SELECT symbol,date,raw_close,adjusted_close,open_price,volume,price_source,adjustment_status,split_coefficient FROM price_cache WHERE date BETWEEN '2025-01-01' AND '2026-09-11' AND raw_close>0 AND price_source='massive:grouped-daily-adjusted' ORDER BY symbol,date"))]
    print('RESEARCH_BASE64='+base64.b64encode(zlib.compress(json.dumps({'rows':rows},default=str,separators=(',',':')).encode())).decode(),flush=True)
    db.rollback()
