"""Read-only stock-price coverage for the approved free options study."""
import base64,json,zlib
from sqlalchemy import text,bindparam
from app.db import SessionLocal
SYMBOLS=['TSM','AAPL','NVDA','MSFT','AMZN','JPM','XOM','WMT','GOOGL','UNH','CAT','KO','SPY']
with SessionLocal() as db:
    db.connection().exec_driver_sql('SET TRANSACTION READ ONLY')
    db.connection().exec_driver_sql("SET LOCAL statement_timeout='60s'")
    query=text("SELECT symbol,date,raw_close,adjusted_close,open_price,volume,price_source,adjustment_status,split_coefficient FROM price_cache WHERE symbol IN :symbols AND date BETWEEN '2024-05-01' AND '2026-09-11' AND raw_close>0 AND price_source IN ('fmp:historical-price-eod/full+corporate_actions','massive:grouped-daily-adjusted') ORDER BY symbol,date").bindparams(bindparam('symbols',expanding=True)).execution_options(stream_results=True,yield_per=2000)
    rows=[list(r) for r in db.execute(query,{'symbols':SYMBOLS})]
    print('RESEARCH_BASE64='+base64.b64encode(zlib.compress(json.dumps({'symbols':SYMBOLS,'rows':rows},default=str,separators=(',',':')).encode())).decode(),flush=True)
    db.rollback()
