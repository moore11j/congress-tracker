"""Read-only coverage audit; no cache hydration or application writes."""
import json
from sqlalchemy import text
from app.db import SessionLocal

QUERIES = {
    'prices': "SELECT substr(date,1,4) AS yr, count(*) AS n_rows, count(distinct symbol) symbols, count(adjusted_close) adjusted, count(open_price) opens, min(date) first, max(date) last FROM price_cache GROUP BY 1 ORDER BY 1",
    'price_sources': "SELECT price_source, adjustment_status, count(*) AS n_rows FROM price_cache GROUP BY 1,2 ORDER BY 3 DESC",
    'benchmarks': "SELECT symbol, count(*) AS n_rows, min(date) first, max(date) last FROM price_cache WHERE symbol IN ('SPY','QQQ','IWM','VIX') GROUP BY 1",
    'insiders': "SELECT extract(year from filing_date) AS yr, count(*) AS n_rows, count(distinct ticker_normalized) symbols FROM insider_transactions_normalized WHERE NOT is_duplicate AND transaction_type_normalized='open_market_purchase' GROUP BY 1 ORDER BY 1",
    'fundamentals': "SELECT source_kind,availability_basis,count(*) AS n_rows,min(snapshot_date) first,max(snapshot_date) last,min(observed_at) first_observed FROM fundamentals_snapshots GROUP BY 1,2",
    'events': "SELECT event_type, extract(year from ts) AS yr, count(*) AS n_rows FROM events GROUP BY 1,2 ORDER BY 1,2",
}
with SessionLocal() as db:
    db.connection().exec_driver_sql('SET TRANSACTION READ ONLY')
    db.connection().exec_driver_sql("SET LOCAL statement_timeout='45s'")
    result = {key: [dict(r) for r in db.execute(text(query)).mappings()] for key,query in QUERIES.items()}
    print('AUDIT_JSON=' + json.dumps(result, default=str))
    db.rollback()

