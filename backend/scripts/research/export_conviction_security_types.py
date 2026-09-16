"""Read-only security-type metadata audit for public trade research."""
import base64,json,zlib
from sqlalchemy import text
from app.db import SessionLocal
with SessionLocal() as db:
    db.connection().exec_driver_sql('SET TRANSACTION READ ONLY')
    db.connection().exec_driver_sql("SET LOCAL statement_timeout='45s'")
    congress={}
    for r in db.execute(text("SELECT id,payload_json FROM events WHERE event_type='congress_trade' AND symbol IS NOT NULL").execution_options(stream_results=True,yield_per=2000)).mappings():
        try:p=json.loads(r['payload_json'] or '{}')
        except ValueError:continue
        congress[str(r['id'])]={k:p.get(k) for k in ['asset_class','description','security_name']}
    insider={str(r[0]):r[1] for r in db.execute(text("SELECT normalized_hash,security_title FROM insider_transactions_normalized WHERE NOT is_duplicate AND NOT is_derivative AND transaction_type_normalized IN ('open_market_purchase','open_market_sale')"))}
    print('RESEARCH_BASE64='+base64.b64encode(zlib.compress(json.dumps({'congress':congress,'insider':insider},separators=(',',':')).encode())).decode(),flush=True)
    db.rollback()
