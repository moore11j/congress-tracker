"""Read-only dated public-disclosure export, excluding user/account information."""
import base64
import hashlib
import json
import zlib
from sqlalchemy import text
from app.db import SessionLocal

with SessionLocal() as db:
    db.connection().exec_driver_sql('SET TRANSACTION READ ONLY')
    db.connection().exec_driver_sql("SET LOCAL statement_timeout='60s'")
    events=[];keys={}
    q=text("SELECT id,event_type,symbol,ts,trade_type,transaction_type,amount_min,amount_max,payload_json FROM events WHERE event_type IN ('congress_trade','insider_trade') AND ts>='2013-01-01' AND ts<'2026-09-14' AND symbol IS NOT NULL ORDER BY ts,id")
    for r in db.execute(q).mappings():
        try:p=json.loads(r['payload_json'] or '{}')
        except ValueError:continue
        date_fields={k:p.get(k) for k in ['filing_date','filingDate','report_date','reportDate','disclosure_date','disclosureDate','date_filed','filing_time','acceptanceDate','transaction_date','transactionDate','trade_date'] if p.get(k)}
        keys.setdefault(r['event_type'],{})
        for k in p:keys[r['event_type']][k]=keys[r['event_type']].get(k,0)+1
        side=r['trade_type'] or r['transaction_type'] or p.get('trade_type') or p.get('transaction_type') or p.get('transactionType')
        events.append({'id':r['id'],'type':r['event_type'],'ticker':r['symbol'],'ts':str(r['ts']),'side':side,'amount_min':r['amount_min'],'amount_max':r['amount_max'],'dates':date_fields})
    raw=json.dumps({'events':events,'payload_key_counts':keys},default=str,separators=(',',':')).encode()
    print('RESEARCH_BASE64='+base64.b64encode(zlib.compress(raw)).decode(),flush=True)
    db.rollback()
