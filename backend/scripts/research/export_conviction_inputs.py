"""Stream dated public trade metadata for offline conviction research. Read-only."""
import base64
import hashlib
import json
import zlib
from sqlalchemy import text
from app.db import SessionLocal

def actor(value):
    return hashlib.sha256(str(value).strip().lower().encode()).hexdigest()[:20] if value else None

with SessionLocal() as db:
    db.connection().exec_driver_sql('SET TRANSACTION READ ONLY')
    db.connection().exec_driver_sql("SET LOCAL statement_timeout='60s'")
    events=[]
    q=text("SELECT id,event_type,symbol,trade_type,amount_min,amount_max,member_bioguide_id,member_name,payload_json FROM events WHERE event_type IN ('congress_trade','insider_trade') AND ts>='2013-01-01' AND ts<'2026-09-14' AND symbol IS NOT NULL AND (lower(trade_type) LIKE '%sale%' OR lower(trade_type) IN ('purchase','p-purchase','buy','p','sell','s','s-sale'))")
    for r in db.execute(q.execution_options(stream_results=True,yield_per=2000)).mappings():
        try:p=json.loads(r['payload_json'] or '{}')
        except ValueError:continue
        raw=p.get('raw') or {};raw=raw if isinstance(raw,dict) else {}
        who=p.get('reporting_cik') or raw.get('reportingCik') or p.get('insider_name') if r['event_type']=='insider_trade' else r['member_bioguide_id'] or p.get('member_id') or r['member_name'] or p.get('member')
        events.append({'id':r['id'],'type':r['event_type'],'ticker':r['symbol'],'side':r['trade_type'],'amount_min':r['amount_min'],'amount_max':r['amount_max'],'actor':actor(who),
            'filing_date':p.get('filing_date') or p.get('filingDate') or p.get('report_date') or p.get('disclosure_date'),
            'transaction_date':p.get('transaction_date') or p.get('trade_date'),
            'filing_id':p.get('filing_id') or raw.get('link'),'transaction_id':p.get('transaction_id'),
            'shares':p.get('shares') or raw.get('securitiesTransacted'),'price':p.get('price') or raw.get('price'),
            'shares_following':raw.get('securitiesOwned') or raw.get('sharesOwnedFollowingTransaction'),
            'role':p.get('role') or raw.get('typeOfOwner'),'ownership':p.get('ownership') or raw.get('directOrIndirect'),
            'market_trade':p.get('is_market_trade'),'source':p.get('source')})
    q2=text("SELECT ticker_normalized,reporting_owner_cik,filing_date,transaction_date,transaction_type_normalized,shares,price,value,shares_owned_following,is_director,is_officer,officer_title,is_ten_percent_owner,ten_b5_1_flag,direct_or_indirect,accession_number,normalized_hash FROM insider_transactions_normalized WHERE NOT is_duplicate AND NOT is_derivative AND transaction_type_normalized IN ('open_market_purchase','open_market_sale') AND filing_date<'2026-09-14'")
    normalized=[]
    for r in db.execute(q2).mappings():
        d=dict(r);d['actor']=actor(d.pop('reporting_owner_cik'));normalized.append(d)
    payload={'events':events,'normalized':normalized}
    print('RESEARCH_BASE64='+base64.b64encode(zlib.compress(json.dumps(payload,default=str,separators=(',',':')).encode())).decode(),flush=True)
    db.rollback()
