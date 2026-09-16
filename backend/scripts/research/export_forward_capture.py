"""Read-only prospective inputs and existing prices; no outcomes or cache hydration."""
import base64
from datetime import datetime, timezone
import hashlib
import json
import zlib
from sqlalchemy import select, text, bindparam
from app.db import SessionLocal
from app.models import ConfirmationScoreSnapshot, OutcomeEntry
from app.services.outcome_ledger import _project_directional_outcome_events


def actor(value):
    return hashlib.sha256(str(value).strip().lower().encode()).hexdigest()[:20] if value else None


def snapshot(s):
    fields=['id','security_id','ticker_at_time','calculated_at','created_at','market_date','score','direction',
            'strength','active_source_count','active_sources_json','source_contributions_json','source_freshness_json',
            'input_hash','methodology_version_id','code_commit_sha','reference_price','reference_price_at','reference_price_source']
    return {k:getattr(s,k) for k in fields}


with SessionLocal() as db:
    db.connection().exec_driver_sql('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY')
    db.connection().exec_driver_sql("SET LOCAL statement_timeout='90s'")
    captured=db.execute(text('SELECT transaction_timestamp()')).scalar_one()
    ss=db.scalars(select(ConfirmationScoreSnapshot).where(ConfirmationScoreSnapshot.calculation_type=='live',
                                                        ConfirmationScoreSnapshot.created_at<=captured)).all()
    verified=set(db.scalars(select(OutcomeEntry.snapshot_id)).all())
    projected=_project_directional_outcome_events(ss,verified_snapshot_ids=verified)
    latest={}
    for s in sorted(ss,key=lambda r:(r.calculated_at,r.id)):latest[s.security_id]=s
    confirmations=[{'anchor':snapshot(e.snapshot),'closed_at':e.closed_at,
                    'verified_public_entry':e.snapshot.id in verified,'current':snapshot(latest[e.snapshot.security_id])} for e in projected]
    events=[]
    query=text("SELECT id,symbol,trade_type,payload_json FROM events WHERE event_type='insider_trade' AND ts>='2023-01-01' AND ts<=:capture AND symbol IS NOT NULL AND (lower(trade_type) LIKE '%sale%' OR lower(trade_type) IN ('purchase','p-purchase','buy','p','sell','s','s-sale'))")
    for r in db.execute(query.execution_options(stream_results=True,yield_per=2000),{'capture':captured}).mappings():
        try:p=json.loads(r['payload_json'] or '{}')
        except ValueError:continue
        raw=p.get('raw') or {};raw=raw if isinstance(raw,dict) else {}
        events.append({'id':r['id'],'type':'insider_trade','ticker':r['symbol'],'side':r['trade_type'],
            'actor':actor(p.get('reporting_cik') or raw.get('reportingCik') or p.get('insider_name')),
            'filing_date':p.get('filing_date') or p.get('filingDate') or p.get('report_date') or p.get('disclosure_date'),
            'transaction_date':p.get('transaction_date') or p.get('trade_date'),
            'shares':p.get('shares') or raw.get('securitiesTransacted'),'price':p.get('price') or raw.get('price'),
            'shares_following':raw.get('securitiesOwned') or raw.get('sharesOwnedFollowingTransaction'),
            'role':p.get('role') or raw.get('typeOfOwner'),'ownership':p.get('ownership') or raw.get('directOrIndirect'),
            'market_trade':p.get('is_market_trade'),'source':p.get('source')})
    normalized=[];titles={}
    query=text("SELECT ticker_normalized,reporting_owner_cik,filing_date,transaction_date,transaction_type_normalized,shares,price,value,shares_owned_following,is_director,is_officer,officer_title,is_ten_percent_owner,ten_b5_1_flag,direct_or_indirect,accession_number,normalized_hash,security_title FROM insider_transactions_normalized WHERE NOT is_duplicate AND NOT is_derivative AND transaction_type_normalized IN ('open_market_purchase','open_market_sale') AND filing_date>='2023-01-01' AND filing_date<=:today")
    for r in db.execute(query,{'today':captured.date()}).mappings():
        d=dict(r);d['actor']=actor(d.pop('reporting_owner_cik'));titles[d['normalized_hash']]=d.pop('security_title');normalized.append(d)
    symbols=sorted({c['anchor']['ticker_at_time'] for c in confirmations}|{'SPY'})
    query=text("SELECT symbol,date,raw_close,adjusted_close,open_price,volume,price_source,adjustment_status,split_coefficient FROM price_cache WHERE symbol IN :symbols AND date>='2026-06-01' AND date<=:today AND raw_close>0 AND price_source IN ('fmp:historical-price-eod/full+corporate_actions','massive:grouped-daily-adjusted') ORDER BY symbol,date").bindparams(bindparam('symbols',expanding=True))
    prices=[list(r) for r in db.execute(query.execution_options(stream_results=True,yield_per=2000),{'symbols':symbols,'today':captured.date().isoformat()})]
    payload={'captured_at':captured,'export_finished_at':datetime.now(timezone.utc),'confirmations':confirmations,
             'trade_inputs':{'events':events,'normalized':normalized},'security_types':{'insider':titles,'congress':{}},
             'prices':prices,'snapshot_rows_read':len(ss),'read_only':True}
    print('RESEARCH_BASE64='+base64.b64encode(zlib.compress(json.dumps(payload,default=str,separators=(',',':')).encode())).decode(),flush=True)
    db.rollback()
