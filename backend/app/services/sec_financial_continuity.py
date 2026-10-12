"""Reviewed accounting continuity, never a generic ticker/CIK alias.

The successor's same-period facts remain authoritative. Historical additions
require identical overlapping facts and retain their original CIK/source hash.
"""
from datetime import date, datetime, timezone
import hashlib
import json
from pathlib import Path

SYMBOL = 'XOM'
CURRENT_CIK = '0002115436'
PREDECESSOR_CIK = '0000034088'
LAST_PREDECESSOR_PERIOD = date(2026, 6, 30)
REVIEWED_AT = datetime(2026, 10, 10, 22, 19, tzinfo=timezone.utc)
PROOF_URL = 'https://www.sec.gov/Archives/edgar/data/2115436/000003408826000093/xom-20260630.htm'
PROOF_SHA = '4dc6e188aa85dd6320818f3465931ba7e00d3ddef101357f218031ca5ec8cd69'
PROOF_FILE = Path(__file__).resolve().parents[2]/'config/financial_continuity/xom-reporting-basis.htm'
VERSION = 'sec_financial_continuity_v1'


class ContinuityError(ValueError):
    pass


def applies(symbol, cik):
    return symbol == SYMBOL and str(cik).zfill(10) == CURRENT_CIK


def fetch_history(symbol, cik, *, client, observed_at):
    if not applies(symbol, cik):
        return None
    if observed_at.tzinfo is None or observed_at < REVIEWED_AT:
        return None
    if hashlib.sha256(PROOF_FILE.read_bytes()).hexdigest() != PROOF_SHA:
        raise ContinuityError('reviewed_accounting_proof_changed')
    return client.get(f'https://data.sec.gov/api/xbrl/companyfacts/CIK{PREDECESSOR_CIK}.json')


def record_sets(payload, *, symbol, cik, available_by, observed_at, history_raw=None, metrics=None, filing_supplement=None, company=None, instant_metrics=()):
    from app.services import sec_fundamentals as sec
    metrics = metrics or {name:(tags,'USD') for name,tags in sec.TAGS.items()}
    instant_names = sec.INSTANT | set(instant_metrics)
    from app.services import sec_inline_financials as inline
    effective = inline.merge(payload,filing_supplement,available_by=available_by)
    current = {name:sec._records(effective,'equity' if name in instant_names else name,available_by,concepts=tags,unit=unit)
               for name,(tags,unit) in metrics.items()}
    inline.annotate(current,filing_supplement,metrics=metrics)
    from app.services.sec_bank_revenue import apply
    current = apply(current,effective,company,available_by=available_by)
    if history_raw is None:
        return current, None
    if (not applies(symbol,cik) or str(payload.get('cik','')).zfill(10) != CURRENT_CIK
            or observed_at.tzinfo is None or observed_at < REVIEWED_AT
            or available_by < date(2026,8,3)):
        raise ContinuityError('unreviewed_accounting_continuity')
    if hashlib.sha256(PROOF_FILE.read_bytes()).hexdigest() != PROOF_SHA:
        raise ContinuityError('reviewed_accounting_proof_changed')
    if len(history_raw)>30_000_000:
        raise ContinuityError('history_size')
    history=json.loads(history_raw)
    if (str(history.get('cik','')).zfill(10) != PREDECESSOR_CIK
            or str(history.get('entityName','')).casefold() != 'exxon mobil corporation'):
        raise ContinuityError('predecessor_identity')
    digest=hashlib.sha256(history_raw).hexdigest()
    proof={'methodology':VERSION,'current_cik':CURRENT_CIK,'predecessor_cik':PREDECESSOR_CIK,
           'effective_date':'2026-07-01','last_predecessor_period':str(LAST_PREDECESSOR_PERIOD),
           'reviewed_at':REVIEWED_AT.isoformat(),'proof_url':PROOF_URL,'proof_sha256':PROOF_SHA,
           'facts_url':f'https://data.sec.gov/api/xbrl/companyfacts/CIK{PREDECESSOR_CIK}.json',
           'facts_sha256':digest,'metrics':{}}
    for name,rows in current.items():
        # Never combine differing standard concepts or currencies across CIKs.
        tags={r['tag'] for r in rows.values() if r}
        if len(tags)!=1:
            proof['metrics'][name]={'status':'current_concept_unavailable'}
            continue
        tag=next(iter(tags));unit=metrics[name][1]
        old=sec._records(history,'equity' if name in instant_names else name,available_by,concepts=(tag,),unit=unit)
        old={key:r for key,r in old.items() if key[1]<=LAST_PREDECESSOR_PERIOD}
        common=set(rows)&set(old)
        if not common:
            proof['metrics'][name]={'status':'no_matching_period'}
            continue
        if any(rows[k] is None or old[k] is None or rows[k]['value']!=old[k]['value'] for k in common):
            proof['metrics'][name]={'status':'overlap_conflict','shared_periods':len(common)}
            continue
        merged={k:({**r,'source_cik':CURRENT_CIK} if r else None) for k,r in rows.items()}
        added=0
        for key,r in old.items():
            if key not in rows:
                merged[key]={**r,'source_cik':PREDECESSOR_CIK,'source_sha256':digest} if r else None
                added+=r is not None
        current[name]=merged
        proof['metrics'][name]={'status':'matched','shared_periods':len(common),'historical_periods_added':added}
    return current,proof
