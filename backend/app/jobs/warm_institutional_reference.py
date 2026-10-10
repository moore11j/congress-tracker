"""Prepare free, dated identifier evidence without enabling institutional publication."""
from datetime import date, datetime, timedelta, timezone
import json
from itertools import zip_longest
import os
import re

from sqlalchemy import and_, or_, func, select
from app.background_job_guard import check_background_job_guard
from app.clients.massive_stocks import MassiveStocksClient, MassiveStocksError
from app.db import SessionLocal
from app.jobs.collect_direct_feeds import collector_lock
from app.models import InsightsSnapshot, InstitutionalPosition
from app.services.direct_feed_store import DirectFeedDocument
from app.services.institutional_reference import FEED, capture_reference, stage_reference
from app.services.institutional_sec_snapshot import mapped_symbol

KEY = 'institutional-reference:warming:v1'
BATCH_SIZE = 2
MAX_DOCUMENTS = 500
MAX_SCOPES = 5000


def _now():
    return datetime.now(timezone.utc)


def _periods(today):
    year, quarter = today.year, (today.month-1)//3+1
    result = set()
    for _ in range(2):
        quarter -= 1
        if quarter == 0: year, quarter = year-1, 4
        end = date(year,12,31) if quarter == 4 else date(year,quarter*3+1,1)-timedelta(days=1)
        result.add(end.isoformat())
    return result


def plan_scopes(db, now, attempted_at, universe=None):
    periods = _periods(now.date())
    if universe is None:
        # Completed identities must not permanently occupy the bounded queue.
        # A later plan can then advance beyond its initial 5000-scope window.
        already_prepared = set()
        for key, raw in db.execute(select(DirectFeedDocument.source_key, DirectFeedDocument.parsed_json).where(
                DirectFeedDocument.feed == FEED, DirectFeedDocument.status == 'parsed',
                or_(*(DirectFeedDocument.source_key.like('%:' + period) for period in periods)))):
            try:
                if json.loads(raw)['identity']['status'] == 'verified':
                    already_prepared.add(key)
            except (ValueError, TypeError, KeyError):
                continue
        mappings = {}
        for cusip,symbol,first in db.execute(select(InstitutionalPosition.cusip,
                InstitutionalPosition.normalized_symbol,func.min(InstitutionalPosition.filing_date))
                .where(InstitutionalPosition.normalized_symbol.is_not(None),InstitutionalPosition.cusip.is_not(None),
                    or_(*(and_(InstitutionalPosition.report_year==int(period[:4]),
                               InstitutionalPosition.report_quarter==int(period[5:7])//3) for period in periods)))
                .group_by(InstitutionalPosition.cusip,InstitutionalPosition.normalized_symbol)):
            if first is not None: mappings.setdefault(cusip,[]).append((symbol,first.isoformat()))
        weights = {}; truncated = False; examined = 0; held = 0
        query = select(DirectFeedDocument.parsed_json).where(DirectFeedDocument.feed=='sec_13f',
            DirectFeedDocument.status=='parsed').order_by(DirectFeedDocument.id.desc()).limit(MAX_DOCUMENTS+1)
        for raw in db.scalars(query.execution_options(yield_per=5)):
            examined += 1
            if examined > MAX_DOCUMENTS: truncated=True;break
            if not raw or len(raw)>16_000_000: held+=1;continue
            try:
                parsed=json.loads(raw);meta=parsed['metadata'];period=meta['report_period'];filed=meta['filing_date']
                if period not in periods:continue
                if not period <= filed <= now.date().isoformat():held+=1;continue
                year,quarter=int(period[:4]),int(period[5:7])//3
                cusips={str(row.get('cusip','')).strip().upper() for row in parsed['positions']
                        if row.get('shareType')=='SH' and not row.get('putCall')}
            except (ValueError,TypeError,KeyError,AttributeError):held+=1;continue
            for cusip in cusips:
                if not re.fullmatch(r'[A-Z0-9]{9}',cusip):continue
                candidates={symbol for symbol,first in mappings.get(cusip,[]) if first<=filed}
                if mapped_symbol(cusip,candidates,year,quarter) is not None:continue
                key=cusip+':'+period
                if key in already_prepared:continue
                if key not in weights and len(weights)>=MAX_SCOPES:truncated=True;continue
                weights[key]=weights.get(key,0)+1
    else:
        weights = universe['weights']
        truncated = universe['universe_truncated']
        held = universe['held_documents']
    prepared=set()
    for row in db.scalars(select(DirectFeedDocument).where(DirectFeedDocument.feed==FEED,
            DirectFeedDocument.source_key.in_(list(weights))) if weights else select(DirectFeedDocument).where(False)):
        try:
            if row.status=='parsed' and json.loads(row.parsed_json)['identity']['status']=='verified':
                prepared.add(row.source_key)
        except (ValueError,TypeError,KeyError):held+=1
    candidates=[]
    for key,weight in weights.items():
        if key in prepared:continue
        stamp=attempted_at.get(key)
        if stamp:
            try:
                previous=datetime.fromisoformat(stamp)
                if previous.tzinfo is None:raise ValueError('naive attempt')
                if now-previous<timedelta(days=1):continue
            except (ValueError,TypeError):pass
        cusip,period=key.split(':')
        candidates.append({'key':key,'cusip':cusip,'report_period':period,'filing_count':weight})
    candidates.sort(key=lambda r:(r['report_period'],r['filing_count'],r['cusip']),reverse=True)
    # Comparisons require both quarters. A large current-quarter backlog must
    # not starve prior-quarter evidence. Keep frequency priority within each
    # quarter, and let remaining work use both slots when one quarter is empty.
    by_period = [[row for row in candidates if row['report_period'] == period]
                 for period in sorted(periods, reverse=True)]
    interleaved = [row for pair in zip_longest(*by_period) for row in pair if row is not None]
    return {'scopes':interleaved[:BATCH_SIZE],'universe_size':len(weights),'pending_scopes':len(candidates),
            'prepared_scopes':len(prepared),'deferred_scopes':len(weights)-len(prepared)-len(candidates),'universe_truncated':truncated,'held_documents':held,
            'attempted_at':{key:stamp for key,stamp in attempted_at.items() if key in weights and key not in prepared},
            'universe':{'weights':weights,'universe_truncated':truncated,'held_documents':held,'periods':sorted(periods)}}


def _run_locked():
    now=_now()
    with SessionLocal() as db:
        row=db.get(InsightsSnapshot,KEY)
        state=json.loads(row.payload_json) if row else {}
        until=state.get('cooldown_until')
        if until and now<datetime.fromisoformat(until):return {'status':'cooldown','until':until}
        last=state.get('last_started_at')
        if last and now-datetime.fromisoformat(last)<timedelta(seconds=60):return {'status':'cooldown'}
        cached_universe=state.get('universe')
        planned_at=state.get('universe_prepared_at')
        fresh_plan=(cached_universe and planned_at and cached_universe.get('periods')==sorted(_periods(now.date()))
                    and now-datetime.fromisoformat(planned_at)<timedelta(minutes=15))
        plan=plan_scopes(db,now,state.get('attempted_at',{}),cached_universe if fresh_plan else None)
        planned_at=planned_at if fresh_plan else now.isoformat()
        if row is None:
            row=InsightsSnapshot(kind=KEY,source=FEED,fetched_at=now,payload_json='{}');db.add(row)
        row.payload_json=json.dumps({**state,'last_started_at':now.isoformat(),
            'universe':plan['universe'],'universe_prepared_at':planned_at},sort_keys=True)
        db.commit()
    # Both planning and every staging transaction finish before provider HTTP.
    results=[];attempts=plan.pop('attempted_at');cooldown=None
    for scope in plan['scopes']:
        try:
            document=capture_reference(MassiveStocksClient(),cusip=scope['cusip'],report_period=scope['report_period'])
            with SessionLocal() as db:
                staged=stage_reference(db,document);db.commit()
            result={**scope,**staged}
            attempts[scope['key']]=now.isoformat()
        except MassiveStocksError as exc:
            reason=str(exc)
            cooldown=(now+timedelta(minutes=5)).isoformat()
            results.append({**scope,'status':'unavailable','reason':reason});break
        except Exception as exc:
            results.append({**scope,'status':'held','reason':type(exc).__name__})
            attempts[scope['key']]=now.isoformat();continue
        results.append(result)
    receipt={'observed_at':now.isoformat(),'status':'partial' if plan['universe_truncated'] or plan['held_documents']
        or any(r.get('identity_status')!='verified' for r in results) else 'ok',
        **{key:value for key,value in plan.items() if key not in {'scopes','universe'}},'results':results,
        'planned_scopes':len(plan['scopes']),'completed_scopes':len(results),
        'public_writes':0,'price_requests':0,'emails':0}
    with SessionLocal() as db:
        row=db.get(InsightsSnapshot,KEY,with_for_update=True)
        state=json.loads(row.payload_json)
        row.payload_json=json.dumps({'last_started_at':now.isoformat(),'cooldown_until':cooldown,
            'universe':plan['universe'],'universe_prepared_at':planned_at,'attempted_at':attempts,'runs':[*state.get('runs',[])[-19:],receipt]},sort_keys=True)
        row.fetched_at=_now();db.commit()
    return receipt


def run():
    if (os.getenv('INSTITUTIONAL_REFERENCE_WARMING_ENABLED','0')!='1'
            or os.getenv('DIRECT_FEEDS_MODE','off')!='shadow'):
        return {'status':'disabled'}
    guard=check_background_job_guard('institutional-reference-warming')
    if not guard.proceed:return {'status':'held',**guard.to_dict()}
    with collector_lock() as acquired:
        if not acquired:return {'status':'busy','reason':'source_collection_active'}
        return _run_locked()


if __name__=='__main__':
    result=run();print(json.dumps(result,sort_keys=True))
    raise SystemExit(1 if result['status']=='partial' else 0)
