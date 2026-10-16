"""Unpublished rating-change evidence, separate from monthly recommendations.

No canonical analyst events, score changes, alerts or FMP fallback. Source times
are provider upgrade/downgrade timestamps, not verified publication/arrival times.
"""
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import re

from app.services import finnhub_research as transport
from app.services.finnhub_paid_preparation import scope as symbol_scope, _now

VERSION = 'finnhub_rating_preparation_v1'
ENDPOINT = 'stock/upgrade-downgrade'
TTL = timedelta(hours=24)
ACTIONS = {'up':'Upgrade','down':'Downgrade','main':'Maintained','init':'Initiated','reit':'Reiterated'}


def fail(reason):
    raise transport.FinnhubUnavailable(reason)


def day(value):
    if not isinstance(value,str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}',value):
        fail('invalid_rating_window')
    try:return date.fromisoformat(value)
    except ValueError:fail('invalid_rating_window')


def scope(symbol, start, end, now):
    now = _now(now)
    symbol, provider_symbol, _ = symbol_scope(symbol,'targets')
    first,last=day(start),day(end)
    if not date(1970,1,1)<=first<=last<=now.date() or (last-first).days>=90:
        fail('invalid_rating_window')
    key=f'finnhub:rating-preparation:{provider_symbol}:{start}:{end}'
    return symbol,provider_symbol,key


def label(value, *, optional=False):
    if optional and value in (None,''):return None
    if not isinstance(value,str) or not value.strip() or len(value)>200 or any(ord(c)<32 or ord(c)==127 for c in value):
        fail('invalid_rating_text')
    return value.strip()


def serialized(value):
    try:return json.dumps(value,sort_keys=True,allow_nan=False,separators=(',',':'))
    except (ValueError,TypeError):fail('invalid_rating_response')


def normalize(raw, symbol, *, start, end, observed_at):
    now=_now(observed_at)
    symbol,provider_symbol,_=scope(symbol,start,end,now)
    if not isinstance(raw,list) or len(raw)>=transport.MAX_ROWS:
        fail('rating_response_invalid_or_at_bound')
    encoded=serialized(raw)
    if len(encoded.encode())>transport.MAX_BYTES:fail('rating_response_too_large')
    items={}
    for row in raw:
        if not isinstance(row,dict) or row.get('symbol')!=provider_symbol:
            fail('rating_security_mismatch')
        stamp=row.get('gradeTime')
        if type(stamp) is not int or not 0<=stamp<=4102444800:
            fail('invalid_rating_timestamp')
        timestamp=datetime.fromtimestamp(stamp,timezone.utc)
        if timestamp>now or not day(start)<=timestamp.date()<=day(end):
            fail('rating_time_outside_window')
        action=row.get('action')
        if not isinstance(action,str) or action not in ACTIONS:fail('unsupported_rating_action')
        actor=label(row.get('company'))
        previous=label(row.get('fromGrade'),optional=True)
        current=label(row.get('toGrade'))
        # No provider ID is supplied. A same-actor/time/action disagreement
        # needs review, rather than selecting one grade or inventing two events.
        identity={'provider':'finnhub','symbol':provider_symbol,'sourceActor':actor,'gradeTime':stamp,'action':action}
        fingerprint=hashlib.sha256(serialized(identity).encode()).hexdigest()
        value={'sourceIdentity':'finnhub:rating:'+fingerprint,'symbol':symbol,'sourceActor':actor,
            'previousGrade':previous,'newGrade':current,'action':ACTIONS[action],'providerAction':action,
            'sourceTimestamp':timestamp.isoformat(),'gradeTime':stamp,'analystName':None}
        if fingerprint in items and items[fingerprint]!=value:fail('conflicting_rating_event')
        items[fingerprint]=value
    return {'version':VERSION,'source':'finnhub','symbol':symbol,'providerSymbol':provider_symbol,
        'window':{'from':start,'to':end},'observedAt':now.isoformat(),
        'rawSha256':hashlib.sha256(encoded.encode()).hexdigest(),
        'items':sorted(items.values(),key=lambda row:(row['gradeTime'],row['sourceIdentity']),reverse=True),
        'status':'prepared' if items else 'empty','publicationEligible':False,
        'sourceTimeBasis':'provider_upgrade_downgrade_timestamp; publication_latency_unverified',
        'coverage':'provider_returned_window_completeness_unverified',
        'crossProviderReconciliationRequired':True,'priceTargetEventsAvailable':False}


def read_prepared(db, symbol, *, start, end, observed_at=None):
    """Inspection only: no fetch, public activation or observation-time rewrite."""
    now=_now(observed_at);symbol,provider_symbol,key=scope(symbol,start,end,now)
    from app.models import InsightsSnapshot
    row=db.get(InsightsSnapshot,key,populate_existing=True)
    unavailable=lambda reason:{'status':'unavailable','reason':reason,'data':None,'publicationEligible':False}
    if row is None or row.source!=VERSION:return unavailable('rating_cache_missing')
    fetched=row.fetched_at
    if fetched.tzinfo is None:fetched=fetched.replace(tzinfo=timezone.utc)
    if not timedelta(0)<=now-fetched<TTL:return unavailable('rating_cache_stale_or_future')
    try:
        if len(row.payload_json.encode())>transport.MAX_BYTES*2:fail('rating_cache_invalid')
        saved=json.loads(row.payload_json)
        original_symbol=saved['normalized']['symbol']
        if scope(original_symbol,start,end,fetched)[1]!=provider_symbol:fail('rating_cache_identity')
        checked=normalize(saved['raw'],original_symbol,start=start,end=end,observed_at=fetched)
        if checked!=saved['normalized']:fail('rating_cache_projection_changed')
    except (KeyError,TypeError,ValueError,transport.FinnhubUnavailable):
        return unavailable('rating_cache_invalid')
    return {'status':checked['status'],'data':checked,'publicationEligible':False}


def prepare(db, symbol, *, start, end, observed_at=None):
    """One explicit bounded scope. Caller owns commit; failures preserve evidence."""
    transport.require_paid_preparation()
    now=_now(observed_at);symbol,provider_symbol,key=scope(symbol,start,end,now)
    from app.models import InsightsSnapshot
    if db.get_bind().dialect.name=='postgresql':
        from sqlalchemy import text
        if not db.scalar(text('SELECT pg_try_advisory_xact_lock(hashtext(:key))'),{'key':key}):
            fail('refresh_in_progress')
    existing=read_prepared(db,symbol,start=start,end=end,observed_at=now)
    if existing['data'] is not None:
        return {'status':'cached','scope':key,'sourceStatus':existing['status'],'publicationEligible':False}
    raw=transport.request_json(ENDPOINT,{'symbol':provider_symbol,'from':start,'to':end})
    normalized=normalize(raw,symbol,start=start,end=end,observed_at=now)
    payload=serialized({'raw':raw,'normalized':normalized})
    row=db.get(InsightsSnapshot,key)
    if row is None:
        row=InsightsSnapshot(kind=key,source=VERSION,fetched_at=now,payload_json=payload);db.add(row)
    row.source,row.fetched_at,row.payload_json=VERSION,now,payload
    db.flush()
    return {'status':'prepared','scope':key,'sourceStatus':normalized['status'],'publicationEligible':False}
