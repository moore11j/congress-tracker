"""Opt-in, bounded Yahoo evidence collection. No public reader or schedule.

The limits below are our conservative preparation budget, not a claim about
Yahoo entitlements or advertised limits. Existing originals are never deleted.
"""
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
import hashlib
import json
import os
import re
import time
from urllib.parse import urlencode

import requests
from sqlalchemy import select, func

from app.db import SessionLocal
from app.models import InsightsSnapshot
from app.clients.yahoo_financials import METRICS, YahooStatementError
from app.services.yahoo_financial_evidence import prepare_bundle
from app.services.direct_feed_store import DirectFeedDocument, DirectFeedRevision, discover, record_document
from app.services.sec_financial_statements import _refresh_lock

FEED = 'yahoo_financial_evidence'
VERSION = 'yahoo_collection_v1'
BUDGET_KEY = 'yahoo:collection-budget:v1'
TTL = timedelta(hours=24)
MAX_BYTES = 1_000_000
MAX_CACHE_BYTES = 2_000_000
MAX_STORED_BYTES = 20_000_000
MAX_REVISIONS = 200
MAX_REVISIONS_PER_DOCUMENT = 8
DAILY_REQUESTS = 30


class YahooCollectionUnavailable(RuntimeError):
    def __init__(self, reason, cooldown_seconds=900):
        super().__init__(reason)
        self.cooldown_seconds = cooldown_seconds


def _utc(value):
    if not isinstance(value, datetime) or value.utcoffset() is None:
        raise YahooCollectionUnavailable('observation_time_required')
    return value.astimezone(timezone.utc)


def _key(symbol, statement):
    if not isinstance(symbol, str) or not re.fullmatch(r'[A-Z][A-Z0-9.-]{0,14}', symbol) or statement not in METRICS:
        raise YahooCollectionUnavailable('invalid_statement_request')
    return f'yahoo:financials:{symbol}:{statement}:v1'


def request_url(symbol, statement, now):
    _key(symbol, statement);now = _utc(now)
    frequencies = ('annual','quarterly') if statement == 'balance' else ('annual','trailing','quarterly')
    return f'https://query1.finance.yahoo.com/ws/fundamentals-timeseries/v1/finance/timeseries/{symbol}?'+urlencode({
        'merge':'false', 'padTimeSeries':'true', 'period1':int((now-timedelta(days=2192)).timestamp()),
        'period2':int(now.timestamp()), 'type':','.join(f+m for f in frequencies for m in sorted(METRICS[statement])),
        'lang':'en-US', 'region':'US'})


def _retry_seconds(value, now):
    try:
        seconds = int(value)
    except (ValueError, TypeError):
        try: seconds = int((_utc(parsedate_to_datetime(value)) - now).total_seconds())
        except (ValueError, TypeError, OverflowError, YahooCollectionUnavailable): seconds = 3600
    return max(3600, min(86400, seconds))


def _fetch(url, now):
    started = time.monotonic()
    try:
        with requests.get(url, headers={'User-Agent':'WalnutMarkets source evaluation/1.0','Accept':'application/json'},
                timeout=(3,8), allow_redirects=False, stream=True) as response:
            if response.status_code != 200:
                if response.status_code == 429:
                    raise YahooCollectionUnavailable('rate_limited', _retry_seconds(response.headers.get('Retry-After'), now))
                if response.status_code in (401,403):
                    raise YahooCollectionUnavailable('access_denied',86400)
                raise YahooCollectionUnavailable('provider_unavailable')
            raw = bytearray()
            for chunk in response.iter_content(65536):
                raw.extend(chunk)
                if len(raw)>MAX_BYTES: raise YahooCollectionUnavailable('response_too_large')
                if time.monotonic()-started>20: raise YahooCollectionUnavailable('provider_timeout')
            return bytes(raw)
    except requests.Timeout:
        raise YahooCollectionUnavailable('provider_timeout') from None
    except requests.RequestException:
        raise YahooCollectionUnavailable('transport_failed') from None


def _cache(db, key, now):
    row = db.get(InsightsSnapshot,key)
    if row is None or row.source != FEED: return None
    stamp = row.fetched_at.replace(tzinfo=timezone.utc) if row.fetched_at.tzinfo is None else row.fetched_at
    if not timedelta(0) <= now-stamp < TTL: return None
    try:
        value = json.loads(row.payload_json)
        source = value['capture']
        if (value['collection_version'] != VERSION or value['canonical_eligible'] is not False
                or value['status'] != 'evidence_only' or _key(value['symbol'],value['statement']) != key
                or _utc(datetime.fromisoformat(source['observed_at'])) != stamp): return None
        revision = db.get(DirectFeedRevision, source['revision_id'])
        document = db.get(DirectFeedDocument, source['document_id'])
        if (revision is None or document is None or document.feed != FEED or document.source_key != key
                or revision.document_id != document.id or not revision.source_bytes
                or revision.content_hash != source['sha256']
                or hashlib.sha256(revision.source_bytes).hexdigest() != source['sha256']): return None
        # Verify cache values against the archived bytes, not just a stored hash.
        expected = prepare_bundle([{'raw':revision.source_bytes,'statement':value['statement'],
            'source_url':source['url'],'observed_at':stamp}],symbol=value['symbol'])
        if expected != value['bundle']: return None
        return value
    except (KeyError, TypeError, ValueError, AttributeError):
        return None


def cached(symbol, statement, *, now=None):
    """Read verified evidence only; a cache miss never triggers collection."""
    now = _utc(now or datetime.now(timezone.utc));key = _key(symbol,statement)
    with SessionLocal() as db: return _cache(db,key,now)


def _budget(db, now):
    row = db.get(InsightsSnapshot,BUDGET_KEY)
    if row is None:
        row = InsightsSnapshot(kind=BUDGET_KEY,source=FEED,fetched_at=now,payload_json='{}');db.add(row)
    try:
        state = json.loads(row.payload_json)
        reservations = state.get('reservations',[])
        if (row.source != FEED or not isinstance(state,dict) or not isinstance(reservations,list)
                or len(reservations)>DAILY_REQUESTS or any(type(t) not in (int,float) or not 0<=t<=now.timestamp() for t in reservations)):
            raise ValueError()
        until = _utc(datetime.fromisoformat(state['blocked_until'])) if state.get('blocked_until') else now
    except (ValueError,TypeError,AttributeError,YahooCollectionUnavailable):
        raise YahooCollectionUnavailable('request_budget_invalid') from None
    if now<until: raise YahooCollectionUnavailable('provider_cooldown')
    recent = [t for t in reservations if t>now.timestamp()-86400]
    if len(recent)>=DAILY_REQUESTS or any(t>now.timestamp()-60 for t in recent):
        raise YahooCollectionUnavailable('request_budget_exhausted')
    recent.append(now.timestamp())
    row.payload_json=json.dumps({'reservations':recent});row.fetched_at=now
    db.commit()  # Reservation survives a failed fetch; no DB transaction over HTTP.


def _storage(db, key, additional=0):
    rows = db.execute(select(DirectFeedRevision.document_id,func.length(DirectFeedRevision.source_bytes),
        func.length(DirectFeedRevision.parsed_json),func.length(DirectFeedRevision.source_text)).join(
        DirectFeedDocument,DirectFeedDocument.id==DirectFeedRevision.document_id).where(DirectFeedDocument.feed==FEED)
        .limit(MAX_REVISIONS+1)).all()
    docs = db.execute(select(DirectFeedDocument.id,DirectFeedDocument.source_key,DirectFeedDocument.metadata_json,
        DirectFeedDocument.parsed_json).where(DirectFeedDocument.feed==FEED).limit(MAX_REVISIONS+1)).all()
    caches = db.execute(select(func.length(InsightsSnapshot.payload_json)).where(InsightsSnapshot.source==FEED)
        .limit(MAX_REVISIONS+2)).scalars().all()
    if len(rows)>=MAX_REVISIONS or len(docs)>MAX_REVISIONS or len(caches)>MAX_REVISIONS+1:
        raise YahooCollectionUnavailable('archive_capacity_reached')
    ids={r.id for r in docs if r.source_key==key}
    if sum(r.document_id in ids for r in rows)>=MAX_REVISIONS_PER_DOCUMENT:
        raise YahooCollectionUnavailable('document_revision_capacity_reached')
    used=sum(sum(v or 0 for v in row[1:]) for row in rows)+sum(caches)
    used+=sum(len((r.metadata_json or '').encode())+len((r.parsed_json or '').encode()) for r in docs)
    if used+additional>MAX_STORED_BYTES: raise YahooCollectionUnavailable('archive_capacity_reached')


def collect(symbol, statement, *, now=None):
    """One opt-in request at most, globally serialized; no public mutations."""
    if os.getenv('YAHOO_FINANCIAL_PREPARATION_ENABLED','0')!='1':
        raise YahooCollectionUnavailable('preparation_disabled')
    now = _utc(now or datetime.now(timezone.utc));key = _key(symbol,statement)
    with SessionLocal() as db:
        value = _cache(db,key,now);bind=db.get_bind()
    if value is not None: return value
    with _refresh_lock(bind,BUDGET_KEY) as acquired:
        if not acquired: raise YahooCollectionUnavailable('collection_busy')
        with SessionLocal() as db:
            value=_cache(db,key,now)
            if value is not None:return value
            _storage(db,key,MAX_BYTES+MAX_CACHE_BYTES+6000)
            _budget(db,now)
        url=request_url(symbol,statement,now)
        try:
            raw=_fetch(url,now)
            bundle=prepare_bundle([dict(raw=raw,statement=statement,source_url=url,observed_at=now)],symbol=symbol)
        except (YahooCollectionUnavailable,YahooStatementError) as exc:
            failure=exc if isinstance(exc,YahooCollectionUnavailable) else YahooCollectionUnavailable('invalid_statement_response')
            with SessionLocal() as db:
                row=db.get(InsightsSnapshot,BUDGET_KEY);state=json.loads(row.payload_json)
                state.update(blocked_until=(now+timedelta(seconds=failure.cooldown_seconds)).isoformat(),last_failure=str(failure))
                row.payload_json=json.dumps(state);db.commit()
            raise failure from None
        fingerprint=hashlib.sha256(raw).hexdigest()
        with SessionLocal() as db:
            _storage(db,key,len(raw)+len(json.dumps(bundle).encode())+6000)
            document=discover(db,FEED,{'key':key,'url':url,'symbol':symbol,'statement':statement})
            # Store exact JSON bytes once per hash. No executable HTML or
            # duplicate parsed financial payload is retained in this archive.
            record_document(db,document,raw,'',{'symbol':symbol,'statement':statement,'sha256':fingerprint})
            revision=db.scalar(select(DirectFeedRevision).where(DirectFeedRevision.document_id==document.id,
                DirectFeedRevision.content_hash==fingerprint))
            value={'collection_version':VERSION,'status':'evidence_only','canonical_eligible':False,
                'symbol':symbol,'statement':statement,'bundle':bundle,
                'capture':{'document_id':document.id,'revision_id':revision.id,'sha256':fingerprint,
                    'url':url,'observed_at':now.isoformat()}}
            payload=json.dumps(value,sort_keys=True,allow_nan=False)
            if len(payload.encode())>MAX_CACHE_BYTES:raise YahooCollectionUnavailable('cache_size_exceeded')
            row=db.get(InsightsSnapshot,key)
            if row is None:row=InsightsSnapshot(kind=key,source=FEED,fetched_at=now,payload_json=payload);db.add(row)
            else:row.source=FEED;row.fetched_at=now;row.payload_json=payload
            db.commit()
        return value
