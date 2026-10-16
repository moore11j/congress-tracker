"""Source-status IPO calendar; no inference of confirmed first trading dates."""
from __future__ import annotations
from datetime import date, datetime, timedelta, timezone
from calendar import monthrange
import hashlib
import json
import math
import os
import re

from app.services import finnhub_research as transport

VERSION = 'finnhub_ipo_calendar_v1'
TTL = timedelta(hours=24)
STATUS_LABELS = {'expected': 'expected', 'priced': 'priced', 'filed': 'filed', 'withdrawn': 'withdrawn'}
SOURCE_URL = 'https://finnhub.io/docs/api/ipo-calendar'


def selected():
    return os.getenv('IPO_CALENDAR_PROVIDER', 'disabled').strip().lower() == 'finnhub'


def fail(reason):
    raise transport.FinnhubUnavailable(reason)


def _now(value=None):
    value = datetime.now(timezone.utc) if value is None else value
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        fail('invalid_ipo_observation_time')
    return value.astimezone(timezone.utc)


def window(month):
    if not isinstance(month, str) or not re.fullmatch(r'\d{4}-\d{2}', month):
        fail('invalid_ipo_month')
    try:
        start = date.fromisoformat(month+'-01')
        return start, start.replace(day=monthrange(start.year, start.month)[1])
    except ValueError:
        fail('invalid_ipo_month')


def key(month):
    window(month)
    return f'free-calendar:ipo:v1:{month}'


def _text(value, *, optional=False, limit=200):
    if optional and value in (None, ''):
        return None
    if (not isinstance(value, str) or not value.strip() or len(value)>limit
            or any(ord(c)<32 or ord(c)==127 for c in value)):
        fail('invalid_ipo_text')
    return value.strip()


def _number(value):
    if value is None:
        return None
    try:
        valid = type(value) in (int, float) and math.isfinite(value) and value >= 0
    except OverflowError:
        valid = False
    if not valid:
        fail('invalid_ipo_number')
    return value


def _json(value):
    try:
        return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)
    except (TypeError, ValueError):
        fail('invalid_ipo_response')


def normalize(raw, month, *, observed_at=None):
    now = _now(observed_at)
    start, end = window(month)
    if not isinstance(raw, dict) or set(raw) != {'ipoCalendar'} or not isinstance(raw['ipoCalendar'], list):
        fail('invalid_ipo_response')
    rows = raw['ipoCalendar']
    if len(rows)>=1000 or len(_json(raw).encode())>transport.MAX_BYTES:
        fail('ipo_response_at_bound')
    items = {}
    for row in rows:
        if not isinstance(row, dict):
            fail('invalid_ipo_row')
        try:
            day = date.fromisoformat(row['date'])
            if day.isoformat()!=row['date'] or not start<=day<=end:
                raise ValueError()
        except (KeyError, ValueError, TypeError):
            fail('invalid_ipo_date')
        status = row.get('status')
        if not isinstance(status, str) or status not in STATUS_LABELS:
            fail('invalid_ipo_status')
        name = _text(row.get('name'))
        exchange = _text(row.get('exchange'), optional=True)
        symbol = _text(row.get('symbol'), optional=True, limit=20)
        if symbol is not None and not re.fullmatch(r'[A-Z0-9]+(?:[./-][A-Z0-9]+)*', symbol):
            fail('invalid_ipo_symbol')
        price = _text(row.get('price'), optional=True, limit=80)
        shares, value = (_number(row.get(field)) for field in ('numberOfShares', 'totalSharesValue'))
        # Provider has no offering ID. This is a status-observation identity,
        # not a canonical issuer/security or economic transaction identity.
        # Dates/prices can be corrected without creating another same-status
        # item. Ambiguous same-name/status offerings are held, never merged.
        identity = hashlib.sha256(_json({'name': name.casefold(), 'status': status}).encode()).hexdigest()
        item = {'id': 'finnhub:ipo:'+identity, 'kind': 'ipo', 'date': day.isoformat(),
            'datetime': None, 'symbol': symbol, 'company': name,
            'exchange': exchange, 'country': None, 'importance': None,
            'title': f'{name} IPO: {STATUS_LABELS[status]}',
            'subtitle': f'Provider date for {status} status; first trading date not confirmed',
            'source': 'finnhub', 'source_url': SOURCE_URL, 'date_basis': 'provider_ipo_status_date',
            'observed_at': now.isoformat(), 'payload': {'status': status, 'exchange': exchange,
                'price': price, 'numberOfShares': shares, 'totalSharesValue': value,
                'currency': None, 'firstTradingDate': None, 'offeringIdentityVerified': False}}
        if identity in items and items[identity]!=item:
            fail('conflicting_ipo_status_identity')
        items[identity] = item
    return {'source': 'finnhub', 'version': VERSION, 'month': month, 'observed_at': now.isoformat(),
        'raw_sha256': hashlib.sha256(_json(raw).encode()).hexdigest(),
        'items': sorted(items.values(), key=lambda item: (item['date'], item['id'])),
        'coverage': 'provider_ipo_status_calendar; not confirmed trading debuts or guaranteed market coverage',
        'status': 'ok' if items else 'empty'}


def cached(db, month, *, observed_at=None):
    from app.models import InsightsSnapshot
    now = _now(observed_at)
    row = db.get(InsightsSnapshot, key(month), populate_existing=True)
    if row is None or row.source!=VERSION:
        return None
    fetched = row.fetched_at
    if fetched.tzinfo is None:
        fetched = fetched.replace(tzinfo=timezone.utc)
    if not timedelta(0)<=now-fetched<TTL:
        return None
    try:
        if len(row.payload_json.encode())>transport.MAX_BYTES*2:
            return None
        saved = json.loads(row.payload_json)
        checked = normalize(saved['raw'], month, observed_at=fetched)
        return checked if checked==saved['normalized'] else None
    except (KeyError, TypeError, ValueError, transport.FinnhubUnavailable):
        return None


def refresh(db, month, *, observed_at=None):
    from app.services.free_calendar import selected as calendar_selected
    if not (calendar_selected() or os.getenv('FREE_RESEARCH_WARMING_ENABLED', '0')=='1'):
        fail('ipo_preparation_disabled')
    from app.services.finnhub_budget import enabled
    if not enabled():
        fail('shared_request_budget_required')
    now = _now(observed_at)
    start, end = window(month)
    if abs((start-now.date()).days)>150:
        fail('ipo_month_outside_refresh_window')
    from app.models import InsightsSnapshot
    if db.get_bind().dialect.name=='postgresql':
        from sqlalchemy import text
        if not db.scalar(text('SELECT pg_try_advisory_xact_lock(hashtext(:key))'), {'key': key(month)}):
            fail('ipo_refresh_in_progress')
    existing = cached(db, month, observed_at=now)
    if existing is not None:
        return {'status': 'cached', 'items': len(existing['items'])}
    raw = transport.request_json('calendar/ipo', {'from': str(start), 'to': str(end)})
    normalized = normalize(raw, month, observed_at=now)
    payload = _json({'raw': raw, 'normalized': normalized})
    row = db.get(InsightsSnapshot, key(month))
    if row is None:
        row = InsightsSnapshot(kind=key(month), source=VERSION, fetched_at=now, payload_json=payload)
        db.add(row)
    row.source, row.fetched_at, row.payload_json = VERSION, now, payload
    db.flush()
    return {'status': normalized['status'], 'items': len(normalized['items'])}
