"""Prepared BLS release dates and optional free Finnhub earnings dates.

Calendar readers and digests never fetch. Unsupported kinds stay explicit.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
from calendar import monthrange
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from app.clients.direct_sources import DirectSourceClient, DirectSourceError
from app.models import InsightsSnapshot
from app.services.finnhub_research import FinnhubUnavailable

BLS_URL = 'https://www.bls.gov/schedule/news_release/bls.ics'
TTL = timedelta(hours=24)


def selected():
    return os.getenv('CALENDAR_PROVIDER', 'fmp').strip().lower() == 'free_direct'


def _unescape(value):
    return re.sub(r'\\([nN,;\\])', lambda m: '\n' if m[1] in 'nN' else m[1], value).strip()


def parse_bls(raw: bytes):
    if len(raw) > 2_000_000:
        raise DirectSourceError('calendar_too_large')
    text = raw.decode('utf-8-sig')
    if 'BEGIN:VCALENDAR' not in text or 'END:VCALENDAR' not in text:
        raise DirectSourceError('invalid_ical_calendar')
    lines = re.sub(r'\r?\n[ \t]', '', text).splitlines()
    records, current = {}, None
    for line in lines:
        if line == 'BEGIN:VEVENT':
            if current is not None:
                raise DirectSourceError('nested_calendar_event')
            current = {}
        elif line == 'END:VEVENT':
            if current is None:
                raise DirectSourceError('invalid_calendar_event')
            try:
                uid, title = current['UID'][1], _unescape(current['SUMMARY'][1])
                header, value = current['DTSTART']
                if not uid or not title or any(k in current for k in ('RRULE', 'RDATE', 'RECURRENCE-ID')):
                    raise ValueError()
                if value.endswith('Z'):
                    stamp = datetime.strptime(value, '%Y%m%dT%H%M%SZ').replace(tzinfo=timezone.utc)
                elif header in {'DTSTART;TZID=US-Eastern', 'DTSTART;TZID=America/New_York'}:
                    stamp = datetime.strptime(value, '%Y%m%dT%H%M%S').replace(tzinfo=ZoneInfo('America/New_York')).astimezone(timezone.utc)
                else:
                    raise ValueError('unsupported calendar clock')
                sequence = int(current.get('SEQUENCE', ('', '0'))[1])
                if sequence < 0:
                    raise ValueError()
                item = {'id': 'bls:economic:' + uid, 'kind': 'economic', 'symbol': None, 'company': None,
                        'date': stamp.astimezone(ZoneInfo('America/New_York')).date().isoformat(),
                        'datetime': stamp.isoformat(), 'title': title, 'subtitle': 'BLS scheduled release',
                        'source': 'bls', 'source_url': BLS_URL, 'date_basis': 'official_scheduled_release',
                        'country': 'US', 'importance': None, 'payload': {'uid': uid, 'sequence': sequence,
                            'status': current.get('STATUS', ('', 'CONFIRMED'))[1], 'actual': None, 'estimate': None}}
            except (KeyError, TypeError, ValueError):
                raise DirectSourceError('invalid_bls_event') from None
            prior = records.get(uid)
            if prior and prior['payload']['sequence'] == sequence and prior != item:
                raise DirectSourceError('conflicting_bls_event')
            if not prior or prior['payload']['sequence'] < sequence:
                records[uid] = item
            current = None
        elif current is not None:
            header, separator, value = line.partition(':')
            key = header.split(';', 1)[0]
            if separator:
                if key in current:
                    raise DirectSourceError('duplicate_ical_property')
                current[key] = (header, value)
    if current is not None or not records:
        raise DirectSourceError('incomplete_bls_calendar')
    return sorted([row for row in records.values() if row['payload']['status'] != 'CANCELLED'], key=lambda row: (row['date'], row['id']))


def _cached(db, key, source, now):
    row = db.get(InsightsSnapshot, key)
    if row is None or row.source != source:
        return None
    stamp = row.fetched_at.replace(tzinfo=timezone.utc) if row.fetched_at.tzinfo is None else row.fetched_at
    if not timedelta(0) <= now - stamp <= TTL:
        return None
    try:
        payload = json.loads(row.payload_json)
        if payload.get('source') == source and isinstance(payload.get('items'), list):
            return payload
    except (TypeError, ValueError):
        pass
    return None


def refresh(db, dataset: str, month: str):
    if not (selected() or os.getenv('FREE_RESEARCH_WARMING_ENABLED', '0') == '1') or dataset not in {'bls', 'earnings'}:
        raise ValueError('Invalid free calendar job')
    start = date.fromisoformat(month + '-01')
    end = start.replace(day=monthrange(start.year, start.month)[1])
    now = datetime.now(timezone.utc)
    if abs((start - now.date()).days) > 150:
        raise ValueError('Calendar month outside refresh window')
    key, source = ('free-calendar:bls:v1', 'bls') if dataset == 'bls' else (f'free-calendar:earnings:{month}', 'finnhub')
    if db.get_bind().dialect.name == 'postgresql':
        from sqlalchemy import text
        if not db.execute(text('SELECT pg_try_advisory_xact_lock(hashtext(:key))'), {'key': key}).scalar():
            raise DirectSourceError('calendar_refresh_in_progress')
    if _cached(db, key, source, now) is not None:
        return {'status': 'cached'}
    if dataset == 'bls':
        raw = DirectSourceClient(issuer_hosts=('www.bls.gov',)).get(BLS_URL)
        items = parse_bls(raw)
        sha = hashlib.sha256(raw).hexdigest()
    else:
        from app.services.finnhub_free_data import fetch_earnings_calendar
        items = fetch_earnings_calendar(start, end, observed_at=now)
        sha = None
    row = db.get(InsightsSnapshot, key)
    if row is None:
        row = InsightsSnapshot(kind=key, source=source, fetched_at=now, payload_json='{}')
        db.add(row)
    row.source, row.fetched_at = source, now
    row.payload_json = json.dumps({'source': source, 'observed_at': now.isoformat(), 'items': items, 'sha256': sha}, sort_keys=True)
    db.flush()
    return {'status': 'ok', 'items': len(items)}


def calendar_for_symbols(db, symbols, *, start, end, scope, enqueue):
    from app.services.event_calendar import CalendarFetchResult
    from app.services.data_enrichment_queue import enqueue_data_enrichment_job
    from app.utils.symbols import normalize_symbol
    if end < start or (end - start).days > 120 or scope not in {'all', 'watchlist'}:
        return CalendarFetchResult([], [{'kind': kind, 'reason': 'unsupported_calendar_scope'} for kind in ('economic', 'earnings', 'ipo', 'dividend', 'split')])
    symbols = {normalize_symbol(s) for s in symbols}
    months, cursor = [], start.replace(day=1)
    while cursor <= end:
        months.append(cursor.strftime('%Y-%m'))
        cursor = (cursor.replace(day=28) + timedelta(days=4)).replace(day=1)
    work = [('bls', months[0], 'free-calendar:bls:v1', 'bls', 'economic')]
    work += [('earnings', month, f'free-calendar:earnings:{month}', 'finnhub', 'earnings') for month in months]
    errors = [{'kind': kind, 'reason': 'coverage_unavailable'} for kind in ('ipo', 'dividend', 'split')]
    # Partial calendar coverage remains visible even if every BLS date loaded.
    errors.append({'kind': 'economic', 'reason': 'bls_only_no_consensus'})
    now, items = datetime.now(timezone.utc), {}
    for dataset, month, key, source, kind in work:
        payload = _cached(db, key, source, now)
        if payload is None:
            errors.append({'kind': kind, 'reason': 'cache_miss'})
            if enqueue and (dataset != 'earnings' or os.getenv('FINNHUB_API_KEY', '').strip()):
                enqueue_data_enrichment_job(job_type='free_calendar', source='page_load', priority=45,
                    window_key=key, payload={'dataset': dataset, 'month': month})
            continue
        records = payload['items']
        if dataset == 'bls' and (not records or max(row['date'] for row in records) < end.isoformat()):
            errors.append({'kind': 'economic', 'reason': 'schedule_horizon_incomplete'})
        for item in records:
            if start.isoformat() <= item['date'] <= end.isoformat() and (kind == 'economic' or scope == 'all' or item.get('symbol') in symbols):
                items[item['id']] = item
    return CalendarFetchResult(sorted(items.values(), key=lambda row: (row['date'], row['id'])), errors)
