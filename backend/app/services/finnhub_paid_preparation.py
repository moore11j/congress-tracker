"""Staged Finnhub estimates/targets, never canonical financials or analyst events.

One explicitly enabled scope per call. Separate readers require reviewed basis evidence.
Currency, accounting basis and share basis require live source review before use.
Schemas: Finnhub-Stock-API/finnhub-go@8071bcd46ca5101adcef7a844c96b81a3eceb76e.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import math
import re

from app.services import finnhub_research as transport

VERSION = 'finnhub_paid_preparation_v1'
TTL = timedelta(hours=24)
MAX_ROWS = 1000
KINDS = {'eps': 'stock/eps-estimate', 'revenue': 'stock/revenue-estimate', 'targets': 'stock/price-target'}


def _fail(reason):
    raise transport.FinnhubUnavailable(reason)


def _now(value):
    value = value or datetime.now(timezone.utc)
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        _fail('invalid_observation_time')
    return value.astimezone(timezone.utc)


def scope(symbol, kind, frequency=None):
    # Keep exchange prefixes and ambiguous identity transformations out of this
    # staging path. Share-class punctuation maps without discarding the class.
    if not isinstance(symbol, str) or not re.fullmatch(r'[A-Za-z]{1,6}(?:[./-][A-Za-z])?', symbol):
        _fail('invalid_symbol')
    symbol = symbol.upper()
    provider_symbol = re.sub(r'[/-]', '.', symbol)
    if kind not in KINDS or (kind == 'targets' and frequency is not None):
        _fail('invalid_scope')
    if kind != 'targets' and frequency not in {'annual', 'quarterly'}:
        _fail('invalid_frequency')
    return symbol, provider_symbol, f'finnhub:paid-preparation:{kind}:{frequency or "current"}:{provider_symbol}'


def _number(value):
    if value is None:
        return None
    try:
        valid = not isinstance(value, bool) and isinstance(value, (int, float)) and math.isfinite(value)
    except OverflowError:
        valid = False
    if not valid:
        _fail('invalid_numeric_value')
    return value


def _integer(value, *, minimum, maximum):
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        _fail('invalid_integer_value')
    return value


def _date(value):
    if not isinstance(value, str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}', value):
        _fail('invalid_source_date')
    try:
        return date.fromisoformat(value)
    except ValueError:
        _fail('invalid_source_date')


def _digest(raw):
    try:
        encoded = json.dumps(raw, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()
    except (ValueError, TypeError, OverflowError):
        _fail('invalid_response')
    if len(encoded) > transport.MAX_BYTES:
        _fail('response_too_large')
    return hashlib.sha256(encoded).hexdigest()


def normalize(raw, symbol, kind, *, frequency=None, observed_at=None):
    now = _now(observed_at)
    symbol, provider_symbol, _ = scope(symbol, kind, frequency)
    if not isinstance(raw, dict) or raw.get('error') or raw.get('symbol') != provider_symbol:
        _fail('response_identity_mismatch')
    evidence_hash = _digest(raw)
    result = {'schemaVersion': VERSION, 'source': 'finnhub', 'symbol': symbol,
              'providerSymbol': provider_symbol, 'kind': kind, 'frequency': frequency,
              'endpoint': KINDS[kind], 'observedAt': now.isoformat(), 'sourceSha256': evidence_hash,
              'publicationEligible': False, 'currency': None, 'accountingBasis': 'unverified',
              'shareBasis': 'unverified', 'sourceUpdatedAt': None}
    if kind == 'targets':
        return _targets(raw, result, now)
    if raw.get('freq') != frequency or not isinstance(raw.get('data'), list):
        _fail('response_frequency_mismatch')
    if len(raw['data']) > MAX_ROWS:
        _fail('response_too_large')
    rows = {}
    fiscal_periods = {}
    for item in raw['data']:
        if not isinstance(item, dict):
            _fail('invalid_estimate_row')
        period = _date(item.get('period'))
        year = _integer(item.get('year'), minimum=1900, maximum=2200)
        quarter = item.get('quarter')
        if frequency == 'quarterly':
            quarter = _integer(quarter, minimum=1, maximum=4)
        elif quarter is not None and (type(quarter) is not int or quarter != 0):
            _fail('invalid_annual_quarter')
        analysts = item.get('numberAnalysts')
        if analysts is not None:
            analysts = _integer(analysts, minimum=0, maximum=10000)
        low, average, high = (_number(item.get(kind + suffix)) for suffix in ('Low', 'Avg', 'High'))
        ordered = [v for v in (low, average, high) if v is not None]
        if ordered != sorted(ordered) or (kind == 'revenue' and any(v < 0 for v in ordered)):
            _fail('invalid_estimate_range')
        row = {'period': period.isoformat(), 'fiscalYear': year, 'fiscalQuarter': quarter,
               'low': low, 'average': average, 'high': high, 'numberAnalysts': analysts}
        # Identical duplicates collapse; disagreement cannot silently win by order.
        if period in rows and rows[period] != row:
            _fail('conflicting_estimate_period')
        fiscal_key = (year, quarter)
        if fiscal_key in fiscal_periods and fiscal_periods[fiscal_key] != period:
            _fail('conflicting_fiscal_period')
        fiscal_periods[fiscal_key] = period
        rows[period] = row
    result['items'] = [rows[p] for p in sorted(rows)]
    # A historical period is never recycled as the next forecast. Fiscal labels
    # remain provider labels; fiscal year isn't inferred from calendar year.
    future = [row for row in result['items'] if _date(row['period']) >= now.date()]
    result['nextPeriod'] = future[0] if future else None
    result['estimateBasis'] = 'provider_average_includes_proprietary_estimates'
    result['status'] = ('empty' if not rows else 'historical_only' if not future else
                        'prepared' if all(row['average'] is not None and (row['numberAnalysts'] or 0) > 0
                                          for row in future) else 'partial')
    return result


def _targets(raw, result, now):
    stamp = raw.get('lastUpdated')
    if isinstance(stamp, str) and len(stamp) == 10:
        updated = _date(stamp)
    else:
        try:
            timestamp = datetime.fromisoformat(stamp.replace('Z', '+00:00'))
            if timestamp.tzinfo is None or timestamp > now:
                _fail('invalid_target_date')
            updated = timestamp.astimezone(timezone.utc).date()
        except (AttributeError, ValueError, TypeError):
            _fail('invalid_target_date')
    if updated > now.date():
        _fail('invalid_target_date')
    values = {name: _number(raw.get(name)) for name in ('targetLow', 'targetMean', 'targetMedian', 'targetHigh')}
    valid = [v for v in values.values() if v is not None]
    if any(v <= 0 for v in valid):
        _fail('invalid_target_range')
    low, high = values['targetLow'], values['targetHigh']
    if (low is not None and any(v < low for v in valid)) or (high is not None and any(v > high for v in valid)):
        _fail('invalid_target_range')
    analysts = raw.get('numberAnalysts')
    if analysts is not None:
        analysts = _integer(analysts, minimum=0, maximum=10000)
    result.update(values)
    result.update(numberAnalysts=analysts, sourceUpdatedAt=stamp, targetHorizon='unverified',
                  estimateBasis='analyst_price_targets')
    result['status'] = ('stale' if (now.date() - updated).days > 45 else
                        'prepared' if values['targetMean'] is not None and (analysts or 0) > 0 else 'partial')
    return result


def prepare(db, symbol, kind, *, frequency=None, observed_at=None):
    """Stage one scope using the shared budget; caller owns commit/rollback.

    No loops, automatic retries, scheduler registration, FMP fallback, events or
    canonical financial writes. Failed requests leave prior evidence untouched.
    """
    transport.require_paid_preparation()
    now = _now(observed_at)
    symbol, provider_symbol, key = scope(symbol, kind, frequency)
    from app.models import InsightsSnapshot
    if db.get_bind().dialect.name == 'postgresql':
        from sqlalchemy import text
        if not db.scalar(text('SELECT pg_try_advisory_xact_lock(hashtext(:key))'), {'key': key}):
            _fail('refresh_in_progress')
    row = db.get(InsightsSnapshot, key, populate_existing=True)
    if row is not None and row.source == VERSION:
        fetched = row.fetched_at
        fetched = fetched.replace(tzinfo=timezone.utc) if fetched.tzinfo is None else fetched
        if timedelta(0) <= now - fetched < TTL:
            try:
                saved = json.loads(row.payload_json)
                original_symbol = saved['normalized']['symbol']
                if scope(original_symbol, kind, frequency)[1] != provider_symbol:
                    raise ValueError('invalid_saved_identity')
                old = normalize(saved['raw'], original_symbol, kind, frequency=frequency, observed_at=fetched)
                if old != saved['normalized']:
                    raise ValueError('invalid_saved_projection')
                # Re-evaluate period and target age at read time. The observation
                # time remains when data was obtained, not when the cache was read.
                current = normalize(saved['raw'], original_symbol, kind, frequency=frequency, observed_at=now)
                return {'status': 'cached', 'scope': key, 'sourceStatus': current['status'],
                        'publicationEligible': False}
            except (KeyError, ValueError, TypeError, transport.FinnhubUnavailable):
                pass
    params = {'symbol': provider_symbol}
    if frequency:
        params['freq'] = frequency
    raw = transport.request_json(KINDS[kind], params)
    normalized = normalize(raw, symbol, kind, frequency=frequency, observed_at=now)
    payload = json.dumps({'raw': raw, 'normalized': normalized}, sort_keys=True, allow_nan=False)
    if row is None:
        row = InsightsSnapshot(kind=key, source=VERSION, fetched_at=now, payload_json=payload)
        db.add(row)
    row.source, row.fetched_at, row.payload_json = VERSION, now, payload
    db.flush()
    return {'status': 'prepared', 'scope': key, 'sourceStatus': normalized['status'],
            'publicationEligible': False}
