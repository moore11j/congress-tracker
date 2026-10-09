"""Free research endpoint contracts. No subscription, price or forecast-series calls."""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import math

from app.services.finnhub_research import FinnhubUnavailable, request_json, request_rows
from app.utils.symbols import normalize_symbol


def number(value):
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (float, int)) or not math.isfinite(value):
        raise FinnhubUnavailable('invalid_numeric_value')
    return value


def earnings_calendar(payload, *, start: date, end: date, observed_at: datetime):
    if not isinstance(payload, dict) or not isinstance(payload.get('earningsCalendar'), list):
        raise FinnhubUnavailable('invalid_earnings_calendar')
    rows = payload['earningsCalendar']
    # A market-wide earnings month routinely exceeds the news feed's 1,000-row
    # limit. The HTTP adapter still enforces its separate two-megabyte bound.
    if len(rows) > 10000:
        raise FinnhubUnavailable('response_too_large')
    items = {}
    for row in rows:
        try:
            symbol = normalize_symbol(row['symbol'])
            day = date.fromisoformat(row['date'])
            quarter, year = row.get('quarter'), row.get('year')
            if not symbol or not start <= day <= end:
                raise ValueError()
            if quarter is not None and (type(quarter) is not int or not 1 <= quarter <= 4):
                raise ValueError()
            if year is not None and (type(year) is not int or not 1900 <= year <= 2200):
                raise ValueError()
            values = {k: number(row.get(k)) for k in ('epsActual', 'epsEstimate', 'revenueActual', 'revenueEstimate')}
        except (ValueError, TypeError, KeyError):
            raise FinnhubUnavailable('invalid_earnings_calendar_row') from None
        # Rescheduling one fiscal period updates the same identity. Conflicting
        # dates in a response are held rather than publishing both as earnings.
        key = f'{symbol}:{year}:{quarter}' if year is not None and quarter is not None else f'{symbol}:{day}'
        period = f'Q{quarter} {year}' if quarter is not None and year is not None else None
        hour = row.get('hour') if row.get('hour') in {'bmo', 'amc', 'dmh'} else None
        item = {'id': 'finnhub:earnings:' + key, 'kind': 'earnings', 'date': day.isoformat(),
                'datetime': None, 'symbol': symbol, 'company': None, 'title': f'{symbol} earnings',
                'subtitle': ' | '.join(x for x in (period, {'bmo': 'Before market open', 'amc': 'After market close', 'dmh': 'During market hours'}.get(hour)) if x) or None,
                'source': 'finnhub', 'source_url': 'https://finnhub.io/docs/api/earnings-calendar',
                'date_basis': 'provider_scheduled_date', 'observed_at': observed_at.isoformat(),
                'payload': {**values, 'year': year, 'quarter': quarter, 'hour': hour,
                            'accounting_basis': 'adjusted_non_gaap', 'estimate_scope': 'scheduled_earnings_only'}}
        if key in items and items[key] != item:
            raise FinnhubUnavailable('conflicting_earnings_period')
        items[key] = item
    return sorted(items.values(), key=lambda item: (item['date'], item['id']))


def fetch_earnings_calendar(start: date, end: date, *, observed_at=None):
    if end < start or (end - start).days > 31:
        raise ValueError('Choose a calendar window of at most 32 days')
    now = observed_at or datetime.now(timezone.utc)
    # Market-wide month responses can hit the provider's result ceiling. Bounded
    # weekly windows retain dates across a busy earnings season without relying
    # on the completeness of one large monthly response.
    combined = []
    def collect(window_start, window_end):
        payload = request_json('calendar/earnings', {'from': str(window_start), 'to': str(window_end)})
        earnings_calendar(payload, start=window_start, end=window_end, observed_at=now)
        rows = payload['earningsCalendar']
        if len(rows) >= 1500:
            if window_start == window_end:
                raise FinnhubUnavailable('earnings_window_possibly_truncated')
            # A saturated week is not evidence that the whole month is lost.
            # Bisect down to single dates. Shared request budget remains in
            # force, and even a one-day saturated result is never published.
            midpoint = window_start + (window_end-window_start)//2
            collect(window_start, midpoint)
            collect(midpoint+timedelta(days=1), window_end)
            return
        combined.extend(rows)
        if len(combined) > 10000:
            raise FinnhubUnavailable('response_too_large')
    cursor = start
    while cursor <= end:
        window_end = min(end, cursor + timedelta(days=6))
        collect(cursor, window_end)
        cursor = window_end + timedelta(days=1)
    return earnings_calendar({'earningsCalendar': combined}, start=start, end=end, observed_at=now)


def fetch_free_dataset(symbol: str, dataset: str, *, observed_at=None):
    """Read-only coverage audit; values are not silently mapped to FMP ratios."""
    symbol = normalize_symbol(symbol)
    if not symbol:
        raise ValueError('Invalid symbol')
    paths = {'metrics': 'stock/metric', 'profile': 'stock/profile2', 'peers': 'stock/peers', 'surprises': 'stock/earnings'}
    if dataset not in paths:
        raise ValueError('Unsupported free dataset')
    params = {'symbol': symbol}
    if dataset == 'metrics':
        params['metric'] = 'all'
    raw = request_json(paths[dataset], params)
    if dataset in {'metrics', 'profile'}:
        if not isinstance(raw, dict):
            raise FinnhubUnavailable('invalid_response')
        returned_symbol = raw.get('symbol') if dataset == 'metrics' else raw.get('ticker')
        if raw and normalize_symbol(returned_symbol) != symbol:
            raise FinnhubUnavailable('response_symbol_mismatch')
        items = [raw] if raw else []
    elif dataset == 'peers':
        if not isinstance(raw, list) or any(not isinstance(value, str) or not normalize_symbol(value) for value in raw):
            raise FinnhubUnavailable('invalid_peers')
        items = sorted(set(normalize_symbol(value) for value in raw))
    else:
        if not isinstance(raw, list) or any(not isinstance(row, dict) or normalize_symbol(row.get('symbol')) != symbol for row in raw):
            raise FinnhubUnavailable('response_symbol_mismatch')
        items = raw
    return {'source': 'finnhub', 'symbol': symbol, 'dataset': dataset, 'items': items,
            'status': 'ok' if items else 'empty', 'observed_at': (observed_at or datetime.now(timezone.utc)).isoformat(),
            'publication_eligible': False, 'coverage': 'Provider fields retained for units, periods and missingness review; not normalized financial statements.'}
