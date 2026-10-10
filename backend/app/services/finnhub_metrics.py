"""Free basic-financial metrics with explicit units, observation and period evidence.

Quarterly margin series are not TTM margins. Valuation snapshots have no supplied
market timestamp and must never acquire a historical filing date by inference.
"""
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import math
import os

from app.db import SessionLocal
from app.models import InsightsSnapshot
from app.services.finnhub_research import FinnhubUnavailable, request_json
from app.utils.symbols import normalize_symbol

VERSION = 'finnhub_metrics_v1'
TTL = timedelta(hours=24)
MAX_PERIOD_AGE = timedelta(days=150)
URL = 'https://finnhub.io/docs/api/company-basic-financials'
SCALARS = {
    'trailing_pe': ('peTTM', 'multiple'),
    'forward_pe': ('forwardPE', 'multiple'),
    'price_to_sales': ('psTTM', 'multiple'),
    'ev_to_ebitda': ('evEbitdaTTM', 'multiple'),
    'gross_margin': ('grossMarginTTM', 'percent'),
    'operating_margin': ('operatingMarginTTM', 'percent'),
    'net_margin': ('netProfitMarginTTM', 'percent'),
    'revenue_growth': ('revenueGrowthTTMYoy', 'percent'),
    'eps_growth': ('epsGrowthTTMYoy', 'percent'),
}
SERIES = {
    'current_ratio': ('currentRatio', 1, 'ratio'),
    'debt_to_equity': ('totalDebtToEquity', 1, 'ratio'),
    'roe': ('roeTTM', 100, 'percent'),
    'roic': ('roicTTM', 100, 'percent'),
}


def _number(value):
    if value is None:
        return None
    if type(value) not in (int, float) or not math.isfinite(value):
        raise FinnhubUnavailable('invalid_metric_number')
    return float(value)


def _latest(rows, observed_at):
    if not isinstance(rows, list) or len(rows) > 1000:
        raise FinnhubUnavailable('invalid_metric_series')
    points = {}
    for row in rows:
        try:
            day = date.fromisoformat(row['period'])
            value = _number(row.get('v'))
        except (ValueError, TypeError, KeyError, AttributeError):
            raise FinnhubUnavailable('invalid_metric_period') from None
        if day > observed_at.date():
            raise FinnhubUnavailable('future_metric_period')
        if day in points and points[day] != value:
            raise FinnhubUnavailable('conflicting_metric_period')
        points[day] = value
    if not points:
        return None
    day = max(points)
    if observed_at.date()-day > MAX_PERIOD_AGE or points[day] is None:
        return None
    return day, points[day]


def normalize(payload, *, symbol, observed_at):
    symbol = normalize_symbol(symbol)
    if not symbol or observed_at.tzinfo is None:
        raise FinnhubUnavailable('invalid_metric_request')
    observed_at = observed_at.astimezone(timezone.utc)
    if not isinstance(payload, dict) or normalize_symbol(payload.get('symbol')) != symbol:
        raise FinnhubUnavailable('metric_symbol_mismatch')
    if payload.get('metricType') != 'all' or not isinstance(payload.get('metric'), dict):
        raise FinnhubUnavailable('invalid_metric_payload')
    series = payload.get('series')
    if not isinstance(series, dict) or not isinstance(series.get('quarterly', {}), dict):
        raise FinnhubUnavailable('invalid_metric_series')
    quarterly = series.get('quarterly', {})
    points = {field: _latest(quarterly.get(key, []), observed_at)
              for field, (key, _, _) in SERIES.items()}
    try:
        fingerprint = hashlib.sha256(json.dumps(payload, sort_keys=True,
            separators=(',', ':'), allow_nan=False).encode()).hexdigest()
    except (ValueError, TypeError):
        raise FinnhubUnavailable('invalid_metric_payload') from None
    result = {'version': VERSION, 'source': 'finnhub', 'symbol': symbol,
              'observed_at': observed_at.isoformat(), 'values': {}, 'fields': {},
              'source_url': URL, 'payload_sha256': fingerprint,
              'limitations': ['Provider observation is not a filing or historical availability date.',
                  'Snapshot metrics have no supplied financial-period or market-price timestamp.',
                  'Forward P/E has no supplied forecast horizon or underlying estimate series.',
                  'No currency amounts, prices, net debt/EBITDA or annual analyst forecasts are inferred.']}
    for field, point in points.items():
        if point is None:
            continue
        key, scale, unit = SERIES[field]
        day, value = point
        if field == 'current_ratio' and value < 0:
            continue
        result['values'][field] = _number(value * scale)
        result['fields'][field] = {'source': 'finnhub', 'path': 'series.quarterly.'+key,
            'period_date': str(day), 'basis': 'ttm' if key.endswith('TTM') else 'quarter_end',
            'unit': unit, 'scale': scale, 'observed_at': observed_at.isoformat()}
    # Refuse current scalar metrics from issuers without any recent dated
    # financial series. This does not assert all scalar fields share that date.
    if result['values']:
        for field, (key, unit) in SCALARS.items():
            value = _number(payload['metric'].get(key))
            if value is None or (unit == 'multiple' and value <= 0):
                continue
            result['values'][field] = value
            result['fields'][field] = {'source': 'finnhub', 'path': 'metric.'+key,
                'period_date': None, 'basis': 'provider_current_snapshot', 'unit': unit,
                'scale': 1, 'observed_at': observed_at.isoformat()}
    result['status'] = 'partial' if result['values'] else 'unavailable'
    return result


def cached(symbol, now):
    with SessionLocal() as db:
        row = db.get(InsightsSnapshot, 'finnhub:metrics:'+symbol)
        if row is None or row.source != 'finnhub':
            return None
        stamp = row.fetched_at.replace(tzinfo=row.fetched_at.tzinfo or timezone.utc)
        if not timedelta(0) <= now-stamp < TTL:
            return None
        try:
            payload = json.loads(row.payload_json)
            observed = datetime.fromisoformat(payload['observed_at'])
            if (payload.get('version') == VERSION and payload.get('symbol') == symbol
                    and payload.get('source') == 'finnhub' and observed == stamp):
                return payload
        except (TypeError, ValueError, KeyError):
            pass
    return None


def prepare(symbol):
    symbol = normalize_symbol(symbol)
    if not symbol or not (os.getenv('FINNHUB_METRICS_WARMING_ENABLED') == '1'
            or os.getenv('FUNDAMENTALS_PROVIDER') == 'free_direct'):
        raise FinnhubUnavailable('metric_preparation_disabled')
    if os.getenv('FINNHUB_SHARED_LIMITER_ENABLED') != '1':
        raise FinnhubUnavailable('shared_budget_required')
    now = datetime.now(timezone.utc)
    prior = cached(symbol, now)
    if prior is not None:
        return prior
    from app.services.sec_financial_statements import _refresh_lock
    with SessionLocal() as db:
        bind = db.get_bind()
    with _refresh_lock(bind, 'finnhub:metrics:'+symbol) as acquired:
        if not acquired:
            raise FinnhubUnavailable('refresh_in_progress')
        prior = cached(symbol, now)
        if prior is not None:
            return prior
        return _prepare_locked(symbol, now)


def _prepare_locked(symbol, now):
    # Cache session closed before transport and its separate shared budget.
    payload = normalize(request_json('stock/metric', {'symbol': symbol, 'metric': 'all'}),
                        symbol=symbol, observed_at=now)
    with SessionLocal() as db:
        key = 'finnhub:metrics:'+symbol
        row = db.get(InsightsSnapshot, key)
        if row is None:
            row = InsightsSnapshot(kind=key, source='finnhub', fetched_at=now, payload_json='{}')
            db.add(row)
        row.source, row.fetched_at, row.payload_json = 'finnhub', now, json.dumps(payload, sort_keys=True)
        db.commit()
    return payload
