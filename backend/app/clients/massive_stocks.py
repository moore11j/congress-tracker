"""Bounded Stocks REST adapter with explicit errors and source timestamps."""
from datetime import datetime, timezone
import json
import math
import os
from urllib.parse import quote, urlsplit

import requests


def selected():
    return os.getenv('STOCK_PRICE_PROVIDER', 'fmp').strip().lower() == 'massive'


def provider_symbol(symbol):
    return symbol.replace('-', '.') if '-' in symbol and len(symbol.rsplit('-', 1)[-1]) == 1 else symbol


class MassiveStocksError(RuntimeError):
    pass


def _number(value, *, positive=False):
    try:
        result = float(value)
    except (ValueError, TypeError):
        return None
    return result if math.isfinite(result) and (result > 0 if positive else result >= 0) else None


def _unique_actions(rows):
    seen = {}
    for row in rows:
        fingerprint = json.dumps(row, sort_keys=True, separators=(',', ':'))
        identity = row.get('id') or fingerprint
        if identity in seen:
            if seen[identity] != fingerprint:
                raise MassiveStocksError('conflicting_action_identity')
            continue
        seen[identity] = fingerprint
        yield row


class MassiveStocksClient:
    def __init__(self):
        self.key = (os.getenv('MASSIVE_API_KEY') or os.getenv('POLYGON_API_KEY') or '').strip()

    def get(self, path, params=None):
        if not self.key:
            raise MassiveStocksError('missing_api_key')
        url = path if path.startswith('https://') else 'https://api.massive.com' + path
        parsed = urlsplit(url)
        if parsed.scheme != 'https' or parsed.netloc != 'api.massive.com' or parsed.fragment:
            raise MassiveStocksError('invalid_pagination_host')
        try:
            response = requests.get(url, params=params, headers={'Authorization': 'Bearer ' + self.key},
                                    timeout=15, allow_redirects=False)
        except requests.RequestException:
            raise MassiveStocksError('transport_unavailable') from None
        if response.status_code != 200:
            raise MassiveStocksError(f'provider_{response.status_code}')
        try:
            payload = response.json()
        except ValueError:
            raise MassiveStocksError('invalid_json') from None
        if not isinstance(payload, dict) or payload.get('status') not in {'OK', 'DELAYED'}:
            raise MassiveStocksError('invalid_response_status')
        return payload

    def pages(self, path, params):
        results, seen = [], set()
        for _ in range(20):
            if path in seen:
                raise MassiveStocksError('pagination_cycle')
            seen.add(path)
            payload = self.get(path, params)
            rows = payload.get('results', [])
            if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
                raise MassiveStocksError('invalid_results')
            results.extend(rows)
            path, params = payload.get('next_url'), None
            if not path:
                return results
        raise MassiveStocksError('pagination_limit')

    def bars(self, symbol, start, end, *, adjusted=True, interval='day'):
        if interval not in {'day', 'minute'}:
            raise MassiveStocksError('invalid_interval')
        return self.pages(f'/v2/aggs/ticker/{quote(provider_symbol(symbol), safe="")}/range/1/{interval}/{start}/{end}',
                          {'adjusted': str(adjusted).lower(), 'sort': 'asc', 'limit': 50000})

    def details(self, symbol):
        """Current reference data only; report-date lookup is not point-in-time evidence."""
        requested = provider_symbol(symbol)
        result = self.get(f'/v3/reference/tickers/{quote(requested, safe="")}').get('results')
        if (not isinstance(result, dict) or result.get('ticker') != requested
                or result.get('market') != 'stocks' or result.get('locale') != 'us'
                or str(result.get('currency_name', '')).lower() != 'usd'):
            raise MassiveStocksError('invalid_ticker_details')
        return result

    def actions(self, symbol, start, end, *, splits_only=False):
        symbol = provider_symbol(symbol)
        splits, dividends = {}, {}
        rows = self.pages('/stocks/v1/splits', {'ticker': symbol, 'execution_date.gte': start,
                          'execution_date.lte': end, 'limit': 1000})
        for row in _unique_actions(rows):
            day = row.get('execution_date', '')
            old, new = _number(row.get('split_from'), positive=True), _number(row.get('split_to'), positive=True)
            if not old or not new or not start <= day <= end or row.get('ticker') != symbol:
                raise MassiveStocksError('invalid_split')
            splits[day] = splits.get(day, 1.0) * old / new
        if not splits_only:
            rows = self.pages('/stocks/v1/dividends', {'ticker': symbol, 'ex_dividend_date.gte': start,
                              'ex_dividend_date.lte': end, 'limit': 1000})
            for row in _unique_actions(rows):
                day, amount = row.get('ex_dividend_date', ''), _number(row.get('cash_amount'))
                if amount is None or not start <= day <= end or row.get('ticker') != symbol or row.get('currency') != 'USD':
                    raise MassiveStocksError('invalid_dividend')
                dividends[day] = dividends.get(day, 0) + amount
        return dividends, splits

    def snapshots(self, symbols):
        if not symbols or len(symbols) > 100:
            raise MassiveStocksError('invalid_snapshot_batch')
        payload = self.get('/v2/snapshot/locale/us/markets/stocks/tickers', {'tickers': ','.join(symbols)})
        results = {}
        now = datetime.now(timezone.utc)
        rows = payload.get('tickers', [])
        if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
            raise MassiveStocksError('invalid_snapshots')
        for row in rows:
            symbol, minute = row.get('ticker'), row.get('min') or {}
            if not isinstance(minute, dict):
                continue
            price = _number(minute.get('c'), positive=True)
            try:
                observed = datetime.fromtimestamp(float(minute['t']) / 1000, tz=timezone.utc)
            except (KeyError, ValueError, TypeError, OverflowError, OSError):
                continue
            if symbol not in symbols or price is None or observed > now:
                continue
            prior = _number((row.get('prevDay') or {}).get('c'), positive=True)
            # Use the minute bar's own timestamp, never fetch time or an
            # unrelated quote/update timestamp paired with a stale close.
            results[symbol] = {'symbol': symbol, 'price': price, 'asof_ts': observed.replace(tzinfo=None),
                'provider_timestamp': observed.replace(tzinfo=None), 'cached_at': now.replace(tzinfo=None),
                'source': 'massive_minute_snapshot', 'price_kind': 'minute_close',
                'is_delayed': True, 'delay_minutes': 15, 'is_stale': (now - observed).total_seconds() > 1800,
                'volume': _number((row.get('day') or {}).get('v')),
                'previous_close': prior, 'change': price - prior if prior else None,
                'change_percent': (price / prior - 1) * 100 if prior else None}
        return results
