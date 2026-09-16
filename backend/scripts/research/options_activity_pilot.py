"""Free, read-only options research collector. No application/database imports."""
import argparse
from bisect import bisect_left
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import time
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qsl, quote, urlencode, urlsplit
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from conviction_features import load_prices

BASE = Path('frontend/test-results/confirmation-research')
ROOT = BASE/'options-activity-pilot'
SYMBOLS = ['TSM', 'AAPL', 'NVDA', 'MSFT', 'AMZN', 'JPM', 'XOM', 'WMT']
LEDGER_HASH = 'de0885ffc1c1b026541b0cb364efd7c0b5dcf9feb62108d7d08473a49303613b'
BUCKETS = [('1_7', 1, 7, 7), ('8_29', 8, 29, 21), ('30_90', 30, 90, 60)]


def ny_timezone():
    try: return ZoneInfo('America/New_York')
    except ZoneInfoNotFoundError:
        path = Path('backend/.venv/Lib/site-packages/tzdata/zoneinfo/America/New_York')
        with path.open('rb') as stream:
            return ZoneInfo.from_file(stream, key='America/New_York')


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix('.tmp')
    temp.write_text(json.dumps(value, indent=2, allow_nan=False), encoding='utf-8')
    temp.replace(path)


def cutoff_session(calculated_at, sessions):
    stamp = datetime.fromisoformat(calculated_at.replace('Z', '+00:00'))
    if stamp.tzinfo is None: raise ValueError('Calculation timestamp needs timezone')
    local_day = stamp.astimezone(ny_timezone()).date().isoformat()
    ix = bisect_left(sessions, local_day)-1
    if ix < 0: raise ValueError('No prior benchmark session')
    return sessions[ix]


def prepare():
    if digest(BASE/'cohort.json') != LEDGER_HASH: raise ValueError('Frozen ledger hash changed')
    records = json.loads((BASE/'cohort.json').read_text())['events']
    prices = load_prices(); sessions = sorted(prices['SPY']); events = []
    for symbol in SYMBOLS:
        rows = sorted([r for r in records if r['ticker'] == symbol], key=lambda r: r['calculated_at'])
        if not rows: raise ValueError('Missing pilot ticker '+symbol)
        r = rows[0]; cutoff = cutoff_session(r['calculated_at'], sessions)
        if cutoff not in prices.get(symbol, {}): raise ValueError('Missing cutoff stock price '+symbol)
        p = prices[symbol][cutoff]
        events.append({'id': r['id'], 'ticker': symbol, 'calculated_at': r['calculated_at'],
                       'cutoff': cutoff, 'selection_close': p[2], 'selection_price_source': p[6]})
    config = {'version': 1, 'ledger_sha256': LEDGER_HASH, 'symbols': SYMBOLS,
              'strike_fraction': .10, 'buckets': BUCKETS, 'events': events,
              'note': 'Frozen membership and dated inputs; contains no outcomes.'}
    path = ROOT/'cohort.json'
    if path.exists() and json.loads(path.read_text()) != json.loads(json.dumps(config)):
        raise ValueError('Existing frozen pilot configuration differs; do not overwrite')
    save(path, config)
    return config


def standard_contract(r):
    return (r.get('contract_type') in ['call', 'put'] and r.get('shares_per_contract') == 100
            and not r.get('additional_underlyings') and isinstance(r.get('strike_price'), (int, float))
            and math.isfinite(r['strike_price']) and r['strike_price'] > 0
            and isinstance(r.get('ticker'), str) and r['ticker'].startswith('O:'))


def select_pairs(rows, cutoff, spot):
    """Contract selection uses dated metadata and spot only; never bar activity."""
    paired = defaultdict(dict)
    for r in sorted(rows, key=lambda r: r.get('ticker', '')):
        if standard_contract(r):
            paired[r['expiration_date'], r['strike_price']][r['contract_type']] = r
    result = {}
    for name, low, high, target in BUCKETS:
        options = []
        for (expiry, strike), pair in paired.items():
            dte = (date.fromisoformat(expiry)-date.fromisoformat(cutoff)).days
            if set(pair) == {'call', 'put'} and low <= dte <= high:
                options.append((abs(dte-target), abs(strike/spot-1), expiry, strike, pair))
        if options:
            chosen = min(options, key=lambda x: x[:4])
            result[name] = [chosen[4]['call'], chosen[4]['put']]
    return result


def safe_next(url):
    parsed = urlsplit(url)
    if parsed.scheme != 'https' or parsed.hostname != 'api.massive.com' or parsed.port not in [None, 443] or parsed.username:
        raise ValueError('Unexpected pagination origin')
    return parsed.path, {k: v for k, v in parse_qsl(parsed.query) if k.lower() not in ['apikey', 'api_key', 'token']}


class StopCollection(Exception): pass


class CachedClient:
    def __init__(self, key, budget):
        self.key = key; self.budget = budget; self.calls = 0
        self.cache = ROOT/'cache'; self.cache.mkdir(parents=True, exist_ok=True)
        self.state = ROOT/'request-clock.json'

    def get(self, path, params):
        if not (path == '/v3/reference/options/contracts' or path.startswith('/v2/aggs/ticker/O:')):
            raise ValueError('Only free reference and aggregate endpoints are permitted')
        identity = json.dumps([path, params], sort_keys=True)
        target = self.cache/(hashlib.sha256(identity.encode()).hexdigest()+'.json')
        if target.exists(): return json.loads(target.read_text())
        if self.calls >= self.budget: raise StopCollection('request_budget_reached')
        last = json.loads(self.state.read_text()).get('last', 0) if self.state.exists() else 0
        delay = 13-(time.time()-last)
        if delay > 0: time.sleep(delay)
        save(self.state, {'last': time.time()}); self.calls += 1
        url = 'https://api.massive.com'+path+'?'+urlencode({**params, 'apiKey': self.key})
        try:
            with urlopen(Request(url, headers={'User-Agent': 'Walnut-free-options-research/1.0'}), timeout=25) as response:
                code = response.status; raw = response.read()
        except HTTPError as exc: code = exc.code; raw = exc.read()
        except (URLError, TimeoutError): raise StopCollection('network_unavailable') from None
        try: payload = json.loads(raw)
        except ValueError: raise StopCollection('invalid_json') from None
        print(json.dumps({'request': self.calls, 'http_status': code, 'path': path,
                          'rows': len(payload.get('results') or [])}), flush=True)
        if code != 200: raise StopCollection('provider_http_'+str(code))
        if payload.get('status') not in ['OK', 'DELAYED'] or not isinstance(payload.get('results', []), list):
            raise StopCollection('unexpected_provider_payload')
        next_request = safe_next(payload['next_url']) if payload.get('next_url') else None
        stored = {'path': path, 'params': params, 'fetched_at': datetime.now(timezone.utc).isoformat(),
                  'http_status': code, 'results': payload.get('results', []), 'next_request': next_request}
        save(target, stored)
        return stored

    def all_pages(self, path, params):
        rows = []; seen = set()
        for _ in range(20):
            identity = json.dumps([path, params], sort_keys=True)
            if identity in seen: raise StopCollection('pagination_loop')
            seen.add(identity); payload = self.get(path, params); rows.extend(payload['results'])
            if not payload['next_request']: return rows
            path, params = payload['next_request']
        raise StopCollection('pagination_cap_incomplete')


def api_key():
    value = os.getenv('MASSIVE_API_KEY')
    if value: return value
    for line in Path('backend/.env.local').read_text(encoding='utf-8-sig').splitlines():
        if '=' in line and not line.lstrip().startswith('#'):
            k, v = line.split('=', 1)
            if k.strip() == 'MASSIVE_API_KEY': return v.strip().strip(chr(34)).strip(chr(39))
    raise ValueError('Configured Massive API key unavailable')


def collect(config, budget):
    # One writer per local collector. OS lock releases automatically on process exit.
    import msvcrt
    lock = (ROOT/'collector.lock').open('a+b'); lock.seek(0)
    if lock.read(1) == b'': lock.write(b'0'); lock.flush()
    lock.seek(0)
    try: msvcrt.locking(lock.fileno(), msvcrt.LK_NBLCK, 1)
    except OSError: lock.close(); raise StopCollection('collector_already_running') from None
    client = CachedClient(api_key(), budget); output = []; status = 'complete'
    try:
        for event in config['events']:
            target = ROOT/'events'/(event['ticker']+'.json')
            if target.exists():
                saved = json.loads(target.read_text())
                if saved['event'] != event: raise ValueError('Cached event identity changed')
                output.append({'ticker': event['ticker'], 'status': 'cached', 'buckets': sorted(saved['buckets'])}); continue
            d = date.fromisoformat(event['cutoff']); spot = event['selection_close']; contracts = {}
            for expired in ['true', 'false']:
                params = {'underlying_ticker': event['ticker'], 'as_of': event['cutoff'],
                          'expired': expired, 'expiration_date.gte': (d+timedelta(days=1)).isoformat(),
                          'expiration_date.lte': (d+timedelta(days=90)).isoformat(),
                          'strike_price.gte': round(spot*.9, 4), 'strike_price.lte': round(spot*1.1, 4),
                          'sort': 'ticker', 'order': 'asc', 'limit': 1000}
                for r in client.all_pages('/v3/reference/options/contracts', params):
                    if r.get('ticker'): contracts[r['ticker']] = r
            pairs = select_pairs(list(contracts.values()), event['cutoff'], spot); buckets = {}
            for bucket, pair in pairs.items():
                collected = []
                for contract in pair:
                    ticker = quote(contract['ticker'], safe=':')
                    path = f'/v2/aggs/ticker/{ticker}/range/1/day/{(d-timedelta(days=90)).isoformat()}/{event["cutoff"]}'
                    rows = client.all_pages(path, {'adjusted': 'false', 'sort': 'asc', 'limit': 50000})
                    collected.append({'contract': contract, 'bars': rows, 'fetch_complete': True})
                buckets[bucket] = collected
            save(target, {'event': event, 'reference_count': len(contracts), 'reference_complete': True,
                          'buckets': buckets, 'selection': 'matched ATM sample, not full chain',
                          'collected_at': datetime.now(timezone.utc).isoformat()})
            output.append({'ticker': event['ticker'], 'status': 'collected', 'buckets': sorted(buckets)})
            print(json.dumps(output[-1]), flush=True)
    except StopCollection as exc:
        status = str(exc)
    finally:
        lock.seek(0); msvcrt.locking(lock.fileno(), msvcrt.LK_UNLCK, 1); lock.close()
        save(ROOT/'collection-status.json', {'status': status, 'requests_this_run': client.calls, 'completed': output,
                                           'cohort_sha256': digest(ROOT/'cohort.json'), 'ledger_sha256': digest(BASE/'cohort.json')})
    if digest(BASE/'cohort.json') != LEDGER_HASH: raise ValueError('Frozen ledger changed')
    print(json.dumps({'status': status, 'requests': client.calls, 'completed': len(output)}), flush=True)


def main():
    parser = argparse.ArgumentParser(); parser.add_argument('--collect', action='store_true')
    parser.add_argument('--max-requests', type=int, default=80); args = parser.parse_args()
    if not 1 <= args.max_requests <= 100: parser.error('Request cap must be 1–100')
    config = prepare()
    if args.collect: collect(config, args.max_requests)
    else: print(json.dumps(config, indent=2))


if __name__ == '__main__': main()
