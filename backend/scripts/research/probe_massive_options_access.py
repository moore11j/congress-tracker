"""Small, rate-limited read-only entitlement probe; never changes app scoring."""
import json
import os
from pathlib import Path
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, quote
from urllib.request import Request, urlopen

BASE = Path('frontend/test-results/confirmation-research')


def main():
    env = {}
    for line in Path('backend/.env.local').read_text(encoding='utf-8-sig').splitlines():
        if '=' in line and not line.lstrip().startswith('#'):
            k, v = line.split('=', 1)
            env[k.strip()] = v.strip().strip(chr(34)).strip(chr(39))
    key = os.getenv('MASSIVE_API_KEY') or env.get('MASSIVE_API_KEY')
    if not key: raise SystemExit('MASSIVE_API_KEY is unavailable; no requests sent.')
    results = []; last = None

    def get(name, path, params):
        nonlocal last
        if last is not None:
            delay = 13-(time.monotonic()-last)
            if delay > 0: time.sleep(delay)
        last = time.monotonic()
        url = 'https://api.massive.com'+path+'?'+urlencode({**params, 'apiKey': key})
        try:
            with urlopen(Request(url, headers={'User-Agent': 'Walnut-options-research/1.0'}), timeout=25) as resp:
                status = resp.status; raw = resp.read()
        except HTTPError as exc:
            status = exc.code; raw = exc.read()
        except (URLError, TimeoutError):
            result = {'name': name, 'path': path, 'error': 'network_unavailable'}
            results.append(result); print(json.dumps(result), flush=True); return {}
        try: payload = json.loads(raw)
        except ValueError: payload = {}
        rows = payload.get('results', [])
        result = {'name': name, 'path': path, 'params': params, 'http_status': status,
                  'provider_status': payload.get('status'),
                  'message': str(payload.get('message') or payload.get('error') or '').replace(key, '[REDACTED]')[:400],
                  'results_count': len(rows) if isinstance(rows, list) else None,
                  'first_result_fields': sorted(rows[0]) if isinstance(rows, list) and rows else [],
                  'has_next_page': bool(payload.get('next_url'))}
        results.append(result); print(json.dumps(result), flush=True)
        # Only public contract/bar fields are retained. Never persist next_url or authentication.
        if status == 200 and name in ['contracts', 'daily_bars', 'minute_bars']:
            (BASE/('massive-options-'+name+'.json')).write_text(json.dumps({'results': rows}, indent=2))
        return payload

    contracts = get('contracts', '/v3/reference/options/contracts', {
        'underlying_ticker': 'TSM', 'as_of': '2026-09-11', 'contract_type': 'call',
        'expiration_date.gte': '2026-09-18', 'expiration_date.lte': '2026-10-30',
        'strike_price.gte': 425, 'strike_price.lte': 435, 'limit': 10, 'sort': 'ticker'})
    rows = contracts.get('results') or []
    ticker = rows[0].get('ticker') if isinstance(rows, list) and rows else None
    if ticker:
        encoded = quote(ticker, safe=':')
        get('daily_bars', f'/v2/aggs/ticker/{encoded}/range/1/day/2026-08-03/2026-09-11', {'adjusted': 'true', 'sort': 'asc', 'limit': 50000})
        get('minute_bars', f'/v2/aggs/ticker/{encoded}/range/1/minute/2026-09-11/2026-09-11', {'adjusted': 'true', 'sort': 'asc', 'limit': 50000})
    get('chain_snapshot', '/v3/snapshot/options/TSM', {'limit': 1})
    if ticker:
        get('trades', f'/v3/trades/{encoded}', {'timestamp': '2026-09-11', 'limit': 1})
        get('quotes', f'/v3/quotes/{encoded}', {'timestamp': '2026-09-11', 'limit': 1})
    out = {'checked_at_utc': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
           'credential_source': 'configured MASSIVE_API_KEY; value omitted', 'contract': ticker, 'probes': results}
    (BASE/'massive-options-access.json').write_text(json.dumps(out, indent=2))


if __name__ == '__main__': main()
