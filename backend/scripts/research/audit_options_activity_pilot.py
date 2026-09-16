"""Audit completed local options inputs without emitting authentication values."""
from datetime import date, datetime, timezone
import json

from options_activity_pilot import BASE, ROOT, LEDGER_HASH, api_key, digest, ny_timezone, save, select_pairs


def main():
    config = json.loads((ROOT/'cohort.json').read_text()); key = api_key().encode()
    failures = []; summary = []; requests = []
    for path in sorted((ROOT/'cache').glob('*.json')):
        raw = path.read_bytes()
        if key and key in raw: failures.append('credential_in_cache:'+path.name)
        payload = json.loads(raw)
        if any(k.lower() in ['apikey','api_key','token'] for k in payload['params']):
            failures.append('credential_parameter_in_cache:'+path.name)
        requests.append(payload['path'])
    for e in config['events']:
        path = ROOT/'events'/(e['ticker']+'.json')
        if not path.exists(): failures.append('missing_event:'+e['ticker']); continue
        data = json.loads(path.read_text()); bars = 0; counts = {}
        if data['event'] != e: failures.append('event_changed:'+e['ticker'])
        contracts = []
        for cached in sorted((ROOT/'cache').glob('*.json')):
            payload = json.loads(cached.read_text())
            if payload['path']=='/v3/reference/options/contracts' and payload['params'].get('underlying_ticker')==e['ticker']:
                if payload['params'].get('as_of')!=e['cutoff']: failures.append('reference_cutoff:'+e['ticker'])
                contracts.extend(payload['results'])
        # Current pilot references fit one page; duplicated records across flags collapse.
        refs = {r['ticker']:r for r in contracts}
        expected = select_pairs(list(refs.values()),e['cutoff'],e['selection_close'])
        for bucket,pair in data['buckets'].items():
            if [c['contract']['ticker'] for c in pair] != [c['ticker'] for c in expected.get(bucket,[])]:
                failures.append('selection_changed:'+e['ticker']+':'+bucket)
            counts[bucket] = []
            for c in pair:
                if c['contract'].get('underlying_ticker')!=e['ticker']: failures.append('wrong_underlying:'+e['ticker'])
                strike = c['contract']['strike_price']
                if not e['selection_close']*.9 <= strike <= e['selection_close']*1.1: failures.append('strike_outside_range:'+e['ticker'])
                for r in c['bars']:
                    day=datetime.fromtimestamp(r['t']/1000,timezone.utc).astimezone(ny_timezone()).date().isoformat()
                    if day>e['cutoff']: failures.append('future_bar:'+e['ticker'])
                bars += len(c['bars']); counts[bucket].append(len(c['bars']))
        summary.append({'ticker':e['ticker'],'reference_contracts':data['reference_count'],
                        'contracts':sum(len(v) for v in data['buckets'].values()),'daily_bars':bars,'bars_by_bucket':counts})
    if digest(BASE/'cohort.json')!=LEDGER_HASH: failures.append('ledger_hash_changed')
    audit={'passed':not failures,'failures':failures,'cached_requests':len(requests),
           'endpoint_families':sorted(set('reference' if p.startswith('/v3/reference/') else 'daily_aggregates' for p in requests)),
           'events':summary,'ledger_sha256':digest(BASE/'cohort.json')}
    save(ROOT/'input-audit.json',audit)
    print(json.dumps(audit,indent=2))
    if failures: raise SystemExit('Input audit failed')


if __name__=='__main__': main()
