"""Reproduce expanded selections and check point-in-time features from local files."""
from collections import Counter
from datetime import datetime, timezone
import json
from pathlib import Path

from build_options_activity_panel import event_features
from options_activity_expanded import ROOT, BASE, LEDGER_HASH, load_stock_prices, select_pair
from options_activity_pilot import digest, ny_timezone, save


def identity(path, params):
    return path, json.dumps(params, sort_keys=True)


def cached_pages(first, cache):
    rows=[]; seen=set(); page=first
    while page is not None:
        key=identity(page['path'],page['params'])
        if key in seen:raise ValueError('Pagination cycle')
        seen.add(key); rows.extend(page['results'])
        nxt=page.get('next_request')
        page=cache[identity(*nxt)] if nxt else None
    return rows


def main():
    config=json.loads((ROOT/'cohort.json').read_text()); failures=[]; cache={}; summary=[]
    checks={BASE/'cohort.json':LEDGER_HASH,
            Path('docs/options-activity-expanded-protocol-2026-09-14.md'):config['protocol_sha256'],
            Path('backend/scripts/research/options_activity_expanded.py'):config['collector_sha256']}
    checks.update({BASE/name:sha for name,sha in config['stock_input_hashes'].items()})
    for path,sha in checks.items():
        if digest(path)!=sha:failures.append('frozen_file_changed:'+str(path))
    for path in (ROOT/'cache').glob('*.json'):
        page=json.loads(path.read_text());cache[identity(page['path'],page['params'])]=page
        if page.get('http_status')!=200:failures.append('http_status:'+path.name)
        if any(k.lower() in {'apikey','api_key','token','authorization'} for k in page['params']):
            failures.append('credential_parameter:'+path.name)
        if not (page['path']=='/v3/reference/options/contracts' or
                page['path'].startswith('/v2/aggs/ticker/O:') and '/range/1/day/' in page['path']):
            failures.append('unexpected_endpoint:'+path.name)
    prices,_=load_stock_prices();sessions=sorted(prices['SPY']);zone=ny_timezone()
    statuses=Counter();ignored=0;decisions=0;contracts=set()
    for group in config['groups']:
        path=ROOT/'groups'/(group['id']+'.json')
        if not path.exists():failures.append('missing_group:'+group['id']);continue
        data=json.loads(path.read_text());selection=json.loads((ROOT/'selections'/(group['id']+'.json')).read_text())
        if data['group']!=group or selection['group']!=group:failures.append('group_changed:'+group['id'])
        first=[p for p in cache.values() if p['path']=='/v3/reference/options/contracts' and
               p['params'].get('underlying_ticker')==group['ticker'] and p['params'].get('as_of')==group['anchor']]
        if len(first)!=1:failures.append('reference_root_count:'+group['id']);continue
        if first[0]['params'].get('expired')!='false':failures.append('historical_expired_filter:'+group['id'])
        refs=list({r['ticker']:r for r in cached_pages(first[0],cache) if r.get('ticker')}.values())
        expected=select_pair(refs,group['anchor'],group['selection_close'],group['ticker'])
        if expected!=selection['contracts'] or expected!=[r['contract'] for r in data['pair']]:
            failures.append('selection_changed:'+group['id'])
        if len(refs)!=data['reference_count']:failures.append('reference_count:'+group['id'])
        for contract in data['pair']:
            contracts.add(contract['contract']['ticker'])
            roots=[p for p in cache.values() if p['path'].startswith('/v2/aggs/ticker/'+contract['contract']['ticker']+'/range/1/day/') and
                   p['path'].endswith('/'+group['window_end']) and p['params'].get('limit')==50000]
            if len(roots)!=1 or cached_pages(roots[0],cache)!=contract['bars']:
                failures.append('bars_changed:'+group['id'])
        for decision in group['decisions']:
            decisions+=1
            if not group['anchor']<=decision['cutoff']<decision['entry_date']:
                failures.append('decision_cutoff:'+group['id'])
            original,audit=event_features(group,data['pair'],decision,prices,sessions)
            statuses[audit['status']]+=1;ignored+=audit.get('future_bars_ignored',0)
            # Removing all future bars must leave the model features exactly unchanged.
            trimmed=[{**p,'bars':[b for b in p['bars'] if datetime.fromtimestamp(b['t']/1000,timezone.utc).astimezone(zone).date().isoformat()<=decision['cutoff']]} for p in data['pair']]
            replay,_=event_features(group,trimmed,decision,prices,sessions)
            if original!=replay:failures.append('future_bar_influence:'+group['id']+':'+decision['entry_date'])
        summary.append({'group':group['id'],'contracts':len(data['pair']),
                        'bars':sum(len(p['bars']) for p in data['pair'])})
    result={'passed':not failures,'failures':failures,'cached_requests':len(cache),'groups':len(summary),
            'unique_contracts':len(contracts),'daily_bars':sum(r['bars'] for r in summary),
            'decision_replays':decisions,'future_bar_exclusions_across_decisions':ignored,
            'feature_statuses':dict(statuses),'ledger_sha256':digest(BASE/'cohort.json'),
            'cohort_sha256':digest(ROOT/'cohort.json'),'groups_detail':summary}
    save(ROOT/'input-audit.json',result)
    print(json.dumps({k:v for k,v in result.items() if k!='groups_detail'},indent=2))
    if failures:raise SystemExit('Expanded input audit failed')


if __name__=='__main__':main()
