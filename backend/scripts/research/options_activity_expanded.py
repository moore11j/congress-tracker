"""Resumable free options panel across frozen historical anchor windows."""
import argparse
from bisect import bisect_left
from collections import defaultdict
from contextlib import redirect_stdout
from datetime import date, datetime, timedelta, timezone
import json
import math
from pathlib import Path
from urllib.parse import quote

from options_activity_pilot import BASE, ROOT as SHARED_ROOT, CachedClient, StopCollection, api_key, digest, save, standard_contract, LEDGER_HASH

ROOT = BASE/'options-activity-expanded'
SYMBOLS = ['TSM','AAPL','NVDA','MSFT','AMZN','JPM','XOM','WMT','GOOGL','UNH','CAT','KO']
ANCHORS = [('2025-01-02','train'),('2025-05-01','train'),('2025-09-02','train'),
           ('2026-01-02','validation'),('2026-05-01','test'),('2026-07-01','test')]
PROVIDERS = ['fmp:historical-price-eod/full+corporate_actions','massive:grouped-daily-adjusted']
STOCK_FILES = ['historical-panel.json','ledger-trailing-prices.json','recent-provider-prices.json','options-study-stock-prices.json']


def load_stock_prices():
    by_provider = {p:defaultdict(dict) for p in PROVIDERS}
    for name in STOCK_FILES:
        for row in json.loads((BASE/name).read_text())['rows']:
            if row[0] not in SYMBOLS+['SPY'] or row[6] not in PROVIDERS: continue
            if date.fromisoformat(row[1]).weekday()>=5: continue
            if not all(isinstance(row[i],(int,float)) and math.isfinite(row[i]) and row[i]>0 for i in [2,3,4]): continue
            by_provider[row[6]][row[0]][row[1]]=row
    merged=defaultdict(dict)
    for provider in reversed(PROVIDERS):
        for symbol, rows in by_provider[provider].items(): merged[symbol].update(rows)
    return dict(merged),by_provider


def select_pair(rows,anchor,spot,symbol):
    pairs=defaultdict(dict)
    for r in sorted(rows,key=lambda r:r.get('ticker','')):
        if not standard_contract(r) or r.get('underlying_ticker')!=symbol: continue
        strike=r['strike_price']; dte=(date.fromisoformat(r['expiration_date'])-date.fromisoformat(anchor)).days
        if not spot*.9<=strike<=spot*1.1 or not 60<=dte<=90: continue
        pairs[r['expiration_date'],strike][r['contract_type']]=r
    eligible=[]
    for (expiry,strike),pair in pairs.items():
        if set(pair)!={'call','put'}: continue
        dte=(date.fromisoformat(expiry)-date.fromisoformat(anchor)).days
        eligible.append((abs(dte-90),abs(strike/spot-1),expiry,strike,pair))
    if not eligible:return []
    p=min(eligible,key=lambda r:r[:4])[4]
    return [p['call'],p['put']]


def weekly_entries(sessions,anchor,end):
    result=[]
    for i,d in enumerate(sessions):
        if i==0 or not anchor<d<=end: continue
        if date.fromisoformat(d).isocalendar()[:2]==date.fromisoformat(sessions[i-1]).isocalendar()[:2]: continue
        if sessions[i-1]<anchor: continue
        result.append({'entry_date':d,'cutoff':sessions[i-1]})
    return result


def prepare():
    assert digest(BASE/'cohort.json')==LEDGER_HASH
    prices,_=load_stock_prices(); sessions=sorted(prices['SPY']); groups=[]
    for requested,split in ANCHORS:
        anchor=sessions[bisect_left(sessions,requested)]; end=(date.fromisoformat(anchor)+timedelta(days=55)).isoformat()
        for symbol in SYMBOLS:
            if anchor not in prices[symbol]: raise ValueError('Missing anchor price '+symbol+':'+anchor)
            row=prices[symbol][anchor]
            groups.append({'id':symbol+'_'+anchor,'ticker':symbol,'anchor':anchor,'window_end':end,'split':split,
                           'selection_close':row[2],'selection_source':row[6],
                           'decisions':weekly_entries(sessions,anchor,end)})
    config={'version':1,'symbols':SYMBOLS,'anchors':ANCHORS,'groups':groups,
            'ledger_sha256':LEDGER_HASH,'stock_input_hashes':{n:digest(BASE/n) for n in STOCK_FILES},
            'protocol_sha256':digest(Path('docs/options-activity-expanded-protocol-2026-09-14.md')),
            'collector_sha256':digest(Path(__file__)),'free_request_cap':260}
    path=ROOT/'cohort.json'
    if path.exists() and json.loads(path.read_text())!=json.loads(json.dumps(config)):
        raise ValueError('Frozen expanded configuration changed; refusing overwrite')
    if not path.exists(): save(path,config)
    print(json.dumps({'groups':len(groups),'planned_weekly_observations':sum(len(g['decisions']) for g in groups),
                      'splits':{s:sum(len(g['decisions']) for g in groups if g['split']==s) for s in ['train','validation','test']}}),flush=True)
    return config


class StudyClient(CachedClient):
    def __init__(self,key,budget):
        super().__init__(key,budget)
        self.cache=ROOT/'cache'; self.cache.mkdir(parents=True,exist_ok=True)
        self.log=(ROOT/'requests.log').open('a',encoding='utf-8',buffering=1)

    def get(self,path,params):
        with redirect_stdout(self.log): return super().get(path,params)


def collect(config,budget):
    import msvcrt
    lock=(SHARED_ROOT/'collector.lock').open('a+b'); lock.seek(0)
    if lock.read(1)==b'': lock.write(b'0');lock.flush()
    lock.seek(0)
    try:msvcrt.locking(lock.fileno(),msvcrt.LK_NBLCK,1)
    except OSError:lock.close();raise StopCollection('options_collector_already_running') from None
    client=None;done=[];status='running'
    def checkpoint():
        save(ROOT/'collection-status.json',{'status':status,'requests_this_run':client.calls if client else 0,
             'completed':done,'total_groups':len(config['groups']),'updated_at':datetime.now(timezone.utc).isoformat(),
             'cohort_sha256':digest(ROOT/'cohort.json'),'ledger_sha256':digest(BASE/'cohort.json')})
    try:
        client=StudyClient(api_key(),budget);checkpoint()
        for group in config['groups']:
            path=ROOT/'groups'/(group['id']+'.json')
            if path.exists():
                saved=json.loads(path.read_text())
                if saved['group']!=group:raise ValueError('Cached group changed')
                done.append(group['id']);checkpoint();continue
            a=date.fromisoformat(group['anchor']);spot=group['selection_close']
            params={'underlying_ticker':group['ticker'],'as_of':group['anchor'],'expired':'false',
                    'expiration_date.gte':(a+timedelta(days=60)).isoformat(),
                    'expiration_date.lte':(a+timedelta(days=90)).isoformat(),
                    'strike_price.gte':round(spot*.9,4),'strike_price.lte':round(spot*1.1,4),
                    'limit':1000,'sort':'ticker','order':'asc'}
            refs=client.all_pages('/v3/reference/options/contracts',params)
            refs=list({r['ticker']:r for r in refs if r.get('ticker')}.values())
            selected=select_pair(refs,group['anchor'],spot,group['ticker']);pair=[]
            # Persist contract selection before fetching any of its future bars.
            selection={'group':group,'reference_count':len(refs),'contracts':selected,'reference_complete':True}
            selection_path=ROOT/'selections'/(group['id']+'.json')
            if selection_path.exists() and json.loads(selection_path.read_text())!=selection:raise ValueError('Frozen contract selection changed')
            save(selection_path,selection)
            for contract in selected:
                symbol=quote(contract['ticker'],safe=':')
                endpoint=f'/v2/aggs/ticker/{symbol}/range/1/day/{(a-timedelta(days=90)).isoformat()}/{group["window_end"]}'
                bars=client.all_pages(endpoint,{'adjusted':'false','sort':'asc','limit':50000})
                pair.append({'contract':contract,'bars':bars,'fetch_complete':True})
            save(path,{'group':group,'pair':pair,'reference_complete':True,'reference_count':len(refs),
                       'status':'complete' if pair else 'no_matched_pair','collected_at':datetime.now(timezone.utc).isoformat()})
            done.append(group['id']);checkpoint()
            print(json.dumps({'completed':len(done),'of':len(config['groups']),'group':group['id'],
                              'requests':client.calls,'contracts':len(pair),'bars':sum(len(c['bars']) for c in pair)}),flush=True)
        status='complete'
    except StopCollection as exc:status=str(exc)
    except BaseException:
        status='failed';raise
    finally:
        checkpoint()
        if client:client.log.close()
        lock.seek(0);msvcrt.locking(lock.fileno(),msvcrt.LK_UNLCK,1);lock.close()
    assert digest(BASE/'cohort.json')==LEDGER_HASH
    print(json.dumps({'status':status,'completed':len(done),'requests':client.calls}),flush=True)
    if status!='complete':raise SystemExit(2)


def main():
    p=argparse.ArgumentParser();p.add_argument('--collect',action='store_true');p.add_argument('--max-requests',type=int,default=260);args=p.parse_args()
    if not 1<=args.max_requests<=260:p.error('Budget must be 1–260')
    config=prepare()
    if args.collect:collect(config,args.max_requests)


if __name__=='__main__':main()
