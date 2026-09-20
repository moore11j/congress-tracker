"""Bounded SEC public-data acquisition, local immutable cache, no application writes."""
import argparse
import hashlib
import json
import time
import urllib.error
import urllib.request
from pathlib import Path

HEADERS={'User-Agent':'Walnut Markets research generator contact@walnutmarkets.com','Accept':'application/json'}


def fetch(url,path):
    if path.exists(): return json.loads(path.read_bytes())
    time.sleep(.55)
    request=urllib.request.Request(url,headers=HEADERS)
    with urllib.request.urlopen(request,timeout=30) as response: raw=response.read()
    payload=json.loads(raw)
    path.write_bytes(raw)
    return payload


def main():
    ap=argparse.ArgumentParser();ap.add_argument('directory',type=Path);args=ap.parse_args()
    out=args.directory;out.mkdir(parents=True,exist_ok=True);(out/'facts').mkdir(exist_ok=True)
    mapping=fetch('https://www.sec.gov/files/company_tickers.json',out/'company-tickers.json')
    by_symbol={r['ticker'].upper():r for r in mapping.values()}
    prior=Path('frontend/test-results/confirmation-research/rank-calibration-2026-09-15/prepared.json')
    groups=json.loads(prior.read_bytes())['groups']; selection={}
    for group,rows in groups.items():
        symbols=sorted(set(r['ticker'] for r in rows),key=lambda s:hashlib.sha256(('sec-filing-v1|'+s).encode()).hexdigest())
        mapped=[s for s in symbols if s in by_symbol][:96]
        selection[group]=[{'ticker':s,'cik':str(by_symbol[s]['cik_str']).zfill(10)} for s in mapped]
    plan=out/'selection.json'
    if plan.exists() and json.loads(plan.read_bytes())!=selection: raise ValueError('Frozen selection changed')
    if not plan.exists(): plan.write_text(json.dumps(selection,indent=2))
    print('Frozen selection', {g:len(v) for g,v in selection.items()},flush=True)
    audit=[]
    for group,items in selection.items():
        for index,item in enumerate(items):
            path=out/'facts'/(item['cik']+'.json')
            try:
                payload=fetch('https://data.sec.gov/api/xbrl/companyfacts/CIK'+item['cik']+'.json',path)
                status='ok' if payload.get('facts',{}).get('us-gaap') else 'no_us_gaap'
            except urllib.error.HTTPError as exc:
                if exc.code in [403,429]: raise SystemExit(f'SEC access/rate limit {exc.code}; stopping without bypass')
                status=f'http_{exc.code}'
            except (urllib.error.URLError,TimeoutError) as exc:
                status=type(exc).__name__
            audit.append({**item,'group':group,'status':status})
            (out/'fetch-audit.json').write_text(json.dumps(audit,indent=2))
            if (index+1)%8==0: print('Downloaded',group,index+1,'of',len(items),flush=True)
    print('Completed SEC acquisition',len(audit),flush=True)


if __name__=='__main__':main()
