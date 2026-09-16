"""Exploratory stock-disclosure features with explicit availability dates."""
import json
from bisect import bisect_left
from collections import Counter,defaultdict
from datetime import date,timedelta
from functools import lru_cache
import numpy as np
from analyze_cot_models import BASE,augment
from analyze_historical_panel import model_configs,split_for,predictions
from analyze_confirmation_models import correct,metrics,original
from analyze_ranked_bullish_gate import ranked_gate
from evaluate_historical_transfer import frozen_features


def disclosure_key(r):
    side=str(r['side'] or '').lower()
    side='buy' if side in ['purchase','p-purchase','buy','p'] else 'sell' if side=='s' or 'sale' in side or side=='sell' else None
    dates=r['dates'];day=next((str(dates[k])[:10] for k in ['filing_date','filingDate','report_date','reportDate','disclosure_date','disclosureDate','date_filed'] if dates.get(k)),None)
    if not side or not day:return None
    try:date.fromisoformat(day)
    except ValueError:return None
    transaction=dates.get('transaction_date') or dates.get('trade_date')
    if transaction and str(transaction)[:10]>day:return None
    return (r['ticker'].upper(),r['type'],side),day


DISCLOSURES=defaultdict(set);AUDIT=Counter()
for event in json.loads((BASE/'disclosure-input.json').read_text())['events']:
    parsed=disclosure_key(event)
    if parsed is None:AUDIT['excluded_nonmarket_or_invalid_date']+=1;continue
    key,day=parsed;AUDIT['eligible_rows']+=1
    DISCLOSURES[key].add(day)
DISCLOSURES={k:sorted(v) for k,v in DISCLOSURES.items()}
AUDIT['distinct_symbol_source_side_days']=sum(len(v) for v in DISCLOSURES.values())


@lru_cache(maxsize=100000)
def disclosure_features(ticker,asof):
    f={};target=date.fromisoformat(asof)
    for source in ['congress_trade','insider_trade']:
        for side in ['buy','sell']:
            days=DISCLOSURES.get((ticker,source,side),[]);end=bisect_left(days,asof)
            key=f'filings:{source}:{side}'
            for n in [30,90,365]:
                start=bisect_left(days,(target-timedelta(days=n)).isoformat())
                f[key+f':days_{n}']=float(np.log1p(end-start))
            f[key+':age']=min((target-date.fromisoformat(days[end-1])).days,366) if end else 366.
        for n in [30,90]:f[f'filings:{source}:net_days_{n}']=f[f'filings:{source}:buy:days_{n}']-f[f'filings:{source}:sell:days_{n}']
    return f


def features(row):
    asof=row.get('decision_date') or row['calculated_at'][:10]
    f=augment(row,True);f.update(disclosure_features(row['ticker'],asof));return f


def matrix(rows,names):return np.array([[f[n] for n in names] for r in rows for f in [features(r)]])


def main():
    panel=json.loads((BASE/'historical-results.panel.json').read_text());groups={s:[r for r in panel if split_for(r)==s] for s in ['train','validation','test']}
    names=sorted(features(groups['train'][0]));x={s:matrix(groups[s],names) for s in ['train','validation']};trained={};candidates=[]
    for name,factory in model_configs():
        models=[factory(),factory()]
        for side,m in zip([1,-1],models):m.fit(x['train'],[correct(r,side) for r in groups['train']])
        trained[name]=models;prob=np.column_stack([m.predict_proba(x['validation'])[:,1] for m in models])
        for mode in ['bull_gate','directional','ranked_bull']:
            for t in ([0.] if mode!='bull_gate' else [.45,.50,.55,.60,.65,.70,.75,.80]):
                pred=ranked_gate(groups['validation'],prob[:,0]) if mode=='ranked_bull' else predictions(prob,mode,t)
                m=metrics(groups['validation'],pred)
                if m['coverage']>=50:candidates.append({'name':name,'mode':mode,'threshold':t,'validation':m})
        print('Validation',name,flush=True)
    selected={mode:max([c for c in candidates if c['mode']==mode],key=lambda c:(c['validation']['wilson95'][0],c['validation']['coverage'])) for mode in ['bull_gate','directional','ranked_bull']}
    (BASE/'disclosure-selection.json').write_text(json.dumps(selected,indent=2))
    payload=json.loads((BASE/'ledger-trailing-prices.json').read_text());prices={s:{} for s in payload['symbols']}
    for r in payload['rows']:prices[r[0]][r[1]]=r
    for r in json.loads((BASE/'recent-provider-prices.json').read_text())['rows']:
        if r[7]=='split_adjusted_price_return' and r[2]==r[3]:prices.setdefault(r[0],{})[r[1]]=r
    cohort=json.loads((BASE/'cohort.json').read_text())['events'];covered=[]
    for r in cohort:
        f=frozen_features(r,prices)
        if f is not None:covered.append({**r,'features':f})
    xtest=matrix(groups['test'],names);xledger=matrix(covered,names)
    result={'data_audit':dict(AUDIT),'features':names,'selected':selected,'validation_candidates':candidates,'historical_test':{},'ledger':{}}
    for mode,c in selected.items():
        models=trained[c['name']];prob=np.column_stack([m.predict_proba(xtest)[:,1] for m in models]);pred=ranked_gate(groups['test'],prob[:,0]) if mode=='ranked_bull' else predictions(prob,mode,c['threshold'])
        result['historical_test'][mode]={h:metrics(groups['test'],pred,h) for h in ['30','7']}
        prob=np.column_stack([m.predict_proba(xledger)[:,1] for m in models]);pred=ranked_gate(covered,prob[:,0],True) if mode=='ranked_bull' else predictions(prob,mode,c['threshold'])
        if mode=='bull_gate':pred=np.where(original(covered)==-1,-1,pred)
        by_id={r['id']:int(p) for r,p in zip(covered,pred)};result['ledger'][mode]={}
        for h in ['30','7']:
            full=[r for r in cohort if h in r['outcomes']];pp=np.array([by_id.get(r['id'],1 if r['direction']=='bullish' else -1) for r in full])
            result['ledger'][mode][h]=metrics(full,pp,h)
    (BASE/'disclosure-results.json').write_text(json.dumps(result,indent=2,allow_nan=False));print(json.dumps({k:result[k] for k in ['data_audit','selected','historical_test','ledger']},indent=2))


if __name__=='__main__':main()
