"""Apply frozen historical candidates to immutable actual ledger outcomes."""
import hashlib
import json
from bisect import bisect_right
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
import numpy as np
from analyze_historical_panel import FMP, price_features, model_configs, split_for, predictions
from analyze_confirmation_models import correct, metrics, original

BASE=Path('frontend/test-results/confirmation-research')


def frozen_features(row,prices):
    cutoff=datetime.fromisoformat(row['calculated_at'].replace('Z','+00:00'))
    if cutoff.tzinfo is None:cutoff=cutoff.replace(tzinfo=timezone.utc)
    # Actual cohort is Aug/Sep: regular-session close is 20:00 UTC.
    days=sorted(d for d in prices['SPY'] if datetime.fromisoformat(d+'T20:00:00+00:00')<=cutoff and datetime.fromisoformat(d).weekday()<5)[-253:]
    if len(days)!=253 or (cutoff.date()-datetime.fromisoformat(days[-1]).date()).days>4:return None
    history=prices.get(row['ticker'],{})
    if any(d not in history for d in days):return None
    c=np.array([history[d][2] for d in days]);spy=np.array([prices['SPY'][d][2] for d in days])
    f=price_features(c,[history[d][5] for d in days]);mf=price_features(spy)
    if f is None or mf is None:return None
    qf=price_features(np.array([prices.get('QQQ',{}).get(d,[None,None,np.nan])[2] for d in days]))
    for n in [5,20,60,126,252]:f[f'relative_strength_{n}']=f[f'momentum_{n}']-mf[f'momentum_{n}']
    lr=np.diff(np.log(c))[-60:];mr=np.diff(np.log(spy))[-60:]
    f['beta60']=float(np.cov(lr,mr)[0,1]/np.var(mr)) if np.var(mr)>0 else 0.
    f['residual_momentum20']=f['momentum_20']-f['beta60']*mf['momentum_20']
    f['market_x_relative20']=mf['momentum_20']*f['relative_strength_20']
    f['market_x_stock60']=mf['momentum_60']*f['momentum_60']
    f.update({'market:'+k:v for k,v in mf.items()})
    f.update({'qqq:'+k:(qf[k] if qf else np.nan) for k in mf})
    return f


def main():
    cohort_raw=(BASE/'cohort.json').read_bytes();cohort=json.loads(cohort_raw)['events']
    payload=json.loads((BASE/'ledger-trailing-prices.json').read_text());prices={s:{} for s in payload['symbols']}
    for r in payload['rows']:
        if r[6]==FMP:prices[r[0]][r[1]]=r
    # Both sources identify these rows as split-adjusted price-return bars.
    # Raw close and inverse-adjusted open stay on their own row's common basis.
    for r in json.loads((BASE/'recent-provider-prices.json').read_text())['rows']:
        if r[7]=='split_adjusted_price_return' and r[2]==r[3]:
            prices.setdefault(r[0],{})[r[1]]=r
    candidates=json.loads((BASE/'historical-results.selection.json').read_text())
    historic=json.loads((BASE/'historical-results.panel.json').read_text());train=[r for r in historic if split_for(r)=='train']
    names=sorted(train[0]['features']);xtrain=np.array([[r['features'][n] for n in names] for r in train])
    models={};factories=dict(model_configs())
    for name in set(c['name'] for c in candidates.values()):
        models[name]=[factories[name](),factories[name]()]
        for side,model in zip([1,-1],models[name]):model.fit(xtrain,[correct(r,side) for r in train])
    rr=[];ff=[]
    for r in cohort:
        f=frozen_features(r,prices)
        if f is not None:rr.append(r);ff.append([f[n] for n in names])
    x=np.array(ff);preds={mode:np.column_stack([m.predict_proba(x)[:,1] for m in models[c['name']]]) for mode,c in candidates.items()}
    result={'cohort_sha256':hashlib.sha256(cohort_raw).hexdigest(),'eligible_events':len(rr),'total_events':len(cohort),'selection':candidates,'horizons':{}}
    for h in ['30','7']:
        ix=[i for i,r in enumerate(rr) if h in r['outcomes']];rows=[rr[i] for i in ix]
        out={'full_original':metrics([r for r in cohort if h in r['outcomes']],original([r for r in cohort if h in r['outcomes']]),h),'covered_original':metrics(rows,original(rows),h),'covered_always_bear':metrics(rows,-np.ones(len(rows)),h)}
        for mode,c in candidates.items():
            pred=predictions(preds[mode][ix],mode,c['threshold'])
            if mode=='bull_gate':pred=np.where(original(rows)==-1,-1,pred)
            out[mode]=metrics(rows,pred,h)
            # Safe missing-feature fallback: retain original classifications.
            by_id={r['id']:int(p) for r,p in zip(rows,pred)}
            full=[r for r in cohort if h in r['outcomes']]
            fullpred=np.array([by_id.get(r['id'],1 if r['direction']=='bullish' else -1) for r in full])
            out[mode+'_full_with_original_fallback']=metrics(full,fullpred,h)
        result['horizons'][h]=out
    (BASE/'historical-transfer.json').write_text(json.dumps(result,indent=2))
    print(json.dumps(result,indent=2))


if __name__=='__main__':main()
