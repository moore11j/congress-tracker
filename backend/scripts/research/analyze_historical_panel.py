"""Offline, chronological price-feature proxy study; never writes application data."""
import argparse
import hashlib
import json
from bisect import bisect_left
from collections import Counter
from datetime import date, timedelta
from pathlib import Path

import numpy as np
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from analyze_confirmation_models import correct, metrics

FMP = 'fmp:historical-price-eod/full+corporate_actions'


def raw_open(row):
    # The cache's open is on the adjusted_close basis. Undo that row's factor
    # before comparing to raw_close; never compare adjusted open to raw close.
    if not row[3] or not row[4] or row[3] <= 0 or row[4] <= 0:
        return None
    return row[4]*row[2]/row[3]


def price_features(close, volumes=None):
    if len(close)<253 or not np.all(np.isfinite(close)) or np.min(close)<=0:
        return None
    log_returns=np.diff(np.log(close))
    # Input-only integrity screen. Extreme FUTURE returns are not removed.
    if np.max(np.abs(log_returns)) > np.log(3):
        return None
    f={f'momentum_{n}':close[-1]/close[-1-n]-1 for n in [1,5,10,20,60,126,252]}
    for n in [20,60,200]:
        f[f'above_sma_{n}']=close[-1]/np.mean(close[-n:])-1
    for n in [20,60]:
        f[f'volatility_{n}']=float(np.std(log_returns[-n:])*np.sqrt(252))
        f[f'drawdown_{n}']=close[-1]/np.max(close[-n:])-1
    f['acceleration']=f['momentum_20']-f['momentum_60']/3
    gains=np.maximum(np.diff(close[-15:]),0).sum()
    losses=-np.minimum(np.diff(close[-15:]),0).sum()
    f['rsi14']=gains/(gains+losses) if gains+losses else .5
    f['range_position_252']=(close[-1]-np.min(close))/(np.max(close)-np.min(close)) if np.max(close)>np.min(close) else .5
    if volumes is not None:
        v=np.array(volumes[-60:],dtype=float)
        f['volume_ratio_5_60']=float(np.nanmean(v[-5:])/np.nanmean(v)) if np.any(np.isfinite(v)) and np.nanmean(v)>0 else np.nan
    return f


def build_panel(payload):
    prices={s:{} for s in payload['symbols']}
    for row in payload['rows']:
        if row[6]==FMP and date.fromisoformat(row[1]).weekday()<5:
            prices[row[0]][row[1]]=row
    days=sorted(prices['SPY']); day_dates=[date.fromisoformat(d) for d in days]
    rows=[]; excluded=Counter()
    for i in range(253,len(days)-1):
        entry=days[i+1]
        if entry<'2014-01-01' or entry>'2026-07-31': continue
        # One opportunity at the first benchmark session of each ISO week.
        if day_dates[i].isocalendar()[:2]==day_dates[i+1].isocalendar()[:2]:continue
        past=days[i-252:i+1]
        spy=np.array([prices['SPY'][d][2] for d in past])
        mf=price_features(spy)
        if mf is None:continue
        base={'market:'+k:v for k,v in mf.items()}
        q=[prices.get('QQQ',{}).get(d,[None,None,np.nan])[2] for d in past]
        qf=price_features(np.array(q))
        base.update({'qqq:'+k:(qf[k] if qf else np.nan) for k in mf})
        targets={str(h):bisect_left(day_dates,date.fromisoformat(entry)+timedelta(days=h)) for h in [7,30]}
        if max(targets.values())>=len(days):continue
        for symbol,history in prices.items():
            if symbol in ['SPY','QQQ']:continue
            if any(d not in history for d in past) or entry not in history:
                excluded['incomplete_trailing_or_entry']+=1;continue
            c=np.array([history[d][2] for d in past]);vol=[history[d][5] for d in past]
            f=price_features(c,vol)
            if f is None:excluded['invalid_or_extreme_trailing_input']+=1;continue
            ep=raw_open(history[entry]); bp=raw_open(prices['SPY'][entry])
            if ep is None or bp is None:excluded['missing_entry_open']+=1;continue
            if abs(np.log(ep/c[-1]))>np.log(3):excluded['entry_basis_discontinuity']+=1;continue
            outcomes={}
            for h,ix in targets.items():
                target=days[ix]
                if target not in history:continue
                rr=(history[target][2]/ep-1)*100;br=(prices['SPY'][target][2]/bp-1)*100
                outcomes[h]={'raw_return':rr,'spy_return':br,'target_date':target}
            if '30' not in outcomes or '7' not in outcomes:excluded['missing_target_price']+=1;continue
            for n in [5,20,60,126,252]:f[f'relative_strength_{n}']=f[f'momentum_{n}']-mf[f'momentum_{n}']
            lr=np.diff(np.log(c))[-60:];mr=np.diff(np.log(spy))[-60:]
            f['beta60']=float(np.cov(lr,mr)[0,1]/np.var(mr)) if np.var(mr)>0 else 0.
            f['residual_momentum20']=f['momentum_20']-f['beta60']*mf['momentum_20']
            f['market_x_relative20']=mf['momentum_20']*f['relative_strength_20']
            f['market_x_stock60']=mf['momentum_60']*f['momentum_60']
            f.update(base)
            rows.append({'ticker':symbol,'entry_date':entry,'decision_date':days[i],'features':f,'outcomes':outcomes})
    return rows,dict(excluded)


def split_for(row):
    entry=row['entry_date']; matured=row['outcomes']['30']['target_date']
    if entry<'2024-12-01' and matured<'2025-01-01':return 'train'
    if '2025-01-01'<=entry<'2025-07-01' and matured<'2025-07-01':return 'validation'
    if '2025-07-01'<=entry<='2026-07-31':return 'test'
    return 'purged'


def model_configs():
    for c in [.01,.1,1.]:yield f'logistic_{c}',lambda c=c:make_pipeline(SimpleImputer(),StandardScaler(),LogisticRegression(C=c,max_iter=1500))
    for leaves in [4,8,15]:yield f'hist_{leaves}',lambda leaves=leaves:HistGradientBoostingClassifier(max_leaf_nodes=leaves,max_iter=150,min_samples_leaf=100,l2_regularization=10,learning_rate=.05,early_stopping=False,random_state=90214)


def predictions(prob,mode,threshold):
    if mode=='bull_gate':return np.where(prob[:,0]>=threshold,1,0)
    side=np.where(prob[:,0]>=prob[:,1],1,-1)
    return side if mode=='directional' else np.where(np.max(prob,axis=1)>=threshold,side,0)


def evaluate(rows,pred):
    result={'30':metrics(rows,pred),'7':metrics(rows,pred,'7')}
    for period in sorted(set(r['entry_date'][:4]+'H'+('1' if r['entry_date'][5:7]<='06' else '2') for r in rows)):
        ix=[i for i,r in enumerate(rows) if r['entry_date'][:4]+'H'+('1' if r['entry_date'][5:7]<='06' else '2')==period]
        result[period]=metrics([rows[i] for i in ix],pred[ix])
    # Date-cluster bootstrap captures shared market-day exposure.
    days=sorted(set(r['entry_date'] for r in rows));clusters=[]
    for day in days:
        pairs=[(r,int(p)) for r,p in zip(rows,pred) if r['entry_date']==day and p]
        clusters.append((sum(correct(r,p) for r,p in pairs),len(pairs)))
    a=np.array(clusters);rng=np.random.default_rng(90214);s=a[rng.integers(0,len(a),(2000,len(a)))].sum(axis=1)
    vals=100*s[:,0]/np.maximum(s[:,1],1)
    result['date_cluster_bootstrap95']=np.percentile(vals,[2.5,97.5]).tolist()
    return result


def main():
    ap=argparse.ArgumentParser();ap.add_argument('input',type=Path);ap.add_argument('output',type=Path);args=ap.parse_args()
    raw=args.input.read_bytes();payload=json.loads(raw);rows,excluded=build_panel(payload)
    names=sorted(rows[0]['features']);groups={s:[r for r in rows if split_for(r)==s] for s in ['train','validation','test']}
    x={s:np.array([[r['features'][n] for n in names] for r in rr]) for s,rr in groups.items()}
    print('Panel:',{s:len(v) for s,v in groups.items()},'features',len(names),flush=True)
    trained={};candidates=[]
    for name,factory in model_configs():
        models=[factory(),factory()]
        for side,model in zip([1,-1],models):model.fit(x['train'],[correct(r,side) for r in groups['train']])
        trained[name]=models;prob=np.column_stack([m.predict_proba(x['validation'])[:,1] for m in models])
        for mode in ['bull_gate','directional','selective']:
            for threshold in ([0.] if mode=='directional' else [.45,.50,.55,.60,.65,.70,.75,.80]):
                p=predictions(prob,mode,threshold);m=metrics(groups['validation'],p)
                if m['coverage']<50:continue
                candidates.append({'name':name,'mode':mode,'threshold':threshold,'validation':m})
        print('Finished validation',name,flush=True)
    selected={mode:max([c for c in candidates if c['mode']==mode],key=lambda c:(c['validation']['wilson95'][0],c['validation']['coverage'])) for mode in ['bull_gate','directional','selective']}
    # Persist selection before any test predictions/evaluation.
    args.output.with_suffix('.selection.json').write_text(json.dumps(selected,indent=2))
    result={'input_sha256':hashlib.sha256(raw).hexdigest(),'n_rows':len(rows),'excluded':excluded,'features':names,'splits':{s:{'n':len(rr),'symbols':len(set(r['ticker'] for r in rr)),'dates':len(set(r['entry_date'] for r in rr)),'first':min(r['entry_date'] for r in rr),'last':max(r['entry_date'] for r in rr)} for s,rr in groups.items()},'validation_candidates':candidates,'selected':selected,'test':{}}
    test=groups['test']
    for name,p in [('always_bull',np.ones(len(test))),('always_bear',-np.ones(len(test))),('stock_trend',np.array([1 if r['features']['above_sma_200']>0 else -1 for r in test])),('market_trend',np.array([1 if r['features']['market:above_sma_200']>0 else -1 for r in test]))]:result['test'][name]=evaluate(test,p)
    for mode,c in selected.items():
        prob=np.column_stack([m.predict_proba(x['test'])[:,1] for m in trained[c['name']]])
        result['test'][mode]=evaluate(test,predictions(prob,mode,c['threshold']))
    result['price_audit']={'future_abs_return_gt100':sum(abs(r['outcomes']['30']['raw_return'])>100 for r in rows),'test_extremes':[(r['ticker'],r['entry_date'],r['outcomes']['30']['raw_return']) for r in test if abs(r['outcomes']['30']['raw_return'])>100]}
    args.output.write_text(json.dumps(result,indent=2,allow_nan=False))
    args.output.with_suffix('.panel.json').write_text(json.dumps(rows,allow_nan=True))
    print(json.dumps({k:{'30':v['30'],'7':v['7'],'date_cluster_bootstrap95':v['date_cluster_bootstrap95']} for k,v in result['test'].items()},indent=2))


if __name__=='__main__':main()
