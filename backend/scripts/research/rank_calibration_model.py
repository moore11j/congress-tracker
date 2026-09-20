"""Offline cross-sectional models; data preparation never uses future outcomes for ranks."""
import argparse
import hashlib
import json
import math
from bisect import bisect_left
from collections import Counter, defaultdict
from datetime import date, timedelta
from pathlib import Path

import numpy as np
from scipy.optimize import minimize
from scipy.special import expit, logit
from scipy.stats import rankdata
from sklearn.ensemble import ExtraTreesClassifier, HistGradientBoostingClassifier, HistGradientBoostingRegressor
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from threadpoolctl import threadpool_limits
from analyze_historical_panel import FMP, price_features, raw_open
from analyze_direct_returns import QUARTERS, TARGETS, target_return


def half_year_before(value):
    d = date.fromisoformat(value)
    return date(d.year-(d.month<=6), d.month+6 if d.month<=6 else d.month-6, 1).isoformat()


def partition(rows, start):
    boundary = half_year_before(start)
    fit = [r for r in rows if r['outcomes']['30']['target_date'] < boundary]
    calibration = [r for r in rows if r['entry_date'] >= boundary and r['outcomes']['30']['target_date'] < start]
    return fit, calibration


def date_weights(rows):
    counts = Counter(r['entry_date'] for r in rows)
    w = np.array([1/counts[r['entry_date']] for r in rows])
    return w / np.mean(w)


def fit_calibrator(probability, actual, weights):
    """Held-out date-balanced Platt fit, no rank reversal or test inputs."""
    x = logit(np.clip(probability, 1e-5, 1-1e-5)); y = np.asarray(actual, dtype=float)
    w = weights / np.sum(weights)
    def loss(coef):
        z = coef[0]*x+coef[1]
        return np.sum(w*(np.logaddexp(0,z)-y*z)) + .01*(coef[0]-1)**2 + .001*coef[1]**2
    result = minimize(loss, [1.,0.], method='L-BFGS-B', bounds=[(0.,2.),(-5.,5.)])
    if not result.success: raise RuntimeError(f'Calibration failed: {result.message}')
    return result.x


def calibrate(probability, coef):
    return expit(coef[0]*logit(np.clip(probability,1e-5,1-1e-5))+coef[1])


def add_cross_sectional_ranks(rows):
    """Only prior-close feature dictionaries are consulted, even for missing-label rows."""
    names = sorted(rows[0]['features'])
    for name in names:
        values = np.array([r['features'][name] for r in rows], dtype=float)
        finite = np.isfinite(values)
        ranked = np.full(len(rows), .5)
        if finite.any(): ranked[finite] = (rankdata(values[finite], method='average')-.5)/finite.sum()
        for r,v in zip(rows,ranked): r.setdefault('ranks',{})[name] = float(v)


def load_prices(paths):
    prices = defaultdict(dict)
    for path in paths:
        payload = json.loads(path.read_bytes())
        for row in payload['rows']:
            if row[6] == FMP and date.fromisoformat(row[1]).weekday() < 5:
                prices[row[0]][row[1]] = row
    return prices


def prepare(paths, group):
    prices = load_prices(paths)
    days = sorted(prices['SPY']); dd = [date.fromisoformat(d) for d in days]
    rows = []; audit = Counter(); stocks = sorted(set(prices)-{'SPY','QQQ'})
    for i in range(253,len(days)-1):
        entry = days[i+1]
        if entry < '2023-01-01' or entry >= '2026-07-01': continue
        if dd[i].isocalendar()[:2] == dd[i+1].isocalendar()[:2]: continue
        past = days[i-252:i+1]
        daily = []
        for ticker in stocks:
            h = prices[ticker]
            if any(d not in h for d in past): audit['missing_prior_history']+=1; continue
            close = np.array([h[d][2] for d in past],dtype=float)
            volume = [h[d][5] for d in past]
            f = price_features(close,volume)
            if f is None: audit['invalid_prior_inputs']+=1; continue
            f['momentum_12_minus_1'] = (1+f['momentum_252'])/(1+f['momentum_20'])-1
            dollars = close[-60:]*np.array(volume[-60:],dtype=float)
            f['dollar_volume60'] = float(np.nanmean(dollars)) if np.isfinite(dollars).any() else float('nan')
            daily.append({'ticker':ticker,'group':group,'entry_date':entry,'decision_date':days[i],'features':f})
        if len(daily)<50: audit['dates_below_50_prior_eligible']+=1; continue
        # This must happen before checking entry/exit prices or outcome availability.
        add_cross_sectional_ranks(daily)
        target_ix = bisect_left(dd,date.fromisoformat(entry)+timedelta(days=30))
        for r in daily:
            r['rank_universe_count'] = len(daily)
            h = prices[r['ticker']]
            entry_row = h.get(entry); benchmark_entry = prices['SPY'].get(entry)
            ep = raw_open(entry_row) if entry_row else None
            bp = raw_open(benchmark_entry) if benchmark_entry else None
            reason = None
            if ep is None or bp is None: reason='missing_entry'
            elif abs(np.log(ep/h[days[i]][2]))>np.log(3): reason='entry_basis_discontinuity'
            elif target_ix>=len(days) or days[target_ix] not in h: reason='missing_target'
            if reason:
                r['outcomes']={}; r['unmeasured_reason']=reason; audit[reason]+=1
            else:
                target=days[target_ix]
                r['outcomes']={'30':{'raw_return':(h[target][2]/ep-1)*100,
                                     'spy_return':(prices['SPY'][target][2]/bp-1)*100,'target_date':target}}
            rows.append(r)
    return rows,dict(audit)


def factories():
    params=dict(max_leaf_nodes=4,max_iter=100,learning_rate=.05,min_samples_leaf=100,
                l2_regularization=10,early_stopping=False,random_state=91626)
    return {
        'rank_logistic':lambda:make_pipeline(StandardScaler(),LogisticRegression(C=.1,max_iter=2000)),
        'rank_hist':lambda:HistGradientBoostingClassifier(**params),
        'rank_extra_trees':lambda:ExtraTreesClassifier(n_estimators=200,max_depth=5,min_samples_leaf=100,max_features=.7,random_state=91626,n_jobs=4),
        'rank_return_hist':lambda:HistGradientBoostingRegressor(**params),
    }


def forward_ranks(rows):
    groups=defaultdict(list)
    for i,r in enumerate(rows): groups[r['entry_date']].append(i)
    result=np.empty(len(rows))
    for ix in groups.values():
        result[ix]=(rankdata([target_return(rows[i],'raw') for i in ix],method='average')-.5)/len(ix)
    return result


def main():
    ap=argparse.ArgumentParser(); ap.add_argument('output_dir',type=Path); ap.add_argument('--prepare-only',action='store_true'); args=ap.parse_args()
    base=Path('frontend/test-results/confirmation-research'); out=args.output_dir; out.mkdir(parents=True,exist_ok=True)
    paths={'development':[base/'historical-panel.json',base/'direct-return-2026-09-15/extension.json'],
           'validation':[out/'validation-prices.json']}
    cache=out/'prepared.json'
    if cache.exists():
        stored=json.loads(cache.read_bytes()); groups=stored['groups']; audits=stored['audits']
        for p in sum(paths.values(),[]):
            if stored['inputs'][str(p)]!=hashlib.sha256(p.read_bytes()).hexdigest(): raise ValueError('Prepared input hash changed')
    else:
        groups={}; audits={}
        for group,pp in paths.items():
            groups[group],audits[group]=prepare(pp,group)
            print('Prepared',group,len(groups[group]),audits[group],flush=True)
        if set(r['ticker'] for r in groups['development']) & set(r['ticker'] for r in groups['validation']): raise ValueError('Symbol overlap')
        stored={'inputs':{str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in sum(paths.values(),[])},'groups':groups,'audits':audits}
        cache.write_text(json.dumps(stored,allow_nan=True))
    if args.prepare_only: return
    names=sorted(groups['development'][0]['ranks']); records=[]; folds=[]
    measured={g:[r for r in rr if '30' in r['outcomes']] for g,rr in groups.items()}
    with threadpool_limits(limits=4):
        for start,end in QUARTERS:
            fit,cal=partition(measured['development'],start)
            fold={'start':start,'end':end,'fit_rows':len(fit),'calibration_rows':len(cal),
                  'fit_dates':len(set(r['entry_date'] for r in fit)),'calibration_dates':len(set(r['entry_date'] for r in cal)), 'calibrators':{}}
            if fold['fit_dates']<12 or fold['calibration_dates']<8:
                fold['status']='insufficient_prior_dates'; folds.append(fold); continue
            fold['last_fit_target']=max(r['outcomes']['30']['target_date'] for r in fit)
            fold['first_calibration_entry']=min(r['entry_date'] for r in cal)
            fold['last_calibration_target']=max(r['outcomes']['30']['target_date'] for r in cal)
            samples=[r for rr in measured.values() for r in rr if start<=r['entry_date']<end]
            x=np.array([[r['ranks'][n] for n in names] for r in fit]); xc=np.array([[r['ranks'][n] for n in names] for r in cal]); xt=np.array([[r['ranks'][n] for n in names] for r in samples])
            wf=date_weights(fit); wc=date_weights(cal)
            batch=[]
            for r in samples:
                ranks=r['ranks']
                factors={'momentum':ranks['momentum_12_minus_1'],'reversal':1-ranks['momentum_5'],'low_volatility':1-ranks['volatility_60']}
                factors['composite']=sum(factors.values())/3
                batch.append({'ticker':r['ticker'],'group':r['group'],'entry_date':r['entry_date'],
                              'target_date':r['outcomes']['30']['target_date'],'rank_universe_count':r['rank_universe_count'],
                              'stock_trend':bool(r['features']['above_sma_200']>0),'factors':factors})
            rank_y=forward_ranks(fit)
            for target in TARGETS:
                y=np.array([target_return(r,target)>0 for r in fit]); yc=np.array([target_return(r,target)>0 for r in cal]); prevalence=float(np.average(yc,weights=wc))
                fold['calibrators'][target]={}
                for item,r in zip(batch,samples): item[target]={'actual':target_return(r,target),'prevalence':prevalence,'predictions':{}}
                for name,factory in factories().items():
                    model=factory(); rank_model=name=='rank_return_hist'
                    if name=='rank_logistic': model.fit(x,y,logisticregression__sample_weight=wf)
                    else: model.fit(x,rank_y if rank_model else y,sample_weight=wf)
                    pc=np.clip(model.predict(xc),1e-5,1-1e-5) if rank_model else model.predict_proba(xc)[:,1]
                    pt=np.clip(model.predict(xt),1e-5,1-1e-5) if rank_model else model.predict_proba(xt)[:,1]
                    coef=fit_calibrator(pc,yc,wc); calibrated=calibrate(pt,coef)
                    fold['calibrators'][target][name]={'slope':float(coef[0]),'intercept':float(coef[1]),'prevalence':prevalence}
                    for item,p,cp in zip(batch,pt,calibrated):
                        item[target]['predictions'][name+'_uncalibrated']=float(p)
                        item[target]['predictions'][name+'_calibrated']=float(cp)
            records.extend(batch); folds.append(fold)
            print('Completed',start,'fit',len(fit),'calibration',len(cal),'predictions',len(batch),flush=True)
    (out/'predictions.json').write_text(json.dumps(records,allow_nan=False))
    (out/'fit-metadata.json').write_text(json.dumps({'features':names,'folds':folds,'audits':audits,'inputs':stored['inputs'],
        'prepared':{g:{'all':len(rr),'measured':len(measured[g]),'unmeasured':len(rr)-len(measured[g])} for g,rr in groups.items()}},indent=2))
    print('Frozen predictions saved:',len(records),flush=True)


if __name__=='__main__': main()
