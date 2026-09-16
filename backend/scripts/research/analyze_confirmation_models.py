"""Offline-only confirmation research. Never imports application/database code.

Inputs: immutable export_confirmation_cohort.py output. Outputs: research JSON.
No product weights, labels, prices, or events are rewritten.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
from collections import Counter
from pathlib import Path

import numpy as np
from sklearn.ensemble import HistGradientBoostingClassifier, RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.tree import DecisionTreeClassifier, export_text

SOURCES = ['congress','insiders','signals','analysts','price_volume','fundamentals','government_contracts','options_flow','institutional_activity','macro_positioning']
DURABLE = {'analysts','fundamentals','institutional_activity','macro_positioning'}
THRESHOLDS = [.35,.40,.45,.50,.55,.60,.65,.70,.75,.80,.85,.90]


def split_for(row):
    digest = hashlib.sha256(f"confirmation-30d-research-v1|{row['security_id']}".encode()).hexdigest()
    bucket = int(digest[:8], 16) % 100
    return 'train' if bucket < 60 else 'validation' if bucket < 80 else 'test'


def features(row):
    """Only frozen opening inputs. Excludes identity, entry prices, dates, outcomes,
    lifecycle, later confirmations, and V2 regimes missing from the 30D cohort.
    """
    f = {'score': float(row['score']), 'active_sources': float(row['active_source_count'])}
    for side in ['bullish','bearish']:
        f[f'{side}_layers'] = f[f'{side}_durable_layers'] = f[f'{side}_fresh_layers'] = 0.
        f[f'{side}_evidence'] = 0.
    for key in SOURCES:
        source = row['sources'].get(key, {})
        present = bool(source.get('present'))
        direction = source.get('direction', 'neutral') if present else 'inactive'
        fresh = row.get('freshness', {}).get(key, {})
        days = fresh.get('freshness_days') if isinstance(fresh, dict) else fresh
        if days is None:
            days = source.get('freshness_days')
        f[f'{key}:present'] = float(present)
        for side in ['bullish','bearish','mixed']:
            f[f'{key}:{side}'] = float(direction == side)
        for attr in ['strength','quality','score_contribution']:
            f[f'{key}:{attr}'] = float(source.get(attr) or 0) if present else 0.
        f[f'{key}:freshness_days'] = min(float(days), 365.) if days is not None and present else float('nan')
        f[f'{key}:freshness_missing'] = float(days is None and present)
        if direction in ['bullish','bearish']:
            f[f'{direction}_layers'] += 1
            f[f'{direction}_durable_layers'] += float(key in DURABLE)
            f[f'{direction}_fresh_layers'] += float(days is not None and days <= 30)
            freshness_score = 0 if days is None else 100 if days <= 3 else 85 if days <= 7 else 65 if days <= 14 else 40 if days <= 30 else 15
            evidence = .5*f[f'{key}:strength'] + .35*f[f'{key}:quality'] + .15*freshness_score + 2*f[f'{key}:score_contribution']
            f[f'{direction}_evidence'] += evidence * (.45 if days is not None and days > 90 else 1.)
    f['evidence_edge'] = f['bullish_evidence'] - f['bearish_evidence']
    f['conflict_layers'] = min(f['bullish_layers'], f['bearish_layers'])
    return f


def correct(row, side, horizon='30'):
    o = row['outcomes'][horizon]
    raw, excess = o['raw_return'], round(o['raw_return'] - o['spy_return'], 2)
    if side == 1:
        return raw > 0 or excess > 0
    return round(-raw, 2) > 0 or round(-excess, 2) > 0


def wilson(wins, n):
    if not n:
        return [None, None]
    z = 1.959963984540054
    p = wins/n
    center = (p + z*z/(2*n))/(1+z*z/n)
    half = z*math.sqrt(p*(1-p)/n + z*z/(4*n*n))/(1+z*z/n)
    return [round(100*(center-half),2), round(100*(center+half),2)]


def metrics(rows, predictions, horizon='30'):
    chosen = [(r, int(p)) for r,p in zip(rows,predictions) if p != 0 and horizon in r['outcomes']]
    n = len(chosen)
    wins = sum(correct(r,p,horizon) for r,p in chosen)
    raw_wins = sum((r['outcomes'][horizon]['raw_return'] > 0 if p==1 else round(-r['outcomes'][horizon]['raw_return'],2)>0) for r,p in chosen)
    returns = [p*r['outcomes'][horizon]['raw_return'] for r,p in chosen]
    result = {'n':n,'wins':wins,'accuracy':round(100*wins/n,2) if n else None,
              'coverage':round(100*n/len(rows),2) if rows else None,'wilson95':wilson(wins,n),
              'raw_accuracy':round(100*raw_wins/n,2) if n else None,
              'mean_directional_return':round(float(np.mean(returns)),3) if n else None,
              'median_directional_return':round(float(np.median(returns)),3) if n else None}
    for name, side in [('bullish',1),('bearish',-1)]:
        rr = [(r,p) for r,p in chosen if p==side]
        w = sum(correct(r,p,horizon) for r,p in rr)
        result[name] = {'n':len(rr),'wins':w,'accuracy':round(100*w/len(rr),2) if rr else None, 'wilson95':wilson(w,len(rr))}
    return result


def original(rows):
    return np.array([1 if r['direction']=='bullish' else -1 for r in rows])


def models():
    for c in [.01,.1,1.]:
        yield f'logistic_C{c}', make_pipeline(SimpleImputer(strategy='median',keep_empty_features=True),StandardScaler(),LogisticRegression(C=c,max_iter=3000,random_state=90214))
    for depth in [2,3,4]:
        yield f'tree_depth{depth}', make_pipeline(SimpleImputer(strategy='median',keep_empty_features=True),DecisionTreeClassifier(max_depth=depth,min_samples_leaf=25,random_state=90214))
    for depth in [4,8]:
        yield f'forest_depth{depth}', make_pipeline(SimpleImputer(strategy='median',keep_empty_features=True),RandomForestClassifier(n_estimators=250,max_depth=depth,min_samples_leaf=15,max_features=.7,random_state=90214,n_jobs=2))
    yield 'boost_depth2', HistGradientBoostingClassifier(max_iter=100,max_depth=2,max_leaf_nodes=4,min_samples_leaf=25,l2_regularization=10,learning_rate=.05,random_state=90214)
    yield 'boost_depth3', HistGradientBoostingClassifier(max_iter=100,max_depth=3,max_leaf_nodes=8,min_samples_leaf=25,l2_regularization=10,learning_rate=.05,random_state=90214)


def source_gates():
    gates = [('all', lambda f: True)]
    for score in [50,60,65,70,75,80,85,90]:
        gates.append((f'score>={score}',lambda f,score=score: f['score']>=score))
    for count in [3,4,5]:
        gates.append((f'sources>={count}',lambda f,count=count:f['active_sources']>=count))
    for k in SOURCES:
        gates.append((f'{k} bullish',lambda f,k=k:f[f'{k}:bullish']==1))
        gates.append((f'{k} not bearish',lambda f,k=k:f[f'{k}:bearish']==0))
        gates.append((f'{k} absent',lambda f,k=k:f[f'{k}:present']==0))
    for count in [1,2,3]:
        gates.append((f'fresh bullish sources>={count}',lambda f,count=count:f['bullish_fresh_layers']>=count))
        gates.append((f'durable bullish sources>={count}',lambda f,count=count:f['bullish_durable_layers']>=count))
    gates += [
        ('no opposing sources',lambda f:f['bearish_layers']==0),
        ('positive evidence edge',lambda f:f['evidence_edge']>0),
        ('fundamentals and price bullish',lambda f:f['fundamentals:bullish']==1 and f['price_volume:bullish']==1),
        ('fresh durable bullish agreement',lambda f:f['bullish_durable_layers']>=2 and f['bullish_fresh_layers']>=2 and f['bearish_layers']==0),
    ]
    return gates


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('cohort',type=Path)
    parser.add_argument('output',type=Path)
    args=parser.parse_args()
    raw=args.cohort.read_bytes()
    all_rows=json.loads(raw)['events']
    rows=sorted([r for r in all_rows if '30' in r['outcomes']],key=lambda r:r['id'])
    ff=[features(r) for r in rows]
    names=list(ff[0])
    X=np.array([[f[k] for k in names] for f in ff])
    parts={p:np.array([i for i,r in enumerate(rows) if split_for(r)==p]) for p in ['train','validation','test']}
    bull=original(rows)==1
    y=np.array([correct(r,1) for r in rows],dtype=int)
    yraw=np.array([r['outcomes']['30']['raw_return']>0 for r in rows],dtype=int)
    report={'data_sha256':hashlib.sha256(raw).hexdigest(),'features':names,'feature_count':len(names),
            'split_policy':'SHA256(confirmation-30d-research-v1|security_id): 60% train, 20% validation, 20% locked test; same market window, not out-of-time validation',
            'horizons':{},'partitions':{},'candidates':[], 'simple_rules':[], 'components':[]}
    for h in ['7','30','90','180','365']:
        rr=[r for r in all_rows if h in r['outcomes']]
        report['horizons'][h]={'baseline':metrics(rr,original(rr),h),'entry_dates':dict(sorted(Counter(r['entry_date'] for r in rr).items()))}
    for p,ii in parts.items():
        rr=[rows[i] for i in ii]
        report['partitions'][p]={'events':len(ii),'securities':len({r['security_id'] for r in rr}),'baseline':metrics(rr,original(rr))}
    # Reconcile frozen exported labels exactly before doing any model research.
    assert all(correct(r, 1 if r['direction']=='bullish' else -1, h)==r['outcomes'][h]['correct'] for r in all_rows for h in r['outcomes'])
    assert not any(set(rows[i]['security_id'] for i in parts[a]) & set(rows[i]['security_id'] for i in parts[b]) for a,b in [('train','validation'),('train','test'),('validation','test')])
    report['temporal_audit']={
        'first_30d_result':min(r['outcomes']['30']['price_at'] for r in rows),
        'last_30d_entry':max(r['entry_date'] for r in rows),
        'eligible_prior_30d_training_events':sum(r['outcomes']['30']['price_at'][:10]<max(e['entry_date'] for e in rows) for r in rows),
        'late_snapshot_creation':sum(r['created_at'][:10]>r['entry_date'] for r in all_rows),
        'late_provenance':sum((r.get('provenance') or {}).get('late_count',0) for r in all_rows),
        'v2_regime_features_30d':sum('__v2_features' in r['sources'] for r in rows),
    }
    for side in ['bullish','bearish']:
        for k in SOURCES:
            for state in ['bullish','bearish','absent']:
                rr=[r for r,f in zip(rows,ff) if r['direction']==side and (not f[f'{k}:present'] if state=='absent' else f[f'{k}:{state}']==1)]
                report['components'].append({'side':side,'source':k,'state':state,**metrics(rr,original(rr))})
    for label,pred in [('original',original(rows)),('always_bullish',np.ones(len(rows))),('always_bearish',-np.ones(len(rows))),('keep_existing_bearish_only',np.where(bull,0,-1)),('bearish_score75plus',np.array([-1 if r['direction']=='bearish' and r['score']>=75 else 0 for r in rows]))]:
        report['candidates'].append({'name':label,'selection':'fixed diagnostic baseline','results':{p:metrics([rows[i] for i in ii],pred[ii]) for p,ii in {**parts,'all':np.arange(len(rows))}.items()}})
    train, val = parts['train'], parts['validation']
    # Fix selection using validation only, including minimum bullish coverage.
    # Test results are evaluated below only after all selected rules are fixed.
    selected=[]
    eligible=[]
    for name,gate in source_gates():
        pred=np.array([1 if gate(f) else 0 for f in ff])
        pred[~bull]=-1
        vmtr=metrics([rows[i] for i in val],pred[val])
        report['simple_rules'].append({'name':name,'validation':vmtr,'all':metrics(rows,pred)})
        if vmtr['bullish']['n']>=max(30,.2*sum(bull[val])):
            eligible.append((vmtr['wilson95'][0],name,pred))
    best=max(eligible,key=lambda a:a[0])
    selected.append(('best_simple_bullish_gate',best[1],best[2],None))
    for mode in ['gate_bullish','flip_weak_bullish','replace_directions','selective_directions']:
        candidates=[]
        tr=train[bull[train]] if mode in ['gate_bullish','flip_weak_bullish'] else train
        target=y if mode in ['gate_bullish','flip_weak_bullish'] else yraw
        for name,model in models():
            model.fit(X[tr],target[tr])
            # Strictly do not call predict on the locked test until selection is fixed.
            pv=model.predict_proba(X[val])[:,1]
            for threshold in (THRESHOLDS if mode!='replace_directions' else [.35,.40,.45,.50,.55,.60,.65]):
                if mode=='gate_bullish': vp=np.where(~bull[val],-1,np.where(pv>=threshold,1,0))
                elif mode=='flip_weak_bullish': vp=np.where(~bull[val],-1,np.where(pv>=threshold,1,-1))
                elif mode=='replace_directions': vp=np.where(pv>=threshold,1,-1)
                else:
                    if threshold < .5: continue
                    vp=np.where(pv>=threshold,1,np.where(pv<=1-threshold,-1,0))
                m=metrics([rows[i] for i in val],vp)
                minimum=max(30,.2*sum(bull[val])) if mode in ['gate_bullish','flip_weak_bullish'] else 0
                if m['bullish']['n']<minimum or m['n']<.35*len(val): continue
                candidates.append((m['wilson95'][0],name,threshold,model,m))
        top=max(candidates,key=lambda a:a[0])
        _,name,threshold,model,validation=top
        selected.append((mode,f'{name}; threshold={threshold}',(mode,model,threshold),validation))
        print(json.dumps({'selected_on_validation':mode,'model':name,'threshold':threshold,'validation':validation}),flush=True)
    # All selectors are fixed. Evaluate test once, without refitting or threshold changes.
    for family,name,recipe,validation in selected:
        if family=='best_simple_bullish_gate': pred=recipe
        else:
            mode,model,threshold=recipe
            prob=model.predict_proba(X)[:,1]
            if mode=='gate_bullish': pred=np.where(~bull,-1,np.where(prob>=threshold,1,0))
            elif mode=='flip_weak_bullish': pred=np.where(~bull,-1,np.where(prob>=threshold,1,-1))
            elif mode=='replace_directions': pred=np.where(prob>=threshold,1,-1)
            else: pred=np.where(prob>=threshold,1,np.where(prob<=1-threshold,-1,0))
        record={'name':family,'recipe':name,'selection':'validation Wilson lower bound, minimum coverage; test not used for selection','results':{p:metrics([rows[i] for i in ii],pred[ii]) for p,ii in {**parts,'all':np.arange(len(rows))}.items()}}
        record['test_7d_same_events']=metrics([rows[i] for i in parts['test']],pred[parts['test']],'7')
        record['test_predictions']=[{'id':rows[i]['id'],'prediction':int(pred[i])} for i in parts['test']]
        if family!='best_simple_bullish_gate':
            final=model.steps[-1][1] if hasattr(model,'steps') else model
            if hasattr(final,'coef_'):
                record['coefficients']=sorted(zip(names,final.coef_[0].tolist()),key=lambda a:abs(a[1]),reverse=True)[:20]
            if isinstance(final,DecisionTreeClassifier): record['tree']=export_text(final,feature_names=names)
            if hasattr(final,'feature_importances_'): record['importances']=sorted(zip(names,final.feature_importances_.tolist()),key=lambda a:a[1],reverse=True)[:20]
        report['candidates'].append(record)
    # Clearly mark these full-sample threshold scans as retrospective, never holdout estimates.
    report['bullish_score_thresholds']=[]
    for cutoff in [40,50,60,65,70,75,80,85,90]:
        rr=[r for r in rows if r['direction']=='bullish' and r['score']>=cutoff]
        report['bullish_score_thresholds'].append({'cutoff':cutoff,**metrics(rr,original(rr))})
    args.output.write_text(json.dumps(report,indent=2,allow_nan=False),encoding='utf-8')
    print('REPORT',str(args.output),flush=True)


if __name__=='__main__': main()
