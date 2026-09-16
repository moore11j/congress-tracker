"""Chronological paired model comparison. Research only; no application writes."""
from collections import Counter,defaultdict
import json
import math
from pathlib import Path
from statistics import mean
import random

from build_options_activity_panel import model_split
from options_activity_expanded import ROOT,BASE,LEDGER_HASH
from options_activity_pilot import digest,save


def probability_metrics(rows,predictions,horizon='30'):
    rr=[r for r in rows if r['outcomes'].get(horizon,{}).get('status')=='measured']
    if not rr:return {'n':0,'log_loss':None,'brier':None}
    pp=[max(1e-9,min(1-1e-9,predictions[r['id']])) for r in rr]
    y=[float(r['outcomes'][horizon]['correct']) for r in rr]
    return {'n':len(rr),'log_loss':mean(-t*math.log(p)-(1-t)*math.log(1-p) for p,t in zip(pp,y)),
            'brier':mean((p-t)**2 for p,t in zip(pp,y))}


def rank_half(rows,probabilities):
    dates=defaultdict(list)
    for r in rows:dates[r['entry_date']].append(r)
    keep=set()
    for rr in dates.values():
        ordered=sorted(rr,key=lambda r:(-probabilities[r['id']],r['ticker'],str(r['id'])))
        keep.update(r['id'] for r in ordered[:math.ceil(len(rr)/2)])
    return keep


def outcome_stats(rows,kept,horizon='30'):
    eligible=[r for r in rows if r['outcomes'].get(horizon,{}).get('status')=='measured']
    rr=[r for r in eligible if r['id'] in kept];n=len(rr)
    wins=sum(r['outcomes'][horizon]['correct'] for r in rr)
    rawwins=sum(r['outcomes'][horizon]['raw_correct'] for r in rr)
    def sign(r):return -1 if r.get('direction')=='bearish' else 1
    return {'eligible':len(eligible),'retained':n,'correct':wins,'accuracy':100*wins/n if n else None,
            'coverage':100*n/len(eligible) if eligible else None,'raw_correct':rawwins,
            'raw_accuracy':100*rawwins/n if n else None,
            'average_directional_return':mean(sign(r)*r['outcomes'][horizon]['raw_return'] for r in rr) if n else None,
            'average_directional_excess':mean(sign(r)*round(r['outcomes'][horizon]['raw_return']-r['outcomes'][horizon]['spy_return'],2) for r in rr) if n else None,
            'tickers':len({r['ticker'] for r in rr}),'entry_dates':len({r['entry_date'] for r in rr}),
            'bullish':sum(r.get('direction','bullish')=='bullish' for r in rr),
            'bearish':sum(r.get('direction')=='bearish' for r in rr)}


def bootstrap_difference(rows,first,second,cluster,horizon='30'):
    groups=defaultdict(list)
    for r in rows:
        if r['outcomes'].get(horizon,{}).get('status')=='measured':groups[r[cluster]].append(r)
    counts=[]
    for rr in groups.values():
        a=[r for r in rr if r['id'] in first];b=[r for r in rr if r['id'] in second]
        counts.append((sum(r['outcomes'][horizon]['correct'] for r in a),len(a),
                       sum(r['outcomes'][horizon]['correct'] for r in b),len(b)))
    if len(counts)<2:return None
    rng=random.Random(90214);values=[]
    for _ in range(3000):
        draw=rng.choices(counts,k=len(counts));w1,n1,w2,n2=[sum(r[i] for r in draw) for i in range(4)]
        if n1 and n2:values.append(100*(w2/n2-w1/n1))
    if not values:return None
    values.sort();n=len(values)
    return {'clusters':len(counts),'replicates':n,'difference_pp95':[values[int(n*.025)],values[min(n-1,int(n*.975))]]}


def matrix(rows,names):
    import numpy as np
    return np.array([[r['features'].get(n) if r['features'].get(n) is not None else np.nan for n in names] for r in rows],dtype=float)


def main():
    import numpy as np
    from sklearn.impute import SimpleImputer
    from sklearn.linear_model import LogisticRegression
    from sklearn.pipeline import make_pipeline
    from sklearn.preprocessing import StandardScaler
    assert digest(BASE/'cohort.json')==LEDGER_HASH
    frozen=json.loads((ROOT/'analysis-plan.json').read_text())
    for path,sha in frozen['source_hashes'].items():
        if digest(Path(path))!=sha:raise ValueError('Frozen analysis source changed: '+path)
    payload=json.loads((ROOT/'panel.json').read_text());covered=[r for r in payload['rows'] if r['features'] is not None]
    groups={s:[r for r in covered if model_split(r)==s] for s in ['train','validation','test']}
    sizes={s:len(v) for s,v in groups.items()}
    if sizes['train']<100 or sizes['validation']<20 or sizes['test']<30:
        save(ROOT/'results.json',{'status':'insufficient_predeclared_sample','sizes':sizes})
        raise SystemExit('Predeclared sample minimum not met: '+json.dumps(sizes))
    if len({r['outcomes']['30']['correct'] for r in groups['train']})<2:
        raise SystemExit('Training outcomes contain only one class; no model fitted')
    allnames=sorted(covered[0]['features'])
    families={'price_market':[n for n in allnames if not n.startswith('options:')], 'price_market_options':allnames}
    selected={};trained={};validation_candidates=[]
    y=np.array([int(r['outcomes']['30']['correct']) for r in groups['train']])
    for family,names in families.items():
        candidates=[]
        for c in [.01,.1,1.]:
            model=make_pipeline(SimpleImputer(strategy='median',add_indicator=True,keep_empty_features=True),
                                StandardScaler(),LogisticRegression(C=c,max_iter=5000,random_state=90214))
            model.fit(matrix(groups['train'],names),y)
            p=model.predict_proba(matrix(groups['validation'],names))[:,1]
            probs={r['id']:float(v) for r,v in zip(groups['validation'],p)}
            m=probability_metrics(groups['validation'],probs)
            candidate={'family':family,'C':c,'validation':m};candidates.append((m['log_loss'],c,model,candidate))
            validation_candidates.append(candidate)
        best=min(candidates,key=lambda x:(x[0],x[1]));trained[family]=best[2];selected[family]=best[3]
    save(ROOT/'model-selection.json',{'selected':selected,'validation_candidates':validation_candidates,
                                      'training_rows':sizes,'features':families,'panel_sha256':digest(ROOT/'panel.json')})
    # Selection is fixed. Predict all available test feature rows, including unmatured labels.
    test=[r for r in covered if r['split']=='test'];predictions={};policies={};coefficients={}
    for family,names in families.items():
        p=trained[family].predict_proba(matrix(test,names))[:,1]
        probs={r['id']:float(v) for r,v in zip(test,p)};predictions[family]=probs
        policies[family]={'top_half':rank_half(test,probs),'probability_50':{k for k,v in probs.items() if v>=.5}}
        feature_names=trained[family].named_steps['simpleimputer'].get_feature_names_out(names)
        coefficients[family]={str(n):float(c) for n,c in zip(feature_names,trained[family].named_steps['logisticregression'].coef_[0])}
    save(ROOT/'test-predictions.json',{'predictions':predictions,'policies':{f:{p:sorted(s) for p,s in pp.items()} for f,pp in policies.items()},
                                       'selection_sha256':digest(ROOT/'model-selection.json')})
    all_ids={r['id'] for r in test}
    results={'status':'complete','sizes':sizes,'selected':selected,'features':families,
             'baseline':{h:outcome_stats(test,all_ids,h) for h in ['30','7']},'test':{},'coefficients_standardized':coefficients,
             'split_dates':{s:{'first_entry':min(r['entry_date'] for r in rr),'last_entry':max(r['entry_date'] for r in rr),
                               'last_30d_maturity':max(r['outcomes']['30']['target_date'] for r in rr),
                               'tickers':len({r['ticker'] for r in rr}),'dates':len({r['entry_date'] for r in rr})} for s,rr in groups.items()}}
    for family,probs in predictions.items():
        results['test'][family]={'probabilities':probability_metrics(test,probs),'policies':{}}
        for policy,kept in policies[family].items():
            results['test'][family]['policies'][policy]={h:outcome_stats(test,kept,h) for h in ['30','7']}
        results['test'][family]['periods']={anchor:{h:outcome_stats([r for r in test if r['anchor']==anchor],policies[family]['top_half'],h) for h in ['30','7']} for anchor in sorted({r['anchor'] for r in test})}
    results['paired_accuracy_difference']={cluster:bootstrap_difference(test,policies['price_market']['top_half'],policies['price_market_options']['top_half'],cluster) for cluster in ['entry_date','ticker']}
    transfer=json.loads((ROOT/'ledger-features.json').read_text())['rows'];valid=[r for r in transfer if r['features'] is not None]
    transfer_probs={f:({r['id']:float(p) for r,p in zip(valid,trained[f].predict_proba(matrix(valid,names))[:,1])} if valid else {}) for f,names in families.items()}
    save(ROOT/'ledger-predictions.json',{'predictions':transfer_probs,'selection_sha256':digest(ROOT/'model-selection.json')})
    originals=json.loads((BASE/'cohort.json').read_text())['events']
    ledger=[{**r,'outcomes':{h:{**o,'status':'measured'} for h,o in r['outcomes'].items()}} for r in originals]
    transfer_ids={r['id'] for r in transfer};valid_ids={r['id'] for r in valid}
    ledger_policies={'original':{r['id'] for r in ledger}}
    for family,probs in transfer_probs.items():
        ledger_policies[family]={r['id'] for r in ledger if r['direction']!='bullish' or r['id'] not in probs or probs[r['id']]>=.5}
    results['ledger_transfer']={'feature_available':len(valid),'considered':len(transfer),
        'coverage':dict(Counter(r['audit']['status'] for r in transfer)),
        'full_ledger_with_fallback':{h:{p:outcome_stats(ledger,ids,h) for p,ids in ledger_policies.items()} for h in ['30','7']},
        'same_covered_events':{h:{p:outcome_stats([r for r in ledger if r['id'] in valid_ids],ids,h) for p,ids in ledger_policies.items()} for h in ['30','7']},
        'rows':[{'id':r['id'],'ticker':r['ticker'],'entry_date':r['entry_date'],'direction':r['direction'],
                 'options_feature_available':r['id'] in valid_ids,'probabilities':{f:probs.get(r['id']) for f,probs in transfer_probs.items()},
                 'original_30d_correct':r['outcomes'].get('30',{}).get('correct')} for r in ledger if r['id'] in transfer_ids]}
    results['manifest']={str(p):digest(p) for p in [BASE/'cohort.json',ROOT/'cohort.json',ROOT/'analysis-plan.json',ROOT/'panel.json',ROOT/'features.json',ROOT/'ledger-features.json',
                                                 ROOT/'model-selection.json',ROOT/'test-predictions.json',Path(__file__)]}
    save(ROOT/'results.json',results);assert digest(BASE/'cohort.json')==LEDGER_HASH
    print(json.dumps({k:results[k] for k in ['sizes','selected','baseline','test','paired_accuracy_difference']},indent=2))


if __name__=='__main__':main()
