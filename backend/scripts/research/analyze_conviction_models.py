"""Targeted dollar/participant research. Does not change any published forecast."""
import hashlib,json,math
from collections import Counter
from pathlib import Path
import numpy as np
from conviction_features import BASE,ConvictionFeatures,load_prices,build_member_history
from analyze_cot_models import augment
from analyze_disclosure_models import disclosure_features
from analyze_historical_panel import model_configs,split_for
from analyze_confirmation_models import correct,metrics,original
from analyze_ranked_bullish_gate import ranked_gate
from evaluate_historical_transfer import frozen_features


def active(f):
    return sum(f[f'conviction:{s}:90:{side}_filings'] for s in ['congress','insider'] for side in ['buy','sell'])>0


def feature_set(row,family):
    if family=='conviction_only':return row['conviction']
    f=augment(row,True)
    if family=='context_conviction':f.update(row['conviction'])
    else:f.update(disclosure_features(row['ticker'],row.get('decision_date') or row['calculated_at'][:10]))
    return f


def fixed_rules():
    def v(f,s,n,k):return f[f'conviction:{s}:{n}:{k}']
    def dollars(x):return math.expm1(x)
    return {
        'congress_net_buy_30':lambda f:v(f,'congress',30,'net_lower_ratio')>0,
        'congress_net_buy_100k_30':lambda f:v(f,'congress',30,'net_lower_log')>=math.log1p(100000),
        'congress_two_buyers_net_positive_30':lambda f:v(f,'congress',30,'buy_actors')>=2 and v(f,'congress',30,'net_lower_ratio')>0,
        'congress_prior_member_accuracy_60_90':lambda f:v(f,'congress',90,'member_past_accuracy')>=.60 and v(f,'congress',90,'net_lower_ratio')>0,
        'insider_buy_100k_net_positive_30':lambda f:v(f,'insider',30,'max_buy_log')>=math.log1p(100000) and v(f,'insider',30,'net_lower_ratio')>0,
        'insider_buy_1m_net_positive_90':lambda f:v(f,'insider',90,'max_buy_log')>=math.log1p(1000000) and v(f,'insider',90,'net_lower_ratio')>0,
        'insider_two_buyers_net_positive_30':lambda f:v(f,'insider',30,'buy_actors')>=2 and v(f,'insider',30,'net_lower_ratio')>0,
        'insider_purchase_twice_usual_90':lambda f:v(f,'insider',90,'unusual_ratio_max_log')>=math.log1p(2) and v(f,'insider',90,'net_lower_ratio')>0,
        'insider_ownership_increase_10pct_100k_90':lambda f:v(f,'insider',90,'ownership_increase_max_log')>=math.log1p(.1) and v(f,'insider',90,'max_buy_log')>=math.log1p(100000) and v(f,'insider',90,'net_lower_ratio')>0,
        'insider_executive_buy_100k_30':lambda f:v(f,'insider',30,'executive_buy_log_dollars')>=math.log1p(100000) and v(f,'insider',30,'net_lower_ratio')>0,
    }


def main():
    artifact=json.loads((BASE/'conviction-trades.json').read_text());trades=artifact['trades'];prices=load_prices()
    history,member_audit=build_member_history(trades,prices);engine=ConvictionFeatures(trades,history)
    panel=json.loads((BASE/'historical-results.panel.json').read_text());cohort_raw=(BASE/'cohort.json').read_bytes();cohort=json.loads(cohort_raw)['events']
    for r in panel:r['conviction']=engine.features(r['ticker'],r['decision_date'])
    ledger=[]
    for r in cohort:
        rr={**r,'conviction':engine.features(r['ticker'],r['calculated_at'][:10])}
        f=frozen_features(r,prices)
        if f is not None:rr['features']=f
        ledger.append(rr)
    groups={s:[r for r in panel if split_for(r)==s and active(r['conviction'])] for s in ['train','validation','test']}
    print('Active historical groups', {s:len(rr) for s,rr in groups.items()},'ledger active',sum(active(r['conviction']) for r in ledger),flush=True)
    result={'cohort_sha256':hashlib.sha256(cohort_raw).hexdigest(),'trade_audit':artifact['audit'],'member_history_audit':member_audit,'active_splits':{s:{'n':len(rr),'dates':len(set(r['entry_date'] for r in rr)),'symbols':len(set(r['ticker'] for r in rr))} for s,rr in groups.items()},'fixed_rules':{},'validation_candidates':[],'selected':{},'historical_active':{},'ledger':{}}
    bull=[r for r in ledger if r['direction']=='bullish' and '30' in r['outcomes']]
    result['fixed_rules']['original_bullish']=metrics(bull,np.ones(len(bull)))
    for name,rule in fixed_rules().items():
        rr=[r for r in bull if rule(r['conviction'])];result['fixed_rules'][name]={**metrics(bull,np.array([1 if rule(r['conviction']) else 0 for r in bull])),'tickers':[r['ticker'] for r in rr]}
    result['fixed_rules_history']={}
    result['fixed_rules_7']={}
    bull7=[r for r in ledger if r['direction']=='bullish' and '7' in r['outcomes']]
    for name,rule in fixed_rules().items():
        result['fixed_rules_history'][name]={}
        for split in ['validation','test']:
            rr=groups[split];pp=np.array([1 if rule(r['conviction']) else 0 for r in rr])
            selected=[r for r,p in zip(rr,pp) if p]
            result['fixed_rules_history'][name][split]={**metrics(rr,pp),'unique_tickers':len(set(r['ticker'] for r in selected)),'unique_dates':len(set(r['entry_date'] for r in selected))}
        result['fixed_rules_7'][name]=metrics(bull7,np.array([1 if rule(r['conviction']) else 0 for r in bull7]),'7')
    trained={};family_names={}
    for family in ['conviction_only','context_counts','context_conviction']:
        names=sorted(feature_set(groups['train'][0],family));family_names[family]=names
        x={s:np.array([[f[n] for n in names] for r in groups[s] for f in [feature_set(r,family)]]) for s in ['train','validation']}
        y=[correct(r,1) for r in groups['train']]
        for name,factory in model_configs():
            key=family+'|'+name;model=factory();model.fit(x['train'],y);trained[key]=model
            prob=model.predict_proba(x['validation'])[:,1]
            for mode in ['gate','ranked']:
                for threshold in ([0.] if mode=='ranked' else [.45,.5,.55,.6,.65,.7,.75,.8]):
                    p=ranked_gate(groups['validation'],prob) if mode=='ranked' else np.where(prob>=threshold,1,0)
                    m=metrics(groups['validation'],p)
                    if m['coverage']>=50:result['validation_candidates'].append({'key':key,'family':family,'mode':mode,'threshold':threshold,'validation':m})
            print('Validation',key,flush=True)
    # Select within each family so the counts control remains directly comparable.
    for family in family_names:
        for mode in ['gate','ranked']:
            cs=[c for c in result['validation_candidates'] if c['family']==family and c['mode']==mode]
            if cs:result['selected'][family+'|'+mode]=max(cs,key=lambda c:(c['validation']['wilson95'][0],c['validation']['coverage']))
    (BASE/'conviction-selection.json').write_text(json.dumps(result['selected'],indent=2))
    result['historical_active']['always_bull']={h:metrics(groups['test'],np.ones(len(groups['test'])),h) for h in ['30','7']}
    result['historical_active']['always_bear']={h:metrics(groups['test'],-np.ones(len(groups['test'])),h) for h in ['30','7']}
    predictions_log={}
    for label,c in result['selected'].items():
        family=c['family'];names=family_names[family];model=trained[c['key']];test=groups['test']
        xtest=np.array([[f[n] for n in names] for r in test for f in [feature_set(r,family)]]);prob=model.predict_proba(xtest)[:,1]
        p=ranked_gate(test,prob) if c['mode']=='ranked' else np.where(prob>=c['threshold'],1,0)
        result['historical_active'][label]={h:metrics(test,p,h) for h in ['30','7']}
        eligible=[r for r in ledger if active(r['conviction']) and (family=='conviction_only' or 'features' in r)]
        x=np.array([[f[n] for n in names] for r in eligible for f in [feature_set(r,family)]]);prob=model.predict_proba(x)[:,1]
        p=ranked_gate(eligible,prob,True) if c['mode']=='ranked' else np.where(original(eligible)==-1,-1,np.where(prob>=c['threshold'],1,0))
        by_id={r['id']:int(v) for r,v in zip(eligible,p)};result['ledger'][label]={}
        for h in ['30','7']:
            full=[r for r in ledger if h in r['outcomes']];pp=np.array([by_id.get(r['id'],1 if r['direction']=='bullish' else -1) for r in full])
            result['ledger'][label][h]=metrics(full,pp,h)
        predictions_log[label]=[{'id':r['id'],'ticker':r['ticker'],'probability':float(v),'prediction':int(p)} for r,v,p in zip(eligible,prob,p)]
    (BASE/'conviction-results.json').write_text(json.dumps(result,indent=2,allow_nan=False))
    (BASE/'conviction-predictions.json').write_text(json.dumps(predictions_log,indent=2))
    (BASE/'conviction-feature-cache.json').write_text(json.dumps({'ledger':ledger,'member_history':history},allow_nan=True))
    print(json.dumps({'fixed_rules':{k:{n:v[n] for n in ['n','accuracy','coverage']} for k,v in result['fixed_rules'].items()},'ledger':{k:{h:{n:m[n] for n in ['n','accuracy','coverage']} for h,m in v.items()} for k,v in result['ledger'].items()}},indent=2))


if __name__=='__main__':main()
