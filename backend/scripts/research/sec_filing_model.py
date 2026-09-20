"""Fixed filing-date fundamentals pilot, comparing identical covered observations."""
import argparse
import hashlib
import json
from collections import Counter
from pathlib import Path
import numpy as np
from sklearn.ensemble import ExtraTreesClassifier
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from threadpoolctl import threadpool_limits
from sec_filing_features import extract_series, build_features
from rank_calibration_model import partition, date_weights
from analyze_direct_returns import QUARTERS, TARGETS, target_return, paired_block_interval
from evaluate_rank_calibration import candidate_metrics, selection_metrics, FACTORS


def make_model(kind):
    impute=SimpleImputer(strategy='median',keep_empty_features=True)
    if kind=='logistic': return make_pipeline(impute,StandardScaler(),LogisticRegression(C=.1,max_iter=2000))
    return make_pipeline(impute,ExtraTreesClassifier(n_estimators=200,max_depth=5,min_samples_leaf=100,max_features=.7,random_state=91626,n_jobs=4))


def prepare(directory):
    selection=json.loads((directory/'selection.json').read_bytes())
    source=Path('frontend/test-results/confirmation-research/rank-calibration-2026-09-15/prepared.json')
    prior=json.loads(source.read_bytes())['groups']; groups={}; counts={}; hashes={str(source):hashlib.sha256(source.read_bytes()).hexdigest()}
    for group,items in selection.items():
        lookup={};c=Counter();rows=[]
        for item in items:
            path=directory/'facts'/(item['cik']+'.json')
            if path.exists():
                lookup[item['ticker']]=extract_series(json.loads(path.read_bytes()))
                hashes[str(path)]=hashlib.sha256(path.read_bytes()).hexdigest()
            else: c['missing_company_facts']+=1
        for r in prior[group]:
            if r['ticker'] not in lookup: continue
            c['price_feature_rows']+=1
            f,eligible,provenance=build_features(lookup[r['ticker']],r['decision_date'])
            if not eligible: c['no_current_revenue_or_assets']+=1;continue
            if '30' not in r['outcomes']: c['missing_outcome']+=1;continue
            rows.append({**r,'fundamentals':f,'filing_provenance':provenance})
        groups[group]=rows;counts[group]=dict(c)
    return {'groups':groups,'counts':counts,'inputs':hashes}


def evaluate_pilot(records):
    out={}
    for group in ['development','validation']:
        rows=[r for r in records if r['group']==group]
        report={'n':len(rows),'symbols':len(set(r['ticker'] for r in rows)),'dates':len(set(r['entry_date'] for r in rows)),'targets':{}}
        for target in TARGETS:
            fm={};fd={}
            for factor in FACTORS: fm[factor],fd[factor]=selection_metrics(rows,np.array([r['factors'][factor] for r in rows]),target)
            models={}
            for name in rows[0][target]['predictions']: models[name]=candidate_metrics(rows,target,name,fm,fd)
            report['targets'][target]={'models':models,'fixed_rankings':fm,'vs_price_only':{}}
            actual=np.array([r[target]['actual']>0 for r in rows]); dates=[r['entry_date'] for r in rows]
            for name in models:
                family,kind=name.rsplit('_',1)
                if family=='price': continue
                base='price_'+kind
                p=np.array([r[target]['predictions'][name] for r in rows]); pb=np.array([r[target]['predictions'][base] for r in rows])
                ag=((p>=.5)==actual).astype(float)-((pb>=.5)==actual)
                bg=(pb-actual)**2-(p-actual)**2
                _,d=selection_metrics(rows,p,target); _,db=selection_metrics(rows,pb,target)
                ds=[r['date'] for r in d];hg=np.array([a['selected_hit']-b['selected_hit'] for a,b in zip(d,db)]);rg=np.array([a['selected_return']-b['selected_return'] for a,b in zip(d,db)])
                report['targets'][target]['vs_price_only'][name]={
                    'accuracy_gain_pp':100*float(ag.mean()),'accuracy_gain_block95_pp':[100*x for x in paired_block_interval(dates,ag)],
                    'brier_improvement':float(bg.mean()),'brier_improvement_block95':paired_block_interval(dates,bg),
                    'selection_hit_gain_pp':100*float(hg.mean()),'selection_hit_gain_block95_pp':[100*x for x in paired_block_interval(ds,hg)],
                    'selection_return_gain_pp':float(rg.mean()),'selection_return_gain_block95_pp':paired_block_interval(ds,rg)}
        out[group]=report
    raw=out['validation']['targets']['raw'];leads=[]
    for name,m in raw['vs_price_only'].items():
        improved=m['accuracy_gain_block95_pp'][0]>0 and m['brier_improvement_block95'][0]>0
        s=raw['models'][name]['top_quartile']
        selection=(s['dates']>=50 and s['selected']>=1000 and s['hit_advantage_block95_pp'][0]>0 and s['return_advantage_block95_pp'][0]>0
                   and m['selection_hit_gain_block95_pp'][0]>0 and m['selection_return_gain_block95_pp'][0]>0
                   and sum(q['return_advantage_pp']>0 for q in s['quarters'].values())>=4)
        if improved: leads.append({'name':name,'directional_improvement':improved,'selection_improvement':selection})
    return {'groups':out,'leads':leads,'status':'Historical pilot; no promotion or final reserve evaluation'}


def main():
    ap=argparse.ArgumentParser();ap.add_argument('directory',type=Path);args=ap.parse_args();out=args.directory
    cache=out/'prepared.json'
    data=prepare(out);cache.write_text(json.dumps(data,allow_nan=True))
    groups=data['groups'];pn=sorted(groups['development'][0]['ranks']);fn=sorted(groups['development'][0]['fundamentals'])
    print('Prepared SEC features', {g:len(rr) for g,rr in groups.items()},data['counts'],flush=True)
    def features(rr,family):
        return np.array([([r['ranks'][n] for n in pn] if family in ['price','combined'] else []) + ([r['fundamentals'][n] for n in fn] if family in ['fundamentals','combined'] else []) for r in rr])
    records=[];folds=[]
    with threadpool_limits(limits=4):
        for start,end in QUARTERS[1:]:
            fit,cal=partition(groups['development'],start)
            if len(set(r['entry_date'] for r in fit))<12 or len(set(r['entry_date'] for r in cal))<8: raise ValueError('Insufficient pilot fit dates')
            samples=[r for rr in groups.values() for r in rr if start<=r['entry_date']<end]
            batch=[]
            for r in samples:
                rk=r['ranks'];factors={'momentum':rk['momentum_12_minus_1'],'reversal':1-rk['momentum_5'],'low_volatility':1-rk['volatility_60']};factors['composite']=sum(factors.values())/3
                batch.append({'ticker':r['ticker'],'group':r['group'],'entry_date':r['entry_date'],'target_date':r['outcomes']['30']['target_date'],
                              'stock_trend':bool(r['features']['above_sma_200']>0),'factors':factors})
            wf=date_weights(fit);wc=date_weights(cal)
            for target in TARGETS:
                y=np.array([target_return(r,target)>0 for r in fit]); prevalence=float(np.average([target_return(r,target)>0 for r in cal],weights=wc))
                for item,r in zip(batch,samples): item[target]={'actual':target_return(r,target),'prevalence':prevalence,'predictions':{}}
                for family in ['price','fundamentals','combined']:
                    x=features(fit,family);xt=features(samples,family)
                    for kind in ['logistic','trees']:
                        model=make_model(kind);weight_key='logisticregression__sample_weight' if kind=='logistic' else 'extratreesclassifier__sample_weight'
                        model.fit(x,y,**{weight_key:wf});p=model.predict_proba(xt)[:,1]
                        for item,v in zip(batch,p):item[target]['predictions'][family+'_'+kind]=float(v)
            records.extend(batch);folds.append({'start':start,'fit':len(fit),'calibration':len(cal),'predictions':len(batch),
                'latest_fit_target':max(r['outcomes']['30']['target_date'] for r in fit),'earliest_calibration_entry':min(r['entry_date'] for r in cal),
                'latest_calibration_target':max(r['outcomes']['30']['target_date'] for r in cal)})
            print('Completed SEC pilot fold',start,len(batch),flush=True)
    (out/'predictions.json').write_text(json.dumps(records,allow_nan=False))
    result=evaluate_pilot(records);result.update(folds=folds,price_features=pn,fundamental_features=fn,coverage=data['counts'],inputs=data['inputs'])
    (out/'results.json').write_text(json.dumps(result,indent=2,allow_nan=False))
    print(json.dumps({'groups':{g:{k:v[k] for k in ['n','symbols','dates']} for g,v in result['groups'].items()},'leads':result['leads']},indent=2),flush=True)


if __name__=='__main__':main()
