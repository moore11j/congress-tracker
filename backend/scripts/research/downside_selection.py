"""Fixed downside overlays on immutable fundamentals forecasts. Offline research only."""
import argparse
import hashlib
import json
import math
from collections import defaultdict
from pathlib import Path
import numpy as np
from sklearn.ensemble import ExtraTreesClassifier, ExtraTreesRegressor, HistGradientBoostingRegressor
from sklearn.impute import SimpleImputer
from sklearn.pipeline import make_pipeline
from threadpoolctl import threadpool_limits
from analyze_direct_returns import QUARTERS, target_return, paired_block_interval
from rank_calibration_model import partition, date_weights, half_year_before

POLICIES=('baseline','low_volatility','expected_loss','large_loss','lower_tail','expected_payoff')


def key(row):
    return row['group'],row['ticker'],row['entry_date']


def tree(classifier=False):
    cls=ExtraTreesClassifier if classifier else ExtraTreesRegressor
    return make_pipeline(SimpleImputer(strategy='median',keep_empty_features=True),
        cls(n_estimators=200,max_depth=5,min_samples_leaf=100,max_features=.7,random_state=91726,n_jobs=4))


def fit_tree(model,x,y,weights,classifier=False):
    prefix='extratreesclassifier' if classifier else 'extratreesregressor'
    model.fit(x,y,**{prefix+'__sample_weight':weights})
    return model


def risk_targets(returns):
    returns=np.asarray(returns,dtype=float)
    return np.maximum(-returns,0.),returns<=-10.


def payoff(probability,gains,losses):
    return probability*gains-(1-probability)*losses


def choose(records,policy):
    """Uses only forecasts and prior volatility; future outcomes cannot enter selection."""
    n=len(records); k=math.ceil(n/4)
    eligible=sorted(range(n),key=lambda i:(-records[i]['base_probability'],records[i]['ticker']))
    if policy=='baseline':return eligible[:k]
    eligible=eligible[:math.ceil(n/2)]
    field={'low_volatility':'prior_volatility','expected_loss':'predicted_loss','large_loss':'predicted_large_loss',
           'lower_tail':'predicted_lower_tail','expected_payoff':'predicted_payoff'}[policy]
    sign=-1 if policy in ('lower_tail','expected_payoff') else 1
    return sorted(eligible,key=lambda i:(sign*records[i][field],records[i]['ticker']))[:k]


def measures(rows):
    raw=np.array([r['raw_return'] for r in rows]); excess=np.array([r['excess_return'] for r in rows])
    return {'hit_rate':float(np.mean(raw>0)),'return':float(raw.mean()),'excess_return':float(excess.mean()),
            'downside':float(np.maximum(-raw,0).mean()),'large_loss_rate':float(np.mean(raw<=-10))}


def pair_metrics(daily,reference):
    dates=[r['date'] for r in daily]
    if dates!=[r['date'] for r in reference]:raise ValueError('Mismatched comparison dates')
    result={}
    for name,sign,scale in [('hit_rate',1,100),('return',1,1),('excess_return',1,1),('downside',-1,1),('large_loss_rate',-1,100)]:
        gains=np.array([sign*(a[name]-b[name]) for a,b in zip(daily,reference)])
        result[name]={'improvement':float(scale*gains.mean()),'block95':[scale*x for x in paired_block_interval(dates,gains)]}
    return result


def evaluate(records):
    groups=defaultdict(list)
    for r in records:groups[r['entry_date']].append(r)
    universe=[{'date':day,**measures(rr)} for day,rr in sorted(groups.items())]
    out={};all_daily={}
    for policy in POLICIES:
        daily=[];pooled=[]
        for day,rr in sorted(groups.items()):
            ix=choose(rr,policy);selected=[rr[i] for i in ix];pooled.extend(selected)
            base=set(choose(rr,'baseline'))
            daily.append({'date':day,'n':len(selected),'overlap':len(base&set(ix))/len(ix),**measures(selected)})
        raw=np.array([r['raw_return'] for r in pooled]);losers=raw[raw<0]
        summary={name:float(np.mean([r[name] for r in daily])) for name in ['hit_rate','return','excess_return','downside','large_loss_rate','overlap']}
        summary.update(n_selected=len(pooled),dates=len(daily),coverage_pct=100*len(pooled)/len(records),
                       pooled_average_losing_return=float(losers.mean()) if len(losers) else None,
                       pooled_worst_decile_mean_return=float(np.sort(raw)[:math.ceil(len(raw)/10)].mean()),
                       selected_symbols=len(set(r['ticker'] for r in pooled)),quarterly={})
        for start,end in QUARTERS[1:]:
            qq=[r for r in daily if start<=r['date']<end]
            if qq:summary['quarterly'][start]={name:float(np.mean([r[name] for r in qq])) for name in ['hit_rate','return','downside','large_loss_rate']}
        out[policy]=summary;all_daily[policy]=daily
    for policy in POLICIES:
        out[policy]['vs_baseline']=pair_metrics(all_daily[policy],all_daily['baseline'])
        out[policy]['vs_universe']=pair_metrics(all_daily[policy],universe)
        out[policy]['vs_low_volatility']=pair_metrics(all_daily[policy],all_daily['low_volatility'])
    return {'n':len(records),'symbols':len(set(r['ticker'] for r in records)),
            'universe':{name:float(np.mean([r[name] for r in universe])) for name in ['hit_rate','return','excess_return','downside','large_loss_rate']},
            'policies':out,'daily':all_daily}


def advancement(report):
    baseline=report['policies']['baseline'];passed=[];checks={}
    for name in POLICIES[2:]:
        s=report['policies'][name];b=s['vs_baseline'];u=s['vs_universe'];v=s['vs_low_volatility']
        checks[name]={
            'sample':s['dates']>=50 and s['n_selected']>=1000,
            'hit_rate_noninferiority':b['hit_rate']['block95'][0]>-2,
            'return_vs_baseline':b['return']['block95'][0]>0,
            'downside_vs_baseline':b['downside']['block95'][0]>0,
            'return_vs_universe':u['return']['block95'][0]>0,
            'lower_large_loss_rate':b['large_loss_rate']['improvement']>0,
            'four_positive_quarters':sum(q['return']>baseline['quarterly'][d]['return'] for d,q in s['quarterly'].items())>=4,
            'adds_to_volatility_control':v['return']['improvement']>0 and v['downside']['improvement']>0}
        if all(checks[name].values()):passed.append({'name':name,'return_improvement_lower_bound':b['return']['block95'][0]})
    passed.sort(key=lambda p:-p['return_improvement_lower_bound'])
    return {'checks':checks,'eligible':passed,'selected':passed[0] if passed else None}


def main():
    ap=argparse.ArgumentParser();ap.add_argument('output_dir',type=Path);args=ap.parse_args();out=args.output_dir;out.mkdir(parents=True,exist_ok=True)
    source=Path('frontend/test-results/confirmation-research/sec-filing-pilot-2026-09-15')
    prepared=source/'prepared.json';forecast=source/'predictions.json'
    groups=json.loads(prepared.read_bytes())['groups'];base=json.loads(forecast.read_bytes());by_key={key(r):r for r in base}
    pn=sorted(groups['development'][0]['ranks']);fn=sorted(groups['development'][0]['fundamentals'])
    def matrix(rows):
        return np.array([[r['ranks'][n] for n in pn]+[r['fundamentals'][n] for n in fn]+[r['features']['volatility_20'],r['features']['volatility_60']] for r in rows])
    records=[];folds=[]
    with threadpool_limits(limits=4):
        for start,end in QUARTERS[1:]:
            fit,_=partition(groups['development'],start)
            if any(r['outcomes']['30']['target_date']>=half_year_before(start) for r in fit):raise AssertionError('Unmatured fit label')
            samples=[r for rr in groups.values() for r in rr if start<=r['entry_date']<end and key(r) in by_key]
            x=matrix(fit);xt=matrix(samples);y=np.array([target_return(r,'raw') for r in fit]);w=date_weights(fit)
            loss,large=risk_targets(y)
            loss_model=fit_tree(tree(),x,loss,w)
            large_model=fit_tree(tree(True),x,large,w,True)
            if list(large_model.classes_)!=[False,True]:raise ValueError('Large-loss target needs both classes')
            tail=make_pipeline(SimpleImputer(strategy='median',keep_empty_features=True),HistGradientBoostingRegressor(
                loss='quantile',quantile=.1,max_leaf_nodes=4,max_iter=100,learning_rate=.05,min_samples_leaf=100,
                l2_regularization=10,early_stopping=False,random_state=91726))
            tail.fit(x,y,histgradientboostingregressor__sample_weight=w)
            win=y>0;cap=float(np.quantile(y[win],.99))
            gain_model=fit_tree(tree(),x[win],np.minimum(y[win],cap),date_weights([r for r,ok in zip(fit,win) if ok]))
            conditional_loss=fit_tree(tree(),x[~win],-y[~win],date_weights([r for r,ok in zip(fit,win) if not ok]))
            ploss=loss_model.predict(xt);pcrash=large_model.predict_proba(xt)[:,1];q10=tail.predict(xt)
            p=np.array([by_key[key(r)]['raw']['predictions']['fundamentals_trees'] for r in samples])
            pay=payoff(p,gain_model.predict(xt),conditional_loss.predict(xt))
            for i,r in enumerate(samples):
                old=by_key[key(r)]
                if target_return(r,'raw')!=old['raw']['actual']:raise ValueError('Frozen outcome mismatch')
                records.append({'group':r['group'],'ticker':r['ticker'],'entry_date':r['entry_date'],'target_date':r['outcomes']['30']['target_date'],
                    'raw_return':old['raw']['actual'],'excess_return':old['excess']['actual'],'base_probability':float(p[i]),
                    'prior_volatility':r['features']['volatility_60'],'predicted_loss':float(ploss[i]),'predicted_large_loss':float(pcrash[i]),
                    'predicted_lower_tail':float(q10[i]),'predicted_payoff':float(pay[i])})
            folds.append({'start':start,'fit_rows':len(fit),'latest_fit_target':max(r['outcomes']['30']['target_date'] for r in fit),
                          'training_win_cap':cap,'predictions':len(samples)})
            print('Completed downside fold',start,len(samples),flush=True)
    if {key(r) for r in records}!={key(r) for r in base}:raise ValueError('Changed evaluation membership')
    (out/'predictions.json').write_text(json.dumps(records,allow_nan=False))
    result={'inputs':{str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in [prepared,forecast]},'folds':folds,
            'groups':{g:evaluate([r for r in records if r['group']==g]) for g in ['development','validation']}}
    result['advancement']=advancement(result['groups']['validation'])
    previous=json.loads((source/'results.json').read_bytes())['groups']['validation']['targets']['raw']['models']['fundamentals_trees']['top_quartile']
    actual=result['groups']['validation']['policies']['baseline']
    if not np.isclose(actual['hit_rate']*100,previous['equal_date_selected_hit_pct']) or not np.isclose(actual['return'],previous['equal_date_selected_return_pct']):
        raise ValueError('Baseline selection did not reproduce')
    (out/'results.json').write_text(json.dumps(result,indent=2,allow_nan=False))
    (out/'selection.json').write_text(json.dumps(result['advancement'],indent=2))
    print(json.dumps({'validation':{n:{k:s[k] for k in ['hit_rate','return','downside','large_loss_rate','overlap']} for n,s in result['groups']['validation']['policies'].items()},'advancement':result['advancement']},indent=2),flush=True)


if __name__=='__main__':main()
