"""Evaluate fixed research forecasts and apply the prespecified continuation gate."""
import argparse
import json
import math
from collections import defaultdict
from pathlib import Path
import numpy as np
from scipy.stats import spearmanr
from analyze_direct_returns import paired_block_interval, QUARTERS, TARGETS

FACTORS=('momentum','reversal','low_volatility','composite')


def interval(dates, gains, scale=1.):
    return [scale*x for x in paired_block_interval(dates,gains)]


def selections(records, scores):
    groups=defaultdict(list)
    for i,r in enumerate(records): groups[r['entry_date']].append(i)
    selected={}
    for day,ix in sorted(groups.items()):
        selected[day]=sorted(ix,key=lambda i:(-scores[i],records[i]['ticker']))[:math.ceil(len(ix)/4)]
    return groups,selected


def selection_metrics(records,scores,target):
    y=np.array([r[target]['actual'] for r in records]); positive=y>0
    groups,selected=selections(records,scores)
    date_rows=[]
    for day,ix in sorted(groups.items()):
        top=selected[day]
        rho=float(spearmanr(scores[ix],y[ix]).statistic) if len(ix)>2 and np.ptp(scores[ix])>0 and np.ptp(y[ix])>0 else None
        date_rows.append({'date':day,'n':len(ix),'selected':len(top),
                          'universe_hit':float(positive[ix].mean()),'selected_hit':float(positive[top].mean()),
                          'universe_return':float(y[ix].mean()),'selected_return':float(y[top].mean()),
                          'rank_correlation':rho})
    dates=[d['date'] for d in date_rows]
    hit=np.array([d['selected_hit']-d['universe_hit'] for d in date_rows])
    ret=np.array([d['selected_return']-d['universe_return'] for d in date_rows])
    correlations=[d['rank_correlation'] for d in date_rows if d['rank_correlation'] is not None]
    result={'dates':len(dates),'selected':sum(d['selected'] for d in date_rows),
            'equal_date_selected_hit_pct':100*float(np.mean([d['selected_hit'] for d in date_rows])),
            'equal_date_universe_hit_pct':100*float(np.mean([d['universe_hit'] for d in date_rows])),
            'hit_advantage_pp':100*float(hit.mean()),'hit_advantage_block95_pp':interval(dates,hit,100),
            'equal_date_selected_return_pct':float(np.mean([d['selected_return'] for d in date_rows])),
            'equal_date_universe_return_pct':float(np.mean([d['universe_return'] for d in date_rows])),
            'return_advantage_pp':float(ret.mean()),'return_advantage_block95_pp':interval(dates,ret),
            'mean_rank_correlation':float(np.mean(correlations)) if correlations else None,'quarters':{}}
    for start,end in QUARTERS:
        mask=np.array([start<=day<end for day in dates])
        if mask.any(): result['quarters'][start]={'hit_advantage_pp':100*float(hit[mask].mean()),'return_advantage_pp':float(ret[mask].mean())}
    return result,date_rows


def candidate_metrics(records,target,name,factor_metrics,factor_dates):
    y=np.array([r[target]['actual'] for r in records]); actual=y>0
    prob=np.array([r[target]['predictions'][name] for r in records]); pos=prob>=.5
    prev=np.array([r[target]['prevalence'] for r in records]); dates=[r['entry_date'] for r in records]
    correctness=(pos==actual).astype(float)
    brier=(prob-actual)**2; base_brier=(prev-actual)**2; bgain=base_brier-brier
    result={'n':len(records),'accuracy_pct':100*float(correctness.mean()),'bullish_coverage_pct':100*float(pos.mean()),
            'bullish_precision_pct':100*float(actual[pos].mean()) if pos.any() else None,
            'brier':float(brier.mean()),'prevalence_brier':float(base_brier.mean()),'brier_improvement':float(bgain.mean()),
            'brier_improvement_block95':interval(dates,bgain),'accuracy_comparisons':{},'quarters':{},'probability_bins':[]}
    baselines={'stock_trend':np.array([r['stock_trend'] for r in records]),'always_nonpositive':np.zeros(len(records),bool),
               'always_positive':np.ones(len(records),bool),'prevalence':prev>=.5}
    for key,baseline in baselines.items():
        gain=correctness-(baseline==actual)
        result['accuracy_comparisons'][key]={'baseline_accuracy_pct':100*float(np.mean(baseline==actual)),
                                            'gain_pp':100*float(gain.mean()),'gain_block95_pp':interval(dates,gain,100)}
    for start,end in QUARTERS:
        ix=np.array([start<=d<end for d in dates])
        if ix.any(): result['quarters'][start]={'n':int(ix.sum()),'accuracy_pct':100*float(correctness[ix].mean()),'brier_improvement':float(bgain[ix].mean())}
    for lo in np.arange(0,1,.1):
        ix=(prob>=lo)&(prob<lo+.1 if lo<.89 else prob<=1)
        if ix.any(): result['probability_bins'].append({'lo':float(lo),'n':int(ix.sum()),'predicted':float(prob[ix].mean()),'observed':float(actual[ix].mean())})
    gate=prob>=.6
    result['probability_60_gate']={'n':int(gate.sum()),'coverage_pct':100*float(gate.mean()),
                                  'hit_pct':100*float(actual[gate].mean()) if gate.any() else None,
                                  'mean_return_pct':float(y[gate].mean()) if gate.any() else None}
    selected,day_rows=selection_metrics(records,prob,target)
    selected['vs_fixed_rankings']={}
    for factor in FACTORS:
        ff=factor_dates[factor]
        assert [d['date'] for d in day_rows]==[d['date'] for d in ff]
        h=np.array([a['selected_hit']-b['selected_hit'] for a,b in zip(day_rows,ff)])
        r=np.array([a['selected_return']-b['selected_return'] for a,b in zip(day_rows,ff)])
        ds=[d['date'] for d in ff]
        selected['vs_fixed_rankings'][factor]={'hit_advantage_pp':100*float(h.mean()),'return_advantage_pp':float(r.mean()),
                                             'hit_advantage_block95_pp':interval(ds,h,100),'return_advantage_block95_pp':interval(ds,r)}
    result['top_quartile']=selected
    return result


def advancement(models):
    candidates=[]
    for name,m in models.items():
        directional=(not name.startswith('rank_return_hist_uncalibrated') and m['brier_improvement_block95'][0]>0
                     and all(m['accuracy_comparisons'][b]['gain_block95_pp'][0]>0 for b in ['stock_trend','always_nonpositive'])
                     and sum(q['brier_improvement']>0 for q in m['quarters'].values())>=4)
        s=m['top_quartile']
        selection=(s['dates']>=50 and s['selected']>=1000 and s['hit_advantage_block95_pp'][0]>0 and s['return_advantage_block95_pp'][0]>0
                   and sum(q['return_advantage_pp']>0 for q in s['quarters'].values())>=4
                   and all(v['hit_advantage_pp']>0 and v['return_advantage_pp']>0 for v in s['vs_fixed_rankings'].values()))
        if directional or selection: candidates.append({'name':name,'directional':directional,'selection':selection,'brier':m['brier'],'return_lower_bound':s['return_advantage_block95_pp'][0]})
    ordered=sorted(candidates,key=lambda c:(not c['directional'],c['brier'] if c['directional'] else -c['return_lower_bound']))
    return {'eligible':ordered,'selected':ordered[0] if ordered else None}


def main():
    ap=argparse.ArgumentParser();ap.add_argument('directory',type=Path);args=ap.parse_args()
    all_rows=json.loads((args.directory/'predictions.json').read_bytes())
    result={'status':'Exploratory historical periods; reserved final symbols not yet evaluated','groups':{}}
    for group in ['development','validation']:
        records=[r for r in all_rows if r['group']==group]
        out={'n':len(records),'symbols':len(set(r['ticker'] for r in records)),'dates':len(set(r['entry_date'] for r in records)),
             'first':min(r['entry_date'] for r in records),'last':max(r['entry_date'] for r in records),
             'extreme_abs_raw_returns_gt100':sum(abs(r['raw']['actual'])>100 for r in records),'targets':{}}
        for target in TARGETS:
            factor_metrics={}; factor_dates={}
            for factor in FACTORS:
                scores=np.array([r['factors'][factor] for r in records])
                factor_metrics[factor],factor_dates[factor]=selection_metrics(records,scores,target)
            models={}
            for name in records[0][target]['predictions']:
                models[name]=candidate_metrics(records,target,name,factor_metrics,factor_dates)
            out['targets'][target]={'fixed_rankings':factor_metrics,'models':models}
        result['groups'][group]=out
        print('Evaluated',group,len(records),flush=True)
    result['advancement']=advancement(result['groups']['validation']['targets']['raw']['models'])
    (args.directory/'results.json').write_text(json.dumps(result,indent=2,allow_nan=False))
    (args.directory/'selection.json').write_text(json.dumps(result['advancement'],indent=2))
    print(json.dumps({'counts':{g:{k:v[k] for k in ['n','symbols','dates']} for g,v in result['groups'].items()},'advancement':result['advancement']},indent=2),flush=True)


if __name__=='__main__':main()
