"""Fixed, purged quarterly direct-return research. Local artifacts only."""
import argparse
import hashlib
import json
import math
from collections import Counter
from pathlib import Path

import numpy as np
import sklearn
from scipy.stats import spearmanr
from sklearn.ensemble import HistGradientBoostingClassifier, HistGradientBoostingRegressor
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression, Ridge
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from threadpoolctl import threadpool_limits

from analyze_historical_panel import build_panel

QUARTERS = [('2025-01-01','2025-04-01'), ('2025-04-01','2025-07-01'),
            ('2025-07-01','2025-10-01'), ('2025-10-01','2026-01-01'),
            ('2026-01-01','2026-04-01'), ('2026-04-01','2026-07-01')]
TARGETS = ('raw', 'excess')


def target_return(row, target):
    outcome = row['outcomes']['30']
    return outcome['raw_return'] - (outcome['spy_return'] if target == 'excess' else 0.)


def training_rows(rows, cutoff):
    return [r for r in rows if r['outcomes']['30']['target_date'] < cutoff]


def factories():
    params = dict(max_leaf_nodes=4, max_iter=100, learning_rate=.05,
                  min_samples_leaf=100, l2_regularization=10,
                  early_stopping=False, random_state=91526)
    return {
        'logistic': lambda: make_pipeline(SimpleImputer(strategy='median', keep_empty_features=True), StandardScaler(), LogisticRegression(C=.1, max_iter=2000)),
        'boost_classifier': lambda: HistGradientBoostingClassifier(**params),
        'ridge': lambda: make_pipeline(SimpleImputer(strategy='median', keep_empty_features=True), StandardScaler(), Ridge(alpha=100)),
        'boost_regressor': lambda: HistGradientBoostingRegressor(**params),
    }


def paired_block_interval(dates, gains, block=8, draws=2000):
    """Event-weighted paired improvement, resampling consecutive weekly date blocks."""
    unique = sorted(set(dates))
    totals = {day: [0., 0] for day in unique}
    for day, gain in zip(dates, gains):
        totals[day][0] += gain
        totals[day][1] += 1
    sums = np.array([totals[day][0] for day in unique])
    counts = np.array([totals[day][1] for day in unique])
    length = min(block, len(unique))
    rng = np.random.default_rng(91526)
    starts = rng.integers(0, len(unique)-length+1, size=(draws, math.ceil(len(unique)/length)))
    indices = (starts[:,:,None] + np.arange(length)).reshape(draws, -1)[:,:len(unique)]
    boot = sums[indices].sum(axis=1) / counts[indices].sum(axis=1)
    return np.percentile(boot, [2.5,97.5]).tolist()


def evaluate(records, target, name):
    y = np.array([r[target]['actual'] for r in records])
    actual = y > 0
    pred = np.array([r[target]['predictions'][name] for r in records])
    classifier = name in ('logistic', 'boost_classifier')
    positive = pred >= .5 if classifier else pred > 0
    base_prob = np.array([r[target]['prevalence'] for r in records])
    baseline = base_prob >= .5
    dates = [r['entry_date'] for r in records]
    accuracy_gain = (positive == actual).astype(float) - (baseline == actual)
    result = {
        'n': len(records), 'direction_accuracy_pct': float(100*np.mean(positive == actual)),
        'bullish_coverage_pct': float(100*np.mean(positive)),
        'bullish_precision_pct': float(100*np.mean(actual[positive])) if np.any(positive) else None,
        'accuracy_gain_pp': float(100*np.mean(accuracy_gain)),
        'accuracy_gain_block95_pp': [100*x for x in paired_block_interval(dates, accuracy_gain)],
    }
    if classifier:
        loss = (pred-actual)**2
        base_loss = (base_prob-actual)**2
        result.update(brier=float(np.mean(loss)), baseline_brier=float(np.mean(base_loss)),
                      brier_improvement=float(np.mean(base_loss-loss)),
                      brier_improvement_block95=paired_block_interval(dates, base_loss-loss))
        result['probability_bins'] = []
        for lo,hi in zip(np.arange(0,1,.1), np.arange(.1,1.1,.1)):
            ix = (pred >= lo) & (pred < hi if hi < .999 else pred <= hi)
            if ix.any(): result['probability_bins'].append({'lo':float(lo),'n':int(ix.sum()),'predicted':float(pred[ix].mean()),'observed':float(actual[ix].mean())})
    else:
        base = np.array([r[target]['mean_return'] for r in records])
        mse = np.mean((pred-y)**2); base_mse = np.mean((base-y)**2)
        result.update(mae_percentage_points=float(np.mean(abs(pred-y))), baseline_mae=float(np.mean(abs(base-y))),
                      rmse_percentage_points=float(np.sqrt(mse)), baseline_rmse=float(np.sqrt(base_mse)),
                      r2_vs_training_mean=float(1-mse/base_mse) if base_mse else None)
    by_date = []
    for day in sorted(set(dates)):
        ix = np.array([i for i,d in enumerate(dates) if d == day])
        order = sorted(ix, key=lambda i:(-pred[i], records[i]['ticker']))
        top = order[:max(1, math.ceil(len(order)/4))]
        correlation = float(spearmanr(pred[ix], y[ix]).statistic) if len(ix)>2 and np.ptp(pred[ix])>0 and np.ptp(y[ix])>0 else None
        by_date.append({'date':day, 'n':len(ix), 'rank_correlation':correlation,
                        'universe_return':float(y[ix].mean()), 'top_quartile_return':float(y[top].mean()),
                        'top_quartile_advantage':float(y[top].mean()-y[ix].mean())})
    correlations = [d['rank_correlation'] for d in by_date if d['rank_correlation'] is not None]
    result['equal_date_mean_rank_correlation'] = float(np.mean(correlations)) if correlations else None
    result['equal_date_top_quartile_return_pct'] = float(np.mean([d['top_quartile_return'] for d in by_date]))
    result['equal_date_universe_return_pct'] = float(np.mean([d['universe_return'] for d in by_date]))
    result['equal_date_top_quartile_advantage_pp'] = float(np.mean([d['top_quartile_advantage'] for d in by_date]))
    result['quarterly'] = {}
    for start,end in QUARTERS:
        ix = np.array([start <= d < end for d in dates])
        if ix.any():
            item = {'n':int(ix.sum()),'accuracy_pct':float(100*np.mean((positive == actual)[ix])),
                    'accuracy_gain_pp':float(100*np.mean(accuracy_gain[ix]))}
            if classifier: item['brier_improvement'] = float(np.mean((base_loss-loss)[ix]))
            result['quarterly'][start] = item
    return result


def summarize(records):
    result = {'n':len(records), 'symbols':len(set(r['ticker'] for r in records)),
              'dates':len(set(r['entry_date'] for r in records)), 'first':min(r['entry_date'] for r in records),
              'last':max(r['entry_date'] for r in records), 'targets':{}}
    for target in TARGETS:
        actual = np.array([r[target]['actual'] > 0 for r in records])
        result['targets'][target] = {
            'baselines': {
                'always_positive_accuracy_pct':float(100*actual.mean()),
                'always_nonpositive_accuracy_pct':float(100*(~actual).mean()),
                'training_majority_accuracy_pct':float(100*np.mean([(r[target]['prevalence']>=.5)==a for r,a in zip(records,actual)])),
                'stock_sma200_accuracy_pct':float(100*np.mean([r['stock_trend']==a for r,a in zip(records,actual)])),
                'market_sma200_accuracy_pct':float(100*np.mean([r['market_trend']==a for r,a in zip(records,actual)])),
            },
            'models':{name:evaluate(records,target,name) for name in factories()}}
    return result


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('original_panel', type=Path)
    ap.add_argument('extension', type=Path)
    ap.add_argument('output_dir', type=Path)
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    original = json.loads(args.original_panel.read_bytes())
    payload = json.loads(args.extension.read_bytes())
    extension, excluded = build_panel(payload)
    overlap = set(r['ticker'] for r in original) & set(r['ticker'] for r in extension)
    if overlap: raise ValueError(f'Original and additional symbols overlap: {sorted(overlap)}')
    names = sorted(original[0]['features'])
    records = []; folds = []
    with threadpool_limits(limits=4):
        for start,end in QUARTERS:
            train = training_rows(original, start)
            samples = [(g,r) for g,rr in [('original',original), ('additional',extension)] for r in rr if start <= r['entry_date'] < end]
            if not train or not samples: raise ValueError('Empty chronological fold')
            x = np.array([[r['features'][n] for n in names] for r in train])
            xt = np.array([[r['features'][n] for n in names] for _,r in samples])
            fold = {'start':start, 'end':end, 'n_train':len(train), 'last_training_target':max(r['outcomes']['30']['target_date'] for r in train), 'evaluation':dict(Counter(g for g,r in samples)), 'targets':{}}
            batch = [{'group':g, 'ticker':r['ticker'], 'entry_date':r['entry_date'], 'target_date':r['outcomes']['30']['target_date'],
                      'stock_trend':bool(r['features']['above_sma_200']>0), 'market_trend':bool(r['features']['market:above_sma_200']>0)} for g,r in samples]
            for target in TARGETS:
                y = np.array([target_return(r,target) for r in train]); bounds = np.quantile(y,[.01,.99])
                clipped = np.clip(y,*bounds); prevalence = float(np.mean(y>0)); mean = float(clipped.mean())
                fold['targets'][target] = {'prevalence':prevalence, 'return_clip_bounds_pct':bounds.tolist(), 'clipped_mean_return':mean}
                for item,(_,r) in zip(batch,samples): item[target] = {'actual':target_return(r,target), 'prevalence':prevalence,'mean_return':mean,'predictions':{}}
                for name,factory in factories().items():
                    model = factory(); is_classifier = name in ('logistic','boost_classifier')
                    model.fit(x,y>0 if is_classifier else clipped)
                    pred = model.predict_proba(xt)[:,1] if is_classifier else model.predict(xt)
                    for item,p in zip(batch,pred): item[target]['predictions'][name] = float(p)
            records.extend(batch); folds.append(fold)
            print('Completed fold', start, 'train', len(train), fold['evaluation'], flush=True)
    # Persist fixed predictions before aggregate evaluation.
    (args.output_dir/'predictions.json').write_text(json.dumps(records,allow_nan=False))
    report = {'study':'direct-return-v1', 'numpy':np.__version__, 'sklearn':sklearn.__version__,
              'inputs':{str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in [args.original_panel,args.extension]},
              'features':names, 'extension_panel_rows':len(extension), 'extension_excluded':excluded, 'folds':folds,
              'price_audit':{'extreme_abs_raw_return_gt100':dict(Counter(r['group'] for r in records if abs(r['raw']['actual'])>100)),
                             'minimum_raw_return':min(r['raw']['actual'] for r in records), 'maximum_raw_return':max(r['raw']['actual'] for r in records)},
              'groups':{g:summarize([r for r in records if r['group']==g]) for g in ['original','additional']}}
    (args.output_dir/'results.json').write_text(json.dumps(report,indent=2,allow_nan=False))
    print(json.dumps({g:{t:{'baseline':v['baselines'], 'models':{n:{k:m[k] for k in ['direction_accuracy_pct','accuracy_gain_pp','accuracy_gain_block95_pp','bullish_coverage_pct']} for n,m in v['models'].items()}} for t,v in group['targets'].items()} for g,group in report['groups'].items()},indent=2),flush=True)


if __name__ == '__main__': main()
