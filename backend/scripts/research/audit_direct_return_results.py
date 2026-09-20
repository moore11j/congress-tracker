"""Post-results diagnostics of frozen predictions; no refitting or selection."""
import argparse
import json
from pathlib import Path
import numpy as np
from analyze_direct_returns import paired_block_interval, factories, TARGETS


def main():
    ap = argparse.ArgumentParser(); ap.add_argument('directory', type=Path); args = ap.parse_args()
    records = json.loads((args.directory/'predictions.json').read_bytes())
    records = [r for r in records if r['group'] == 'additional']
    dates = [r['entry_date'] for r in records]
    result = {'status':'Post-results diagnostic, unchanged predictions; no fresh holdout claim', 'targets':{}}
    for target in TARGETS:
        y = np.array([r[target]['actual'] for r in records]); actual = y>0
        extreme = np.array([abs(r['raw']['actual'])>100 for r in records])
        models = {}
        for name in factories():
            pred = np.array([r[target]['predictions'][name] for r in records])
            classifier = name in ('logistic','boost_classifier')
            positive = pred>=.5 if classifier else pred>0
            correct = (positive == actual).astype(float)
            metrics = {'accuracy_comparisons':{}}
            for label,baseline in [('always_positive',np.ones(len(y),dtype=bool)), ('always_nonpositive',np.zeros(len(y),dtype=bool)), ('stock_trend',np.array([r['stock_trend'] for r in records]))]:
                gains = correct-(baseline==actual)
                metrics['accuracy_comparisons'][label] = {'gain_pp':float(100*gains.mean()), 'block95_pp':[100*x for x in paired_block_interval(dates,gains)]}
            if not classifier:
                squared = (pred-y)**2
                metrics['squared_error_share_from_extreme_raw_outcomes_pct'] = float(100*squared[extreme].sum()/squared.sum())
                base = np.array([r[target]['mean_return'] for r in records])
                metrics['mse_improvement_block95'] = paired_block_interval(dates,(base-y)**2-squared)
            models[name] = metrics
        result['targets'][target] = models
    (args.directory/'diagnostics.json').write_text(json.dumps(result,indent=2,allow_nan=False))
    print(json.dumps(result,indent=2))


if __name__ == '__main__': main()
