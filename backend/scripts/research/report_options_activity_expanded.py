"""Render the frozen expanded study without selecting new rules from its results."""
import json
from pathlib import Path

from options_activity_expanded import ROOT

REPORT=Path('docs/options-activity-expanded-results-2026-09-14.md')


def num(value,digits=1):
    return 'n/a' if value is None else f'{value:.{digits}f}'


def metric_row(name,h,stats):
    return (f'| {name} | {h}D | {stats["correct"]}/{stats["retained"]} | {num(stats["accuracy"])}% | '
            f'{num(stats["coverage"])}% | {num(stats["raw_accuracy"])}% | '
            f'{num(stats["average_directional_return"],2)}% | {num(stats["average_directional_excess"],2)}% |')


def table_header():
    return ['| Model / policy | Horizon | Correct / retained | Ledger-style accuracy | Retained coverage | Raw-positive accuracy | Mean directional return | Mean excess vs SPY |',
            '|---|---:|---:|---:|---:|---:|---:|---:|']


def main():
    r=json.loads((ROOT/'results.json').read_text());a=json.loads((ROOT/'panel-audit.json').read_text())
    audit=json.loads((ROOT/'input-audit.json').read_text());cfg=json.loads((ROOT/'cohort.json').read_text())
    if not audit['passed']:raise SystemExit('Cannot report successful research before input audit passes')
    lines=['# Expanded free options activity study — September 14, 2026','',
           'Local research using the existing Massive account. No subscription upgrade, production write, live scoring change, or modification to historical public outcomes.','',
           '## Scope and coverage','',
           f'The frozen sample contains {a["planned"]} planned weekly observations across {len(cfg["symbols"])} surviving liquid stocks and six anchor windows. '
           f'Collection used {audit["cached_requests"]} cached API requests, {audit["unique_contracts"]} distinct contracts, and {audit["daily_bars"]:,} daily bars. '
           'Each anchor selects one matched call/put pair, held fixed within that window; this is sampled unsigned options activity, not full-chain or buyer-initiated flow.','',
           '| Feature availability | Planned observations |','|---|---:|']
    lines.extend(f'| {k} | {v} |' for k,v in sorted(a['feature_coverage'].items()))
    lines+=['','| Split | Measured 30D rows with features |','|---|---:|']
    lines.extend(f'| {k} | {v} |' for k,v in a['model_rows'].items())
    lines+=['','All comparisons use identical feature-covered observations in both model families. Availability exclusions are determined from pre-entry inputs, not future returns. Missing features are not fabricated.','',
            'Outcome availability across all planned observations: `'+json.dumps(a['labels'],sort_keys=True)+'`.','']
    if r['status']!='complete':
        lines+=['## Result','',
                'The predeclared minimum sample was not met. No model was fitted or promoted, and no replacement split or looser coverage rule was selected after seeing this failure.','',
                'Status: `'+r['status']+'`.']
    else:
        base=r['test']['price_market']['policies']['top_half']['30']
        opt=r['test']['price_market_options']['policies']['top_half']['30']
        delta=opt['accuracy']-base['accuracy']
        lines+=['## Primary result','',
                f'On later dates, the frozen top-half policy achieved **{num(opt["accuracy"])}% 30D accuracy with options** '
                f'({opt["correct"]}/{opt["retained"]}), versus **{num(base["accuracy"])}% without options** '
                f'({base["correct"]}/{base["retained"]}): **{delta:+.1f} percentage points**. '
                'This compares a historical price/market model with the same inputs plus options. It is not the live confirmation model’s accuracy.','',
                'The unfiltered bullish baseline is essential context: strong market periods can produce high headline accuracy without predictive options information.','']
        lines+=table_header()
        for h in ['30','7']:lines.append(metric_row('All covered observations, bullish',h,r['baseline'][h]))
        for family,label in [('price_market','Price + market'),('price_market_options','Price + market + options')]:
            for policy,p_label in [('top_half','top half per date'),('probability_50','probability ≥ 0.50')]:
                for h in ['30','7']:
                    lines.append(metric_row(label+'; '+p_label,h,r['test'][family]['policies'][policy][h]))
        lines+=['','The primary policy ranks every feature-eligible observation before excluding unmatured or unpriced outcomes from measurement. '
                'With an odd number of eligible stocks on a date, it keeps the rounded-up half. The 0.50 gate is a prespecified secondary result.','',
                '## Period checks and uncertainty','',
                '| Test anchor | Model, top-half policy | Horizon | Correct / retained | Accuracy |',
                '|---|---|---:|---:|---:|']
        for family,label in [('price_market','Price + market'),('price_market_options','Price + market + options')]:
            for anchor,period in r['test'][family]['periods'].items():
                for h in ['30','7']:
                    s=period[h];lines.append(f'| {anchor} | {label} | {h}D | {s["correct"]}/{s["retained"]} | {num(s["accuracy"])}% |')
        lines+=['','Paired 95% bootstrap intervals for the options-minus-baseline 30D accuracy difference, resampling whole groups:','']
        for cluster,v in r['paired_accuracy_difference'].items():
            if v:
                lo,hi=v['difference_pp95'];lines.append(f'- {cluster}: {lo:+.1f} to {hi:+.1f} percentage points ({v["clusters"]} groups, {v["replicates"]:,} draws).')
            else:lines.append(f'- {cluster}: insufficient groups.')
        lines+=['','These are fixed-prediction, one-cluster-dimension-at-a-time sensitivity intervals. They do not account for the full history of model searches, '
                'retraining uncertainty, or simultaneous date/ticker dependence. Weekly 30D outcomes overlap.','',
                '## Selection and probability quality','',
                'Training used the three 2025 windows. The January 2026 window selected regularization by log loss from C = 0.01, 0.1, 1. '
                'Both models retained their training-only coefficients and preprocessing for the May/July tests. No threshold was tuned on those test outcomes.','',
                '| Model | Selected C | Validation log loss | Test log loss | Test Brier score |',
                '|---|---:|---:|---:|---:|']
        for family in ['price_market','price_market_options']:
            s=r['selected'][family];p=r['test'][family]['probabilities']
            lines.append(f'| {family} | {s["C"]} | {num(s["validation"]["log_loss"],4)} | {num(p["log_loss"],4)} | {num(p["brier"],4)} |')
        lines+=['','Lower log loss/Brier is better. Accuracy alone can improve through retaining fewer or easier calls.','',
                '## Transfer to the immutable public ledger','']
        transfer=r['ledger_transfer']
        lines+=[f'Of {transfer["considered"]} original events in the 12-stock universe, {transfer["feature_available"]} had eligible historical options features. '
                'The frozen 0.50 probability gate hypothetically filters bullish events; bearish and unknown-feature events retain their original decisions. '
                'This is an overlapping, already-inspected ledger transfer check, not an independent validation set.','',
                'Feature coverage: `'+json.dumps(transfer['coverage'],sort_keys=True)+'`.','',
                '### Full ledger, original fallback for unavailable inputs','']
        lines+=table_header()
        for h,policies in transfer['full_ledger_with_fallback'].items():
            for name,stats in policies.items():lines.append(metric_row(name,h,stats))
        lines+=['','### Identical events with options features','']+table_header()
        for h,policies in transfer['same_covered_events'].items():
            for name,stats in policies.items():lines.append(metric_row(name,h,stats))
        lines+=['','No public event was removed or rescored. These tables are separate research copies.','']
    lines+=['## Interpretation limits','',
            '- The broad study lacks historic snapshots of every confirmation input. It cannot establish how adding options would have changed the complete live model.',
            '- Daily volume and VWAP do not identify whether a call/put was bought or sold, opened or closed. Treating call premium as automatic bullish buying would be unsupported.',
            '- Twelve surviving, liquid stocks and two test windows do not represent the full ledger or all market regimes. Contract aging and incomplete historical activity limit coverage.',
            '- Historical prices and some ledger results were examined in earlier research. The frozen chronological test does not erase that prior exposure or multiple-model-search risk.',
            '- The existing ledger definition counts a bullish call correct when its raw return is positive OR its SPY excess is positive. Raw-positive accuracy is also shown, without changing public grading.',
            '- Neither a high historical percentage nor a small positive difference establishes future 75% accuracy or supports a production promotion on its own.','',
            '## Verification and artifacts','',
            f'The input audit passed {audit["decision_replays"]} decision replays: removing every post-cutoff option bar left the model inputs unchanged. '
            f'It checked {audit["groups"]} historical selections against the cached reference responses and verified all frozen input hashes.','',
            'Immutable ledger SHA-256: `'+audit['ledger_sha256']+'`.','',
            'Protocol: [options-activity-expanded-protocol-2026-09-14.md](options-activity-expanded-protocol-2026-09-14.md).','',
            'Local machine-readable artifacts are under `frontend/test-results/confirmation-research/options-activity-expanded/`: '
            '`cohort.json`, `analysis-plan.json`, `input-audit.json`, `features.json`, `panel.json`, `panel-audit.json`, `model-selection.json`, '
            '`test-predictions.json`, `ledger-predictions.json`, and `results.json`. Research scripts are under `backend/scripts/research/`.','']
    REPORT.write_text('\n'.join(lines),encoding='utf-8')
    print(str(REPORT))


if __name__=='__main__':main()
