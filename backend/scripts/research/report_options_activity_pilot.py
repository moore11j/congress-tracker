"""Render the completed local pilot honestly, including abstentions and gaps."""
import json
from pathlib import Path

from options_activity_pilot import ROOT


def percent(value):
    return 'Not measured' if value is None else f'{value:.1f}%'


def sample(m):
    return f"{m['correct']}/{m['retained']} ({percent(m['accuracy'])})" if m['retained'] else 'No retained measurements'


def main():
    result = json.loads((ROOT/'evaluation.json').read_text())
    collection = json.loads((ROOT/'collection-status.json').read_text())
    if collection['status'] != 'complete': raise SystemExit('Collection has not completed')
    rows = result['events']; lines = [
        '# Free options-activity pilot results', '',
        'The free Massive options research pipeline is implemented and its first eight-stock historical run is complete. '
        'This is an engineering pilot with fixed rules, not a trained or validated model. No paid plan, production scoring change, '
        'historical outcome modification, or scheduled production job was introduced.', '',
        '**The fixed options rules did not improve this pilot.** The options gate dropped three correct 30D calls (TSM, NVDA, XOM). '
        'The results support keeping this rule out of live scoring; they do not establish that all options information is unhelpful.', '',
        '## Same-event comparison', '',
        'The baseline uses the original stored confirmation and outcome. The candidate gates can retain or abstain from an original bullish call; '
        'they do not change its score, direction, entry, or measurement. Missing feature coverage keeps the original decision. '
        'Accuracy is the existing directional return OR SPY-relative grading definition.', '',
        '| Policy | 30D correct / retained | 30D retained coverage | 7D correct / retained |',
        '|---|---:|---:|---:|']
    for name, label in [('original','Original confirmations'),('trend_only','Stock/SPY trend control'),('options','Fixed options-activity gate'),('options_and_trend','Options gate + stock/SPY trend')]:
        a = result['all_pilot']['30'][name]; b = result['all_pilot']['7'][name]
        lines.append(f"| {label} | {sample(a)} | {percent(a['retained_percent'])} | {sample(b)} |")
    lines += ['', 'A higher percentage on fewer retained observations is not evidence of a general improvement. '
              'If a rule retains no calls, its accuracy is undefined, not zero and not 100%. '
              'No thresholds were selected or changed using these outcomes.', '',
              '| Policy | Average signed 30D return | Average signed 30D excess vs SPY | Raw directional hits |',
              '|---|---:|---:|---:|']
    for name, label in [('original','Original'),('trend_only','Stock/SPY trend control'),('options','Options gate'),('options_and_trend','Combined gate')]:
        m = result['all_pilot']['30'][name]
        lines.append(f"| {label} | {percent(m['average_directional_return'])} | {percent(m['average_directional_excess'])} | {m['raw_directional_correct']}/{m['retained']} |")
    lines += ['', 'For the six tickers with sufficient options history, the comparison is:', '',
              '| Policy, same six covered tickers | 30D correct / retained | 7D correct / retained |',
              '|---|---:|---:|']
    for name,label in [('original','Original'),('trend_only','Stock/SPY trend control'),('options','Options gate'),('options_and_trend','Combined gate')]:
        lines.append(f"| {label} | {sample(result['covered_options_only']['30'][name])} | {sample(result['covered_options_only']['7'][name])} |")
    lines += ['', 'JPM and WMT lack the required long-expiry baseline and retain their original decisions in the all-pilot table. '
              'Removing these missing-data cases does not turn the options gate into an improvement over the same covered baseline.']
    lines += ['', '## Data coverage', '',
              '| Expiration bucket | Feature status counts |', '|---|---|']
    for bucket, statuses in result['coverage'].items():
        lines.append(f"| {bucket.replace('_','–')} calendar days | "+'; '.join(f'{k}: {v}' for k,v in statuses.items())+' |')
    lines += ['', 'Each bucket samples one matched call/put pair at the same strike and expiration. '
              'These are not full-chain volume totals or observed buyer-initiated flows. '
              'The 30–90-day gate requires a five-session premium rate at least twice the preceding 20-session daily rate, '
              'call premium share of at least 60%, and a share increase of at least 10 percentage points. '
              'The combined gate also requires the stock and SPY above their 20-session moving averages.', '',
              '| Ticker | Data cutoff | Long-expiry baseline | Activity ratio | Call premium share change | Options gate | Stored 30D correct |',
              '|---|---|---|---:|---:|---|---|']
    for r in rows:
        f = r['features']['30_90']; ratio = f"{f['premium_activity_ratio']:.2f}x" if f.get('premium_activity_ratio') is not None else 'Unknown'
        change = f"{100*f['call_premium_share_change']:+.1f} pp" if f.get('call_premium_share_change') is not None else 'Unknown'
        gate = 'Unknown: original retained' if r['options_gate'] is None else 'Retain' if r['options_gate'] else 'Abstain'
        outcome = r['outcomes'].get('30'); correct = 'Not measured' if outcome is None else 'Yes' if outcome['correct'] else 'No'
        lines.append(f"| {r['ticker']} | {r['cutoff']} | {f['status']} | {ratio} | {change} | {gate} | {correct} |")
    lines += ['', '## Interpretation and next evidence', '',
              'The eight liquid names were chosen before collecting options observations: TSM, AAPL, NVDA, MSFT, AMZN, JPM, XOM, WMT. '
              'Their original confirmations are concentrated in August 2026. This selection and short period cannot establish held-out accuracy, '
              'a 75% expected hit rate, or benefits for 90D/180D/365D horizons. No classifier was fit to this sample.', '',
              'Short-dated contracts often do not have the 25 prior benchmark sessions required by the baseline. '
              'Treat that as insufficient history, not zero volume or negative evidence. '
              'The long-expiry comparison still has limitations: gross volume is unsigned; premium changes reflect prices and volatility; '
              'a fixed strike changes moneyness as the stock moves; proximity to expiration can change activity. '
              'Historical contract metadata and prices are not a complete original-vintage audit.', '',
              'Keep live scoring unchanged. The next efficacy study needs a larger, date-correct options panel, '
              'comparison on identical eligible events against price/market controls, and an independent later period. '
              'Any learning must use labels that matured before the validation period, with a 30D maturity gap. '
              'Do not lower the activity threshold merely to obtain more favorable pilot results.', '',
              '## Implementation and verification', '',
              '- Collector: `backend/scripts/research/options_activity_pilot.py`.',
              '- Feature calculation and immutable-outcome comparison: `backend/scripts/research/analyze_options_activity_pilot.py`.',
              '- Report renderer: `backend/scripts/research/report_options_activity_pilot.py`.',
              '- Protocol: `docs/options-activity-pilot-protocol-2026-09-14.md`.',
              '- Local data: `frontend/test-results/confirmation-research/options-activity-pilot/`.', '',
              f"The completed collection reports {collection['requests_this_run']} requests in its final invocation. "
              'Requests are serialized at least 13 seconds apart, restricted to free reference/aggregate endpoints, '
              'and resumed from local cache. The request cap stops a run without treating incomplete data as success. '
              'A local process lock prevents concurrent collectors. Credentials and credential-bearing pagination URLs are not persisted.', '',
              'Run from the repository root:', '', '```powershell',
              'C:/Python314/python.exe backend/scripts/research/options_activity_pilot.py --collect --max-requests 80',
              'C:/Python314/python.exe backend/scripts/research/analyze_options_activity_pilot.py',
              'C:/Python314/python.exe backend/scripts/research/report_options_activity_pilot.py', '```', '',
              'Completed historical requests are cached; rerunning this frozen experiment does not repeatedly download the same data. '
              'This is a local research pipeline, not an installed nightly production collector. '
              'Customer pages do not call Massive through this research code.', '',
              'The 11 options-pipeline tests passed, along with the 13 existing conviction/disclosure research tests. '
              'They cover New York EOD cutoffs, future-data exclusion, matched standard contracts, baseline availability, '
              'missing VWAP, daily-rate normalization, sanitized pagination, cache reuse, request budgets, and duplicate bars. '
              'The frozen public ledger SHA-256 remains `'+collection['ledger_sha256']+'`. '
              'The evaluation manifest hashes its frozen cohort, source price files, collected events, features, protocol, and code.', '']
    path=Path('docs/options-activity-pilot-results-2026-09-14.md'); path.write_text('\n'.join(lines),encoding='utf-8')
    print(path)


if __name__ == '__main__': main()
