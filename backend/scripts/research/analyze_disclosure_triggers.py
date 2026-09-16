"""Fixed disclosure-triggered unusual-buy research. No application writes/imports."""
from bisect import bisect_left, bisect_right
from collections import Counter, defaultdict
from datetime import date, timedelta
import hashlib
import json
import math
from pathlib import Path
import random

from conviction_features import BASE, ConvictionFeatures

PROVIDERS = ['fmp:historical-price-eod/full+corporate_actions', 'massive:grouped-daily-adjusted']
PRICE_FILES = ['historical-panel.json', 'ledger-trailing-prices.json', 'recent-provider-prices.json', 'disclosure-trigger-prices.json']
LEDGER_HASH = 'de0885ffc1c1b026541b0cb364efd7c0b5dcf9feb62108d7d08473a49303613b'


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def cooldown(rows, days=90):
    """Sampling depends only on signals, even when an earlier label is missing."""
    last = {}; selected = []
    for r in sorted(rows, key=lambda r: (r['filing'], r['ticker'])):
        d = date.fromisoformat(r['filing'])
        if r['ticker'] in last and (d-last[r['ticker']]).days < days:
            continue
        selected.append(r); last[r['ticker']] = d
    return selected


def signal_cohorts(trades, seen):
    engine = ConvictionFeatures(trades); filings = defaultdict(list); audit = Counter()
    for r in trades:
        if r['source'] != 'insider' or r['side'] != 'buy':
            continue
        audit['purchase_groups'] += 1
        if r['unusual_ratio'] is None:
            audit['insufficient_prior_history_or_unknown_amount'] += 1
            continue
        filings[r['ticker'], r['filing']].append(r)
    groups = {'unusual': [], 'ordinary': []}
    for (ticker, filing), rr in sorted(filings.items()):
        asof = (date.fromisoformat(filing)+timedelta(days=1)).isoformat()
        net = engine.features(ticker, asof)['conviction:insider:90:net_lower_ratio']
        if not math.isfinite(net):
            audit['unknown_net_filing_groups'] += 1; continue
        if net <= 0:
            audit['nonpositive_net_filing_groups'] += 1; continue
        unusual = [r for r in rr if r['unusual_ratio'] >= 2]
        label = 'unusual' if unusual else 'ordinary'
        actors = unusual or rr
        groups[label].append({'ticker': ticker, 'filing': filing, 'available_date': asof,
            'previously_unexamined_security': ticker not in seen,
            'net_ratio': net, 'max_unusual_ratio': max(r['unusual_ratio'] for r in actors),
            'actors': sorted(set(r['actor'] for r in actors if r['actor'])),
            'trigger_buys': [{k: r[k] for k in ['actor', 'transaction', 'low', 'unusual_ratio', 'prior_purchase_count', 'role']} for r in actors]})
    cohorts = {}
    for group, rows in groups.items():
        ordered = sorted(rows, key=lambda r: (r['filing'], r['ticker']))
        cohorts[group] = {'all_filings': ordered, 'cooldown90': cooldown(ordered),
                         'first_per_ticker': cooldown(ordered, 1000000)}
    return cohorts, dict(audit)


def read_prices():
    result = {p: defaultdict(dict) for p in PROVIDERS}
    for name in PRICE_FILES:
        for r in json.loads((BASE/name).read_text())['rows']:
            if r[6] in result and date.fromisoformat(r[1]).weekday() < 5:
                if all(isinstance(r[i], (int, float)) and math.isfinite(r[i]) and r[i] > 0 for i in [2, 3, 4]):
                    result[r[6]][r[0]][r[1]] = r
    return result


def measure(signal, horizon, prices, sessions):
    filing = signal['filing']; i = bisect_right(sessions, filing)
    if i == len(sessions): return {'status': 'awaiting_entry'}
    entry = sessions[i]; ed = date.fromisoformat(entry)
    if (ed-date.fromisoformat(filing)).days > 4:
        return {'status': 'entry_calendar_gap'}
    target_day = (ed+timedelta(days=horizon)).isoformat()
    info = {'entry_date': entry, 'due_date': target_day}
    j = bisect_left(sessions, target_day)
    if j == len(sessions): return {**info, 'status': 'immature'}
    target = sessions[j]; info['target_date'] = target
    if (date.fromisoformat(target)-date.fromisoformat(target_day)).days > 4:
        return {**info, 'status': 'target_calendar_gap'}
    for provider in PROVIDERS:
        stock = prices[provider].get(signal['ticker'], {}); spy = prices[provider].get('SPY', {})
        if not all(d in book for book in [stock, spy] for d in [entry, target]): continue
        e = stock[entry]; b = spy[entry]
        ep = e[4]*e[2]/e[3]; bp = b[4]*b[2]/b[3]
        rr = 100*(stock[target][2]/ep-1); br = 100*(spy[target][2]/bp-1)
        excess = round(rr-br, 2)
        return {**info, 'status': 'measured', 'provider': provider, 'entry_price': ep,
                'exit_price': stock[target][2], 'raw_return': rr, 'spy_return': br,
                'excess_return': excess, 'correct': rr > 0 or excess > 0, 'raw_correct': rr > 0}
    return {**info, 'status': 'missing_consistent_prices'}


def wilson(wins, n):
    if not n: return None
    p = wins/n; z = 1.959963984540054; den = 1+z*z/n
    center = (p+z*z/(2*n))/den
    radius = z*math.sqrt(p*(1-p)/n+z*z/(4*n*n))/den
    return [100*(center-radius), 100*(center+radius)]


def stats(rows, horizon):
    h = str(horizon); measured = [r for r in rows if r['outcomes'][h]['status'] == 'measured']
    n = len(measured); wins = sum(r['outcomes'][h]['correct'] for r in measured)
    rawwins = sum(r['outcomes'][h]['raw_correct'] for r in measured)
    clusters = defaultdict(list)
    for r in measured: clusters[r['ticker']].append(r['outcomes'][h]['correct'])
    counts = [(sum(v), len(v)) for v in clusters.values()]
    bootstrap = None
    if len(counts) >= 2:
        rng = random.Random(90214); samples = []
        for _ in range(3000):
            draw = rng.choices(counts, k=len(counts))
            samples.append(100*sum(w for w, _ in draw)/sum(c for _, c in draw))
        samples.sort(); bootstrap = [samples[74], samples[2924]]
    return {'signals': len(rows), 'measured': n, 'correct': wins,
            'accuracy': 100*wins/n if n else None, 'raw_positive': rawwins,
            'raw_positive_accuracy': 100*rawwins/n if n else None,
            'ticker_count': len(clusters), 'actor_count': len({a for r in measured for a in r['actors']}),
            'entry_dates': len({r['outcomes'][h]['entry_date'] for r in measured}),
            'status_counts': dict(Counter(r['outcomes'][h]['status'] for r in rows)),
            'wilson95_descriptive': wilson(wins, n), 'ticker_cluster_bootstrap95': bootstrap}


def main():
    original = sha(BASE/'cohort.json')
    assert original == LEDGER_HASH, 'Frozen ledger differs; stop research.'
    trades = json.loads((BASE/'conviction-trades.json').read_text())['trades']
    ledger = json.loads((BASE/'cohort.json').read_text())['events']
    historical = json.loads((BASE/'historical-panel.json').read_text())
    seen = {r['ticker'] for r in ledger} | set(historical['symbols'])
    cohorts, audit = signal_cohorts(trades, seen)
    # Persist membership and exposure classification before calculating any labels.
    frozen = BASE/'disclosure-trigger-cohorts.json'
    frozen.write_text(json.dumps({'audit': audit, 'cohorts': cohorts}, indent=2))
    prices = read_prices(); sessions = sorted({d for p in prices.values() for d in p['SPY']})
    result = {'last_price_session': sessions[-1], 'audit': audit, 'cohorts': {}}
    for group, variants in cohorts.items():
        result['cohorts'][group] = {}
        for variant, rows in variants.items():
            labeled = [{**r, 'outcomes': {str(h): measure(r, h, prices, sessions) for h in [7, 30]}} for r in rows]
            result['cohorts'][group][variant] = {
                'summary': {str(h): stats(labeled, h) for h in [7, 30]},
                'by_year': {y: {str(h): stats([r for r in labeled if r['filing'][:4] == y], h) for h in [7, 30]} for y in sorted({r['filing'][:4] for r in labeled})},
                'previously_unexamined_security': {str(h): stats([r for r in labeled if r['previously_unexamined_security']], h) for h in [7, 30]},
                'events': labeled}
    result['manifest'] = {str(p): sha(p) for p in [BASE/'cohort.json', BASE/'conviction-trades.json', frozen, Path(__file__), Path('backend/scripts/research/conviction_features.py'), Path('docs/confirmation-disclosure-trigger-protocol-2026-09-14.md')] + [BASE/n for n in PRICE_FILES]}
    assert sha(BASE/'cohort.json') == original
    (BASE/'disclosure-trigger-results.json').write_text(json.dumps(result, indent=2, allow_nan=False))
    print(json.dumps({g: {v: {'summary': r['summary'], 'unexamined': r['previously_unexamined_security']} for v, r in vv.items()} for g, vv in result['cohorts'].items()}, indent=2))


if __name__ == '__main__':
    main()
