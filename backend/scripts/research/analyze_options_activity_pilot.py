"""Dated, unsigned activity features and fixed gates against immutable outcomes."""
from bisect import bisect_right
from collections import Counter
from datetime import datetime, timezone
import json
import math
from pathlib import Path
from statistics import mean

from conviction_features import load_prices
from options_activity_pilot import BASE, ROOT, LEDGER_HASH, digest, ny_timezone, save


def valid_number(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def dated_bars(raw, cutoff):
    rows = {}; discarded_future = 0
    for r in raw:
        if not valid_number(r.get('t')): return {}, {'error': 'invalid_timestamp'}
        day = datetime.fromtimestamp(r['t']/1000, timezone.utc).astimezone(ny_timezone()).date().isoformat()
        if day > cutoff: discarded_future += 1; continue
        if not valid_number(r.get('v')) or r['v'] < 0: return {}, {'error': 'invalid_volume'}
        if r['v'] > 0 and (not valid_number(r.get('vw')) or r['vw'] <= 0):
            return {}, {'error': 'missing_vwap'}
        if day in rows and rows[day] != r: return {}, {'error': 'conflicting_duplicate_day'}
        rows[day] = r
    return rows, {'discarded_future_bars': discarded_future}


def activity_features(pair, cutoff, sessions):
    if not pair or len(pair) != 2: return {'status': 'missing_pair'}
    if any(r.get('fetch_complete') is not True for r in pair): return {'status': 'incomplete_fetch'}
    sides = {r['contract']['contract_type']: r for r in pair}
    if set(sides) != {'call', 'put'}: return {'status': 'unmatched_sides'}
    cs = [r['contract'] for r in pair]
    if any(c.get('shares_per_contract') != 100 or c.get('additional_underlyings') for c in cs):
        return {'status': 'nonstandard_contract'}
    if len({(c['strike_price'], c['expiration_date']) for c in cs}) != 1:
        return {'status': 'unmatched_contracts'}
    days = sessions[:bisect_right(sessions, cutoff)][-25:]
    if len(days) != 25 or days[-1] != cutoff: return {'status': 'insufficient_calendar'}
    volumes = {}; premiums = {}; first_days = {}; audits = {}
    for side, record in sides.items():
        bars, audit = dated_bars(record['bars'], cutoff); audits[side] = audit
        if audit.get('error'): return {'status': audit['error'], 'audit': audits}
        if not bars: return {'status': 'no_observed_trades', 'audit': audits}
        first_days[side] = min(bars)
        # Without a prior observation, absent bars may reflect a not-yet-listed contract.
        if first_days[side] > days[0]:
            return {'status': 'insufficient_contract_history', 'first_observed': first_days,
                    'required_first_session': days[0]}
        volumes[side] = [bars.get(d, {}).get('v', 0) for d in days]
        premiums[side] = [bars[d]['v']*bars[d].get('vw', 0)*100 if d in bars else 0 for d in days]
    recent_v = {s: sum(v[-5:]) for s, v in volumes.items()}; base_v = {s: sum(v[:20]) for s, v in volumes.items()}
    recent_p = {s: sum(v[-5:]) for s, v in premiums.items()}; base_p = {s: sum(v[:20]) for s, v in premiums.items()}
    total_recent = sum(recent_p.values()); total_base = sum(base_p.values())
    if not total_base: return {'status': 'zero_baseline_premium'}
    recent_share = recent_p['call']/total_recent if total_recent else None
    base_share = base_p['call']/total_base
    return {'status': 'ok', 'window_start': days[0], 'cutoff': cutoff, 'recent_sessions': 5, 'baseline_sessions': 20,
            'sample_contracts': {s: r['contract']['ticker'] for s, r in sides.items()},
            'expiry': cs[0]['expiration_date'], 'strike': cs[0]['strike_price'],
            'recent_call_volume': recent_v['call'], 'recent_put_volume': recent_v['put'],
            'recent_call_volume_share': recent_v['call']/sum(recent_v.values()) if sum(recent_v.values()) else None,
            'baseline_call_volume_share': base_v['call']/sum(base_v.values()) if sum(base_v.values()) else None,
            'volume_activity_ratio': (sum(recent_v.values())/5)/(sum(base_v.values())/20) if sum(base_v.values()) else None,
            'recent_call_premium': recent_p['call'], 'recent_put_premium': recent_p['put'],
            'baseline_call_premium': base_p['call'], 'baseline_put_premium': base_p['put'],
            'premium_activity_ratio': (total_recent/5)/(total_base/20),
            'recent_call_premium_share': recent_share, 'baseline_call_premium_share': base_share,
            'call_premium_share_change': recent_share-base_share if recent_share is not None else None,
            'audit': audits, 'semantic_type': 'unsigned_gross_activity_sample'}


def fixed_gate(features):
    if features.get('status') != 'ok': return None
    return (features['premium_activity_ratio'] >= 2 and
            (features['recent_call_premium_share'] or 0) >= .60 and
            features['call_premium_share_change'] is not None and features['call_premium_share_change'] >= .10)


def market_context(ticker, cutoff, prices):
    days = sorted(d for d in prices['SPY'] if d <= cutoff)[-20:]
    if len(days) != 20 or days[-1] != cutoff: return {'status': 'missing_market_history'}
    result = {'status': 'ok'}
    for name, symbol in [('stock', ticker), ('market', 'SPY')]:
        if any(d not in prices.get(symbol, {}) for d in days): return {'status': 'missing_stock_history'}
        close = [prices[symbol][d][2] for d in days]
        if any(not valid_number(c) or c <= 0 for c in close): return {'status': 'invalid_stock_price'}
        if any(abs(close[i]/close[i-1]-1) >= .5 for i in range(1, len(close))): return {'status': 'price_basis_discontinuity'}
        result[name+'_above_sma20'] = close[-1] > mean(close)
        result[name+'_distance_sma20'] = close[-1]/mean(close)-1
    return result


def summarize(rows, horizon, policy):
    measured = [r for r in rows if horizon in r['outcomes']]
    retained = [r for r in measured if r['retained'][policy]]
    n = len(retained); correct = sum(r['outcomes'][horizon]['correct'] for r in retained)
    raw_wins = sum(r['outcomes'][horizon].get('raw_correct',
                   r['outcomes'][horizon]['raw_return'] > 0 if r['direction']=='bullish'
                   else round(-r['outcomes'][horizon]['raw_return'], 2) > 0) for r in retained)
    def directional(r, field):
        return (1 if r['direction'] == 'bullish' else -1)*r['outcomes'][horizon][field]
    return {'eligible_measured': len(measured), 'retained': n, 'correct': correct,
            'accuracy': 100*correct/n if n else None, 'retained_percent': 100*n/len(measured) if measured else None,
            'raw_directional_correct': raw_wins, 'raw_directional_accuracy': 100*raw_wins/n if n else None,
            'average_directional_return': mean(directional(r, 'raw_return') for r in retained) if n else None,
            'average_directional_excess': mean((1 if r['direction']=='bullish' else -1)*round(r['outcomes'][horizon]['raw_return']-r['outcomes'][horizon]['spy_return'], 2) for r in retained) if n else None,
            'unknown_options_fallback': sum(r['options_gate'] is None for r in retained),
            'bullish_retained': sum(r['direction']=='bullish' for r in retained),
            'bearish_retained': sum(r['direction']=='bearish' for r in retained),
            'opening_dates': len({r['entry_date'] for r in retained})}


def main():
    assert digest(BASE/'cohort.json') == LEDGER_HASH
    config = json.loads((ROOT/'cohort.json').read_text()); prices = load_prices(); sessions = sorted(prices['SPY'])
    missing = [e['ticker'] for e in config['events'] if not (ROOT/'events'/(e['ticker']+'.json')).exists()]
    if missing:
        raise SystemExit('Collection incomplete; no comparison published. Missing: '+', '.join(missing))
    feature_rows = []
    for event in config['events']:
        path = ROOT/'events'/(event['ticker']+'.json')
        data = json.loads(path.read_text()) if path.exists() else {}
        if data and data['event'] != event: raise ValueError('Event identity mismatch')
        features = {name: activity_features(data.get('buckets', {}).get(name), event['cutoff'], sessions) for name in ['1_7', '8_29', '30_90']}
        f = features['30_90']; context = market_context(event['ticker'], event['cutoff'], prices)
        gate = fixed_gate(f)
        trend = (context['stock_above_sma20'] and context['market_above_sma20']) if context['status']=='ok' else None
        combined = (gate and trend) if trend is not None and gate is not None else None
        feature_rows.append({'id': event['id'], 'ticker': event['ticker'], 'cutoff': event['cutoff'],
                             'features': features, 'context': context, 'options_gate': gate, 'trend_gate': trend, 'combined_gate': combined})
    # Persist inputs and fixed decisions before joining any historical outcome.
    save(ROOT/'features.json', {'events': feature_rows, 'cohort_sha256': digest(ROOT/'cohort.json')})
    originals = {r['id']: r for r in json.loads((BASE/'cohort.json').read_text())['events']}
    rows = []
    for f in feature_rows:
        o = originals[f['id']]; bullish = o['direction'] == 'bullish'
        rows.append({**f, 'direction': o['direction'], 'entry_date': o['entry_date'], 'outcomes': o['outcomes'],
                     'retained': {'original': True, 'options': not bullish or f['options_gate'] is not False,
                                  'trend_only': not bullish or f['trend_gate'] is not False,
                                  'options_and_trend': not bullish or f['combined_gate'] is not False}})
    policies = ['original', 'trend_only', 'options', 'options_and_trend']
    result = {'stage': 'engineering pilot, not validated model', 'events': rows,
              'coverage': {b: dict(Counter(r['features'][b]['status'] for r in rows)) for b in ['1_7','8_29','30_90']},
              'all_pilot': {h: {p: summarize(rows, h, p) for p in policies} for h in ['30','7']},
              'covered_options_only': {h: {p: summarize([r for r in rows if r['options_gate'] is not None], h, p) for p in policies} for h in ['30','7']},
              'manifest': {str(p): digest(p) for p in [BASE/'cohort.json', ROOT/'cohort.json', ROOT/'features.json', Path(__file__), Path('backend/scripts/research/options_activity_pilot.py'), Path('docs/options-activity-pilot-protocol-2026-09-14.md')]
                           + sorted((ROOT/'events').glob('*.json'))
                           + [BASE/n for n in ['historical-panel.json','ledger-trailing-prices.json','recent-provider-prices.json']]}}
    save(ROOT/'evaluation.json', result)
    assert digest(BASE/'cohort.json') == LEDGER_HASH
    print(json.dumps({k: result[k] for k in ['stage', 'coverage', 'all_pilot', 'covered_options_only']}, indent=2))


if __name__ == '__main__': main()
