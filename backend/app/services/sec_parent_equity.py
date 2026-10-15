"""Parent equity from reviewed issuer components, never an inclusive-tag alias.

Every component is required. Both the reported equity total and the independent
assets-minus-liabilities balance must reconcile, with unchanged same-filing
observations. The policy is limited to the reviewed AMAL/FRST/FUSB layouts.
"""
from copy import deepcopy
from datetime import date
import math
from functools import lru_cache
import json
from pathlib import Path

VERSION = 'sec_parent_equity_components_v1'
PROFILES = {
    'AMAL': ('0001823608', 'AdditionalPaidInCapital', 'TreasuryStockCommonValue'),
    'FRST': ('0001325670', 'AdditionalPaidInCapital', 'TreasuryStockCommonValue'),
    'FUSB': ('0000717806', 'AdditionalPaidInCapitalCommonStock', 'TreasuryStockValue'),
}
BASIS = 'parent common stock plus paid-in capital plus retained earnings plus accumulated other comprehensive income minus treasury stock; reconciled to reported equity and assets less liabilities'
COMMON_TAGS = {
    'equity_common': ('CommonStockValue',),
    'equity_retained': ('RetainedEarningsAccumulatedDeficit',),
    'equity_aoci': ('AccumulatedOtherComprehensiveIncomeLossNetOfTax',),
    'equity_inclusive': ('StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest',),
    'equity_assets': ('Assets',),
    'equity_liabilities': ('Liabilities',),
}
GUARD_TAGS = ('MinorityInterest', 'PreferredStockValue', 'PreferredStockCarryingValue',
    'PreferredStockRedemptionValue', 'RedeemableNoncontrollingInterestEquityCarryingAmount',
    'RedeemableNoncontrollingInterestEquityFairValue', 'TemporaryEquityCarryingAmountAttributableToParent')
COEFFICIENTS = {'equity_common': 1, 'equity_paid_in': 1, 'equity_retained': 1,
                'equity_aoci': 1, 'equity_treasury': -1}


@lru_cache(maxsize=1)
def reviewed_sources():
    return json.loads(Path(__file__).with_name("sec_parent_equity_reviews.json").read_text(encoding="utf-8"))


def tags_for(symbol, cik):
    profile = PROFILES.get(symbol)
    if not profile or str(cik).zfill(10) != profile[0]:
        return {}
    return {**COMMON_TAGS, 'equity_paid_in': (profile[1],), 'equity_treasury': (profile[2],),
            **{'equity_guard_'+t: (t,) for t in GUARD_TAGS}}


def _finite(value):
    try:
        return type(value) in (int, float) and math.isfinite(value)
    except OverflowError:
        return False


def _align(point, anchor, payload):
    """A later identical comparative can confirm, never replace, a filing input."""
    identity = ('start', 'end', 'filed', 'accession', 'unit')
    if all(point.get(k) == anchor.get(k) for k in identity):
        return point
    if (any(point.get(k) != anchor.get(k) for k in ('start', 'end', 'unit'))
            or not point.get('filed') or point['filed'] <= anchor['filed']
            or point.get('source_kind') or point.get('derived')):
        return None
    rows = payload.get('facts', {}).get('us-gaap', {}).get(point['tag'], {}).get('units', {}).get('USD', [])
    matches = [r for r in rows if r.get('start') is None and r.get('end') == anchor['end']
               and r.get('filed') == anchor['filed'] and r.get('accn') == anchor['accession']
               and r.get('form') in {'10-K', '10-Q', '10-K/A', '10-Q/A'}]
    if not matches or any(not _finite(r.get('val')) or r['val'] != point['value'] for r in matches):
        return None
    return {**point, 'filed': anchor['filed'], 'accession': anchor['accession'],
            'later_identical_confirmation': deepcopy(point)}


def apply(records, *, symbol, cik, payload):
    tags = tags_for(symbol, cik)
    if not tags:
        return records, None
    cik = PROFILES[symbol][0]
    if str(payload.get('cik', '')).zfill(10) != cik:
        raise ValueError('equity_component_payload_identity')
    parent = dict(records.get('equity', {}))
    proof = {'methodology': VERSION, 'basis': BASIS, 'derived_periods': [], 'holds': [],
             'missing_components': 'Unavailable, never assumed zero.',
             'reported_parent_priority': True, 'reviewed_sources': deepcopy(reviewed_sources()[symbol])}
    for key, anchor in sorted(records.get('equity_common', {}).items()):
        start, end = key
        if start is not None or end < date(2024, 12, 31) or key in parent:
            continue
        reason = None
        inputs = {name: records.get(name, {}).get(key) for name in (*COEFFICIENTS,
            'equity_inclusive', 'equity_assets', 'equity_liabilities')}
        if not all(inputs.values()):
            reason = 'missing_or_conflicting_component'
        elif any(not _finite(p.get('value')) for p in inputs.values()):
            reason = 'nonfinite_component'
        elif (not anchor.get('filed') or not anchor.get('accession')
              or any(p.get('unit') != 'USD' or p.get('start') is not None
                     or p.get('end') != str(end) or p.get('source_cik', cik) != cik
                     or p.get('derived') or p.get('tag') not in tags[name]
                     for name, p in inputs.items())):
            reason = 'incompatible_components'
        else:
            inputs = {name: _align(p, anchor, payload) for name, p in inputs.items()}
            if not all(inputs.values()):
                reason = 'incompatible_filings_or_restatement'
        guards = {}
        if reason is None:
            for tag in GUARD_TAGS:
                rows = records.get('equity_guard_'+tag, {})
                if key not in rows:
                    continue
                p = rows[key]
                if (not p or not _finite(p.get('value')) or p['value'] != 0
                        or p.get('unit') != 'USD' or p.get('source_cik', cik) != cik
                        or p.get('derived') or p.get('tag') != tag or _align(p, anchor, payload) is None):
                    reason = 'changed_preferred_or_minority_structure'
                    break
                guards[tag] = deepcopy(p)
        if reason is None and any(inputs[n]['value'] < 0 for n in
                ('equity_common', 'equity_paid_in', 'equity_treasury', 'equity_assets', 'equity_liabilities')):
            reason = 'invalid_component_sign'
        if reason is None:
            value = sum(inputs[n]['value'] * factor for n, factor in COEFFICIENTS.items())
            balance = inputs['equity_assets']['value'] - inputs['equity_liabilities']['value']
            inclusive = inputs['equity_inclusive']['value']
            if not _finite(value) or not _finite(balance):
                reason = 'nonfinite_derivation'
            elif not (math.isclose(value, inclusive, rel_tol=1e-12, abs_tol=.01)
                      and math.isclose(value, balance, rel_tol=1e-12, abs_tol=.01)):
                reason = 'equity_or_balance_reconciliation_failed'
        if reason:
            proof['holds'].append({'end': str(end), 'reason': reason})
            continue
        parent[key] = {**anchor, 'tag': 'derived_parent_equity_components', 'value': value,
            'derived': True, 'derivation': VERSION, 'source_cik': cik, 'formula': BASIS,
            'inputs': deepcopy(inputs), 'allocation_guards': guards, 'balance_reconciliation': balance}
        proof['derived_periods'].append({'end': str(end), 'value': value,
            'filed': anchor['filed'], 'accession': anchor['accession']})
    return {**records, 'equity': parent}, proof
