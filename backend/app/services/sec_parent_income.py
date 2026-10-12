"""Parent income from explicitly reported consolidated profit and NCI income."""
import math

VERSION = 'sec_parent_income_reconciliation_v1'
TAGS = {'parent_income_consolidated': ('ProfitLoss',),
        'parent_income_noncontrolling': ('NetIncomeLossAttributableToNoncontrollingInterest',),
        'parent_income_redeemable': ('NetIncomeLossAttributableToRedeemableNoncontrollingInterest',)}


def apply(records):
    result = dict(records)
    reported = records.get('net_income', {})
    consolidated = records.get('parent_income_consolidated', {})
    noncontrolling = records.get('parent_income_noncontrolling', {})
    derived = {}
    holds = {}
    for key in consolidated.keys() & noncontrolling.keys():
        # An explicit conflict in an existing parent observation stays held.
        if key in reported:
            continue
        profit, nci = consolidated[key], noncontrolling[key]
        if not profit or not nci:
            holds['conflicting_component'] = holds.get('conflicting_component', 0)+1
            continue
        identity = ('start', 'end', 'filed', 'accession', 'unit', 'source_cik')
        if any(profit.get(k) != nci.get(k) for k in identity):
            holds['incompatible_components'] = holds.get('incompatible_components', 0)+1
            continue
        redeemable_rows = records.get('parent_income_redeemable', {})
        redeemable = redeemable_rows.get(key)
        if key in redeemable_rows and (not redeemable or redeemable['value'] != 0):
            holds['ambiguous_redeemable_allocation'] = holds.get('ambiguous_redeemable_allocation', 0)+1
            continue
        values = (profit['value'], nci['value'])
        if any(type(v) not in (int, float) or not math.isfinite(v) for v in values):
            continue
        value = values[0]-values[1]
        if not math.isfinite(value):
            continue
        derived[key] = {**profit, 'tag': 'derived_parent_net_income', 'value': value,
            'derived': True, 'derivation': VERSION,
            'formula': 'reported consolidated profit/loss minus reported income/loss attributable to noncontrolling interests',
            'inputs': [profit, nci]}
    for name in ('net_income', 'leverage_net_income'):
        if name in records:
            result[name] = {**derived, **records[name]}
    return result, {'methodology': VERSION, 'derived_periods': len(derived), 'holds': holds,
        'missing_noncontrolling_interest': 'unavailable, never assumed zero',
        'reported_parent_priority': True}


def is_derived(point):
    return bool(point and any(f.get('derivation') == VERSION for f in point.get('inputs', [])))
