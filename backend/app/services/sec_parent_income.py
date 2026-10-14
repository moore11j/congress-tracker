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
    for name in ('net_income',):
        if name in records:
            result[name] = {**derived, **records[name]}
    return result, {'methodology': VERSION, 'derived_periods': len(derived), 'holds': holds,
        'missing_noncontrolling_interest': 'unavailable, never assumed zero',
        'reported_parent_priority': True}


def is_derived(point):
    return bool(point and any(f.get('derivation') == VERSION for f in point.get('inputs', [])))


def consolidated_for_leverage(records):
    """Use consolidated profit for consolidated debt/expense calculations.

    A missing consolidated observation can be reconstructed only from explicit
    compatible parent and NCI amounts. A reported conflict is never filled.
    Parent earnings used for shareholder returns remain untouched.
    """
    result = dict(records)
    reported = records.get('leverage_net_income', {})
    parent = records.get('net_income', {})
    nci = records.get('parent_income_noncontrolling', {})
    redeemable = records.get('parent_income_redeemable', {})
    derived, holds = {}, {}
    for key in parent.keys() & nci.keys():
        if key in reported:
            continue
        income, minority = parent[key], nci[key]
        reason = None
        if not income or not minority:
            reason = 'conflicting_component'
        elif income.get('derived') or income.get('tag') != 'NetIncomeLoss':
            reason = 'reported_parent_required'
        elif any(income.get(k) != minority.get(k) for k in
                 ('start','end','filed','accession','unit','source_cik')):
            reason = 'incompatible_components'
        elif not income.get('start') or not income.get('end') or not income.get('accession') or not income.get('filed') or income.get('unit') != 'USD':
            reason = 'missing_identity'
        elif key in redeemable and (not redeemable[key] or redeemable[key]['value'] != 0 or any(
                redeemable[key].get(k) != income.get(k) for k in ('start','end','filed','accession','unit','source_cik'))):
            reason = 'ambiguous_redeemable_allocation'
        elif any(type(r.get('value')) not in (int,float) or not math.isfinite(r['value']) for r in (income,minority)):
            reason = 'nonfinite_component'
        else:
            value = income['value']+minority['value']
            if not math.isfinite(value):reason = 'nonfinite_calculation'
        if reason:
            holds[reason] = holds.get(reason,0)+1
            continue
        derived[key] = {**income,'tag':'derived_consolidated_net_income','value':value,
            'derived':True,'derivation':'sec_consolidated_income_reconciliation_v1',
            'formula':'reported parent net income plus reported income attributable to noncontrolling interests',
            'inputs':[income,minority]}
    result['leverage_net_income'] = {**derived, **reported}
    return result, {'methodology':'sec_consolidated_income_reconciliation_v1',
        'income_basis':'consolidated net income before allocation to noncontrolling interests',
        'derived_periods':len(derived),'holds':holds,'reported_consolidated_priority':True,
        'missing_noncontrolling_interest':'unavailable, never assumed zero'}
