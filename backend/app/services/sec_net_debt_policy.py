"""One disclosed SEC leverage definition for the replacement financial feed.

Finance leases belong in debt; operating leases and issuer adjustments do not.
An absent disclosure never supplies a zero or proves a lease addition disjoint.
"""
from copy import deepcopy
import math

from app.services import sec_leverage

VERSION = 'sec_net_debt_finance_leases_v1'
FORMULA = '(debt including finance leases - cash and cash equivalents) / unadjusted trailing consolidated EBITDA'
NOTE = 'Net debt includes finance leases and excludes operating leases. EBITDA uses consolidated net income, tax, interest and depreciation/amortization, without issuer adjustments.'
DIRECT = ('current_plus_noncurrent_debt_including_finance_leases',
          'reported_debt_including_finance_leases')
ADDITIONS = {
    'current_debt_plus_noncurrent_borrowings_and_finance_leases': 'leverage_noncurrent_borrowings',
    'borrowings_plus_finance_leases': 'leverage_borrowings',
}


def select(panel, company):
    result = {'version': VERSION, 'status': 'unavailable', 'formula': FORMULA,
              'lease_policy': 'include_finance_exclude_operating', 'source': 'sec_edgar',
              'unit': 'multiple', 'period_date': panel.get('period_date')}
    def held(reason):
        return {**result, 'reason': reason}
    if panel.get('version') != sec_leverage.VERSION:
        return held('unverified_leverage_version')
    try:
        sic = int(company.get('sic', ''))
    except (ValueError, TypeError):
        return held('unverified_industry')
    if not 100 <= sic <= 8999:
        return held('unverified_industry')
    # Banking, securities, insurance and investment vehicles require a
    # separate industry policy. Do not use an industrial EBITDA default.
    if 6000 <= sic <= 6499 or 6700 <= sic <= 6799:
        return held('financial_industry_requires_separate_policy')
    ebitda = panel.get('ebitda') or {}
    if (panel.get('reporting_currency', 'USD') != 'USD' or ebitda.get('unit') != 'USD'):
        return held('unsupported_reporting_currency')
    if (ebitda.get('status') != 'components_complete' or
            not sec_leverage._finite(ebitda.get('value')) or ebitda['value'] <= 0):
        return held('incomplete_or_nonpositive_ebitda')
    candidates = []
    seen = set()
    for row in panel.get('candidates', []):
        basis = row.get('basis')
        if basis not in (*DIRECT, *ADDITIONS):
            continue
        if basis in seen:
            return held('duplicate_debt_representation')
        seen.add(basis)
        if row.get('status') != 'ratio_candidate':
            continue
        if basis in ADDITIONS:
            proof = panel.get('debt_addition_reconciliations', {}).get(ADDITIONS[basis], {})
            if proof.get('status') != 'reported_total_verified':
                continue
        if (row.get('period_date') != panel.get('period_date') or row.get('unit') != 'USD'
                or any(not sec_leverage._finite(row.get(k)) for k in
                       ('debt', 'cash', 'net_debt', 'net_debt_to_ebitda'))
                or min(row['debt'], row['cash']) < 0
                or not math.isclose(row['net_debt'], row['debt'] - row['cash'], rel_tol=1e-12, abs_tol=.01)
                or not math.isclose(row['net_debt_to_ebitda'], row['net_debt']/ebitda['value'], rel_tol=1e-12, abs_tol=1e-12)):
            return held('invalid_debt_calculation')
        candidates.append(row)
    if not candidates:
        return held('no_verified_finance_inclusive_debt')
    # Complete surviving alternatives must agree. A contradicted subtotal is
    # already held by the projector; it cannot erase an independent full sum.
    first = candidates[0]
    rounding = None
    if any(not math.isclose(row[k], first[k], rel_tol=1e-12, abs_tol=.01)
           for row in candidates[1:] for k in ('debt', 'cash', 'net_debt')):
        rounding = _original_rounding(panel, candidates)
        if rounding is None:
            return held('conflicting_complete_debt_representations')
    priority = {basis: i for i, basis in enumerate((*DIRECT, *ADDITIONS))}
    chosen = min(candidates, key=lambda row: priority[row['basis']])
    return {**result, 'status': 'available', 'value': chosen['net_debt_to_ebitda'],
            'selected_basis': chosen['basis'], 'basis': NOTE,
            'candidate': deepcopy(chosen), 'reconciled_bases': sorted(row['basis'] for row in candidates),
            'rounding_reconciliation': rounding,
            'ebitda': deepcopy(ebitda)}


def _original_rounding(panel, candidates):
    """Choose the precise disjoint sum only with the existing original proof."""
    from app.services.sec_debt_precision import reconcile, NAMES
    by_basis = {row['basis']: row for row in candidates}
    maturity, aggregate = (by_basis.get(name) for name in DIRECT)
    if not maturity or not aggregate:
        return None
    stored = panel.get('debt_precision_reconciliation') or {}
    points = stored.get('inputs', [])
    if len(points) != len(NAMES) or stored.get('components') != list(NAMES):
        return None
    proof = reconcile(dict(zip(NAMES, points)))
    if proof.get('status') != 'contained_under_round_to_nearest':
        return None
    # Bind the proof to the actual surviving candidates, not similar numbers
    # from another filing or a different debt subtotal.
    if (aggregate.get('debt_inputs') != [points[0]] or
            maturity.get('debt_inputs') != points[1:] or
            aggregate['cash_input'] != maturity['cash_input']):
        return None
    if any(not math.isclose(row[k], maturity[k], rel_tol=1e-12, abs_tol=.01)
           for row in candidates if row['basis'] != DIRECT[1]
           for k in ('debt', 'cash', 'net_debt')):
        return None
    return {**proof, 'canonical_selection': True,
            'selection': 'Use the more precise complete current plus noncurrent debt sum.',
            'limitation': 'Original precision supports this reconciliation under round-to-nearest; it does not authorize new lease additions.'}
