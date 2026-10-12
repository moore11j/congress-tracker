"""Period-aligned leverage candidates, never silently selected public ratios.

Debt policies differ between providers. Keep each complete reported basis apart
until coverage and the score/alert transition have been validated.
"""
from datetime import date
import math

VERSION = 'sec_leverage_evidence_v4'
DURATION = {
    'leverage_net_income': ('NetIncomeLoss',),
    'leverage_tax': ('IncomeTaxExpenseBenefit',),
    'leverage_interest': ('InterestExpense', 'InterestExpenseNonoperating'),
    'leverage_da': ('DepreciationDepletionAndAmortization', 'DepreciationAndAmortization'),
}
INSTANT = {
    'leverage_cash': ('CashAndCashEquivalentsAtCarryingValue',),
    'leverage_borrowings': ('DebtLongtermAndShorttermCombinedAmount',),
    'leverage_debt_with_finance_leases': ('DebtAndCapitalLeaseObligations',),
    'leverage_current_debt': ('DebtCurrent',),
    'leverage_noncurrent_debt_with_finance_leases': ('LongTermDebtAndCapitalLeaseObligations',),
    'leverage_noncurrent_borrowings': ('LongTermDebtNoncurrent',),
    'leverage_finance_leases_current': ('FinanceLeaseLiabilityCurrent',),
    'leverage_finance_leases_noncurrent': ('FinanceLeaseLiabilityNoncurrent',),
    'leverage_operating_leases_current': ('OperatingLeaseLiabilityCurrent',),
    'leverage_operating_leases_noncurrent': ('OperatingLeaseLiabilityNoncurrent',),
    'leverage_finance_leases': ('FinanceLeaseLiability',),
    'leverage_operating_leases': ('OperatingLeaseLiability',),
}
TAGS = {**DURATION, **INSTANT}

def _finite(value):
    try:
        return type(value) in (int, float) and math.isfinite(value)
    except OverflowError:
        return False

def _compatible(facts):
    # Combining stale accounting components with a later restatement is unsafe.
    return bool(facts) and all(facts) and len({
        (f['start'], f['end'], f['filed'], f['accession'], f['unit'], f.get('source_cik'))
        for f in facts
    }) == 1


def _matching_legs(components):
    legs = [{(f['start'], f['end']): f for f in point['inputs']} for point in components.values()]
    return (all(len(leg) == len(point['inputs']) for leg, point in zip(legs, components.values()))
        and all(set(leg) == set(legs[0]) for leg in legs[1:])
        and all(_compatible([leg[key] for leg in legs]) for key in legs[0]))

def _align_reported_ttm(records, components, end):
    """Accept a common decomposition only if it proves every selected TTM value."""
    from app.services import sec_fundamentals as sec
    anchors = {tuple(sorted((f['start'], f['end']) for f in point['inputs']))
        for point in components.values() if len(point['inputs']) > 1}
    accepted = []
    for anchor in sorted(anchors):
        keys = {(date.fromisoformat(start), date.fromisoformat(stop)) for start, stop in anchor}
        aligned = {}
        for name, primary in components.items():
            candidate = sec._ttm({key: value for key, value in records.get(name, {}).items() if key in keys}, end)
            if (candidate is None or candidate['value'] != primary['value']
                    or candidate['start'] != primary['start'] or candidate['end'] != primary['end']):
                break
            # A direct TTM observation cannot be reconciled using a different
            # closing filing, even when the numerical totals happen to match.
            if primary['method'] in {'reported_annual', 'derived_annual'}:
                closing = [f for f in candidate['inputs'] if f['end'] == str(end)]
                identity = ('end', 'filed', 'accession', 'unit', 'source_cik')
                if (len(primary['inputs']) != 1 or len(closing) != 1 or any(
                        primary['inputs'][0].get(key) != closing[0].get(key) for key in identity)):
                    break
            aligned[name] = candidate
        if len(aligned) == len(components) and _matching_legs(aligned):
            accepted.append(aligned)
    # Competing complete decompositions require explicit resolution.
    return accepted[0] if len(accepted) == 1 else None

def _ebitda(records, end, start):
    from app.services import sec_fundamentals as sec
    components = {name: sec._ttm(records.get(name, {}), end) for name in DURATION}
    missing = [name for name, point in components.items()
        if point is None or point['start'] != start or not _finite(point['value'])]
    result = {'period_start': start, 'period_end': str(end), 'unit': 'USD',
        'formula': 'net income + income tax expense/benefit + interest expense + depreciation and amortization',
        'components': components, 'missing_components': missing}
    if missing:
        return {**result, 'status': 'incomplete'}
    if not _matching_legs(components):
        aligned = _align_reported_ttm(records, components, end)
        if aligned is None:
            return {**result, 'status': 'incompatible_filing_components'}
        result['reported_ttm_reconciliation'] = {
            'method': 'identical_value_common_filing_decomposition_v1',
            'selected_reported_components': components,
            'requirement': 'exact values, identical full periods and closing filings; common filing for every derived leg'}
        components = aligned
        result['components'] = components
    # A negative income or tax benefit is legitimate; negative expense addbacks
    # require issuer-specific reconciliation and cannot make a generic metric.
    if any(f['value'] < 0 for name in ('leverage_interest', 'leverage_da')
           for f in components[name]['inputs']):
        return {**result, 'status': 'negative_expense_component'}
    value = sum(point['value'] for point in components.values())
    if not _finite(value):
        return {**result, 'status': 'nonfinite_calculation'}
    return {**result, 'status': 'components_complete', 'value': value}


def _lease_totals(instant):
    """Reconcile reported totals against complete, disjoint maturity components."""
    reconciliations = {}
    for name in ('leverage_finance_leases', 'leverage_operating_leases'):
        total = instant[name]
        parts = [instant[name+'_current'], instant[name+'_noncurrent']]
        if not all(parts):
            # A reported aggregate is independently sufficient; a lone maturity
            # component can never manufacture its missing counterpart as zero.
            continue
        if not _compatible(parts) or (total is not None and not _compatible([total, *parts])):
            reconciliations[name] = {'status': 'incompatible_lease_components', 'inputs': parts}
            instant[name] = None
            instant[name+'_current'] = instant[name+'_noncurrent'] = None
            continue
        if any(not _finite(f['value']) or f['value'] < 0 for f in parts):
            reconciliations[name] = {'status': 'invalid_lease_components', 'inputs': parts}
            instant[name] = None
            instant[name+'_current'] = instant[name+'_noncurrent'] = None
            continue
        value = sum(f['value'] for f in parts)
        if not _finite(value):
            reconciliations[name] = {'status': 'nonfinite_lease_total', 'inputs': parts}
            instant[name] = None
            instant[name+'_current'] = instant[name+'_noncurrent'] = None
            continue
        if total is not None:
            if not _finite(total['value']) or not math.isclose(total['value'], value, rel_tol=1e-12, abs_tol=.01):
                reconciliations[name] = {'status': 'conflicting_lease_total', 'reported': total, 'inputs': parts}
                instant[name] = None
                instant[name+'_current'] = instant[name+'_noncurrent'] = None
                continue
            reconciliations[name] = {'status': 'reported_total_verified', 'reported': total, 'inputs': parts}
        else:
            derived = {**parts[0], 'value': value, 'tag': name+'_current_plus_noncurrent',
                'formula': 'reported current plus reported noncurrent lease liability',
                'inputs': parts, 'derived': True}
            instant[name] = derived
            reconciliations[name] = {'status': 'complete_maturity_sum', 'derived': derived}
    return reconciliations


def _debt_bounds(instant):
    """Hold totals contradicted by same-filing components that they must include.

    Standard tags alone do not establish statement meaning. For example, ON's
    July 2026 filing tags a small guarantee as DebtAndCapitalLeaseObligations.
    A compatible noncurrent debt component disproves that apparent total.
    Bounds never reconstruct totals or assume that omitted components are zero.
    """
    included = {
        'leverage_borrowings': ('leverage_noncurrent_borrowings',),
        'leverage_debt_with_finance_leases': (
            'leverage_borrowings', 'leverage_current_debt',
            'leverage_noncurrent_debt_with_finance_leases',
            'leverage_noncurrent_borrowings', 'leverage_finance_leases'),
        'leverage_noncurrent_debt_with_finance_leases': (
            'leverage_noncurrent_borrowings', 'leverage_finance_leases_noncurrent'),
        'leverage_current_debt': ('leverage_finance_leases_current',),
    }
    holds = {}
    # Compare the original independent observations, not a partially masked set.
    for name, parts in included.items():
        total = instant.get(name)
        if total is None or not _finite(total['value']) or total['value'] < 0:
            continue
        contradictions = []
        for part in parts:
            component = instant.get(part)
            if (component is not None and _compatible([total, component])
                    and _finite(component['value']) and component['value'] >= 0
                    and component['value'] > total['value']
                    and not math.isclose(component['value'], total['value'], rel_tol=1e-12, abs_tol=.01)):
                contradictions.append({'component':part, 'reported':component})
        if contradictions:
            holds[name] = {'status':'aggregate_below_reported_component',
                'reported_aggregate':total, 'contradictions':contradictions,
                'requirement':'A same-filing reported total must include each compatible nonnegative component.'}
    for name in holds:
        instant[name] = None
    return holds


def project(records, *, end, start):
    """Return auditable candidates. Caller must not select a public metric here."""
    if not isinstance(end, date) or date.fromisoformat(start) > end:
        raise ValueError('invalid_leverage_period')
    ebitda = _ebitda(records, end, start)
    instant = {name: records.get(name, {}).get((None, end)) for name in INSTANT}
    reconciliations = _lease_totals(instant)
    debt_reconciliations = _debt_bounds(instant)
    candidates = []
    holds = {}

    def candidate(basis, names, definition):
        facts = [instant.get(name) for name in names]
        cash = instant['leverage_cash']
        missing = [name for name, fact in zip(names, facts) if fact is None]
        if cash is None:
            missing.append('leverage_cash')
        if any(name in debt_reconciliations for name in names):
            holds[basis] = {'reason':'conflicting_reported_debt_total',
                'components':[name for name in names if name in debt_reconciliations]}
            return
        if missing:
            holds[basis] = {'reason': 'missing_reported_components', 'components': missing}
            return
        if not _compatible([*facts, cash]):
            holds[basis] = {'reason': 'incompatible_balance_sheet_components'}
            return
        if any(not _finite(f['value']) or f['value'] < 0 for f in [*facts, cash]):
            holds[basis] = {'reason': 'invalid_balance_sheet_component'}
            return
        debt = sum(f['value'] for f in facts)
        net_debt = debt-cash['value']
        if not _finite(debt) or not _finite(net_debt):
            holds[basis] = {'reason': 'nonfinite_calculation'}
            return
        row = {'basis': basis, 'definition': definition, 'period_date': str(end),
            'debt': debt, 'cash': cash['value'], 'net_debt': net_debt, 'unit': 'USD',
            'debt_inputs': facts, 'cash_input': cash, 'status': 'debt_components_complete'}
        if ebitda.get('status') == 'components_complete' and ebitda['value'] > 0:
            closing = [f for f in ebitda['components']['leverage_net_income']['inputs']
                       if f['end'] == str(end)]
            keys = ('end', 'filed', 'accession', 'unit', 'source_cik')
            if len(closing) != 1 or any(closing[0].get(k) != cash.get(k) for k in keys):
                row['ratio_hold'] = 'income_balance_filing_mismatch'
            else:
                ratio = net_debt/ebitda['value']
                if _finite(ratio):
                    row.update(net_debt_to_ebitda=ratio, ratio_unit='multiple', status='ratio_candidate')
        candidates.append(row)

    candidate('reported_borrowings', ('leverage_borrowings',),
        'Reported aggregate short- and long-term borrowings less cash; no separately reported lease liabilities added.')
    # These are independent alternatives, never added together. One aggregate
    # already includes finance leases; the other adds a separately reported sum.
    candidate('reported_debt_including_finance_leases', ('leverage_debt_with_finance_leases',),
        'Reported debt and capital lease obligations less cash; excludes separately reported operating leases.')
    candidate('current_plus_noncurrent_debt_including_finance_leases',
        ('leverage_current_debt', 'leverage_noncurrent_debt_with_finance_leases'),
        'Current debt plus noncurrent debt and capital leases less cash; excludes separately reported operating leases.')
    # DebtCurrent already includes current capital lease obligations. Add only
    # noncurrent finance leases to the explicitly lease-exclusive noncurrent debt.
    candidate('current_debt_plus_noncurrent_borrowings_and_finance_leases',
        ('leverage_current_debt', 'leverage_noncurrent_borrowings', 'leverage_finance_leases_noncurrent'),
        'Current debt including current capital leases plus noncurrent borrowings and noncurrent finance leases less cash.')
    candidate('current_debt_plus_noncurrent_borrowings_finance_and_operating_leases',
        ('leverage_current_debt', 'leverage_noncurrent_borrowings', 'leverage_finance_leases_noncurrent', 'leverage_operating_leases'),
        'Current debt including current capital leases plus noncurrent borrowings, noncurrent finance leases and total operating leases less cash.')
    candidate('current_plus_noncurrent_debt_including_finance_and_operating_leases',
        ('leverage_current_debt', 'leverage_noncurrent_debt_with_finance_leases', 'leverage_operating_leases'),
        'Current debt plus noncurrent debt and capital leases plus operating lease liabilities less cash.')
    candidate('borrowings_plus_finance_leases', ('leverage_borrowings', 'leverage_finance_leases'),
        'Reported borrowings plus separately reported finance lease liabilities less cash; requires overlap review before selection.')
    candidate('reported_debt_plus_operating_leases',
        ('leverage_debt_with_finance_leases', 'leverage_operating_leases'),
        'Reported debt and capital leases plus operating lease liabilities less cash.')
    candidate('borrowings_plus_finance_and_operating_leases',
        ('leverage_borrowings', 'leverage_finance_leases', 'leverage_operating_leases'),
        'Reported borrowings plus finance and operating leases less cash; requires overlap review before selection.')
    return {'version': VERSION, 'status': 'preparation_only', 'period_date': str(end),
        'ebitda': ebitda, 'candidates': candidates, 'holds': holds, 'lease_reconciliations': reconciliations, 'debt_reconciliations': debt_reconciliations,
        'limitations': ['No candidate is selected as the canonical net-debt/EBITDA field.',
            'Missing debt, lease, interest or D&A components are not assumed zero.',
            'Issuer-adjusted EBITDA and D&A including other items are not substituted.',
            'Parent/consolidated income and lease overlap require issuer reconciliation.',
            'Negative or zero EBITDA cannot produce a leverage ratio.']}
