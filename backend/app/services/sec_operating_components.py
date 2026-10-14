"""Reviewed HCA operating result, with independent pretax reconciliation.

The issuer reports individual costs, not an operating-income subtotal. This
calculation excludes affiliate earnings, asset-disposal gains and financing.
"""
from copy import deepcopy
from datetime import date
import math

VERSION = 'sec_hca_operating_components_v1'
SYMBOL = 'HCA'
CIK = '0000860730'
TAG = 'walnut:RevenueLessReviewedOperatingExpenses'
BASIS = 'Revenue less salaries, supplies, other operating expenses and depreciation/amortization; excludes affiliate earnings, asset-disposal gains and interest'
TAGS = {
    'hca_labor': ('LaborAndRelatedExpense',),
    'hca_supplies': ('SuppliesExpense',),
    'hca_other': ('OtherCostAndExpenseOperating',),
    'hca_da': ('DepreciationDepletionAndAmortization',),
    'hca_pretax': ('IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest',),
    'hca_interest': ('InterestExpense',),
    'hca_affiliates': ('IncomeLossFromEquityMethodInvestments',),
    'hca_disposals': ('GainLossOnDispositionOfAssets',),
}
PROOFS = [
    {'url':'https://www.sec.gov/Archives/edgar/data/860730/000119312526321077/hca-20260630.htm',
     'sha256':'71e56da9c844f15467842d30c40ebb7110a2f9733e05d78cd8a502825eb0ef5f'},
    {'url':'https://www.sec.gov/Archives/edgar/data/860730/000119312526044769/hca-20251231.htm',
     'sha256':'cc4733afa5ba5a7fa97b3faaddafee3e7c1d376bf482e49ecba6461a5e26ac34'},
]


def applies(symbol, cik):
    return symbol == SYMBOL and str(cik).zfill(10) == CIK


def _finite(value):
    try:
        return type(value) in (int,float) and math.isfinite(value)
    except OverflowError:
        return False


def apply(records, *, symbol, cik):
    if not applies(symbol, cik):
        return records, None
    proof = {'methodology':VERSION, 'basis':BASIS, 'reviewed_sources':deepcopy(PROOFS),
             'derived_periods':[], 'holds':[]}
    # Reported observations and reported conflicts take precedence. Do not
    # splice a derived series into a differently defined operating result.
    if records.get('operating_income'):
        proof['status'] = 'reported_series_retained'
        return records, proof
    derived = {}
    names = ('revenue', *TAGS)
    for period, revenue in sorted(records.get('revenue', {}).items()):
        start, end = period
        if start is None or start < date(2024,1,1):
            continue
        fields = {name: records.get(name, {}).get(period) for name in names}
        rows = list(fields.values())
        reason = None
        if not all(rows):
            reason = 'missing_or_conflicting_component'
        elif any(not _finite(row.get('value')) for row in rows):
            reason = 'nonfinite_component'
        elif (len({(r.get('start'),r.get('end'),r.get('filed'),r.get('accession'),r.get('unit')) for r in rows}) != 1
              or any(not r.get('filed') or not r.get('accession') for r in rows)
              or any(r.get('unit') != 'USD' or r.get('source_cik',CIK) != CIK for r in rows)
              or any(r.get('start') != str(start) or r.get('end') != str(end) for r in rows)):
            reason = 'incompatible_components'
        elif any(fields[name]['value'] < 0 for name in ('hca_labor','hca_supplies','hca_other','hca_da','hca_interest')):
            reason = 'negative_expense_component'
        if reason is None:
            value = revenue['value'] - sum(fields[n]['value'] for n in ('hca_labor','hca_supplies','hca_other','hca_da'))
            reconciled = fields['hca_pretax']['value'] + fields['hca_interest']['value'] - fields['hca_affiliates']['value'] - fields['hca_disposals']['value']
            if not _finite(value) or not _finite(reconciled):
                reason = 'nonfinite_derivation'
            elif abs(value-reconciled) > max(.01, max(abs(value),abs(reconciled))*1e-12):
                reason = 'pretax_reconciliation_failed'
        if reason:
            proof['holds'].append({'start':str(start),'end':str(end),'reason':reason})
            continue
        fact = {k:revenue[k] for k in ('start','end','filed','accession','unit')}
        fact.update(value=value, tag=TAG, priority=0, source_cik=CIK,
            source_kind='derived_sec_operating_components', derived=True, basis=BASIS,
            inputs=deepcopy(fields), reconciliation_value=reconciled)
        derived[period]=fact
        proof['derived_periods'].append({'start':str(start),'end':str(end),'value':value,'accession':fact['accession']})
    result = dict(records)
    result['operating_income']=derived
    proof['status']='derived' if derived else 'held'
    return result, proof
