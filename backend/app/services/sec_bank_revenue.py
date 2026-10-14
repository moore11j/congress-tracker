"""Derived bank revenue with aligned, same-filing standard SEC inputs."""
from app.services import sec_fundamentals as sec

BANK_SICS = {'6021', '6022', '6029', '6035', '6036'}
INTEREST = 'InterestIncomeExpenseNet'
FEES = 'NoninterestIncome'
TAG = 'walnut:NetInterestIncomePlusNoninterestIncome'
CONTRACT_TAGS = frozenset({'RevenueFromContractWithCustomerIncludingAssessedTax',
    'RevenueFromContractWithCustomerExcludingAssessedTax'})
BASIS = 'Net interest income before credit-loss provisions plus non-interest income'


def apply(records, payload, company, *, available_by):
    if str((company or {}).get('sic', '')) not in BANK_SICS:
        return records
    if str(payload.get('cik', '')).zfill(10) != str(company.get('cik', '')).zfill(10):
        raise sec.SecFundamentalsError('bank_company_identity')
    interest = sec._records(payload, 'revenue', available_by, concepts=(INTEREST,))
    fees = sec._records(payload, 'revenue', available_by, concepts=(FEES,))
    if not interest or not fees:
        return records
    combined = {}
    for period in interest.keys() | fees.keys():
        left, right = interest.get(period), fees.get(period)
        combined[period] = None
        if (not left or not right or left['accession'] != right['accession']
                or left['filed'] != right['filed']):
            continue
        combined[period] = {
            'value': left['value'] + right['value'], 'tag': TAG, 'unit': 'USD',
            'start': left['start'], 'end': left['end'], 'filed': left['filed'],
            'accession': left['accession'], 'priority': 0,
            'source_kind': 'derived_sec_bank_revenue', 'source_cik': str(payload['cik']).zfill(10),
            'method': 'net_interest_income_plus_noninterest_income', 'basis': BASIS,
            'inputs': [left, right],
        }
    ordinary = records.get('revenue', {})
    if ordinary:
        # A bank's discontinued contract-revenue component must not block its
        # current complete banking denominator. Never splice those two bases.
        if any(not row or row.get('tag') not in CONTRACT_TAGS for row in ordinary.values()):
            return records
        totals = sec._records(payload, 'revenue', available_by,
            concepts=tuple(tag for tag in sec.TAGS['revenue'] if tag not in CONTRACT_TAGS))
        if totals:
            return records
        old_end = max(end for _, end in ordinary)
        end = max(combined, key=lambda key: key[1])[1] if combined else None
        if (end is None or (available_by-old_end).days <= 380
                or end <= old_end or not 0 <= (available_by-end).days <= 180
                or sec._ttm(combined, end) is None):
            return records
        selection = {'method': 'complete_bank_basis_replaces_obsolete_contract_component_v1',
            'superseded_last_period': str(old_end),
            'superseded_concepts': sorted({row['tag'] for row in ordinary.values()}),
            'basis_spliced': False}
        combined = {key: {**row, 'revenue_selection': selection} if row else None
                    for key, row in combined.items()}
    return {**records, 'revenue': combined}


def basis(records):
    return BASIS if any(r and r.get('tag') == TAG for r in records.get('revenue', {}).values()) else None
