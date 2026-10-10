"""Derived bank revenue with aligned, same-filing standard SEC inputs."""
from app.services import sec_fundamentals as sec

BANK_SICS = {'6021', '6022', '6029', '6035', '6036'}
INTEREST = 'InterestIncomeExpenseNet'
FEES = 'NoninterestIncome'
TAG = 'walnut:NetInterestIncomePlusNoninterestIncome'
BASIS = 'Net interest income before credit-loss provisions plus non-interest income'


def apply(records, payload, company, *, available_by):
    # Never replace a reported total, bypass a conflicting total or combine a
    # banking denominator with an existing ordinary-revenue concept series.
    if records.get('revenue') or str((company or {}).get('sic', '')) not in BANK_SICS:
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
    return {**records, 'revenue': combined}


def basis(records):
    return BASIS if any(r and r.get('tag') == TAG for r in records.get('revenue', {}).values()) else None
