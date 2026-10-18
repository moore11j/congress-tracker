"""Derived bank revenue with aligned, same-filing standard SEC inputs."""
from datetime import date
from app.services import sec_fundamentals as sec

BANK_SICS = {'6021', '6022', '6029', '6035', '6036'}
INTEREST = 'InterestIncomeExpenseNet'
FEES = 'NoninterestIncome'
TAG = 'walnut:NetInterestIncomePlusNoninterestIncome'
CONTRACT_TAGS = frozenset({'RevenueFromContractWithCustomerIncludingAssessedTax',
    'RevenueFromContractWithCustomerExcludingAssessedTax'})
REVIEWED_SYMBOL = 'FUSB'
REVIEWED_CIK = '0000717806'
REVIEWED_VERSION = 'sec_fusb_bank_revenue_v1'
REVIEWED_START = date(2024, 1, 1)
REVIEWED_CONTRACT = 'RevenueFromContractWithCustomerIncludingAssessedTax'
REVIEW = {
    'basis': 'Contract revenue is deposit service charges, a subset of non-interest income',
    'sources': [
        {'url': 'https://www.sec.gov/Archives/edgar/data/717806/000119312526337583/fusb-20260630.htm',
         'sha256': '89ec0af760fa23e70097fb0188ec205ad49bae5cd75a8e0122c903f0cb22d0d8'},
        {'url': 'https://www.sec.gov/Archives/edgar/data/717806/000119312526103635/fusb-20251231.htm',
         'sha256': 'f886ae0c02db76dfbae8f85435c4ced55dc38ed6ed756268cc45478b045a6699'},
        {'url': 'https://www.sec.gov/Archives/edgar/data/717806/000095017025104822/fusb-20250630.htm',
         'sha256': '5c45be682ec466a8dafea11e40633c8de24515d9fee73dc4909891921b5eb8e7'},
    ],
}
BASIS = 'Net interest income before credit-loss provisions plus non-interest income'


def apply(records, payload, company, *, available_by):
    if str((company or {}).get('sic', '')) not in BANK_SICS:
        return records
    if str(payload.get('cik', '')).zfill(10) != str(company.get('cik', '')).zfill(10):
        raise sec.SecFundamentalsError('bank_company_identity')
    interest = sec._records(payload, 'revenue', available_by, concepts=(INTEREST,))
    fees = sec._records(payload, 'revenue', available_by, concepts=(FEES,))
    reviewed = (str(company.get('cik', '')).zfill(10) == REVIEWED_CIK
                and company.get('tickers', []).count(REVIEWED_SYMBOL) == 1)
    if (not interest or not fees) and not reviewed:
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
    if reviewed:
        return _reviewed_contract(records, payload, ordinary, combined, fees, available_by)
    if ordinary and all(row and row.get('tag') in {'Revenues', 'RevenuesNetOfInterestExpense'} for row in ordinary.values()):
        replacement = _obsolete_reported_total(ordinary, combined, company, available_by)
        return {**records, 'revenue': replacement} if replacement is not None else records
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


def _reviewed_contract(records, payload, ordinary, combined, fees, available_by):
    """Use one reviewed bank basis; never fall back to known fee-only revenue."""
    totals = sec._records(payload, 'revenue', available_by,
        concepts=tuple(tag for tag in sec.TAGS['revenue'] if tag not in CONTRACT_TAGS))
    if any(start >= REVIEWED_START for start, _ in totals):
        # An explicit total within the reviewed series (including conflicts)
        # needs no inference. A newer fee-only period must still block fallback.
        selected = {period: None for period in ordinary if period[0] >= REVIEWED_START}
        selected.update({period: row for period, row in totals.items()
                         if period[0] >= REVIEWED_START})
        return {**records, 'revenue': selected}
    selected = {}
    for period in combined.keys() | ordinary.keys():
        if period[0] < REVIEWED_START:
            continue
        selected[period] = None
        total, component, noninterest = combined.get(period), ordinary.get(period), fees.get(period)
        if (not total or not component or not noninterest
                or component.get('tag') != REVIEWED_CONTRACT
                or component['accession'] != total['accession']
                or component['filed'] != total['filed']
                or not 0 <= component['value'] <= noninterest['value']):
            continue
        selected[period] = {**total, 'revenue_selection': {
            'method': REVIEWED_VERSION, 'review': REVIEW,
            'superseded_component': component, 'basis_spliced': False}}
    return {**records, 'revenue': selected}


def _obsolete_reported_total(ordinary, combined, company, available_by):
    """Replace an abandoned total series with a wholly current bank basis.

    Never combine legacy totals with component periods or bypass a newer filing.
    This is the same disclosed net-interest bank definition, not an EBITDA proxy.
    """
    old_end = max(end for _, end in ordinary)
    end = max((end for _, end in combined), default=None)
    if (end is None or (available_by-old_end).days <= 380 or end <= old_end
            or not 0 <= (available_by-end).days <= 180):
        return None
    trailing = sec._ttm(combined, end)
    if trailing is None:
        return None
    recent = company.get('filings', {}).get('recent', {})
    filings = []
    for index, form in enumerate(recent.get('form', [])):
        if form not in {'10-K', '10-Q', '10-K/A', '10-Q/A', '10-KT', '10-QT', '10-KT/A', '10-QT/A', '20-F', '20-F/A', '40-F', '40-F/A'}:
            continue
        try:
            report = date.fromisoformat(recent['reportDate'][index])
            filed = date.fromisoformat(recent['filingDate'][index])
            accession = recent['accessionNumber'][index]
        except (ValueError, TypeError, KeyError, IndexError):
            return None
        if report <= filed <= available_by and accession:
            filings.append((report, filed, accession, form))
    if not filings:
        return None
    report, filed, accession, form = max(filings)
    if (form not in {'10-K', '10-Q', '10-K/A', '10-Q/A'} or report != end
            or len({(a, kind) for e, f, a, kind in filings if (e, f)==(report, filed)}) != 1):
        return None
    closing = [row for row in trailing['inputs'] if row['end'] == str(end)]
    if not closing or any((row['filed'], row['accession']) != (str(filed), accession) for row in closing):
        return None
    selection = {
        'method': 'complete_bank_basis_replaces_obsolete_reported_total_v1',
        'superseded_last_period': str(old_end),
        'superseded_concepts': sorted({row['tag'] for row in ordinary.values()}),
        'current_report': {'end': str(report), 'filed': str(filed), 'accession': accession, 'form': form},
        'basis_spliced': False,
    }
    return {key: {**row, 'revenue_selection': selection} if row else None
            for key, row in combined.items()}
