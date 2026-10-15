"""Reviewed Vale USD IFRS basis; combined intangible/PPE cash is not PPE-only."""
from copy import deepcopy
import re

VERSION = 'sec_vale_ifrs_interim_v1'
SYMBOL = 'VALE'
CIK = '0000917851'
TAGS = {
    'revenue': ('Revenue',),
    'gross_profit': ('GrossProfit',),
    'operating_income': ('ProfitLossFromOperatingActivities',),
    'net_income': ('ProfitLossAttributableToOwnersOfParent',),
    'equity': ('EquityAttributableToOwnersOfParent',),
    'current_assets': ('CurrentAssets',),
    'current_liabilities': ('CurrentLiabilities',),
    # Latest net CFO uses a custom concept, and capital expenditure combines
    # PPE with intangible purchases. Neither maps to the existing FCF contract.
    'operating_cash_flow': (),
    'capex': (),
}
REVIEW = {
    'symbol': SYMBOL, 'cik': CIK, 'namespace': 'ifrs-full', 'currency': 'USD',
    'amount_basis': 'USD financial statements used for ratios; no cash amounts, FX conversion or ADR per-share inference.',
    'revenue_basis': 'reported consolidated net operating revenue',
    'margin_basis': 'Margin denominators use reported consolidated net operating revenue.',
    'income_basis': 'reported profit attributable to owners of the parent',
    'equity_basis': 'reported equity attributable to owners of the parent',
    'cash_flow_basis': 'Free cash flow remains unavailable: reported acquisition cash combines property, plant and equipment with intangible assets; cash generated from operations is before additional operating payments and is not net operating cash flow.',
    'reviewed_on': '2026-10-15',
    'source_url': 'https://www.sec.gov/Archives/edgar/data/917851/000129281426004322/valedfifrs2q26_6ka.htm',
    'source_sha256': '025a02d00b1b45cbc5232d2e372c3c7a1a25ef652eda589f523abbe2f05a6b32',
    'annual_url': 'https://www.sec.gov/Archives/edgar/data/917851/000129281426001844/valeform20f_2025.htm',
    'annual_sha256': '8612cc1e0754469208f8cded5b8c8bc2d6d40e00bd499551982736815750c467',
}


def basis(cik):
    if str(cik).zfill(10) != CIK:
        raise ValueError('reviewed_vale_cik_mismatch')
    return {'namespace': 'ifrs-full', 'currency': 'USD', 'tags': dict(TAGS), 'review': deepcopy(REVIEW)}


def periodic_document(form, report, filename):
    if not re.fullmatch(r'[0-9]{4}-(?:03-31|06-30|09-30|12-31)', report):
        return False
    year = report[:4]
    if form in {'20-F', '20-F/A'}:
        return report.endswith('-12-31') and filename == 'valeform20f_'+year+'.htm'
    if form not in {'6-K', '6-K/A'}:
        return False
    quarter = {'03': '1', '06': '2', '09': '3', '12': '4'}[report[5:7]]
    suffix = '_6ka.htm' if form == '6-K/A' else '_6k.htm'
    # Exclude event-only and BR GAAP submissions even when their directory
    # reportDate happens to equal a quarter end.
    return filename == 'valedfifrs'+quarter+'q'+year[2:]+suffix
