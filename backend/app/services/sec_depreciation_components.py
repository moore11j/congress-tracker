"""Reviewed, filing-bound depreciation plus intangible amortization evidence."""
from copy import deepcopy
from datetime import date
import math

VERSION = 'sec_msft_da_evidence_v1'
SYMBOL = 'MSFT'
CIK = '0000789019'
ACCESSION = '0001193125-26-323660'
PROOF_URL = 'https://www.sec.gov/Archives/edgar/data/789019/000119312526323660/msft-20260630.htm'
PROOF_SHA256 = 'f5248cff45f5296701cd5b77065ca2f85598bcbc6eaf13883823dcc9bc6e88ec'
TAGS = {'da_depreciation': ('Depreciation',),
        'da_intangible_amortization': ('AmortizationOfIntangibleAssets',),
        'da_finance_lease_amortization': ('FinanceLeaseRightOfUseAssetAmortization',),
        'da_cash_flow_adjustment': ('DepreciationAmortizationAndAccretionNet',)}
PERIODS = {(date(year-1, 7, 1), date(year, 6, 30)) for year in (2024, 2025, 2026)}


def applies(symbol, cik):
    return symbol == SYMBOL and str(cik).zfill(10) == CIK


def reconcile(records, *, symbol, cik):
    """Keep a component sum separate until all expense overlap is reconciled.

    No record is inserted into leverage_da and no EBITDA or ratio is produced.
    Finance-lease amortization is retained separately, never assumed additive.
    """
    if not applies(symbol, cik):
        return None
    reported = records.get('leverage_da', {})
    derived = []
    holds = {}
    for key in sorted(PERIODS):
        parts = [records.get(name, {}).get(key) for name in ('da_depreciation', 'da_intangible_amortization')]
        reason = None
        if not all(parts):
            reason = 'missing_or_conflicting_component'
        elif any(p.get('accession') != ACCESSION or p.get('filed') != '2026-07-29' or p.get('unit') != 'USD'
                 or p.get('source_cik') not in (None, CIK)
                 or (p.get('start'), p.get('end')) != tuple(map(str, key)) for p in parts):
            reason = 'unreviewed_component_identity'
        elif any(parts[0].get(k) != parts[1].get(k)
                 for k in ('start', 'end', 'filed', 'accession', 'unit', 'source_cik')):
            reason = 'incompatible_components'
        elif any(type(p.get('value')) not in (int, float)
                 or not math.isfinite(p['value']) or p['value'] < 0 for p in parts):
            reason = 'invalid_component'
        if reason:
            holds[str(key[1])] = reason
            continue
        value = sum(p['value'] for p in parts)
        if not math.isfinite(value):
            holds[str(key[1])] = 'nonfinite_sum'
            continue
        fact = {**parts[0], 'value': value, 'tag': 'derived_depreciation_plus_intangible_amortization',
                'derived': True, 'derivation': VERSION,
                'formula': 'reported depreciation expense plus reported intangible asset amortization expense',
                'inputs': deepcopy(parts), 'proof_url': PROOF_URL, 'proof_sha256': PROOF_SHA256}
        supplemental = {}
        for name in ('da_finance_lease_amortization', 'da_cash_flow_adjustment'):
            point = records.get(name, {}).get(key)
            identity = ('start', 'end', 'filed', 'accession', 'unit', 'source_cik')
            if (point and all(point.get(k) == parts[0].get(k) for k in identity)
                    and type(point.get('value')) in (int, float)
                    and math.isfinite(point['value']) and point['value'] >= 0):
                supplemental[name] = deepcopy(point)
        derived.append({'period_end': str(key[1]), 'component_sum': fact,
                        'reported_supplements': supplemental,
                        'reported_total': deepcopy(reported.get(key)),
                        'reported_total_conflicted': key in reported and reported[key] is None})
    return {'methodology': VERSION, 'cik': CIK, 'accession': ACCESSION,
        'proof_url': PROOF_URL, 'proof_sha256': PROOF_SHA256,
        'status': 'reconciliation_required', 'periods': derived, 'holds': holds,
        'basis': 'Depreciation plus intangible asset amortization as separately disclosed in Notes 6 and 9.',
        'limitations': ['Component sum is not a verified aggregate D&A or EBITDA input.',
            'Finance-lease amortization and capitalized software overlap remain unresolved; missing costs are not zero.',
            'Cash-flow adjustment includes other items and is not substituted.',
            'No public financial value or leverage candidate changes. Future filings require review.']}
