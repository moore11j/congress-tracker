"""Keep reviewed discontinued-operation reporting scopes apart.

This supplies no amounts and never substitutes pro forma results. Unreviewed
later accessions may supply reported periods, but cannot bridge older scopes.
"""
from datetime import date

VERSION = 'sec_reporting_scope_v1'
CIK = '0001749723'
FILED = '2026-11-16'
ACCESSION = '0001749723-26-000185'
TAGS = frozenset({'Revenues', 'RevenueFromContractWithCustomerExcludingAssessedTax',
    'RevenueFromContractWithCustomerIncludingAssessedTax', 'SalesRevenueNet',
    'OperatingIncomeLoss', 'GrossProfit', 'CostOfRevenue'})
SOURCES = {
    'current': ('https://www.sec.gov/Archives/edgar/data/1749723/000174972326000185/nfe-20260930.htm',
        '2d7dbe195a9f816a05ce05f091dde73d2a345033ab05f576bfd173d3eb7cb3b2'),
    'annual': ('https://www.sec.gov/Archives/edgar/data/1749723/000174972326000032/nfe-20251231.htm',
        '9aeecfe78dc4cd7b7cb552bd1d18b04b04fc7337ecc7160cb427313d48fcd72b'),
    'separation': ('https://www.sec.gov/Archives/edgar/data/1749723/000174972326000124/nfe-20260911.htm',
        '3230795fd83943b3dfb0293a3453132efa021bcdd6840818279196caa515f6f6'),
}


def policy(cik, available_by):
    if str(cik).zfill(10) != CIK or available_by < date.fromisoformat(FILED):
        return None
    return {'methodology': VERSION, 'cik': CIK, 'effective_filed': FILED,
        'reviewed_accession': ACCESSION,
        'reason': 'discontinued_operations_reporting_scope_changed',
        'affected_concepts': sorted(TAGS),
        'sources': {key: {'url': value[0], 'sha256': value[1]} for key, value in SOURCES.items()},
        'rule': 'Derived periods and growth comparisons require one reporting scope; pro forma adjustments are not reported history.'}


def annotate(records, *, cik, available_by, namespace):
    if namespace != 'us-gaap' or policy(cik, available_by) is None:
        return records
    result = {}
    for key, point in records.items():
        if point and point.get('tag') in TAGS:
            if point['filed'] < FILED:
                scope = 'historical_including_brazil'
            elif point['filed'] == FILED and point['accession'] == ACCESSION:
                scope = 'reported_continuing_excluding_brazil'
            else:
                # No assumption that a later filing shares a reviewed scope.
                scope = 'unreviewed_accession:' + point['accession']
            point = {**point, 'reporting_scope': {'policy': VERSION, 'cik': CIK, 'scope': scope}}
        result[key] = point
    return result


def compatible(points):
    """Unannotated histories retain existing behavior; mixed scopes fail closed."""
    points = list(points)
    scopes = [point.get('reporting_scope') for point in points if point]
    if not any(scope is not None for scope in scopes):
        return True
    if len(scopes) != len(points) or any(not isinstance(s, dict) for s in scopes):
        return False
    return all(s == scopes[0] and set(s) == {'policy', 'cik', 'scope'}
        and s['policy'] == VERSION and s['cik'] == CIK
        and isinstance(s['scope'], str) and bool(s['scope']) for s in scopes)


def comparable(left, right):
    return bool(left and right and compatible([*left['inputs'], *right['inputs']]))
