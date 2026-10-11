"""Reviewed issuer associations, never price/share/ADR equivalence."""
from copy import deepcopy

from app.services.finnhub_research import FinnhubUnavailable

POLICY = 'reviewed_consolidated_issuer_v1'
ISSUER_FIELDS = frozenset({'current_ratio', 'debt_to_equity', 'roe', 'roic',
    'gross_margin', 'operating_margin', 'net_margin', 'revenue_growth', 'ebitda_growth'})
_BERKSHIRE = ('0001067983', [
    'https://www.sec.gov/Archives/edgar/data/1067983/000119312526083899/R1.htm'])
_REVIEWED = {
    ('BRK.B', 'BRK.A'): _BERKSHIRE,
    ('BRK-B', 'BRK.A'): _BERKSHIRE,
    ('TSM', '2330.TW'): ('0001046179', [
        'https://investor.tsmc.com/english/faq',
        'https://www.sec.gov/Archives/edgar/data/1046179/000162828026025362/tsm-20251231.htm']),
    ('CX', 'CEMEXCPO.MX'): ('0001076378', [
        'https://www.sec.gov/Archives/edgar/data/1076378/000119312525097693/R9.htm',
        'https://www.sec.gov/Archives/edgar/data/1076378/000119312526285670/d110687dex1.htm']),
}


def association(requested, reported):
    if not isinstance(requested, str) or not isinstance(reported, str):
        raise FinnhubUnavailable('metric_symbol_mismatch')
    record = _REVIEWED.get((requested, reported))
    if record is None:
        raise FinnhubUnavailable('metric_symbol_mismatch')
    cik, sources = record
    return {'policy': POLICY, 'requested_symbol': requested,
        'source_symbol': reported, 'cik': cik, 'sources': deepcopy(sources)}


def verify_combination(metrics, sec_evidence):
    identity = metrics.get('issuer_identity')
    if identity is None:
        return
    if not isinstance(identity, dict):
        raise FinnhubUnavailable('metric_issuer_identity_invalid')
    expected = association(metrics.get('symbol'), identity.get('source_symbol'))
    cik = str(sec_evidence.get('cik') or '')
    if identity != expected or not cik.isdigit() or cik.zfill(10) != expected['cik']:
        raise FinnhubUnavailable('metric_issuer_identity_invalid')
    if not set(metrics['values']) <= ISSUER_FIELDS:
        raise FinnhubUnavailable('metric_issuer_fields_invalid')
    for field in metrics['values']:
        evidence = metrics['fields'].get(field, {})
        if (evidence.get('source_symbol') != expected['source_symbol']
                or evidence.get('identity_basis') != 'consolidated_issuer'):
            raise FinnhubUnavailable('metric_issuer_fields_invalid')
