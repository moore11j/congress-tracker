"""Reviewed issuer associations, never price/share/ADR equivalence."""
from copy import deepcopy

from app.services.finnhub_research import FinnhubUnavailable

POLICY = 'reviewed_consolidated_issuer_v1'
ISSUER_FIELDS = frozenset({'current_ratio', 'debt_to_equity', 'roe', 'roic',
    'gross_margin', 'operating_margin', 'net_margin', 'revenue_growth', 'ebitda_growth'})
_BERKSHIRE = ('0001067983', [
    'https://www.sec.gov/Archives/edgar/data/1067983/000119312526083899/R1.htm'])
_REVIEWED = {
    ('ASML', 'ASML.AS'): ('0000937966', [
        'https://investor.asml.com/share-information',
        'https://www.sec.gov/Archives/edgar/data/937966/000162828026011378/asml-20251231.htm']),
    ('SAP', 'SAP.DE'): ('0001000184', [
        'https://www.sap.com/investors/en/stock/basic-data.html',
        'https://www.sec.gov/Archives/edgar/data/1000184/000110465926020058/sap-20251231x20f.htm']),
    ('NVS', 'NOVN.SW'): ('0001114448', [
        'https://www.novartis.com/investors/shareholder-information/frequently-asked-questions',
        'https://www.sec.gov/Archives/edgar/data/1114448/000111444826000004/nvs-20251231.htm']),
    ('FOX', 'FOXA'): ('0001754301', [
        'https://investor.foxcorporation.com/establishment-of-fox/',
        'https://www.sec.gov/Archives/edgar/data/1754301/000162828026053960/fox-20260630.htm']),
    # These are two classes of the same Formula One group, not permission to
    # transfer ratios from another Liberty tracking stock or spun-off company.
    ('FWONK', 'FWONA'): ('0001560385', [
        'https://www.libertymedia.com/investors/stock-data/faq',
        'https://www.sec.gov/Archives/edgar/data/1560385/000110465926091894/lmca-20260630x10q.htm']),
    ('GOOG', 'GOOGL'): ('0001652044', [
        'https://www.sec.gov/Archives/edgar/data/1652044/000165204426000071/goog-20260630.htm']),
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
