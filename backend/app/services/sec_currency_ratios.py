"""Reviewed issuer currency and accounting bases for fundamentals.

Amounts remain in the reported currency. This grants no FX conversion,
per-share mapping, or permission to combine them with USD market values.
"""
VERSION = 'sec_cad_ratios_v1'
SYMBOL = 'ENB'
CIK = '0000895728'
REVIEW = {
    'symbol': SYMBOL, 'cik': CIK, 'namespace': 'us-gaap', 'currency': 'CAD',
    'revenue_concept': 'Revenues',
    'income_basis': 'reported income attributable to controlling interests',
    'equity_basis': 'reported parent shareholders equity including preference shares',
    'reviewed_on': '2026-10-14',
    'source_url': 'https://www.sec.gov/Archives/edgar/data/895728/000119312526326752/enb-20260630.htm',
    'source_sha256': 'b187cf2fa5591d4801a01bd5807c2deb57872e536f65491446730102c4d7de00',
}


def basis(symbol, cik):
    if symbol == 'VALE':
        from app.services.sec_vale_financials import basis as vale_basis
        return vale_basis(cik)
    if symbol == 'NVS':
        from app.services.sec_nvs_financials import basis as nvs_basis
        return nvs_basis(cik)
    if symbol != SYMBOL:
        return None
    if str(cik).zfill(10) != CIK:
        raise ValueError('reviewed_currency_cik_mismatch')
    from app.services.sec_fundamentals import TAGS
    return {'namespace': REVIEW['namespace'], 'currency': REVIEW['currency'],
            'tags': {**TAGS, 'revenue': (REVIEW['revenue_concept'],)}}
