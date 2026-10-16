"""Reviewed Shell USD IFRS ratios, with split cover/exhibit identity.

Issuer free cash flow includes investing cash flows. It is not substituted for
Walnut's CFO less PPE-only purchases; adjusted EBITDA is not inferred EBITDA.
"""
from copy import deepcopy
from datetime import date
import hashlib
import re
from urllib.parse import urljoin
from lxml import html

VERSION = 'sec_shell_ifrs_interim_v1'
SYMBOL = 'SHEL'
CIK = '0001306965'
TAGS = {
    'revenue': ('Revenue',),
    'gross_profit': (),
    'operating_income': (),
    'net_income': ('ProfitLossAttributableToOwnersOfParent',),
    'operating_cash_flow': ('CashFlowsFromUsedInOperatingActivities',),
    'capex': (),
    'equity': ('EquityAttributableToOwnersOfParent',),
    'current_assets': ('CurrentAssets',),
    'current_liabilities': ('CurrentLiabilities',),
}
REVIEW = {
    'symbol': SYMBOL, 'cik': CIK, 'namespace': 'ifrs-full', 'currency': 'USD',
    'revenue_basis': 'reported revenue excluding separate joint venture profit and interest/other income',
    'margin_basis': 'Margin denominator uses reported revenue, excluding separate joint venture profit and interest/other income.',
    'income_basis': 'reported profit attributable to Shell plc shareholders',
    'equity_basis': 'reported equity attributable to Shell plc shareholders',
    'cash_flow_basis': 'Free cash flow remains unavailable: Shell defines it as operating plus investing cash flows; capital expenditure is not established as PPE-only purchases. Neither issuer FCF nor adjusted EBITDA is substituted for Walnut metrics.',
    'reviewed_on': '2026-10-16',
    'source_url': 'https://www.sec.gov/Archives/edgar/data/1306965/000162828026050763/shel-20260630.htm',
    'source_sha256': 'ecb5eefe4dcacec619d8836c485d248b944264f0aadfbaa2ff394dccdd8b0d90',
    'annual_url': 'https://www.sec.gov/Archives/edgar/data/1306965/000162828026017024/shel-20251231.htm',
    'annual_sha256': '16742aa705c0b2dd9731b49b8bb7e40f400f3df002f0bb132beef73a4435bb63',
}


def basis(cik):
    if str(cik).zfill(10) != CIK:
        raise ValueError('reviewed_shell_cik_mismatch')
    return {'namespace': 'ifrs-full', 'currency': 'USD', 'tags': dict(TAGS), 'review': deepcopy(REVIEW)}


def cover_tree(raw, cover_url, *, cik):
    from app.services.sec_inline_financials import InlineFactsError
    if (str(cik).zfill(10) != CIK or not raw or len(raw) > 200_000
            or not isinstance(cover_url, str) or not re.fullmatch(r'https://www\.sec\.gov/Archives/edgar/data/1306965/[0-9]{18}/[A-Za-z0-9_.-]+\.html?', cover_url)):
        raise InlineFactsError('shell_cover_identity_or_size')
    try:
        tree = html.fromstring(raw)
    except (ValueError, html.etree.ParserError) as exc:
        raise InlineFactsError('shell_cover_html') from exc
    if (tree.get('xmlns:ix') != 'http://www.xbrl.org/2013/inlineXBRL'
            or not re.fullmatch(r'https?://xbrl.sec.gov/dei/[0-9]{4}', tree.get('xmlns:dei', ''))):
        raise InlineFactsError('shell_cover_namespaces')
    for n in tree.iter():
        if any(v != tree.get(k) for k,v in n.attrib.items() if k.startswith('xmlns:') and k in tree.attrib):
            raise InlineFactsError('shell_cover_namespace_redefinition')
    names = [n for n in tree.iter() if n.tag == 'ix:nonnumeric' and n.get('name') == 'dei:EntityRegistrantName']
    if len(names) != 1 or ' '.join(names[0].text_content().split()) != 'Shell plc':
        raise InlineFactsError('shell_cover_registrant')
    return tree


def exhibit_url(raw, cover_url, *, cik):
    from app.services.sec_inline_financials import InlineFactsError
    tree = cover_tree(raw, cover_url, cik=cik)
    links = [a for a in tree.xpath('//a[@href]') if ' '.join(a.text_content().split()) == '99.2']
    if len(links) != 1:
        raise InlineFactsError('shell_interim_exhibit_missing_or_ambiguous')
    url = urljoin(cover_url, links[0].get('href'))
    directory = cover_url.rsplit('/', 1)[0] + '/'
    if not url.startswith(directory) or not re.fullmatch(r'[A-Za-z0-9_.-]+\.html?', url[len(directory):]):
        raise InlineFactsError('shell_interim_exhibit_outside_filing')
    return url


def form_proof(raw, cover_url, *, url, cik, form, report_date, contexts):
    """The inline document set splits DocumentType and financial contexts.

    Only the exact linked exhibit may supply the cover's referenced context.
    The exhibit independently verifies CIK, period, namespace and USD units.
    """
    from app.services.sec_inline_financials import InlineFactsError
    if form not in {'6-K', '6-K/A'} or exhibit_url(raw, cover_url, cik=cik) != url:
        raise InlineFactsError('shell_cover_exhibit_mismatch')
    tree = cover_tree(raw, cover_url, cik=cik)
    forms = [n for n in tree.iter() if n.tag == 'ix:nonnumeric' and n.get('name') == 'dei:DocumentType']
    names = [n for n in tree.iter() if n.tag == 'ix:nonnumeric' and n.get('name') == 'dei:EntityRegistrantName']
    if len(forms) != 1 or forms[0].text_content().strip() != form or names[0].get('contextref') != forms[0].get('contextref'):
        raise InlineFactsError('shell_cover_form')
    context_id = forms[0].get('contextref')
    period = contexts.get(context_id)
    end = date.fromisoformat(report_date)
    if period != (date(end.year, 1, 1), end):
        raise InlineFactsError('shell_cover_context')
    return {'source_url': cover_url, 'source_sha256': hashlib.sha256(raw).hexdigest(),
            'exhibit_url': url, 'context_id': context_id, 'form': form,
            'basis': 'DocumentType in linked SEC cover; CIK and period in exhibit context'}
