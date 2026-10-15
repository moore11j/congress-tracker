"""Reviewed Novartis USD IFRS financial basis and SEC exhibit identity.

Parent earnings/equity are distinct from consolidated income. Revenue is net
sales, excluding other revenue. Cash flow uses consolidated reported CFO.
"""
from copy import deepcopy
from datetime import date
import hashlib
import re
from urllib.parse import urljoin
from lxml import html

VERSION = 'sec_nvs_ifrs_interim_v1'
SYMBOL = 'NVS'
CIK = '0001114448'
TAGS = {
    'revenue': ('RevenueFromSaleOfGoods',),
    'gross_profit': ('GrossProfit',),
    'operating_income': ('ProfitLossFromOperatingActivities',),
    'net_income': ('ProfitLossAttributableToOwnersOfParent',),
    'operating_cash_flow': ('CashFlowsFromUsedInOperatingActivities',),
    'capex': ('PurchaseOfPropertyPlantAndEquipmentClassifiedAsInvestingActivities',),
    'equity': ('EquityAttributableToOwnersOfParent',),
    'current_assets': ('CurrentAssets',),
    'current_liabilities': ('CurrentLiabilities',),
}
REVIEW = {
    'symbol': SYMBOL, 'cik': CIK, 'namespace': 'ifrs-full', 'currency': 'USD',
    'revenue_basis': 'net sales to third parties; separately reported other revenue excluded',
    'income_basis': 'reported profit attributable to owners of the parent',
    'equity_basis': 'reported equity attributable to owners of the parent',
    'cash_flow_basis': 'consolidated operating cash flow less cash purchases of property, plant and equipment; includes discontinued cash flows where reported and excludes intangible purchases; not issuer-adjusted FCF',
    'reviewed_on': '2026-10-15',
    'source_url': 'https://www.sec.gov/Archives/edgar/data/1114448/000111444826000012/nvs-20260630-99_2.htm',
    'source_sha256': 'fd11714b58b3c49b8e09759a553f790d3160b04da179787b3b441a9a8646eaee',
    'annual_url': 'https://www.sec.gov/Archives/edgar/data/1114448/000111444826000004/nvs-20251231.htm',
    'annual_sha256': 'ee4e72876e1e8fe464286874b883191ffac5a9ba630aacf7023e59a66878eda0',
}


def basis(cik):
    if str(cik).zfill(10) != CIK:
        raise ValueError('reviewed_nvs_cik_mismatch')
    return {'namespace': 'ifrs-full', 'currency': 'USD', 'tags': dict(TAGS), 'review': deepcopy(REVIEW)}


def cover_tree(raw, cover_url, *, cik):
    from app.services.sec_inline_financials import InlineFactsError
    if (str(cik).zfill(10) != CIK or not raw or len(raw) > 200_000
            or not isinstance(cover_url, str) or not re.fullmatch(r'https://www\.sec\.gov/Archives/edgar/data/1114448/[0-9]{18}/[A-Za-z0-9_.-]+\.html?', cover_url)):
        raise InlineFactsError('nvs_cover_identity_or_size')
    try:
        tree = html.fromstring(raw)
    except (ValueError, html.etree.ParserError) as exc:
        raise InlineFactsError('nvs_cover_html') from exc
    if (tree.get('xmlns:ix') != 'http://www.xbrl.org/2013/inlineXBRL'
            or not re.fullmatch(r'https?://xbrl.sec.gov/dei/[0-9]{4}', tree.get('xmlns:dei', ''))):
        raise InlineFactsError('nvs_cover_namespaces')
    for n in tree.iter():
        if any(v != tree.get(k) for k,v in n.attrib.items() if k.startswith('xmlns:') and k in tree.attrib):
            raise InlineFactsError('nvs_cover_namespace_redefinition')
    names = [n for n in tree.iter() if n.tag == 'ix:nonnumeric' and n.get('name') == 'dei:EntityRegistrantName']
    if len(names) != 1 or ' '.join(names[0].text_content().split()) != 'Novartis AG':
        raise InlineFactsError('nvs_cover_registrant')
    return tree


def exhibit_url(raw, cover_url, *, cik):
    from app.services.sec_inline_financials import InlineFactsError
    tree = cover_tree(raw, cover_url, cik=cik)
    links = [a for a in tree.xpath('//a[@href]') if ' '.join(a.text_content().split()) == '99.2 Interim Financial Report']
    if len(links) != 1:
        raise InlineFactsError('nvs_interim_exhibit_missing_or_ambiguous')
    url = urljoin(cover_url, links[0].get('href'))
    directory = cover_url.rsplit('/', 1)[0] + '/'
    if not url.startswith(directory) or not re.fullmatch(r'[A-Za-z0-9_.-]+\.html?', url[len(directory):]):
        raise InlineFactsError('nvs_interim_exhibit_outside_filing')
    return url


def form_proof(raw, cover_url, *, url, cik, form, report_date, contexts):
    """The inline document set splits DocumentType and financial contexts.

    Only the exact linked exhibit may supply the cover's referenced context.
    The exhibit independently verifies CIK, period, namespace and USD units.
    """
    from app.services.sec_inline_financials import InlineFactsError
    if form not in {'6-K', '6-K/A'} or exhibit_url(raw, cover_url, cik=cik) != url:
        raise InlineFactsError('nvs_cover_exhibit_mismatch')
    tree = cover_tree(raw, cover_url, cik=cik)
    forms = [n for n in tree.iter() if n.tag == 'ix:nonnumeric' and n.get('name') == 'dei:DocumentType']
    names = [n for n in tree.iter() if n.tag == 'ix:nonnumeric' and n.get('name') == 'dei:EntityRegistrantName']
    if len(forms) != 1 or forms[0].text_content().strip() != form or names[0].get('contextref') != forms[0].get('contextref'):
        raise InlineFactsError('nvs_cover_form')
    context_id = forms[0].get('contextref')
    period = contexts.get(context_id)
    end = date.fromisoformat(report_date)
    if period != (date(end.year, 1, 1), end):
        raise InlineFactsError('nvs_cover_context')
    return {'source_url': cover_url, 'source_sha256': hashlib.sha256(raw).hexdigest(),
            'exhibit_url': url, 'context_id': context_id, 'form': form,
            'basis': 'DocumentType in linked SEC cover; CIK and period in exhibit context'}
