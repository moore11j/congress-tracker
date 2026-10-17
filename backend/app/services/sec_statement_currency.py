"""Explicit reporting-currency evidence from a complete SEC financial document.

This identifies a declared reporting/presentation currency, never a listing or
functional currency. It does not authorize ratios, FX conversion or publication.
"""
import hashlib
import re
from urllib.parse import urlsplit
from lxml import html

VERSION = 'sec_statement_currency_declaration_v1'
MAX_BYTES = 20_000_000
ALIASES = {
    'USD': r'(?:U\.?S\.?\s+dollars?|United States dollars?|USD)',
    'EUR': r'(?:euros?|EUR)',
    'CNY': r'(?:renminbi|RMB|Chinese yuan|CNY)',
    'CAD': r'(?:Canadian dollars?|CAD)',
    'GBP': r'(?:pounds? sterling|British pounds?|GBP)',
    'JPY': r'(?:Japanese yen|JPY)',
    'TWD': r'(?:New Taiwan dollars?|TWD)',
    'CHF': r'(?:Swiss francs?|CHF)',
    'BRL': r'(?:Brazilian reais|Brazilian real|BRL)',
    'DKK': r'(?:Danish kroner|DKK)',
    'NOK': r'(?:Norwegian kroner|NOK)',
    'SEK': r'(?:Swedish kronor|SEK)',
    'AUD': r'(?:Australian dollars?|AUD)',
    'HKD': r'(?:Hong Kong dollars?|HKD)',
    'MXN': r'(?:Mexican pesos?|MXN)',
    'INR': r'(?:Indian rupees?|INR)',
    'KRW': r'(?:South Korean won|KRW)',
    'SGD': r'(?:Singapore dollars?|SGD)',
    'ZAR': r'(?:South African rand|ZAR)',
}
SUBJECT = r"(?:our|the (?:company|group)(?:['\u2019\uFFFD]s)?)"
PREFIXES = (
    SUBJECT + r'\s+(?:reporting|presentation)\s+currency\s+is\s+(?:the\s+)?',
    r'we\s+use\s+(?:the\s+)?',
    r'(?:the|these|our)\s+(?:(?:condensed|consolidated|interim|unaudited)\s+){0,4}'
    r'financial\s+statements\s+are\s+presented\s+in\s+(?:(?:millions|thousands)\s+of\s+)?',
)


class StatementCurrencyError(ValueError):
    pass


def source_identity(url):
    """Validate a complete SEC original address and return its filing identity."""
    try:
        parsed = urlsplit(url)
        match = re.fullmatch(
            r'/Archives/edgar/data/([0-9]{1,10})/([0-9]{18})/[A-Za-z0-9][A-Za-z0-9_.-]{0,180}\.html?',
            parsed.path)
        valid = (parsed.scheme == 'https' and parsed.netloc == 'www.sec.gov'
                 and not parsed.query and not parsed.fragment and not parsed.username
                 and not parsed.password and parsed.port is None and match)
    except (ValueError, TypeError, AttributeError):
        valid = False
    if not valid:
        raise StatementCurrencyError('statement_currency_source_url')
    cik, accession = match.groups()
    return cik.zfill(10), accession[:10] + '-' + accession[10:12] + '-' + accession[12:]


def declaration(raw, *, url):
    """Return explicit declarations and source hashes; ambiguity stays held."""
    source_identity(url)
    if not isinstance(raw, bytes) or not raw or len(raw) > MAX_BYTES:
        raise StatementCurrencyError('statement_currency_source_size')
    try:
        tree = html.fromstring(raw)
    except (ValueError, html.etree.ParserError) as exc:
        raise StatementCurrencyError('statement_currency_source_html') from exc
    for node in tree.xpath('//script|//style|//template|//blockquote'):
        node.drop_tree()
    text = ' '.join(' '.join(tree.itertext()).split())
    matches = []
    for currency, alias in ALIASES.items():
        for i, prefix in enumerate(PREFIXES):
            suffix = r'\s+as\s+our\s+(?:reporting|presentation)\s+currency\b' if i == 1 else ''
            pattern = r'\b' + prefix + '(' + alias + r')\b' + suffix
            for match in re.finditer(pattern, text, re.I):
                matches.append({'currency': currency, 'text': match.group(0), 'offset': match.start()})
                if len(matches) > 50:
                    raise StatementCurrencyError('statement_currency_declaration_bound')
    currencies = sorted({m['currency'] for m in matches})
    return {'methodology': VERSION, 'source_url': url,
            'source_sha256': hashlib.sha256(raw).hexdigest(),
            'status': 'declared' if len(currencies) == 1 else 'ambiguous' if currencies else 'unavailable',
            'currency': currencies[0] if len(currencies) == 1 else None,
            'declarations': sorted(matches, key=lambda m: (m['offset'], m['currency'])),
            'limitations': ['A declaration is not proof of complete statements, share basis, or comparable periods.']}


def bind(parsed, proof):
    """Require declaration, parsed facts and immutable original to agree."""
    if not isinstance(parsed, dict) or not isinstance(proof, dict):
        raise StatementCurrencyError('statement_currency_basis_mismatch')
    evidence = parsed.get('evidence', {})
    if not isinstance(evidence, dict):
        raise StatementCurrencyError('statement_currency_basis_mismatch')
    cik, accession = source_identity(proof.get('source_url'))
    currency = proof.get('currency')
    observation = evidence.get('currency_observation') or {}
    if (proof.get('methodology') != VERSION or proof.get('status') != 'declared'
            or currency not in ALIASES or not proof.get('declarations')
            or evidence.get('cik') != cik or evidence.get('accession') != accession
            or not re.fullmatch(r'[0-9a-f]{64}', str(proof.get('source_sha256', '')))
            or any(not isinstance(d, dict) or d.get('currency') != currency
                   for d in proof['declarations'])
            or evidence.get('currency') != currency
            or evidence.get('namespace') not in {'us-gaap', 'ifrs-full'}
            or evidence.get('url') != proof.get('source_url')
            or evidence.get('sha256') != proof.get('source_sha256')
            or observation.get('methodology') != 'entity_wide_statement_currencies_v1'
            or currency not in observation.get('current_statement_currencies', [])):
        raise StatementCurrencyError('statement_currency_basis_mismatch')
    return {**proof, 'cik': evidence['cik'], 'accession': evidence['accession'],
            'report_date': evidence['report_date'], 'filed': evidence['filed'],
            'namespace': evidence['namespace']}


# These are statement concepts, not interchangeable inputs to ratio formulas.
# In particular parent income/equity and consolidated income/equity remain
# separate. No per-share concept or currency conversion is inferred here.
IFRS_TAGS = {
    'revenue': ('Revenue', 'RevenueFromContractsWithCustomers'),
    'gross_profit': ('GrossProfit',),
    'operating_income': ('ProfitLossFromOperatingActivities',),
    'net_income': ('ProfitLossAttributableToOwnersOfParent',),
    'consolidated_income': ('ProfitLoss',),
    'equity': ('EquityAttributableToOwnersOfParent',),
    'consolidated_equity': ('Equity',),
    'assets': ('Assets',),
    'current_assets': ('CurrentAssets',),
    'current_liabilities': ('CurrentLiabilities',),
    'operating_cash_flow': ('CashFlowsFromUsedInOperatingActivities',),
    'capex': ('PurchaseOfPropertyPlantAndEquipmentClassifiedAsInvestingActivities',),
}


def accounting_basis(raw, *, url):
    """Choose a candidate basis from explicit text and trusted root taxonomy.

    This does not qualify a statement or admit it to product consumers. The
    complete numeric parse, filing identity and current periods must agree.
    """
    proof = declaration(raw, url=url)
    if proof['status'] != 'declared':
        raise StatementCurrencyError('statement_currency_' + proof['status'])
    tree = html.fromstring(raw)
    patterns = {
        'us-gaap': r'https?://fasb\.org/us-gaap/[0-9]{4}',
        'ifrs-full': r'https?://xbrl\.ifrs\.org/taxonomy/[0-9]{4}-[0-9]{2}-[0-9]{2}/ifrs-full',
    }
    used = {node.get('name', '').split(':', 1)[0] for node in tree.iter()
            if node.tag == 'ix:nonfraction'}
    namespaces = [name for name, pattern in patterns.items()
                  if name in used and re.fullmatch(pattern, tree.get('xmlns:' + name, ''))]
    if tree.tag != 'html' or tree.xpath('//base') or len(namespaces) != 1:
        raise StatementCurrencyError('statement_currency_taxonomy')
    namespace = namespaces[0]
    if namespace == 'us-gaap':
        from app.services.sec_fundamentals import TAGS
        from app.services.sec_parent_income import TAGS as parent_tags
        tags = {**TAGS, **parent_tags, 'assets': ('Assets',)}
    else:
        tags = dict(IFRS_TAGS)
    return {'namespace': namespace, 'currency': proof['currency'], 'tags': tags}, proof


def acquire_candidate(metadata, company_raw, *, symbol, cik, available_by, client):
    """Acquire and bind one directory-qualified statement in its native currency.

    This returns source evidence for normalization. It does not assert that the
    candidate is the latest complete statement or authorize consumer cutover.
    Each original is fetched at most once, including linked-cover parsing.
    """
    from app.services import sec_document_sets as sets
    from app.services.sec_inline_financials import InlineFactsError
    if metadata not in sets.candidates(company_raw, symbol=symbol, cik=cik, available_by=available_by):
        raise InlineFactsError('document_set_candidate_not_in_directory')
    originals = {}

    class Originals:
        def get(self, url):
            if url not in originals:
                originals[url] = client.get(url)
            return originals[url]

    retained = Originals()
    url = metadata['cover_url']
    cover = retained.get(url)
    try:
        basis, _ = accounting_basis(cover, url=url)
    except StatementCurrencyError as exc:
        # An explicit conflict must not be hidden by an exhibit fallback.
        if str(exc) not in {'statement_currency_unavailable', 'statement_currency_taxonomy'}:
            raise
        links = sets.statement_exhibits(cover, metadata)
        if len(links) != 1:
            raise StatementCurrencyError('statement_currency_link_missing_or_ambiguous')
        url = links[0]['url']
        basis, _ = accounting_basis(retained.get(url), url=url)
    parsed = sets.acquire_candidate(metadata, company_raw, symbol=symbol, cik=cik,
        available_by=available_by, client=retained, basis=basis)
    if parsed is None:
        raise StatementCurrencyError('statement_currency_no_statement')
    actual_url = parsed['evidence']['url']
    actual_basis, proof = accounting_basis(originals[actual_url], url=actual_url)
    if actual_basis != basis:
        raise StatementCurrencyError('statement_currency_basis_changed')
    bound = bind(parsed, proof)
    currency = basis['currency']
    rows = [point for concept in parsed['facts'].values()
            for point in concept.get('units', {}).get(currency, [])
            if point.get('end') == metadata['report_date']]
    if not any(not row.get('start') for row in rows) or not any(row.get('start') for row in rows):
        raise StatementCurrencyError('statement_currency_current_balance_and_duration_required')
    parsed['evidence']['statement_currency_declaration'] = bound
    return parsed


INSTANT_METRICS = frozenset({'equity', 'parent_equity', 'consolidated_equity',
    'assets', 'current_assets', 'current_liabilities'})


def normalized_records(parsed, company_raw, *, symbol, cik, available_by):
    """Normalize native amounts into the existing financial-record contract.

    Only this exact original contributes values. Other filings, TTM derivation,
    FX and listed-share data require their own evidence and are not inferred.
    """
    from app.services import sec_document_sets as sets, sec_fundamentals as sec
    from app.services import sec_inline_financials as inline
    evidence = parsed.get('evidence', {})
    proof = bind(parsed, evidence.get('statement_currency_declaration', {}))
    discovery = evidence.get('document_set_discovery') or evidence.get('unindexed_primary_discovery')
    candidates = sets.candidates(company_raw, symbol=symbol, cik=cik, available_by=available_by)
    candidate = next((c for c in candidates if c['accession'] == proof['accession']), None)
    if (candidate is None or not isinstance(discovery, dict)
            or any(discovery.get(k) != v for k,v in candidate.items())
            or any(evidence.get(k) != candidate[k] for k in ('cik','accession','form','filed','report_date'))):
        raise StatementCurrencyError('statement_currency_directory_binding')
    expected_url = discovery.get('statement_url', candidate['cover_url'])
    if expected_url != proof['source_url']:
        raise StatementCurrencyError('statement_currency_directory_binding')
    namespace, currency = proof['namespace'], proof['currency']
    if namespace == 'us-gaap':
        from app.services.sec_parent_income import TAGS as parent_tags
        tags = {**sec.TAGS, **parent_tags, 'assets': ('Assets',)}
    else:
        tags = dict(IFRS_TAGS)
    metrics = {name:(concepts,currency) for name,concepts in tags.items()}
    payload = {'facts':{namespace:parsed['facts']}}
    records = {name:sec._records(payload, 'equity' if name in INSTANT_METRICS else name,
        available_by, concepts=concepts, unit=currency, namespace=namespace, forms={evidence['form']})
        for name,concepts in tags.items()}
    inline.annotate(records,parsed,metrics=metrics,expected_cik=cik,available_by=available_by)
    for series in records.values():
        for point in series.values():
            if point is None:
                continue
            if (point.get('source_url') != proof['source_url']
                    or point.get('source_sha256') != proof['source_sha256']
                    or point.get('source_cik') != proof['cik']
                    or point.get('accession') != proof['accession']
                    or point.get('filed') != proof['filed'] or point.get('unit') != currency):
                raise StatementCurrencyError('statement_currency_fact_provenance')
    return records
