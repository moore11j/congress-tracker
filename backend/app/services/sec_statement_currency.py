"""Explicit reporting-currency evidence from a complete SEC financial document.

Shared acquisition and normalization require a declared statement currency,
matched filing identity and qualified reporting periods. No FX or ADS conversion
is inferred. Publication remains controlled by the existing provider selection.
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


REPORTING_POLICY = 'explicit_native_statement_currency_v1'


def reporting_basis(supplement):
    """Get canonical concept roles only from bound parsed-statement evidence."""
    evidence = (supplement or {}).get('evidence', {})
    if 'native_reporting' not in evidence:
        return None
    proof = bind(supplement, evidence.get('statement_currency_declaration', {}))
    if proof['namespace'] == 'us-gaap':
        from app.services.sec_fundamentals import TAGS
        from app.services.sec_parent_income import TAGS as parent_tags
        tags = {**TAGS, **parent_tags, 'assets': ('Assets',)}
    else:
        tags = dict(IFRS_TAGS)
    return {'namespace':proof['namespace'], 'currency':proof['currency'], 'tags':tags,
        'policy':REPORTING_POLICY, 'review':{
            'policy':REPORTING_POLICY, 'source_url':proof['source_url'],
            'source_sha256':proof['source_sha256'], 'declaration':proof,
            'amount_basis':'Reported amounts in the declared statement currency; no FX or ADS conversion.',
            'margin_basis':'Margins use aligned reported revenue and matching parent income or operating subtotals.',
            'cash_flow_basis':'Free cash flow subtracts reported property and equipment purchases; other investing and financing flows are excluded.'}}


def authorized(payload, company, *, symbol, cik, available_by, supplement):
    """Recompute qualifying financial accessions; do not trust saved selection."""
    from app.services import sec_tagged_foreign as foreign, sec_inline_financials as inline
    basis = reporting_basis(supplement)
    if basis is None:
        return None
    proof = supplement['evidence']
    effective = inline.merge(payload, supplement, available_by=available_by)
    selection = foreign.plan(effective, company, symbol=symbol, cik=cik,
        available_by=available_by, statement_basis=basis)
    if selection is None or selection != proof.get('native_reporting'):
        raise StatementCurrencyError('native_reporting_selection_mismatch')
    current = selection['current']
    if any(proof.get(k) != current[k] for k in ('accession','form','filed','report_date')):
        raise StatementCurrencyError('native_reporting_current_mismatch')
    discovery = proof.get('document_set_discovery') or proof.get('unindexed_primary_discovery') or {}
    if (discovery.get('cover_url') != current['url'] or discovery.get('cik') != selection['cik']
            or any(discovery.get(k) != current[k] for k in ('accession','form','filed','report_date'))
            or not re.fullmatch(r'[0-9a-f]{64}', str(discovery.get('directory_sha256', '')))
            or proof['url'] != discovery.get('statement_url', current['url'])):
        raise StatementCurrencyError('native_reporting_original_binding')
    cover = proof.get('cover_evidence')
    if cover is not None and (cover.get('source_url') != current['url']
            or cover.get('exhibit_url') != proof['url'] or cover.get('exhibit_sha256') != proof['sha256']
            or not re.fullmatch(r'[0-9a-f]{64}',str(cover.get('source_sha256','')))):
        raise StatementCurrencyError('native_reporting_original_binding')
    return selection


def acquire(payload, company_raw, *, symbol, cik, available_by, client):
    """Discover native statements using the same bounded financial filing rules."""
    import json
    from datetime import date
    from app.services import sec_document_sets as sets, sec_inline_financials as inline
    from app.services import sec_tagged_foreign as foreign
    from app.services.sec_reporting_policy import reviewed, DOMESTIC_FORMS
    from app.services.sec_currency_ratios import basis as reviewed_basis
    from app.services.sec_foreign_statements import profile
    if reviewed(symbol,cik) or reviewed_basis(symbol,cik) or profile(symbol,cik):
        return None
    company = json.loads(company_raw)
    recent = company.get('filings',{}).get('recent',{})
    if not any(f in {'6-K','6-K/A'} for f in recent.get('form',[])):
        return None
    candidates = sets.candidates(company_raw,symbol=symbol,cik=cik,available_by=available_by)
    domestic = [foreign._day(end) for form,end,filed in zip(recent.get('form',[]),recent.get('reportDate',[]),recent.get('filingDate',[]))
        if form in DOMESTIC_FORMS and foreign._day(end) and foreign._day(filed)
        and foreign._day(end)<=foreign._day(filed)<=available_by]
    if domestic:
        candidates = [c for c in candidates if date.fromisoformat(c['report_date']) > max(domestic)]
    candidates.sort(key=lambda c:(c['report_date']<c['filed'],c['report_date'],c['filed']),reverse=True)
    attempts = []
    for candidate in candidates[:3]:
        try:
            parsed = acquire_candidate(candidate,company_raw,symbol=symbol,cik=cik,available_by=available_by,client=client)
        except StatementCurrencyError as exc:
            if str(exc) not in {'statement_currency_unavailable','statement_currency_taxonomy','statement_currency_link_missing_or_ambiguous'}:
                raise
            attempts.append({'accession':candidate['accession'],'reason':str(exc)})
            continue
        except inline.InlineFactsError as exc:
            if str(exc) not in {'filing_namespaces','latest_reported_revenue_unavailable','document_set_cover_size'}:
                raise
            attempts.append({'accession':candidate['accession'],'reason':str(exc)})
            continue
        # This marker enables basis construction; authorization below recomputes
        # and compares the complete selection before a consumer can use it.
        parsed['evidence']['native_reporting'] = {}
        basis = reporting_basis(parsed)
        effective = inline.merge(payload,parsed,available_by=available_by)
        selection = foreign.plan(effective,company,symbol=symbol,cik=cik,available_by=available_by,statement_basis=basis)
        if selection is None:
            continue
        if selection['current']['accession'] != candidate['accession']:
            raise StatementCurrencyError('native_reporting_candidate_not_current')
        parsed['evidence']['native_reporting'] = selection
        parsed['evidence']['native_discovery'] = {'candidate_bound':3,'total_candidates':len(candidates),
            'preceding_holds':attempts,'complete_universe_coverage':False}
        authorized(payload,company,symbol=symbol,cik=cik,available_by=available_by,supplement=parsed)
        return parsed
    return None


def record_sets(payload, company, *, symbol, cik, available_by, supplement, metrics=None, instant_metrics=()):
    """Merge only exact qualified financial accessions in the declared currency."""
    from app.services import sec_tagged_foreign as foreign, sec_inline_financials as inline
    from app.services import sec_fundamentals as sec
    selection = authorized(payload,company,symbol=symbol,cik=cik,available_by=available_by,supplement=supplement)
    if selection is None:
        raise StatementCurrencyError('native_reporting_required')
    basis = reporting_basis(supplement)
    effective = foreign.filtered(inline.merge(payload,supplement,available_by=available_by),selection)
    metrics = metrics or {k:(ts,basis['currency']) for k,ts in basis['tags'].items()}
    if any(unit != basis['currency'] and not (name == 'eps' and unit == basis['currency']+'/shares')
           for name,(_,unit) in metrics.items()):
        raise StatementCurrencyError('native_reporting_requested_unit')
    records = {name:({} if name == 'eps' else sec._records(effective,'equity' if name in INSTANT_METRICS | set(instant_metrics) else name,
        available_by,concepts=tags,unit=unit,namespace=basis['namespace'],forms=foreign.FORMS))
        for name,(tags,unit) in metrics.items()}
    inline.annotate(records,supplement,metrics=metrics,expected_cik=cik,available_by=available_by)
    return records



def cached_currency_valid(evidence):
    """Validate the native-currency proof in an already prepared cache."""
    proof = evidence.get('inline_filing_supplement') or {}
    if 'native_reporting' not in proof:
        return True
    try:
        declaration_proof = bind({'evidence':proof},proof.get('statement_currency_declaration',{}))
        selection = proof['native_reporting']
        return (isinstance(selection,dict) and selection.get('currency_policy') == REPORTING_POLICY
            and selection.get('currency') == declaration_proof['currency']
            and selection.get('namespace') == declaration_proof['namespace']
            and selection.get('cik') == declaration_proof['cik']
            and all(selection.get('current',{}).get(k) == proof.get(k)
                    for k in ('accession','form','filed','report_date')))
    except (StatementCurrencyError,TypeError,AttributeError,KeyError):
        return False
