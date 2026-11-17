"""Reviewed accounting predecessor originals for a reverse acquisition.

The surviving legal CIK is not an accounting-history identifier. Never merge
its old entity facts into the accounting acquirer's comparative statements.
This review is deliberately limited to the three pinned original statements;
a new filing requires review before it can extend the accounting history.
"""
from copy import deepcopy
from datetime import date, timedelta
from functools import lru_cache
import hashlib
import json
from pathlib import Path
from zoneinfo import ZoneInfo

from app.services.sec_fundamentals import SecFundamentalsCoverageError

VERSION = 'sec_reviewed_reverse_acquisition_v1'
CIK = '0000906107'
PREDECESSOR_CIK = '0000915912'
FIRST_FILED = date(2026, 11, 9)
TAGS = {
    'revenue': ('Revenues',),
    'net_income': ('NetIncomeLoss',),
    'operating_cash_flow': ('NetCashProvidedByUsedInOperatingActivities',),
    'equity': ('StockholdersEquity',),
    'current_assets': ('AssetsCurrent',),
    'current_liabilities': ('LiabilitiesCurrent',),
    'statement_assets': ('Assets',),
    'common_income': ('NetIncomeLossAvailableToCommonStockholdersBasic',),
    'parent_income_consolidated': ('ProfitLoss',),
    'parent_income_noncontrolling': ('NetIncomeLossAttributableToNoncontrollingInterest',),
}
INSTANT = {'equity', 'current_assets', 'current_liabilities', 'statement_assets'}


def review():
    return json.loads(Path(__file__).with_name('sec_reverse_acquisition_review.json').read_text(encoding='utf-8'))


def applies(cik, available_by):
    return str(cik).zfill(10) == CIK and available_by >= FIRST_FILED


def cache_requires_review(cik, available_by):
    if str(cik).zfill(10) != CIK:
        return False
    try:
        return date.fromisoformat(available_by) >= FIRST_FILED
    except (ValueError, TypeError):
        return True


def _validate_identity(payload, company, *, symbol, cik, available_by):
    if (str(cik).zfill(10) != CIK or symbol != 'VMRK'
            or any(str(obj.get('cik', '')).zfill(10) != CIK for obj in (payload, company))
            or company.get('tickers', []).count(symbol) != 1):
        raise SecFundamentalsCoverageError('reverse_acquisition_identity')
    recent = company.get('filings', {}).get('recent', {})
    columns = [recent.get(k, []) for k in ('accessionNumber', 'form', 'filingDate', 'reportDate', 'primaryDocument')]
    if not columns[0] or len({len(c) for c in columns}) != 1:
        raise SecFundamentalsCoverageError('reverse_acquisition_directory')
    eligible = []
    for accn, form, filed, end, filename in zip(*columns):
        if form not in {'10-K', '10-Q', '10-K/A', '10-Q/A', '10-KT', '10-QT',
                        '10-KT/A', '10-QT/A', '20-F', '20-F/A', '40-F', '40-F/A'}:
            continue
        try:
            if date.fromisoformat(end) <= date.fromisoformat(filed) <= available_by:
                eligible.append((end, filed, accn, form, filename))
        except (ValueError, TypeError):
            raise SecFundamentalsCoverageError('reverse_acquisition_directory') from None
    current = review()['sources']['current']
    expected = tuple(current[k] for k in ('report_date', 'filed', 'accession', 'form', 'filename'))
    latest = max((p[:2] for p in eligible), default=None)
    if not eligible or [p for p in eligible if p[:2] == latest] != [expected]:
        raise SecFundamentalsCoverageError('reverse_acquisition_latest_filing_unreviewed')


def acquire(facts_raw, company_raw, *, symbol, cik, observed_at, client):
    available = observed_at.astimezone(ZoneInfo('America/New_York')).date() - timedelta(days=1)
    _validate_identity(json.loads(facts_raw), json.loads(company_raw), symbol=symbol, cik=cik, available_by=available)
    profile = review()
    documents = {}
    for name, source in profile['sources'].items():
        raw = client.get(source['url'])
        _validate_original(raw, source)
        documents[name] = raw
    return {'reverse_acquisition': documents, 'evidence': {
        'methodology': VERSION, 'cik': CIK, 'report_date': profile['sources']['current']['report_date'],
        'accounting_predecessor_cik': PREDECESSOR_CIK,
        'sources': deepcopy(profile['sources'])}}


def _validate_original(raw, source):
    if (not isinstance(raw, bytes) or len(raw) != source['bytes']
            or hashlib.sha256(raw).hexdigest() != source['sha256']):
        raise SecFundamentalsCoverageError('reverse_acquisition_original_changed')


@lru_cache(maxsize=3)
def _parsed_original(raw, source_json):
    """Only the three hash-validated originals reach this bounded parse cache.

    Retain at most 16,269,280 original bytes and their small fact dictionaries,
    never the HTML trees. Each consumer receives a separate records dictionary.
    """
    from app.services import sec_inline_financials as inline
    source = json.loads(source_json)
    _validate_original(raw, source)
    return inline.parse(raw, **{k: source[k] for k in
        ('cik', 'accession', 'filed', 'report_date', 'form', 'url')},
        basis={'namespace': 'us-gaap', 'currency': 'USD', 'tags': TAGS})


def record_sets(payload, company, *, symbol, cik, available_by, observed_at, supplement, metrics):
    from app.services import sec_fundamentals as sec, sec_inline_financials as inline
    from app.services import sec_parent_income
    _validate_identity(payload, company, symbol=symbol, cik=cik, available_by=available_by)
    if (observed_at.tzinfo is None or available_by >
            observed_at.astimezone(ZoneInfo('America/New_York')).date() - timedelta(days=1)):
        raise SecFundamentalsCoverageError('reverse_acquisition_availability')
    profile = review()
    documents = (supplement or {}).get('reverse_acquisition', {})
    if set(documents) != set(profile['sources']):
        raise SecFundamentalsCoverageError('reverse_acquisition_originals_required')
    sets = {}
    for name, source in profile['sources'].items():
        raw = documents[name]
        _validate_original(raw, source)
        if date.fromisoformat(source['filed']) > available_by:
            raise SecFundamentalsCoverageError('reverse_acquisition_original_not_available')
        parsed = _parsed_original(raw, json.dumps(source, sort_keys=True))
        original = {'facts': {'us-gaap': parsed['facts']}}
        rows = {metric: sec._records(original, 'equity' if metric in INSTANT else metric,
                    available_by, concepts=tags, unit='USD') for metric, tags in TAGS.items()}
        inline.annotate(rows, parsed, metrics={m: (tags, 'USD') for m, tags in TAGS.items()},
                        expected_cik=source['cik'], available_by=available_by)
        # Only the predecessor annual needs parent-income reconciliation. Its
        # explicit consolidated income less explicit NCI also equals the
        # separately reported common-shareholder amount. Do not treat the
        # successor's partial NCI allocation as its complete minority total.
        if name == 'annual':
            rows, _ = sec_parent_income.apply(rows)
            for key, point in rows['net_income'].items():
                common = rows['common_income'].get(key)
                if not point or not common or point['value'] != common['value']:
                    raise SecFundamentalsCoverageError('reverse_acquisition_parent_ownership')
        sets[name] = rows
    comparisons = []
    for left, right, metric, start, end in profile['required_overlaps']:
        key = (date.fromisoformat(start) if start else None, date.fromisoformat(end))
        a, b = sets[left][metric].get(key), sets[right][metric].get(key)
        if not a or not b or (a['value'], a['unit']) != (b['value'], b['unit']):
            raise SecFundamentalsCoverageError('reverse_acquisition_overlap')
        comparisons.append({'sources': [left, right], 'metric': metric,
                            'start': start, 'end': end, 'value': a['value'], 'unit': a['unit']})
    merged = {m: {} for m in TAGS}
    for name in ('comparative', 'annual', 'current'):
        for metric, rows in sets[name].items():
            for key, point in rows.items():
                previous = merged[metric].get(key)
                if key in merged[metric] and (not point or not previous or point['value'] != previous['value']):
                    raise SecFundamentalsCoverageError('reverse_acquisition_conflicting_overlap')
                if point:
                    merged[metric][key] = {**point, 'accounting_basis_cik': PREDECESSOR_CIK,
                        'accounting_continuity': VERSION}
    # Same-filing companyfacts must agree with the complete original. Older
    # legal-entity facts are excluded entirely, even when a value happens to fit.
    current = profile['sources']['current']
    for metric, tags in TAGS.items():
        for tag in tags:
            for fact in payload.get('facts', {}).get('us-gaap', {}).get(tag, {}).get('units', {}).get('USD', []):
                if fact.get('accn') != current['accession']:
                    continue
                if fact.get('filed') != current['filed']:
                    raise SecFundamentalsCoverageError('reverse_acquisition_fact_filing')
                key = (sec._day(fact.get('start')), sec._day(fact.get('end')))
                original = sets['current'][metric].get(key)
                if original is not None and sec._number(fact.get('val')) != original['value']:
                    raise SecFundamentalsCoverageError('reverse_acquisition_fact_conflict')
    # Only explicitly reviewed scalar metrics can leave this boundary. EPS and
    # share counts need the merger exchange ratio; capex is not inferred from
    # real-estate acquisitions, and sector leverage policy remains unchanged.
    allowed = set(TAGS) - {'parent_income_consolidated', 'parent_income_noncontrolling'}
    result = {m: merged[m] if m in allowed and unit == 'USD' else {}
              for m, (_, unit) in metrics.items()}
    proof = {'methodology': VERSION, 'current_cik': CIK, 'predecessor_cik': PREDECESSOR_CIK,
        'effective_date': '2026-08-17', 'reviewed_at': profile['reviewed_at'],
        'sources': deepcopy(profile['sources']), 'overlaps': comparisons,
        'excluded_history': 'Pre-merger Equity Residential facts under the surviving legal CIK',
        'limitations': ['Only three reviewed complete originals; future filings require review.',
            'No inferred capex, per-share conversion, or REIT scoring methodology.']}
    return result, proof


def cached_valid(evidence):
    """Retire old mixed-basis caches and bind accepted caches to this review."""
    if not isinstance(evidence, dict):
        return False
    if (evidence.get('financial_status') == 'unavailable'
            and str(evidence.get('financial_error', '')).startswith('reverse_acquisition_')):
        return all(not evidence.get(k) for k in ('current', 'prior', 'instant', 'calculations'))
    profile = review()
    proof = evidence.get('financial_continuity')
    inline = evidence.get('inline_filing_supplement')
    if not isinstance(proof, dict) or not isinstance(inline, dict):
        return False
    if (proof.get('methodology') != VERSION or inline.get('methodology') != VERSION
            or proof.get('current_cik') != CIK or inline.get('cik') != CIK
            or proof.get('predecessor_cik') != PREDECESSOR_CIK
            or inline.get('accounting_predecessor_cik') != PREDECESSOR_CIK
            or proof.get('sources') != profile['sources'] or inline.get('sources') != profile['sources']
            or proof.get('reviewed_at') != profile['reviewed_at']
            or inline.get('report_date') != profile['sources']['current']['report_date']):
        return False
    expected = profile['required_overlaps']
    overlaps = proof.get('overlaps')
    if not isinstance(overlaps, list) or len(overlaps) != len(expected):
        return False
    for index, (row, (left, right, metric, start, end)) in enumerate(zip(overlaps, expected)):
        if (not isinstance(row, dict) or row.get('sources') != [left, right]
                or (row.get('metric'), row.get('start'), row.get('end'), row.get('unit')) != (metric, start, end, 'USD')
                or row.get('value') != profile['overlap_values'][index]):
            return False
    return True
