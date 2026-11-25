"""Recover an entirely missing operating series from reconciled originals.

Companyfacts identifies candidate periods. Complete primary statements must
independently prove every revenue, expense and nonoperating reconciliation.
"""
from copy import deepcopy
from datetime import date, timedelta
import hashlib
import math
import re

VERSION = 'sec_primary_operating_history_v1'
BASIS = 'Calculated operating result: reported revenue less fully reconciled operating costs in the same consolidated primary statement.'
IDENTITY = ('start', 'end', 'filed', 'accession', 'unit')


def plan(payload, company, *, cik, available_by):
    from app.services import sec_fundamentals as sec
    if any(str(x.get('cik', '')).zfill(10) != cik for x in (payload, company)):
        raise ValueError('primary_operating_identity')
    # Do not combine a derived definition with a reported series or conflict.
    if sec._records(payload, 'operating_income', available_by):
        return None
    recent = company.get('filings', {}).get('recent', {})
    arrays = [recent.get(k, []) for k in ('accessionNumber', 'form', 'filingDate', 'reportDate', 'primaryDocument')]
    if len({len(a) for a in arrays}) != 1:
        raise ValueError('primary_operating_directory')
    filings = {}
    for accession, form, filed, report, filename in zip(*arrays):
        if form not in {'10-K', '10-Q', '10-K/A', '10-Q/A'} or not sec._day(report) or not sec._day(filed) or not report <= filed <= str(available_by):
            continue
        # Current and prior TTM inputs cannot require decades-old text filings.
        # Validate every in-scope filing, including a newer unsupported format.
        if date.fromisoformat(report) < available_by-timedelta(days=4*366):
            continue
        if not re.fullmatch(r'\d{10}-\d{2}-\d{6}', accession) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,180}\.html?', filename):
            raise ValueError('primary_operating_directory_identity')
        item = dict(cik=cik, accessionNumber=accession, form=form, filingDate=filed,
                    reportDate=report, primaryDocument=filename,
                    url=f'https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession.replace("-", "")}/{filename}')
        if accession in filings and filings[accession] != item:
            raise ValueError('primary_operating_directory_conflict')
        filings[accession] = item
    if not filings:
        return None
    latest = max((f['reportDate'], f['filingDate']) for f in filings.values())
    if sum((f['reportDate'], f['filingDate']) == latest for f in filings.values()) != 1:
        return None
    if any(form in {'20-F', '20-F/A', '40-F', '40-F/A', '6-K', '6-K/A'} and sec._day(report) and sec._day(filed)
           and latest[0] <= report <= filed <= str(available_by) for _, form, filed, report, _ in zip(*arrays)):
        return None
    end = date.fromisoformat(latest[0])
    if not 0 <= (available_by-end).days <= 180:
        return None
    revenue = sec._records(payload, 'revenue', available_by)
    costs = sec._records(payload, 'operating_income', available_by, concepts=('CostsAndExpenses',))
    current = sec._ttm(revenue, end)
    prior_end = sec._prior_end(revenue, end)
    prior = sec._ttm(revenue, prior_end) if prior_end else None
    if not current or not prior:
        return None
    from app.services.sec_reporting_scope import comparable
    if not comparable(current, prior):
        return None
    needed = {(date.fromisoformat(p['start']), date.fromisoformat(p['end']))
              for point in (current, prior) for p in point['inputs']}
    rows = []
    for key in sorted(needed):
        left, right = revenue.get(key), costs.get(key)
        if not left or not right or any(left.get(k) != right.get(k) for k in IDENTITY):
            return None
        if any(type(p.get('value')) not in (int, float) or not math.isfinite(p['value']) for p in (left, right)) or right['value'] < 0:
            return None
        filing = filings.get(left['accession'])
        if not filing or filing['filingDate'] != left['filed'] or left['end'] > filing['reportDate']:
            return None
        rows.append(dict(revenue=left, costs=right, filing=filing))
    if not rows or len({r['filing']['url'] for r in rows}) > 3:
        return None
    return dict(cik=cik, period_end=str(end), rows=rows)


def qualify(expected, documents, *, company):
    from app.services.sec_primary_operating_subtotal import qualify as original
    filings = {r['filing']['url']: r['filing'] for r in expected['rows']}
    if set(documents) != set(filings):
        raise ValueError('primary_operating_source_set')
    verified = {}
    for url, filing in filings.items():
        raw = documents[url]
        source = dict(filing, sha256=hashlib.sha256(raw).hexdigest(), symbol='', role='operating_history')
        proof = original(raw, source, company)
        for period in proof['proof']['periods']:
            verified[(period['start'], period['end'], filing['accessionNumber'])] = (period, proof, source['sha256'])
    rows = []
    for item in expected['rows']:
        point = item['revenue']
        found = verified.get((point['start'], point['end'], point['accession']))
        if not found:
            raise ValueError('primary_operating_period_missing')
        period, proof, digest = found
        if period['revenue']['value'] != point['value'] or period['cost_total']['value'] != item['costs']['value']:
            raise ValueError('primary_operating_source_conflict')
        rows.append({**{k: point[k] for k in IDENTITY}, 'value': period['operating_subtotal'],
                     'source_cik': expected['cik'], 'source_url': item['filing']['url'], 'source_sha256': digest,
                     'basis': BASIS, 'statement_heading': proof['proof']['headings'],
                     'primary_reconciliation': deepcopy(period), 'geometry': proof['geometry']})
    return dict(methodology=VERSION, cik=expected['cik'], period_end=expected['period_end'], rows=rows)


def acquire(payload, company, *, cik, available_by, client):
    expected = plan(payload, company, cik=cik, available_by=available_by)
    if expected is None:
        return None
    documents = {r['filing']['url']: None for r in expected['rows']}
    for url in documents:
        documents[url] = client.get(url)
    return dict(documents=documents, evidence=qualify(expected, documents, company=company))


def apply(records, payload, company, *, cik, available_by, supplement):
    from app.services.sec_inline_financials import InlineFactsError
    item = supplement['operating_history']
    expected = plan(payload, company, cik=cik, available_by=available_by)
    if (expected is None or qualify(expected, item['documents'], company=company) != item['evidence']
            or supplement['evidence'].get('operating_statement_history') != item['evidence']):
        raise InlineFactsError('primary_operating_proof_mismatch')
    if records.get('operating_income'):
        return records
    operating = {}
    for proof in item['evidence']['rows']:
        key = (date.fromisoformat(proof['start']), date.fromisoformat(proof['end']))
        operating[key] = {**{k: proof[k] for k in IDENTITY}, 'value': proof['value'],
                          'tag': 'derived_primary_operating_income', 'priority': 0, 'derived': True,
                          'source_kind': VERSION, 'source_cik': cik, 'source_url': proof['source_url'],
                          'source_sha256': proof['source_sha256'], 'original_statement': deepcopy(proof)}
    return {**records, 'operating_income': operating}


def cached_valid(proof, *, cik):
    """Check persisted reconciliation structure; source replay remains mandatory."""
    from decimal import Decimal
    from app.services.sec_statement_currency import source_identity
    from app.services.sec_primary_operating_subtotal import revenue_tags, pretax
    allowed = {'OperatingCostsAndExpenses', 'OtherCostAndExpenseOperating', 'DepreciationDepletionAndAmortization',
               'GeneralAndAdministrativeExpense', 'ProductionTaxExpense', 'AssetImpairmentCharges',
               'BusinessCombinationAcquisitionRelatedCosts', 'ManagementFeeExpense'}
    finite = lambda v: type(v) in (int, float) and math.isfinite(v)
    def value(fact, row):
        if (not finite(fact['value']) or fact['start'] != row['start'] or fact['end'] != row['end']
                or type(fact['row']) is not int or fact['row'] < 0 or not fact['context']):
            raise ValueError('invalid_component')
        return Decimal(str(fact['value']))
    try:
        if proof['methodology'] != VERSION or proof['cik'] != cik or not 1 <= len(proof['rows']) <= 12:
            return False
        end = date.fromisoformat(proof['period_end']); seen = set(); sources = {}
        for row in proof['rows']:
            key = (row['start'], row['end'])
            start, stop = map(date.fromisoformat, key)
            if (key in seen or not 1 <= (stop-start).days+1 <= 380 or stop > end or stop > date.fromisoformat(row['filed'])
                    or source_identity(row['source_url']) != (cik, row['accession']) or row['source_cik'] != cik
                    or row['unit'] != 'USD' or row['basis'] != BASIS or not finite(row['value'])
                    or not re.fullmatch(r'[0-9a-f]{64}', row['source_sha256'])
                    or not isinstance(row['statement_heading'], list) or not row['statement_heading']
                    or any(not isinstance(h, str) or not h.strip() for h in row['statement_heading'])):
                return False
            seen.add(key)
            if row['source_url'] in sources and sources[row['source_url']] != row['source_sha256']:
                return False
            sources[row['source_url']] = row['source_sha256']
            p = row['primary_reconciliation']; rev, cost, expenses = p['revenue'], p['cost_total'], p['expenses']
            if (p['start'] != row['start'] or p['end'] != row['end'] or p['operating_subtotal'] != row['value']
                    or rev['tag'] not in {'us-gaap:'+t for t in revenue_tags} or rev['dimensions']
                    or cost['tag'] != 'us-gaap:CostsAndExpenses' or cost['dimensions']
                    or not expenses or len({x['row'] for x in expenses}) != len(expenses)):
                return False
            if any(x['tag'] not in {'us-gaap:'+t for t in allowed} or not rev['row'] < x['row'] < cost['row'] for x in expenses):
                return False
            total = value(cost, row); derived = value(rev, row)-total
            if total < 0 or sum((value(x, row) for x in expenses)) != total or derived != Decimal(str(row['value'])):
                return False
            bridge = p['bridge']
            if set(bridge) == {'net_income'}:
                net = bridge['net_income']
                if net['tag'] != 'us-gaap:NetIncomeLoss' or net['dimensions'] or net['row'] <= cost['row'] or value(net, row) != derived:
                    return False
            elif set(bridge) == {'nonoperating', 'components', 'pretax'}:
                nonop, pre, components = bridge['nonoperating'], bridge['pretax'], bridge['components']
                if (nonop['tag'] != 'us-gaap:NonoperatingIncomeExpense' or pre['tag'] != 'us-gaap:'+pretax
                        or nonop['dimensions'] or pre['dimensions'] or not cost['row'] < nonop['row'] < pre['row']
                        or not components or any(not cost['row'] < x['row'] < nonop['row'] for x in components)
                        or len({x['row'] for x in components}) != len(components)
                        or sum((value(x, row) for x in components)) != value(nonop, row)
                        or derived+value(nonop, row) != value(pre, row)):
                    return False
            else:
                return False
            geometry = row['geometry']
            if not geometry['checked_numeric_cells'] > 0 or geometry['sourcewide_matching_facts'] < geometry['checked_numeric_cells']:
                return False
        return len(sources) <= 3
    except (KeyError, ValueError, TypeError, AttributeError, OverflowError):
        return False
