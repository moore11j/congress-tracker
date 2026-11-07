"""Rental income is revenue only when complete primary statements prove it.

Never treat a lease-income component or a note table as total revenue. Every
input in the selected current/prior history must have its own original proof.
"""
from datetime import date
from copy import deepcopy
import hashlib
import math
import re
from lxml import html

VERSION = 'sec_single_rental_revenue_v1'
CONCEPT = 'OperatingLeaseLeaseIncome'
FORMS = {'10-K', '10-Q', '10-K/A', '10-Q/A'}
IDENTITY = ('value', 'unit', 'filed', 'accession')
TITLE = re.compile(r'(?:condensed )?consolidated statements? of (?:operations|income|earnings)(?: and comprehensive income)?', re.I)


def plan(payload, company, *, cik, available_by):
    from app.services import sec_fundamentals as sec
    if any(str(obj.get('cik', '')).zfill(10) != cik for obj in (payload, company)):
        raise ValueError('rental_revenue_identity')
    if CONCEPT not in payload.get('facts', {}).get('us-gaap', {}):
        return None
    recent = company.get('filings', {}).get('recent', {})
    columns = [recent.get(k, []) for k in ('accessionNumber', 'form', 'filingDate', 'reportDate', 'primaryDocument')]
    if len({len(c) for c in columns}) != 1:
        raise ValueError('rental_revenue_directory_shape')
    filings = {}
    for accession, form, filed, end, filename in zip(*columns):
        if form not in FORMS or not sec._day(end) or not sec._day(filed) or not end <= filed <= str(available_by):
            continue
        if not re.fullmatch(r'\d{10}-\d{2}-\d{6}', accession) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,180}\.html?', filename):
            raise ValueError('rental_revenue_filing_identity')
        row = dict(cik=cik, accession=accession, form=form, filed=filed, report_date=end,
                   url=f'https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession.replace("-", "")}/{filename}')
        if accession in filings and filings[accession] != row:
            raise ValueError('rental_revenue_ambiguous_directory')
        filings[accession] = row
    if not filings:
        return None
    latest = max((f['report_date'], f['filed']) for f in filings.values())
    closing = [f for f in filings.values() if (f['report_date'], f['filed']) == latest]
    if len(closing) != 1:
        raise ValueError('rental_revenue_ambiguous_latest')
    end = date.fromisoformat(latest[0])
    if not 0 <= (available_by-end).days <= 180:
        return None
    ordinary = sec._records(payload, 'revenue', available_by)
    # A current supported total retains priority, even if rental income is also
    # available. No existing total is silently replaced by a component.
    if sec._ttm(ordinary, end) is not None:
        return None
    candidate = sec._records(payload, 'revenue', available_by, concepts=(CONCEPT,))
    current = sec._ttm(candidate, end)
    if not current or not any(p['end'] == str(end) and p['accession'] == closing[0]['accession']
                             and p['filed'] == closing[0]['filed'] for p in current['inputs']):
        return None
    prior_end = sec._prior_end(candidate, end)
    prior = sec._ttm(candidate, prior_end) if prior_end else None
    needed = {(p['start'], p['end']): p for period in (current, prior) if period for p in period['inputs']}
    for (start, finish), point in needed.items():
        key = (date.fromisoformat(start), date.fromisoformat(finish))
        if key in ordinary and (not ordinary[key] or any(ordinary[key][k] != point[k] for k in IDENTITY)):
            raise ValueError('rental_revenue_total_conflict')
    required = {p['accession'] for p in needed.values()}
    if len(required) > 6 or not required <= filings.keys():
        raise ValueError('rental_revenue_originals_unavailable')
    return dict(end=str(end), needed=needed, candidate=candidate, floor=min(start for start, _ in needed),
                filings={a: filings[a] for a in sorted(required)})


def statement_rows(raw, filing, parsed=None):
    from app.services import sec_inline_financials as inline
    from app.services.sec_tabular_financials import preceding_text, clean
    parsed = parsed or inline.parse(raw, **filing)
    evidence = parsed['evidence']
    if (evidence.get('sha256') != hashlib.sha256(raw).hexdigest()
            or any(evidence.get(k) != filing[k] for k in ('cik', 'accession', 'filed', 'report_date', 'form'))
            or evidence.get('currency') != 'USD' or evidence.get('namespace') != 'us-gaap'):
        raise ValueError('rental_revenue_original_identity')
    tree = html.fromstring(raw)
    contexts = {}
    for node in inline.nodes(tree, 'context'):
        if inline.nodes(node, 'segment') or inline.nodes(node, 'scenario'):
            continue
        ids = inline.nodes(node, 'identifier')
        starts, ends = inline.nodes(node, 'startdate'), inline.nodes(node, 'enddate')
        if (len(ids) == len(starts) == len(ends) == 1 and ids[0].get('scheme') == 'http://www.sec.gov/CIK'
                and ids[0].text_content().strip().zfill(10) == filing['cik']):
            contexts[node.get('id')] = (starts[0].text_content().strip(), ends[0].text_content().strip())
    units = {n.get('id') for n in inline.nodes(tree, 'unit')
             if [x.text_content().strip() for x in inline.nodes(n, 'measure')] == ['iso4217:USD']
             and not inline.nodes(n, 'divide')}
    accepted = {}; tables = {}
    for node in inline.nodes(tree, 'nonfraction'):
        if (node.get('name') == 'us-gaap:'+CONCEPT and node.get('contextref') in contexts
                and node.get('unitref') in units):
            parents = node.xpath('ancestor::table')
            if parents and not parents[-1].xpath('.//table'):
                tables[id(parents[-1])] = parents[-1]
    for table in tables.values():
        headings = preceding_text(table)[-6:]
        if (not any(TITLE.fullmatch(h) for h in headings)
                or any(re.search(r'non.?gaap|adjusted|reconciliation|pro.?forma|notes to', h, re.I) for h in headings)):
            continue
        rows = []
        for row in table.xpath('./tr|./tbody/tr|./thead/tr'):
            cells = row.xpath('./td|./th')
            text = clean(' '.join(row.itertext()))
            label = next((clean(' '.join(c.itertext())).lower().rstrip(':') for c in cells if clean(' '.join(c.itertext()))), '')
            if text:
                rows.append((label, text.lower().rstrip(':'), row))
        starts = [i for i, (_, text, _) in enumerate(rows) if text == 'revenues']
        ends = [i for i, (_, text, _) in enumerate(rows) if text == 'expenses']
        if len(starts) != 1 or len(ends) != 1 or ends[0] != starts[0]+2:
            continue
        label, _, row = rows[starts[0]+1]
        if label != 'rental income':
            continue
        for node in inline.nodes(row, 'nonfraction'):
            if node.get('name') != 'us-gaap:'+CONCEPT or node.get('unitref') not in units:
                continue
            period = contexts.get(node.get('contextref'))
            if not period or any(a.tag == 'ix:hidden' for a in node.iterancestors()):
                continue
            if inline._nil_fact(node):
                raise ValueError('rental_revenue_nil')
            value = inline._number(node, tree)
            if value < 0:
                raise ValueError('rental_revenue_negative')
            proof = dict(start=period[0], end=period[1], value=value, unit='USD',
                accession=filing['accession'], filed=filing['filed'], context_id=node.get('contextref'),
                row_label=label, revenue_section=['rental income'], statement_heading=headings,
                source_url=filing['url'], source_sha256=evidence['sha256'], source_cik=filing['cik'])
            if period in accepted and accepted[period]['value'] != value:
                raise ValueError('rental_revenue_statement_conflict')
            accepted[period] = proof
    return accepted


def acquire(payload, company, *, cik, available_by, client, originals=None):
    wanted = plan(payload, company, cik=cik, available_by=available_by)
    if wanted is None:
        return None
    verified = {}
    for filing in wanted['filings'].values():
        raw = client.get(filing['url'])
        for key, proof in statement_rows(raw, filing, (originals or {}).get(filing['url'])).items():
            verified[key+(filing['accession'],)] = proof
    selected = []
    for key, point in sorted(wanted['needed'].items()):
        proof = verified.get(key+(point['accession'],))
        if not proof or any(proof[k] != point[k] for k in IDENTITY):
            raise ValueError('rental_revenue_primary_statement_missing')
    for (start, finish), point in sorted(wanted['candidate'].items()):
        if not point or str(start) < wanted['floor']:
            continue
        proof = verified.get((str(start), str(finish), point['accession']))
        if proof and all(proof[k] == point[k] for k in IDENTITY):
            selected.append({**point, 'original_statement': proof})
    return dict(methodology=VERSION, cik=cik, period_end=wanted['end'], selected_concept=CONCEPT,
                basis_spliced=False, rows=selected,
                basis='Rental income is the sole line in the explicit revenue section of each original consolidated income statement.')


def apply(records, payload, company, *, cik, available_by, supplement, preserve_reported_history=False):
    proof = (supplement or {}).get('evidence', {}).get('rental_revenue_selection')
    if proof is None:
        return records
    if not cached_valid(proof, cik=cik):
        raise ValueError('rental_revenue_cache_proof')
    wanted = plan(payload, company, cik=cik, available_by=available_by)
    if (wanted is None or proof.get('methodology') != VERSION or proof.get('cik') != cik
            or proof.get('period_end') != wanted['end'] or proof.get('selected_concept') != CONCEPT
            or proof.get('basis_spliced') is not False):
        raise ValueError('rental_revenue_proof_mismatch')
    selected = {}
    for point in proof.get('rows', []):
        key = (point.get('start'), point.get('end'))
        expected = wanted['candidate'].get((date.fromisoformat(key[0]), date.fromisoformat(key[1])))
        original = point.get('original_statement') or {}
        if (key in selected or not expected or key[0] < wanted['floor'] or point.get('tag') != CONCEPT
                or any(point.get(k) != expected[k] or original.get(k) != expected[k] for k in IDENTITY)
                or original.get('source_cik') != cik or original.get('row_label') != 'rental income'
                or original.get('revenue_section') != ['rental income']
                or not any(TITLE.fullmatch(h) for h in original.get('statement_heading', []))
                or any(original.get(k) != point[k] for k in ('start', 'end'))
                or original.get('source_url') != wanted['filings'].get(point['accession'], {}).get('url')
                or not re.fullmatch(r'[a-f0-9]{64}', original.get('source_sha256', ''))):
            raise ValueError('rental_revenue_row_mismatch')
        selected[key] = deepcopy(point)
    if not set(wanted['needed']) <= set(selected):
        raise ValueError('rental_revenue_period_missing')
    revenue = {(date.fromisoformat(start), date.fromisoformat(end)): point for (start, end), point in selected.items()}
    if preserve_reported_history:
        for key, point in records.get('revenue', {}).items():
            revenue.setdefault(key, point)
    return {**records, 'revenue': revenue}


def cached_valid(proof, *, cik):
    from app.services.sec_statement_currency import source_identity
    from app.services import sec_fundamentals as sec
    try:
        if (proof['methodology'] != VERSION or proof['cik'] != cik or proof['basis_spliced'] is not False
                or proof['selected_concept'] != CONCEPT):
            return False
        end = date.fromisoformat(proof['period_end']); records = {}; hashes = {}
        for p in proof['rows']:
            original = p['original_statement']; key = (date.fromisoformat(p['start']), date.fromisoformat(p['end']))
            headings = original['statement_heading']
            if (key in records or not key[0] <= key[1] <= end or key[1] > date.fromisoformat(p['filed'])
                    or p['tag'] != CONCEPT or p['unit'] != 'USD' or type(p['value']) not in (int, float)
                    or not math.isfinite(p['value']) or p['value'] < 0
                    or any(original[k] != p[k] for k in IDENTITY+('start', 'end'))
                    or original['source_cik'] != cik or source_identity(original['source_url']) != (cik, p['accession'])
                    or not re.fullmatch(r'[a-f0-9]{64}', original['source_sha256'])
                    or not isinstance(original['context_id'], str) or not original['context_id']
                    or original['row_label'] != 'rental income' or original['revenue_section'] != ['rental income']
                    or not isinstance(headings, list) or not all(isinstance(h, str) for h in headings)
                    or not any(TITLE.fullmatch(h) for h in headings)
                    or any(re.search(r'non.?gaap|adjusted|reconciliation|pro.?forma|notes to', h, re.I) for h in headings)):
                return False
            url, digest = original['source_url'], original['source_sha256']
            if url in hashes and hashes[url] != digest:
                return False
            hashes[url] = digest; records[key] = p
        return 1 <= len(hashes) <= 6 and sec._ttm(records, end) is not None
    except (KeyError, ValueError, TypeError, AttributeError):
        return False
