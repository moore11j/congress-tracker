"""Recover missing operating subtotals from complete original statements.

Every value is calculated from gross profit and total operating expenses
in the same issuer-owned primary statement and matching inline columns. No ticker allowlist,
implicit zero, EBITDA inference, or override of an existing conflict.
"""
from datetime import date
from copy import deepcopy
import math
import re
from lxml import html

VERSION = 'sec_original_operating_history_v1'
IDENTITY = ('start', 'end', 'filed', 'accession', 'unit')
FORMS = {'10-K', '10-Q', '10-K/A', '10-Q/A'}


def plan(payload, company, *, cik, available_by):
    from app.services import sec_fundamentals as sec
    if any(str(x.get('cik', '')).zfill(10) != cik for x in (payload, company)):
        raise ValueError('operating_history_identity')
    recent = company.get('filings', {}).get('recent', {})
    keys = ('accessionNumber', 'form', 'filingDate', 'reportDate', 'primaryDocument')
    arrays = [recent.get(k, []) for k in keys]
    if len({len(a) for a in arrays}) != 1:
        raise ValueError('operating_history_directory')
    filings = {}
    for accession, form, filed, report, filename in zip(*arrays):
        if form not in FORMS or not sec._day(report) or not sec._day(filed) or not report <= filed <= str(available_by):
            continue
        if not re.fullmatch(r'\d{10}-\d{2}-\d{6}', accession) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,180}\.html?', filename):
            raise ValueError('operating_history_directory_identity')
        item = dict(cik=cik, accession=accession, form=form, filed=filed, report_date=report,
                    url=f'https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession.replace("-", "")}/{filename}')
        if accession in filings and filings[accession] != item:
            raise ValueError('operating_history_directory_conflict')
        filings[accession] = item
    if not filings:
        return None
    latest = max((f['report_date'], f['filed']) for f in filings.values())
    if sum((f['report_date'], f['filed']) == latest for f in filings.values()) != 1:
        return None
    # A subsequent foreign reporting basis cannot be supplemented with stale
    # domestic USD components, even while those components pass freshness.
    if any(form in {'20-F', '20-F/A', '40-F', '40-F/A', '6-K', '6-K/A'}
           and sec._day(report) and sec._day(filed)
           and latest[0] <= report <= filed <= str(available_by)
           for _, form, filed, report, _ in zip(*arrays)):
        return None
    end = date.fromisoformat(latest[0])
    if not 0 <= (available_by-end).days <= 180:
        return None
    revenue = sec._records(payload, 'revenue', available_by)
    current = sec._ttm(revenue, end)
    if not current:
        return None
    prior_end = sec._prior_end(revenue, end)
    prior = sec._ttm(revenue, prior_end) if prior_end else None
    operating = sec._records(payload, 'operating_income', available_by)
    if sec._ttm(operating, end) and (not prior or sec._ttm(operating, prior_end)):
        return None
    gross = sec._records(payload, 'gross_profit', available_by)
    expense = sec._records(payload, 'operating_income', available_by, concepts=('OperatingExpenses',))
    needed = {(date.fromisoformat(f['start']), date.fromisoformat(f['end']))
              for point in (current, prior) if point for f in point['inputs']}
    rows = []
    for key in sorted(needed):
        # Explicit missing/conflicting observations must not be resurrected.
        if key in operating:
            continue
        left, right = gross.get(key), expense.get(key)
        if not left or not right or any(left.get(k) != right.get(k) for k in IDENTITY):
            continue
        if any(type(p.get('value')) not in (int, float) or not math.isfinite(p['value']) for p in (left, right)) or right['value'] < 0:
            continue
        filing = filings.get(left['accession'])
        if not filing or filing['filed'] != left['filed'] or left['end'] > filing['report_date']:
            continue
        value = left['value']-right['value']
        if not math.isfinite(value):
            continue
        rows.append(dict(gross=left, expenses=right, value=value, filing=filing))
    if not rows or len({r['filing']['url'] for r in rows}) > 3:
        return None
    return {'cik':cik, 'period_end':str(end), 'rows':rows}


def statement_rows(raw, filing, *, issuer):
    from app.services import sec_inline_financials as inline, sec_tabular_financials as tables
    # Validate actual document identity, root namespaces, contexts and transforms.
    parsed = inline.parse(raw, **filing)
    tree = html.fromstring(raw)
    contexts = {}
    for n in inline.nodes(tree, 'context'):
        if inline.nodes(n, 'segment') or inline.nodes(n, 'scenario'):
            continue
        ids, starts, ends = (inline.nodes(n, k) for k in ('identifier', 'startdate', 'enddate'))
        if len(ids) == len(starts) == len(ends) == 1 and ids[0].get('scheme') == 'http://www.sec.gov/CIK' and ids[0].text_content().strip().zfill(10) == filing['cik']:
            contexts[n.get('id')] = (starts[0].text_content().strip(), ends[0].text_content().strip())
    units = {n.get('id') for n in inline.nodes(tree, 'unit')
             if [x.text_content().strip() for x in inline.nodes(n, 'measure')] == ['iso4217:USD'] and not inline.nodes(n, 'divide')}
    result = {}
    for table in tree.xpath('//table'):
        if table.xpath('.//table') or table.xpath('ancestor::blockquote'):
            continue
        try: geometry = tables.grid(table)
        except tables.TabularFinancialError: continue
        tagged = {}
        for row_index, row in enumerate(table.xpath('./tr|./tbody/tr|./thead/tr|./tfoot/tr')):
            cells = row.xpath('./td|./th')
            label = next((tables.visible_text(c).lower().rstrip(':') for c in cells if tables.visible_text(c)), '')
            positions = [c for c in geometry[row_index] if c['row'] == row_index]
            if len(positions) != len(cells): continue
            cell_positions = {id(c): pos for c, pos in zip(cells, positions)}
            for n in inline.nodes(row, 'nonfraction'):
                tag = n.get('name')
                labels = {'us-gaap:GrossProfit': {'gross profit'}, 'us-gaap:OperatingExpenses': {'total operating expenses'}}
                if tag not in labels or label not in labels[tag] or n.get('unitref') not in units or any(a.tag == 'ix:hidden' for a in n.iterancestors()):
                    continue
                period = contexts.get(n.get('contextref'))
                if period:
                    ancestors = n.xpath('ancestor::td|ancestor::th')
                    cell = ancestors[-1] if ancestors else None
                    pos = cell_positions.get(id(cell))
                    if pos is not None:
                        tagged.setdefault(period, {}).setdefault(tag, set()).add((inline._number(n, tree),pos['start'],pos['end'],row_index))
        if not any(set(v) == {'us-gaap:GrossProfit', 'us-gaap:OperatingExpenses'} for v in tagged.values()):
            continue
        # USD is established separately for each accepted column by the two
        # entity-wide inline inputs below, never by a bare dollar sign.
        try:
            panel = tables.parse_table(table, issuer=issuer, filed=date.fromisoformat(filing['filed']),
                currency_scope={'issuer':issuer, 'currency':'USD', 'basis':'same-column entity-wide gross profit and operating expense inline units'})
        except tables.TabularFinancialError:
            continue
        if not panel or panel['kind'] != 'income':
            continue
        for point in panel['observations']:
            if point['metric'] != 'gross_profit':
                continue
            period = (point['start'], point['end']);inputs = tagged.get(period, {})
            if set(inputs) != {'us-gaap:GrossProfit','us-gaap:OperatingExpenses'} or any(len(v) != 1 for v in inputs.values()):
                continue
            gross_node, expense_node = (next(iter(inputs[t])) for t in ('us-gaap:GrossProfit','us-gaap:OperatingExpenses'))
            if any(not point['column_start'] <= n[1] < n[2] <= point['column_end'] for n in (gross_node,expense_node)) or gross_node[3] >= expense_node[3]:
                continue
            gross, expenses = gross_node[0], expense_node[0]
            if point['value'] != gross or expenses < 0 or not math.isfinite(gross-expenses):
                continue
            expense_section = [r[0]['text'].lower().rstrip(':') for r in geometry[gross_node[3]+1:expense_node[3]] if r]
            if 'operating expenses' not in expense_section:
                continue
            # A printed subtotal remains authoritative even if its label is
            # not yet projected by the general statement reader. Unreadable
            # values cannot silently authorize a competing derived result.
            reported = []
            for row in geometry:
                if not row:
                    continue
                label = tables.clean(row[0]['text']).lower().rstrip(':')
                if (tables.metric_for(label, 'income', issuer) != 'operating_income'
                    and label not in {'operating income (loss)', 'operating profit',
                                      'operating profit (loss)', 'operating (loss) profit'}):
                    continue
                value = tables.amount(row, point['column_start'], point['column_end'], point['scale'])
                if value is None:
                    raise ValueError('operating_history_reported_subtotal_unreadable')
                reported.append(value)
            if reported and (len(reported)!=1 or reported[0] != gross-expenses):
                raise ValueError('operating_history_reported_subtotal_conflict')
            proof = {'start':period[0], 'end':period[1], 'value':gross-expenses, 'gross_profit':gross,
                     'operating_expenses':expenses, 'unit':'USD', 'filed':filing['filed'], 'accession':filing['accession'],
                     'source_cik':filing['cik'], 'source_url':filing['url'], 'source_sha256':parsed['evidence']['sha256'],
                     'row_labels':['Gross profit','Total operating expenses'], 'statement_heading':panel['heading'],
                     'reported_operating_subtotal':reported[0] if reported else None,
                     'basis':'Calculated operating result: reported gross profit less reported total operating expenses in one consolidated statement.'}
            if period in result and result[period] != proof:
                raise ValueError('operating_history_statement_conflict')
            result[period] = proof
    return result


def qualify(expected, documents, *, issuer):
    verified = {}
    for filing in {r['filing']['url']:r['filing'] for r in expected['rows']}.values():
        raw = documents[filing['url']]
        for period, proof in statement_rows(raw, filing, issuer=issuer).items():
            verified[period+(filing['accession'],)] = proof
    rows = []
    for item in expected['rows']:
        point = item['gross'];proof = verified.get((point['start'],point['end'],point['accession']))
        if not proof or any(proof[k] != point[k] for k in IDENTITY) or proof['value'] != item['value'] or proof['gross_profit'] != point['value'] or proof['operating_expenses'] != item['expenses']['value']:
            continue
        rows.append(proof)
    return {'methodology':VERSION, 'cik':expected['cik'], 'period_end':expected['period_end'], 'rows':rows}


def acquire(payload, company, *, cik, available_by, client):
    expected = plan(payload, company, cik=cik, available_by=available_by)
    if expected is None:
        return None
    documents = {r['filing']['url']:None for r in expected['rows']}
    for url in documents:
        documents[url] = client.get(url)
    proof = qualify(expected, documents, issuer=company['name'])
    return {'documents':documents, 'evidence':proof} if proof['rows'] else None


def apply(records, payload, company, *, cik, available_by, supplement):
    from app.services.sec_inline_financials import InlineFactsError
    item = (supplement or {}).get('operating_history')
    if not item or 'operating_income' not in records:
        return records
    expected = plan(payload, company, cik=cik, available_by=available_by)
    if expected is None or qualify(expected,item['documents'],issuer=company['name']) != item['evidence'] or (supplement.get('evidence') or {}).get('operating_statement_history') != item['evidence']:
        raise InlineFactsError('operating_history_proof_mismatch')
    result = dict(records);operating = dict(records['operating_income'])
    for proof in item['evidence']['rows']:
        key = (date.fromisoformat(proof['start']), date.fromisoformat(proof['end']))
        if key not in operating:
            operating[key] = {**{k:proof[k] for k in IDENTITY}, 'value':proof['value'],
                              'tag':'derived_original_operating_income', 'priority':0, 'derived':True, 'source_kind':'derived_original_operating_components',
                              'source_cik':cik, 'source_url':proof['source_url'], 'source_sha256':proof['source_sha256'],
                              'original_statement':deepcopy(proof)}
    result['operating_income'] = operating
    return result


def cached_valid(proof, *, cik):
    """Validate persisted proof structure, not a replacement for source replay."""
    from app.services.sec_statement_currency import source_identity
    try:
        if proof['methodology'] != VERSION or proof['cik'] != cik or not 1 <= len(proof['rows']) <= 12:
            return False
        end = date.fromisoformat(proof['period_end']);seen=set();hashes={}
        for row in proof['rows']:
            key=(row['start'],row['end'])
            if key in seen or not date.fromisoformat(row['start']) <= date.fromisoformat(row['end']) <= end or date.fromisoformat(row['end']) > date.fromisoformat(row['filed']):
                return False
            seen.add(key)
            if (source_identity(row['source_url']) != (cik,row['accession']) or row['source_cik'] != cik or row['unit'] != 'USD'
                or not re.fullmatch(r'[0-9a-f]{64}',row['source_sha256'])
                or row['row_labels'] != ['Gross profit','Total operating expenses']
                or not isinstance(row['statement_heading'],list) or not row['statement_heading']
                or any(not isinstance(h,str) or not h.strip() for h in row['statement_heading'])
                or any(type(row[k]) not in (int,float) or not math.isfinite(row[k]) for k in ('value','gross_profit','operating_expenses'))
                or row['operating_expenses'] < 0 or row['value'] != row['gross_profit']-row['operating_expenses']):
                return False
            if row['reported_operating_subtotal'] is not None and (type(row['reported_operating_subtotal']) not in (int,float) or row['reported_operating_subtotal'] != row['value']):
                return False
            if row['source_url'] in hashes and hashes[row['source_url']] != row['source_sha256']:
                return False
            hashes[row['source_url']]=row['source_sha256']
        return len(hashes) <= 3
    except (KeyError,ValueError,TypeError,AttributeError):
        return False
