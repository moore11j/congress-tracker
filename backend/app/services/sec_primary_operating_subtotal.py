"""Offline qualification of primary-statement operating subtotals.

Pure retained-source evaluation only. No acquisition, cache merge, reader or
score integration. Derived subtotals remain candidates until consumer review.
Every guard uses exceptions and remains active under optimized Python.
"""
from datetime import date
from decimal import Decimal
import hashlib
import re
from lxml import html
from app.services.sec_inline_financials import parse, nodes, _number, _nil_fact, MAX_INLINE_BYTES
from app.services.sec_tabular_financials import preceding_text, company_key, grid, period_for, amount
from app.services.sec_revenue_statement_layout import _visible

VERSION = 'sec_primary_operating_subtotal_candidate_v1'
class SubtotalQualificationError(ValueError):
    """The complete retained original does not support this narrow derivation."""

revenue_tags = {'RevenueFromContractWithCustomerExcludingAssessedTax', 'RevenueFromContractWithCustomerIncludingAssessedTax', 'Revenues'}
pretax = 'IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest'
wanted = revenue_tags | {'CostsAndExpenses', 'NonoperatingIncomeExpense', pretax, 'NetIncomeLoss'}

def qualify(raw, s, company):
    if not isinstance(raw, bytes) or not raw or len(raw) > MAX_INLINE_BYTES:
        raise SubtotalQualificationError('filing_size')
    required = {'sha256', 'cik', 'accessionNumber', 'filingDate', 'reportDate', 'form', 'url', 'primaryDocument', 'symbol', 'role'}
    if not isinstance(s, dict) or not required <= s.keys() or not isinstance(company, dict) or not {'cik', 'name'} <= company.keys():
        raise SubtotalQualificationError('source_metadata')
    if not re.fullmatch(r'[0-9a-f]{64}', str(s['sha256'])) or not re.fullmatch(r'[0-9]{10}', str(s['cik'])):
        raise SubtotalQualificationError('source_identity')
    if (not re.fullmatch(r'[0-9]{10}-[0-9]{2}-[0-9]{6}', str(s['accessionNumber']))
            or not re.fullmatch(r'[A-Za-z0-9_.-]+\.(?:htm|html)', str(s['primaryDocument']))
            or s['form'] not in {'10-Q', '10-K', '10-Q/A', '10-K/A'}):
        raise SubtotalQualificationError('source_filing')
    expected_url = (f"https://www.sec.gov/Archives/edgar/data/{int(s['cik'])}/"
                    f"{s['accessionNumber'].replace('-', '')}/{s['primaryDocument']}")
    if s['url'] != expected_url:
        raise SubtotalQualificationError('source_url')
    if date.fromisoformat(s['filingDate']) < date.fromisoformat(s['reportDate']):
        raise SubtotalQualificationError('filing_before_report')
    if not hashlib.sha256(raw).hexdigest() == s['sha256']:
        raise SubtotalQualificationError('source_checksum')
    if not str(company['cik']).zfill(10) == s['cik']:
        raise SubtotalQualificationError('company_identity')
    parse(raw, cik=s['cik'], accession=s['accessionNumber'], filed=s['filingDate'], report_date=s['reportDate'], form=s['form'], url=s['url'], basis={'namespace': 'us-gaap', 'currency': 'USD', 'tags': {'revenue': tuple(revenue_tags), 'subtotal': tuple(wanted)}})
    tree = html.fromstring(raw, parser=html.HTMLParser(encoding='utf-8'))
    contexts = {}
    usd = set()
    for c in nodes(tree, 'context'):
        ids, starts, ends = (nodes(c, 'identifier'), nodes(c, 'startdate'), nodes(c, 'enddate'))
        if len(ids) == len(starts) == len(ends) == 1 and ids[0].get('scheme') == 'http://www.sec.gov/CIK' and (ids[0].text_content().strip().zfill(10) == s['cik']):
            contexts[c.get('id')] = dict(start=starts[0].text_content(), end=ends[0].text_content(), dimensions=bool(nodes(c, 'segment') or nodes(c, 'scenario')))
    for u in nodes(tree, 'unit'):
        if not nodes(u, 'divide') and [n.text_content().strip() for n in nodes(u, 'measure')] == ['iso4217:USD']:
            usd.add(u.get('id'))
    candidates = []
    for table in tree.iter('table'):
        if table.xpath('.//table|ancestor::blockquote') or not _visible(table):
            continue
        if not any((n.get('name') == 'us-gaap:CostsAndExpenses' for n in nodes(table, 'nonfraction'))):
            continue
        headings = preceding_text(table, nonempty=True)[-5:]
        titles = [i for i, h in enumerate(headings) if re.fullmatch('(?:CONDENSED )?CONSOLIDATED (?:INCOME STATEMENTS|STATEMENTS OF OPERATIONS)(?: \\(unaudited\\))?', h)]
        if len(titles) != 1:
            continue
        owners = [i for i, h in enumerate(headings[:titles[0]]) if company_key(h) == company_key(company['name'])]
        if len(owners) != 1:
            continue
        if any((not re.fullmatch('\\(A [A-Za-z ]+ Limited Partnership\\)', h) for h in headings[owners[0] + 1:titles[0]])):
            continue
        if not any(('thousands' in h.lower() for h in headings[titles[0] + 1:])):
            continue
        rows = table.xpath('./tr|./tbody/tr|./thead/tr|./tfoot/tr')
        facts = []
        for i, row in enumerate(rows):
            for n in nodes(row, 'nonfraction'):
                c = contexts.get(n.get('contextref'))
                if c is None or n.get('unitref') not in usd:
                    continue
                if not (_visible(n) and (not _nil_fact(n)) and (n.get('scale') == '3')):
                    raise SubtotalQualificationError('statement_reconciliation')
                facts.append(dict(row=i, tag=n.get('name'), context=n.get('contextref'), value=_number(n, tree), label=' '.join(row.text_content().split())[:250], **c))
        if any((f['tag'] == 'us-gaap:CostsAndExpenses' for f in facts)):
            candidates.append((headings, rows, facts))
    if not len(candidates) == 1:
        raise SubtotalQualificationError((s['symbol'], s['role'], 'primary owner/layout', len(candidates)))
    headings, rows, facts = candidates[0]
    proofs = []
    for period in sorted({(f['start'], f['end']) for f in facts if f['tag'] == 'us-gaap:CostsAndExpenses' and (not f['dimensions'])}):
        start, end = map(date.fromisoformat, period)
        if not 1 <= (end - start).days + 1 <= 380 or end > date.fromisoformat(s['reportDate']):
            raise SubtotalQualificationError('period_outside_filing')
        selected = [f for f in facts if (f['start'], f['end']) == period]

        def one(tags):
            values = [f for f in selected if f['tag'].split(':')[-1] in tags and (not f['dimensions'])]
            if not len(values) == 1:
                raise SubtotalQualificationError((s['symbol'], s['role'], period, tags, len(values)))
            return values[0]
        rev = one(revenue_tags)
        cost = one({'CostsAndExpenses'})
        expenses = [f for f in selected if rev['row'] < f['row'] < cost['row']]
        if not (expenses and len({f['row'] for f in expenses}) == len(expenses)):
            raise SubtotalQualificationError('statement_reconciliation')
        allowed = {'OperatingCostsAndExpenses', 'OtherCostAndExpenseOperating', 'DepreciationDepletionAndAmortization', 'GeneralAndAdministrativeExpense', 'ProductionTaxExpense', 'AssetImpairmentCharges', 'BusinessCombinationAcquisitionRelatedCosts', 'ManagementFeeExpense'}
        if not all((f['tag'] in {'us-gaap:' + t for t in allowed} for f in expenses)):
            raise SubtotalQualificationError('unsupported_expense_inventory')
        if not sum((Decimal(str(f['value'])) for f in expenses)) == Decimal(str(cost['value'])):
            raise SubtotalQualificationError('statement_reconciliation')
        derived = Decimal(str(rev['value'])) - Decimal(str(cost['value']))
        after = [f for f in selected if f['row'] > cost['row']]
        net = next((f for f in after if f['tag'] == 'us-gaap:NetIncomeLoss'), None)
        if net and min((f['row'] for f in after)) == net['row']:
            if not derived == Decimal(str(net['value'])):
                raise SubtotalQualificationError('statement_reconciliation')
            bridge = {'net_income': net}
        else:
            nonop = one({'NonoperatingIncomeExpense'})
            pre = one({pretax})
            components = [f for f in after if f['row'] < nonop['row']]
            if not (components and len({f['row'] for f in components}) == len(components)):
                raise SubtotalQualificationError('statement_reconciliation')
            if not sum((Decimal(str(f['value'])) for f in components)) == Decimal(str(nonop['value'])):
                raise SubtotalQualificationError('statement_reconciliation')
            if not derived + Decimal(str(nonop['value'])) == Decimal(str(pre['value'])):
                raise SubtotalQualificationError('statement_reconciliation')
            if any((nonop['row'] < f['row'] < pre['row'] for f in after)):
                raise SubtotalQualificationError('statement_reconciliation')
            bridge = {'nonoperating': nonop, 'components': components, 'pretax': pre}
        proofs.append(dict(start=period[0], end=period[1], operating_subtotal=float(derived), revenue=rev, expenses=expenses, cost_total=cost, bridge=bridge))
    if not proofs:
        raise SubtotalQualificationError('no_consolidated_periods')
    result = dict(symbol=s['symbol'], role=s['role'], url=s['url'], sha256=s['sha256'], bytes=len(raw), headings=headings, periods=proofs)
    geometry = _check_geometry(raw, s, result)
    return dict(proof=result, geometry=geometry)

def _check_geometry(raw, s, p):
    tree = html.fromstring(raw, parser=html.HTMLParser(encoding='utf-8'))
    tables = [t for t in tree.iter('table') if not t.xpath('.//table') and preceding_text(t, nonempty=True)[-5:] == p['headings'] and any((n.get('name') == 'us-gaap:CostsAndExpenses' for n in nodes(t, 'nonfraction')))]
    if not len(tables) == 1:
        raise SubtotalQualificationError('primary_table')
    table = tables[0]
    rows = grid(table)
    elements = table.xpath('./tr|./tbody/tr|./thead/tr|./tfoot/tr')
    if table.xpath('.//*[@rowspan]'):
        raise SubtotalQualificationError('unsupported_rowspan')
    contexts = {}
    usd = set()
    for c in nodes(tree, 'context'):
        key = c.get('id')
        if not (key and key not in contexts):
            raise SubtotalQualificationError('duplicate_context')
        contexts[key] = None
        ids, starts, ends = (nodes(c, 'identifier'), nodes(c, 'startdate'), nodes(c, 'enddate'))
        if len(ids) == len(starts) == len(ends) == 1 and ids[0].get('scheme') == 'http://www.sec.gov/CIK' and (ids[0].text_content().strip().zfill(10) == s['cik']):
            if nodes(c, 'scenario') or any((n.tag == 'xbrldi:typedmember' for n in c.iter())):
                continue
            members = tuple(sorted(((n.get('dimension'), n.text_content().strip()) for n in c.iter() if n.tag == 'xbrldi:explicitmember')))
            contexts[key] = (starts[0].text_content().strip(), ends[0].text_content().strip(), members)
    for u in nodes(tree, 'unit'):
        if not nodes(u, 'divide') and [n.text_content().strip() for n in nodes(u, 'measure')] == ['iso4217:USD']:
            usd.add(u.get('id'))
    first = min((i for i, r in enumerate(elements) if nodes(r, 'nonfraction')))
    headers = rows[:first]
    anchors = [(i, [c for c in row if re.fullmatch('20[0-9]{2}', c['text'])]) for i, row in enumerate(headers)]
    anchors = [(i, a) for i, a in anchors if len(a) >= 2]
    if not len(anchors) == 1:
        raise SubtotalQualificationError('year_anchors')
    columns = []
    for anchor in anchors[0][1]:
        text = ' '.join((c['text'] for row in headers for c in row if c['start'] <= anchor['start'] < c['end'] and c['text']))
        shared = [h for h in p['headings'] if re.search('\\byears? ended\\b', h, re.I)]
        if shared:
            text += ' ' + ' '.join(shared)
        start, end, months = period_for(text, 'income', date.fromisoformat(s['filingDate']))
        columns.append(dict(start=anchor['start'], end=anchor['end'], period=(str(start), str(end)), header=text))
    if not len({c['period'] for c in columns}) == len(columns):
        raise SubtotalQualificationError('duplicate_columns')
    count = 0
    checks = []
    source_expected = {}
    for period in p['periods']:
        key = (period['start'], period['end'])
        col = [c for c in columns if c['period'] == key]
        if not len(col) == 1:
            raise SubtotalQualificationError('period_column')
        col = col[0]
        bridge = period['bridge']
        endpoint = bridge.get('pretax', bridge.get('net_income'))
        expected = [period['revenue'], period['cost_total'], *period['expenses']]
        expected += [bridge['net_income']] if 'net_income' in bridge else [bridge['nonoperating'], bridge['pretax'], *bridge['components']]
        byrow = {f['row']: f for f in expected}
        if not len(byrow) == len(expected):
            raise SubtotalQualificationError('duplicate_row')
        for ri in range(period['revenue']['row'], endpoint['row'] + 1):
            row = rows[ri]
            cells = elements[ri].xpath('./td|./th')
            if not len(cells) == len(row):
                raise SubtotalQualificationError('row_geometry')
            inside = [(cell, c) for cell, c in zip(cells, row) if col['start'] <= c['start'] < col['end']]
            if not all((c['end'] <= col['end'] for cell, c in inside)):
                raise SubtotalQualificationError('cell_crosses_period')
            selected = [n for cell, c in inside for n in nodes(cell, 'nonfraction')]
            text = ''.join((c['text'] for cell, c in inside)).replace('$', '').strip()
            if ri not in byrow:
                if not (not selected and text == ''):
                    raise SubtotalQualificationError('unaccounted_numeric_cell')
                continue
            f = byrow[ri]
            if not len(selected) == 1:
                raise SubtotalQualificationError('one_tag_per_amount')
            n = selected[0]
            if not (_visible(n) and (not _nil_fact(n))):
                raise SubtotalQualificationError('hidden_or_nil')
            ctx = contexts.get(n.get('contextref'))
            if not (ctx and ctx[:2] == key and (n.get('unitref') in usd)):
                raise SubtotalQualificationError('context_column_mismatch')
            if ctx[2]:
                if not (f in period['expenses'] and f['tag'] == 'us-gaap:GeneralAndAdministrativeExpense'):
                    raise SubtotalQualificationError('dimension_on_total')
                member = 'RelatedPartyMember' if row[0]['text'] == 'General and administrative expenses - related party' else 'NonrelatedPartyMember' if row[0]['text'] == 'General and administrative expenses' else None
                if not (member and ctx[2] == (('us-gaap:RelatedPartyTransactionsByRelatedPartyAxis', 'us-gaap:' + member),)):
                    raise SubtotalQualificationError('unsupported_component_dimension')
                if not re.fullmatch('https?://fasb.org/us-gaap/20[0-9]{2}', tree.get('xmlns:us-gaap', '')):
                    raise SubtotalQualificationError('dimension_namespace')
                if any((x.get('xmlns:us-gaap', tree.get('xmlns:us-gaap')) != tree.get('xmlns:us-gaap') for x in tree.iter() if isinstance(x.tag, str))):
                    raise SubtotalQualificationError('rebound_dimension_namespace')
            if not (n.get('name') == f['tag'] and _number(n, tree) == f['value']):
                raise SubtotalQualificationError('fact_mismatch')
            printed = amount(row, col['start'], col['end'], Decimal(1000))
            if printed is None:
                if not (text in {'-', '–', '—'} and f['value'] == 0):
                    raise SubtotalQualificationError('unproved_printed_zero')
            elif not printed == f['value']:
                raise SubtotalQualificationError('printed_value_sign')
            source_expected[f['tag'], ctx] = f['value']
            count += 1
        dimensional = [contexts[f['context']][2] for f in period['expenses'] if contexts.get(f['context']) and contexts[f['context']][2]]
        if dimensional:
            if not (set(dimensional) == {(('us-gaap:RelatedPartyTransactionsByRelatedPartyAxis', 'us-gaap:RelatedPartyMember'),), (('us-gaap:RelatedPartyTransactionsByRelatedPartyAxis', 'us-gaap:NonrelatedPartyMember'),)} and len(dimensional) == 2):
                raise SubtotalQualificationError('component_pair')
        checks.append(dict(start=key[0], end=key[1], header=col['header'], checked_rows=len(expected)))
    duplicates = 0
    for n in nodes(tree, 'nonfraction'):
        key = (n.get('name'), contexts.get(n.get('contextref')))
        if key not in source_expected or n.get('unitref') not in usd:
            continue
        if not (not _nil_fact(n) and _number(n, tree) == source_expected[key]):
            raise SubtotalQualificationError('sourcewide_conflict')
        duplicates += 1
    return dict(symbol=s['symbol'], role=s['role'], sha256=s['sha256'], periods=checks, checked_numeric_cells=count, sourcewide_matching_facts=duplicates)
