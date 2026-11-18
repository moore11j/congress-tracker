"""Missing comparative revenue recovered from explicit primary-statement zeros.

An absent amount is never zero. Two bounded originals must show the printed
comparative cells, matching USD entity contexts, headers and expense equations.
"""
from copy import deepcopy
from datetime import date
from decimal import Decimal
import calendar
import re
from lxml import html

VERSION = 'sec_domestic_revenue_zeros_v1'
TAGS = {'Revenues', 'RevenueFromContractWithCustomerExcludingAssessedTax'}
TITLE = re.compile(r'(?:CONDENSED )?CONSOLIDATED STATEMENTS OF OPERATIONS(?: AND COMPREHENSIVE LOSS)?', re.I)
KINDS = 'sec_domestic_printed_revenue_zero'


def plan(payload, company, *, cik, available_by):
    from app.services import sec_fundamentals as sec
    if any(str(x.get('cik', '')).zfill(10) != cik for x in (payload, company)):
        raise ValueError('domestic_zero_identity')
    recent = company.get('filings', {}).get('recent', {})
    keys = ('accessionNumber', 'form', 'filingDate', 'reportDate', 'primaryDocument')
    arrays = [recent.get(k, []) for k in keys]
    if len({len(a) for a in arrays}) != 1:
        raise ValueError('domestic_zero_directory')
    filings = []
    for accession, form, filed, report, document in zip(*arrays):
        if form not in {'10-Q', '10-K', '10-Q/A', '10-K/A', '20-F', '40-F', '6-K'}:
            continue
        if not sec._day(report) or not sec._day(filed) or not report <= filed <= str(available_by):
            continue
        filings.append(dict(cik=cik, accession=accession, form=form, filed=filed,
            report_date=report, url=f'https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession.replace("-", "")}/{document}'))
    if not filings:
        return None
    latest = max(p['report_date'] for p in filings)
    closing = [p for p in filings if p['report_date'] == latest]
    end = date.fromisoformat(latest)
    if (len(closing) != 1 or closing[0]['form'] != '10-Q'
            or (end.month, end.day) not in {(3, 31), (6, 30), (9, 30)}
            or not 0 <= (available_by-end).days <= 180):
        return None
    records = sec._records(payload, 'revenue', available_by)
    current = sec._ttm(records, end)
    prior_end = end.replace(year=end.year-1)
    selected = {p['tag'] for p in records.values() if p}
    if not current or len(selected) != 1 or not selected <= TAGS or sec._ttm(records, prior_end):
        return None
    # This first layout supports calendar-year annual + YTD comparatives only.
    wanted = [(date(end.year-2, 1, 1), date(end.year-2, 12, 31)),
              (date(end.year-2, 1, 1), end.replace(year=end.year-2))]
    if any(key in records for key in wanted):
        return None  # Preserve reported values, including conflicts.
    rows = []
    for report, form, period in ((date(end.year-1, 12, 31), '10-K', wanted[0]),
                                (prior_end, '10-Q', wanted[1])):
        found = [p for p in filings if p['report_date'] == str(report)]
        if len(found) != 1 or found[0]['form'] != form:
            return None
        filing = found[0]
        if (not re.fullmatch(r'\d{10}-\d{2}-\d{6}', filing['accession'])
                or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,180}\.html?', filing['url'].rsplit('/', 1)[-1])):
            raise ValueError('domestic_zero_directory_identity')
        rows.append(dict(filing=filing, start=str(period[0]), end=str(period[1]), tag=next(iter(selected))))
    return dict(methodology=VERSION, cik=cik, period_end=str(end), rows=rows)


def qualify(raw, item, *, issuer):
    from app.services import sec_inline_financials as inline
    from app.services.sec_tabular_financials import grid, preceding_text, company_key
    from app.services.sec_revenue_statement_layout import _visible
    filing = item['filing']
    parsed = inline.parse(raw, **filing)
    tree = html.fromstring(raw, parser=html.HTMLParser(encoding='utf-8'))
    target = (item['start'], item['end']); tag = item['tag']
    if tag not in TAGS or date.fromisoformat(target[0]) != date(date.fromisoformat(target[1]).year, 1, 1):
        raise ValueError('domestic_zero_target')
    contexts = {}
    for n in inline.nodes(tree, 'context'):
        if inline.nodes(n, 'segment') or inline.nodes(n, 'scenario'):
            continue
        ids, starts, ends = [inline.nodes(n, kind) for kind in ('identifier', 'startdate', 'enddate')]
        if (len(ids) == len(starts) == len(ends) == 1 and ids[0].get('scheme') == 'http://www.sec.gov/CIK'
                and ids[0].text_content().strip().zfill(10) == filing['cik']):
            contexts[n.get('id')] = (starts[0].text_content().strip(), ends[0].text_content().strip())
    units = {n.get('id') for n in inline.nodes(tree, 'unit') if not inline.nodes(n, 'divide')
             and [m.text_content().strip() for m in inline.nodes(n, 'measure')] == ['iso4217:USD']}
    if any(n.get('name') == 'us-gaap:'+tag and contexts.get(n.get('contextref')) == target
           for n in inline.nodes(tree, 'nonfraction')):
        raise ValueError('domestic_zero_reported_or_nil')
    results = []
    for table in tree.xpath('//table'):
        if not _visible(table) or table.xpath('.//table') or table.xpath('ancestor::blockquote'):
            continue
        headings = preceding_text(table, nonempty=True)[-5:]
        titles = [i for i, h in enumerate(headings) if TITLE.fullmatch(h)]
        if len(titles) != 1 or titles[0] == 0 or company_key(headings[titles[0]-1]) != company_key(issuer):
            continue
        rows = table.xpath('./tr|./tbody/tr|./thead/tr|./tfoot/tr')
        try:
            geometry = grid(table)
        except ValueError:
            continue
        mapped = [[(g, c) for g, c in zip([g for g in geometry[i] if g['row'] == i], r.xpath('./td|./th'))] for i, r in enumerate(rows)]
        if any(len([g for g in geometry[i] if g['row'] == i]) != len(r.xpath('./td|./th')) for i, r in enumerate(rows)):
            continue
        labels = [next((g['text'].lower().rstrip(':') for g, c in row if g['text']), '') for row in mapped]
        locate = lambda names: [i for i, label in enumerate(labels) if label in names]
        revenue = locate({'total revenue', 'revenues'})
        expenses = locate({'total operating expenses'})
        operating = locate({'operating loss', 'loss from operations'})
        if not (len(revenue) == len(expenses) == len(operating) == 1):
            continue
        ri, ei, oi = revenue[0], expenses[0], operating[0]
        if not ri < ei < oi:
            continue
        def point(index, column, expected_tag, period):
            cells = [(g, c) for g, c in mapped[index] if (g['start'], g['end']) == column]
            if len(cells) != 1 or not _visible(cells[0][1]): return None
            found = inline.nodes(cells[0][1], 'nonfraction')
            if len(found) != 1: return None
            n = found[0]
            if (n.get('name') != 'us-gaap:'+expected_tag or contexts.get(n.get('contextref')) != period
                    or n.get('unitref') not in units or inline._nil_fact(n) or not _visible(n)): return None
            return dict(tag=expected_tag, value=inline._number(n, tree), context_id=n.get('contextref'))
        # Require positive revenue in the corresponding current comparative column.
        current_period = (str(date.fromisoformat(filing['report_date']).replace(month=1, day=1)), filing['report_date'])
        anchors = [(g, point(ri, (g['start'], g['end']), tag, current_period)) for g, c in mapped[ri]]
        if len([p for g, p in anchors if p and p['value'] > 0]) != 1:
            continue
        for g, cell in mapped[ri]:
            if (g['text'] not in {'-', '\u2014'} or inline.nodes(cell, 'nonfraction')
                    or any(not _visible(n) for n in cell.iter())):
                continue
            column = (g['start'], g['end'])
            op = point(oi, column, 'OperatingIncomeLoss', target)
            exp = point(ei, column, 'OperatingExpenses', target)
            if not op or not exp or op['context_id'] != exp['context_id'] or exp['value'] <= 0 or op['value'] >= 0:
                continue
            if Decimal(str(op['value'])) != -Decimal(str(exp['value'])):
                continue
            headers = [h['text'] for line in geometry[:ri] for h in line
                       if h['start'] <= column[0] < column[1] <= h['end'] and h['text']]
            end = date.fromisoformat(target[1])
            duration = 'year' if end.month == 12 else {3:'three', 6:'six', 9:'nine'}.get(end.month)
            if (str(end.year) not in headers or duration is None
                    or not any(re.search(r'\b'+duration+r'(?:s)?(?: months)? ended\b', h, re.I) for h in headers)
                    or not any(re.search(r'\b'+calendar.month_name[end.month]+r' '+str(end.day)+r'\b', h, re.I) for h in headers)):
                continue
            optional = []
            for label in ('total cost of revenue', 'gross profit'):
                found = locate({label})
                if len(found) > 1: break
                if found:
                    cells = [(p, c) for p, c in mapped[found[0]] if (p['start'], p['end']) == column]
                    if len(cells) != 1 or cells[0][0]['text'] not in {'-', '\u2014'} or inline.nodes(cells[0][1], 'nonfraction') or any(not _visible(n) for n in cells[0][1].iter()): break
                    optional.append(label)
            else:
                results.append(dict(start=target[0], end=target[1], tag=tag, value=0, unit='USD',
                    filed=filing['filed'], accession=filing['accession'], source_cik=filing['cik'],
                    source_url=filing['url'], source_sha256=parsed['evidence']['sha256'],
                    printed_token=g['text'], column=list(column), statement_heading=headings,
                    issuer=issuer, header_cells=headers, row_label=labels[ri], operating=op, expenses=exp, zero_subtotals=optional))
    if len(results) != 1:
        raise ValueError('domestic_zero_not_uniquely_qualified')
    return results[0]


def acquire(payload, company, *, cik, available_by, client):
    expected = plan(payload, company, cik=cik, available_by=available_by)
    if expected is None: return None
    documents = {item['filing']['url']: client.get(item['filing']['url']) for item in expected['rows']}
    rows = [qualify(documents[item['filing']['url']], item, issuer=company['name']) for item in expected['rows']]
    return dict(documents=documents, evidence=dict(methodology=VERSION, cik=cik, period_end=expected['period_end'], rows=rows))


def apply(records, payload, company, *, cik, available_by, supplement):
    item = (supplement or {}).get('domestic_revenue_zeros')
    if not item: return records
    class Retained:
        def get(self, url): return item['documents'][url]
    verified = acquire(payload, company, cik=cik, available_by=available_by, client=Retained())
    if verified != item or item['evidence'] != supplement['evidence'].get('domestic_revenue_zeros'):
        raise ValueError('domestic_zero_proof_mismatch')
    result = dict(records);revenue = dict(records['revenue'])
    for row in item['evidence']['rows']:
        key = (date.fromisoformat(row['start']), date.fromisoformat(row['end']))
        if key in revenue: raise ValueError('domestic_zero_existing_record')
        revenue[key] = {k:row[k] for k in ('start', 'end', 'filed', 'accession', 'unit', 'tag', 'value')}
        revenue[key].update(priority=0, source_kind=KINDS, source_cik=cik,
            source_url=row['source_url'], source_sha256=row['source_sha256'], original_statement=deepcopy(row))
    result['revenue'] = revenue
    return result


def cached_valid(evidence):
    """Check persisted proofs and every selected derived input; originals are replayed on apply."""
    from app.services.sec_statement_currency import source_identity
    from app.services.sec_tabular_financials import company_key
    import math
    def points(value):
        if isinstance(value, dict):
            if value.get('source_kind') == KINDS: yield value
            for child in value.values(): yield from points(child)
        elif isinstance(value, list):
            for child in value: yield from points(child)
    selected=list(points(evidence.get('current', {})))+list(points(evidence.get('prior', {})))
    proof=(evidence.get('inline_filing_supplement') or {}).get('domestic_revenue_zeros')
    if proof is None: return not selected
    try:
        if proof['methodology'] != VERSION or proof['cik'] != evidence['cik'] or len(proof['rows']) != 2:
            return False
        end=date.fromisoformat(proof['period_end'])
        if str(end) != evidence['period_end'] or (end.month,end.day) not in {(3,31),(6,30),(9,30)}:
            return False
        expected=[(str(date(end.year-2,1,1)),str(date(end.year-2,12,31))),
                  (str(date(end.year-2,1,1)),str(end.replace(year=end.year-2)))]
        if [(r['start'],r['end']) for r in proof['rows']] != expected: return False
        for row in proof['rows']:
            if (row['tag'] not in TAGS or type(row['value']) is not int or row['value'] != 0 or row['unit'] != 'USD'
                    or row['source_cik'] != proof['cik'] or source_identity(row['source_url']) != (proof['cik'],row['accession'])
                    or not re.fullmatch(r'[0-9a-f]{64}',row['source_sha256'])
                    or not row['end'] <= row['filed'] <= evidence['available_by']
                    or row['printed_token'] not in {'-','\u2014'} or row['row_label'] not in {'total revenue','revenues'}): return False
            op,exp=row['operating'],row['expenses']
            if (op['tag'] != 'OperatingIncomeLoss' or exp['tag'] != 'OperatingExpenses'
                    or not op['context_id'] or op['context_id'] != exp['context_id']
                    or any(type(p['value']) not in (int,float) or not math.isfinite(p['value']) for p in (op,exp))
                    or exp['value'] <= 0 or op['value'] != -exp['value']): return False
            bounds=row['column']
            if len(bounds)!=2 or any(type(i) is not int for i in bounds) or not 0<=bounds[0]<bounds[1]<=128: return False
            headings=row['statement_heading'];titles=[i for i,h in enumerate(headings) if TITLE.fullmatch(h)]
            if len(titles)!=1 or titles[0]==0 or company_key(headings[titles[0]-1])!=company_key(row['issuer']): return False
            day=date.fromisoformat(row['end']);headers=row['header_cells']
            duration='year' if day.month==12 else {3:'three',6:'six',9:'nine'}[day.month]
            if (str(day.year) not in headers
                    or not any(re.search(r'\b'+duration+r'(?:s)?(?: months)? ended\b', h, re.I) for h in headers)
                    or not any(re.search(r'\b'+calendar.month_name[day.month]+r' '+str(day.day)+r'\b', h, re.I) for h in headers)
                    or len(set(row['zero_subtotals']))!=len(row['zero_subtotals'])
                    or not set(row['zero_subtotals']) <= {'total cost of revenue','gross profit'}): return False
        for point in selected:
            matches=[r for r in proof['rows'] if (r['start'],r['end'])==(point['start'],point['end'])]
            if len(matches)!=1: return False
            row=matches[0]
            if point.get('original_statement') != row or any(point.get(k)!=row[k] for k in ('value','unit','tag','filed','accession','source_cik','source_url','source_sha256')): return False
        return bool(selected) and {p['source_url'] for p in selected} == {r['source_url'] for r in proof['rows']}
    except (KeyError, TypeError, ValueError, AttributeError, IndexError):
        return False
