"""Whole revenue concepts qualified against complete primary statements.

Never splice totals and components. Exact overlapping facts are necessary but
not sufficient: every selected period also needs original statement evidence.
"""
from copy import deepcopy
from datetime import date
import re
import math
import hashlib
from lxml import html

VERSION = 'sec_primary_statement_revenue_series_v1'
TOTAL = 'Revenues'
CONTRACT = 'RevenueFromContractWithCustomerExcludingAssessedTax'
FORMS = {'10-K', '10-Q', '10-K/A', '10-Q/A'}
IDENTITY = ('value', 'unit', 'filed', 'accession')
LABELS = {'total revenues', 'total revenue', 'total net revenues', 'total net revenue',
          'total net sales', 'net sales', 'net revenues', 'net revenue', 'revenues', 'revenue', 'revenue from services'}


def _primary_heading(headings):
    # A narrative reference to the statements is not a statement title.
    return (isinstance(headings,list) and all(isinstance(h,str) for h in headings)
        and any(re.fullmatch(r'(?:condensed )?consolidated statements? of (?:operations|earnings|income|comprehensive income)',
                             h, re.I) for h in headings)
        and not any(re.search(r'notes to|non.?gaap|adjusted|reconciliation|pro.?forma',h,re.I) for h in headings))


def _plan(payload, company, *, available_by, cik):
    from app.services import sec_fundamentals as sec
    if any(str(obj.get('cik', '')).zfill(10) != cik for obj in (payload, company)):
        raise ValueError('revenue_series_identity')
    recent = company.get('filings', {}).get('recent', {})
    keys = ('accessionNumber', 'form', 'filingDate', 'reportDate', 'primaryDocument')
    arrays = [recent.get(k, []) for k in keys]
    if len({len(a) for a in arrays}) != 1:
        raise ValueError('revenue_series_directory_shape')
    filings = {}
    for accession, form, filed, end, filename in zip(*arrays):
        if form not in FORMS:
            continue
        if not sec._day(end) or not sec._day(filed) or not end <= filed <= str(available_by):
            continue
        if (not re.fullmatch(r'\d{10}-\d{2}-\d{6}', accession)
                or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,180}\.html?', filename)):
            raise ValueError('revenue_series_filing_identity')
        item = dict(cik=cik, accession=accession, form=form, filed=filed, report_date=end,
            url=f'https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession.replace("-", "")}/{filename}')
        if accession in filings and filings[accession] != item:
            raise ValueError('revenue_series_ambiguous_directory')
        filings[accession] = item
    if not filings:
        return None
    latest = max((p['report_date'], p['filed']) for p in filings.values())
    closing = [p for p in filings.values() if (p['report_date'], p['filed']) == latest]
    if len(closing) != 1:
        raise ValueError('revenue_series_ambiguous_latest_filing')
    end = date.fromisoformat(latest[0])
    if not 0 <= (available_by-end).days <= 180:
        return None
    ordinary = sec._records(payload, 'revenue', available_by)
    if (not ordinary or max(e for _, e in ordinary) != end
            or any(p and p['tag'] != TOTAL for p in ordinary.values())
            or sec._ttm(ordinary, end) is not None):
        return None
    candidate = sec._records(payload, 'revenue', available_by, concepts=(CONTRACT,))
    current = sec._ttm(candidate, end)
    if not current or not any(p['end'] == str(end) and p['accession'] == closing[0]['accession']
                              and p['filed'] == closing[0]['filed'] for p in current['inputs']):
        return None
    prior_end = sec._prior_end(candidate, end)
    prior = sec._ttm(candidate, prior_end) if prior_end else None
    needed = { (p['start'],p['end']):p for point in (current,prior) if point for p in point['inputs'] }
    floor = min(start for start, _ in needed)
    overlaps = []
    for key, left in ordinary.items():
        if str(key[0]) < floor:
            continue
        right = candidate.get(key)
        if not left or not right or any(left[k] != right[k] for k in IDENTITY):
            raise ValueError('revenue_series_total_contract_conflict')
        overlaps.append({'start':left['start'], 'end':left['end'], **{k:left[k] for k in IDENTITY}})
    if len(overlaps) < 2:
        return None
    required = {p['accession'] for p in needed.values()}
    if len(required) > 6 or not required <= filings.keys():
        raise ValueError('revenue_series_originals_unavailable')
    return dict(candidate=candidate, needed=needed, floor=floor, end=str(end),
                filings={key:filings[key] for key in sorted(required)}, overlaps=overlaps)


def statement_rows(raw, filing, parsed=None):
    from app.services import sec_inline_financials as inline
    from app.services.sec_tabular_financials import preceding_text, clean
    parsed = parsed or inline.parse(raw, **filing)
    if (parsed['evidence'].get('sha256') != hashlib.sha256(raw).hexdigest()
            or any(parsed['evidence'].get(k) != filing[k] for k in ('cik','accession','filed','form','report_date'))):
        raise ValueError('revenue_series_original_identity')
    tree = html.fromstring(raw)
    contexts = {}
    for n in inline.nodes(tree, 'context'):
        if inline.nodes(n, 'segment') or inline.nodes(n, 'scenario'):
            continue
        ids = inline.nodes(n, 'identifier')
        starts, ends = inline.nodes(n, 'startdate'), inline.nodes(n, 'enddate')
        if (len(ids) == len(starts) == len(ends) == 1
                and ids[0].get('scheme') == 'http://www.sec.gov/CIK'
                and ids[0].text_content().strip().zfill(10) == filing['cik']):
            contexts[n.get('id')] = (starts[0].text_content().strip(), ends[0].text_content().strip())
    units = {n.get('id') for n in inline.nodes(tree, 'unit')
             if [x.text_content().strip() for x in inline.nodes(n, 'measure')] == ['iso4217:USD']
             and not inline.nodes(n, 'divide')}
    def service_reconciliation(table, period, revenue):
        # A service-only income statement can name its whole top line this way.
        # Require the explicit cost/gross-profit equation, not label inference.
        observed = {'CostOfServices':set(), 'GrossProfit':set()}
        labels = []
        for row in table.xpath('./tr|./tbody/tr|./thead/tr'):
            cells = row.xpath('./td|./th')
            label = next((clean(' '.join(c.itertext())).lower().rstrip(':') for c in cells if clean(' '.join(c.itertext()))), '')
            if re.search(r'revenue|sales', label) and not re.search(r'cost|expense|percentage|margin',label):
                labels.append(label)
            for n in inline.nodes(row,'nonfraction'):
                original_tag = n.get('name','').removeprefix('us-gaap:')
                tag = 'CostOfServices' if original_tag == 'CostOfGoodsAndServicesSold' else original_tag
                if (tag in observed and n.get('name') == 'us-gaap:'+original_tag
                        and contexts.get(n.get('contextref')) == period and n.get('unitref') in units
                        and not any(a.tag == 'ix:hidden' for a in n.iterancestors())
                        and label == {'CostOfServices':'cost of services','GrossProfit':'gross profit'}[tag]):
                    observed[tag].add(inline._number(n,tree))
        if set(labels) != {'revenue from services'} or any(len(v)!=1 for v in observed.values()):
            return None
        cost, gross = (next(iter(observed[k])) for k in ('CostOfServices','GrossProfit'))
        if cost < 0 or not math.isclose(revenue-cost,gross,rel_tol=1e-12,abs_tol=.01):
            return None
        return {'formula':'reported service revenue minus reported cost of services equals reported gross profit',
                'revenue':revenue,'cost_of_services':cost,'gross_profit':gross,'unit':'USD',
                'start':period[0],'end':period[1]}

    accepted = {}
    facts = {(p.get('start'),p['end']):p for p in parsed['facts'].get(CONTRACT, {}).get('units', {}).get('USD', [])}
    tables = {}
    for node in inline.nodes(tree, 'nonfraction'):
        if (node.get('name') == 'us-gaap:'+CONTRACT and node.get('contextref') in contexts
                and node.get('unitref') in units):
            parents = node.xpath('ancestor::table')
            if parents and not parents[-1].xpath('.//table'):
                tables[id(parents[-1])] = parents[-1]
    for table in tables.values():
        headings = preceding_text(table)[-4:]
        if not _primary_heading(headings):
            continue
        for row in table.xpath('./tr|./tbody/tr|./thead/tr'):
            cells = row.xpath('./td|./th')
            label = next((clean(' '.join(cell.itertext())).lower().rstrip(':') for cell in cells
                          if clean(' '.join(cell.itertext()))), '')
            if label not in LABELS:
                continue
            for n in inline.nodes(row, 'nonfraction'):
                if n.get('name') != 'us-gaap:'+CONTRACT or n.get('unitref') not in units:
                    continue
                period = contexts.get(n.get('contextref'))
                point = facts.get(period)
                if (not point or any(a.tag == 'ix:hidden' for a in n.iterancestors())
                        or inline._number(n, tree) != point['val']):
                    continue
                reconciliation = service_reconciliation(table,period,point['val']) if label == 'revenue from services' else None
                if label == 'revenue from services' and reconciliation is None:
                    continue
                proof = dict(start=period[0], end=period[1], value=point['val'], unit='USD',
                    filed=filing['filed'], accession=filing['accession'], context_id=n.get('contextref'),
                    row_label=label, statement_heading=headings, source_url=filing['url'],
                    source_sha256=parsed['evidence']['sha256'], source_cik=filing['cik'])
                if reconciliation is not None:
                    proof['service_total_reconciliation'] = reconciliation
                if period in accepted and accepted[period]['value'] != proof['value']:
                    raise ValueError('revenue_series_statement_conflict')
                accepted[period] = proof
    return accepted


def acquire(payload, company, *, cik, available_by, client, originals=None):
    plan = _plan(payload, company, cik=cik, available_by=available_by)
    if plan is None:
        return None
    verified = {}
    for filing in plan['filings'].values():
        raw = client.get(filing['url'])
        parsed = (originals or {}).get(filing['url'])
        for key, proof in statement_rows(raw, filing, parsed).items():
            verified[key+(filing['accession'],)] = proof
    for key, point in plan['needed'].items():
        proof = verified.get(key+(point['accession'],))
        if not proof or any(proof[k] != point[k] for k in IDENTITY):
            raise ValueError('revenue_series_primary_statement_missing')
    selected = []
    for (start,end), point in plan['candidate'].items():
        if not point or str(start) < plan['floor']:
            continue
        proof = verified.get((str(start),str(end),point['accession']))
        if proof and all(proof[k] == point[k] for k in IDENTITY):
            selected.append({**point, 'original_statement':proof})
    return {'methodology':VERSION, 'cik':cik, 'period_end':plan['end'], 'basis_spliced':False,
            'selected_concept':CONTRACT, 'superseded_concept':TOTAL,
            'overlap':plan['overlaps'], 'rows':selected,
            'basis':'One contract-revenue history independently identified as total revenue in each original consolidated income statement.'}


def apply(records, payload, company, *, cik, available_by, supplement, preserve_reported_history=False):
    proof = (supplement or {}).get('evidence', {}).get('revenue_series_selection')
    if not proof:
        return records
    if any(p.get('tag') in {TOTAL,CONTRACT} for p in supplement['evidence'].get('nil_facts', [])):
        raise ValueError('revenue_series_nil_revenue')
    plan = _plan(payload, company, cik=cik, available_by=available_by)
    if (plan is None or proof.get('methodology') != VERSION or proof.get('cik') != cik
            or proof.get('period_end') != plan['end'] or proof.get('basis_spliced') is not False
            or proof.get('overlap') != plan['overlaps'] or proof.get('selected_concept') != CONTRACT
            or proof.get('superseded_concept') != TOTAL):
        raise ValueError('revenue_series_proof_mismatch')
    selected = {}
    for point in proof.get('rows', []):
        key = (date.fromisoformat(point['start']),date.fromisoformat(point['end']))
        expected = plan['candidate'].get(key)
        original = point.get('original_statement') or {}
        if (key in selected or not expected or point.get('tag') != CONTRACT
                or any(point.get(k) != expected[k] or original.get(k) != expected[k] for k in IDENTITY)
                or original.get('source_cik') != cik or original.get('row_label') not in LABELS
                or not _primary_heading(original.get('statement_heading'))
                or any(original.get(k) != point[k] for k in ('start','end'))
                or original.get('source_url') != plan['filings'].get(point['accession'],{}).get('url')
                or not re.fullmatch(r'[a-f0-9]{64}',original.get('source_sha256',''))):
            raise ValueError('revenue_series_row_mismatch')
        if original.get('row_label') == 'revenue from services':
            reconciliation = original.get('service_total_reconciliation') or {}
            if (reconciliation.get('revenue') != point['value'] or reconciliation.get('unit') != 'USD'
                    or reconciliation.get('start') != point['start'] or reconciliation.get('end') != point['end']
                    or type(reconciliation.get('cost_of_services')) not in (int,float)
                    or type(reconciliation.get('gross_profit')) not in (int,float)
                    or reconciliation['cost_of_services'] < 0
                    or not math.isclose(point['value']-reconciliation['cost_of_services'],reconciliation['gross_profit'],rel_tol=1e-12,abs_tol=.01)):
                raise ValueError('revenue_series_service_total_mismatch')
        selected[key] = {**deepcopy(point), 'revenue_selection':{k:v for k,v in proof.items() if k!='rows'}}
    if not all((date.fromisoformat(s),date.fromisoformat(e)) in selected for s,e in plan['needed']):
        raise ValueError('revenue_series_incomplete_proof')
    if preserve_reported_history:
        # The history reader displays separate reported periods, not one TTM
        # series. Keep its existing totals and add verified periods. Its YTD
        # derivation separately requires matching concepts. Core TTM callers
        # always receive only the single qualified concept above.
        selected = {**records['revenue'], **selected}
    return {**records, 'revenue':selected}
