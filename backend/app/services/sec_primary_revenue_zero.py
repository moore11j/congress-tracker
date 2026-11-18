"""Qualify a printed comparative zero through its whole primary statement.

This deliberately narrow layout requires a single revenue line, two operating
expense lines, and an exact operating-result reconciliation in both columns.
An absent, nil, blank or unaligned amount never supplies zero.
"""
from datetime import date
from decimal import Decimal
import re
import math

VERSION = 'sec_primary_comparative_revenue_zero_v1'
REVENUE = 'RevenueFromContractWithCustomerExcludingAssessedTax'
EXPENSES = ('ResearchAndDevelopmentExpense', 'GeneralAndAdministrativeExpense')
TITLE = re.compile(r'(?:unaudited )?(?:interim )?(?:condensed )?consolidated statements? of '
                   r'(?:comprehensive loss|operations|income|earnings)(?: and comprehensive (?:income|loss))?', re.I)


def qualify(tree, *, contexts, units, report_date):
    from app.services import sec_inline_financials as inline
    from app.services.sec_tabular_financials import grid, preceding_text
    from app.services.sec_revenue_statement_layout import _visible

    end = date.fromisoformat(report_date)
    if (end.month, end.day) != (6, 30):
        return []
    tables = {}
    for node in inline.nodes(tree, 'nonfraction'):
        if node.get('name') != 'us-gaap:' + REVENUE:
            continue
        ancestors = node.xpath('ancestor::table')
        if ancestors and not ancestors[-1].xpath('.//table'):
            tables[id(ancestors[-1])] = ancestors[-1]
    accepted = []
    for table in tables.values():
        if not _visible(table):
            continue
        headings = preceding_text(table, nonempty=True)[-3:]
        title_positions = [i for i, text in enumerate(headings) if TITLE.fullmatch(text)]
        if len(title_positions) != 1:
            continue
        suffix = headings[title_positions[0]+1:]
        if suffix != [f'FOR THE SIX MONTHS ENDED JUNE 30, {end.year-1} AND {end.year}']:
            continue
        rows = table.xpath('./tr|./tbody/tr|./thead/tr|./tfoot/tr')
        try:
            geometry = grid(table)
        except ValueError:
            continue
        mapped = []
        for index, row in enumerate(rows):
            direct = [c for c in geometry[index] if c['row'] == index]
            cells = row.xpath('./td|./th')
            if len(direct) != len(cells) or not _visible(row):
                mapped = []
                break
            mapped.append([(g, c) for g, c in zip(direct, cells)])
        if not mapped:
            continue
        meaningful = [(i, [(g, c) for g, c in row if g['text']])
                      for i, row in enumerate(mapped) if any(g['text'] for g, c in row)]
        starts = [j for j, (_, row) in enumerate(meaningful)
                  if len(row) == 1 and row[0][0]['text'].lower().rstrip(':') == 'revenue']
        if len(starts) != 1:
            continue
        start = starts[0]
        section = meaningful[start+1:start+6]
        if len(section) != 5:
            continue
        labels = [row[0][0]['text'].lower().rstrip(':') for _, row in section]
        if (labels[0] not in {'revenue', 'revenues', 'licensing and collaboration revenue'}
                or labels[1:] != ['operating expenses and income', 'research and development expenses',
                                   'administrative expenses', 'loss from operations']):
            continue
        revenue_row = mapped[section[0][0]]
        operating_rows = [mapped[section[i][0]] for i in (2, 3, 4)]
        tagged = [(g, cell, inline.nodes(cell, 'nonfraction')) for g, cell in revenue_row
                  if inline.nodes(cell, 'nonfraction')]
        zeros = [(g, cell) for g, cell in revenue_row if g['text'] == '\u2014'
                 and not inline.nodes(cell, 'nonfraction')]
        if len(tagged) != 1 or len(zeros) != 1:
            continue
        current_geometry, current_cell, current_nodes = tagged[0]
        zero_geometry, zero_cell = zeros[0]
        if (len(current_nodes) != 1 or not _visible(current_cell) or not _visible(zero_cell)
                or current_geometry['end']-current_geometry['start'] != 1
                or zero_geometry['end']-zero_geometry['start'] != 1):
            continue
        current = current_nodes[0]
        if (current.get('name') != 'us-gaap:'+REVENUE or units.get(current.get('unitref')) != 'USD'
                or contexts.get(current.get('contextref')) != (date(end.year,1,1),end)
                or inline._nil_fact(current) or not _visible(current)):
            continue
        try:
            current_value = inline._number(current, tree)
        except ValueError:
            continue
        if current_value <= 0:
            continue
        columns = []
        for g, year, revenue in ((zero_geometry,end.year-1,0), (current_geometry,end.year,current_value)):
            column = (g['start'], g['end'])
            header = [x['text'] for row in mapped[:meaningful[start][0]] for x, _ in row
                      if (x['start'], x['end']) == column and x['text']]
            if header != [str(year), 'US$']:
                break
            facts = []
            for row, tag in zip(operating_rows, EXPENSES+('OperatingIncomeLoss',)):
                cells = [(x,c) for x,c in row if (x['start'],x['end']) == column]
                if len(cells) != 1 or not _visible(cells[0][1]):
                    break
                nodes = inline.nodes(cells[0][1], 'nonfraction')
                if len(nodes) != 1:
                    break
                node = nodes[0]
                if (node.get('name') != 'us-gaap:'+tag or units.get(node.get('unitref')) != 'USD'
                        or contexts.get(node.get('contextref')) != (date(year,1,1),date(year,6,30))
                        or inline._nil_fact(node) or not _visible(node)):
                    break
                try:
                    value = inline._number(node,tree)
                except ValueError:
                    break
                facts.append(dict(tag=tag, value=value, context_id=node.get('contextref')))
            if len(facts) != 3 or any(f['value'] < 0 for f in facts[:2]) or facts[2]['value'] >= 0:
                break
            if len({f['context_id'] for f in facts}) != 1:
                break
            if Decimal(str(revenue))-sum(Decimal(str(f['value'])) for f in facts[:2]) != Decimal(str(facts[2]['value'])):
                break
            columns.append(dict(start=f'{year}-01-01',end=f'{year}-06-30',revenue=revenue,
                                column=list(column),operating_facts=facts))
        if len(columns) != 2:
            continue
        accepted.append(dict(methodology=VERSION, tag=REVENUE, unit='USD', value=0,
            start=columns[0]['start'], end=columns[0]['end'], printed_token='\u2014',
            statement_heading=headings, row_label=labels[0], columns=columns))
    return accepted


def proof_valid(proof, report_date):
    """Validate the saved arithmetic and period contract, without new requests."""
    try:
        end=date.fromisoformat(report_date)
        if (end.month,end.day)!=(6,30) or not isinstance(proof,dict):return False
        if any(proof.get(k)!=v for k,v in dict(methodology=VERSION,tag=REVENUE,unit='USD',value=0,
                start=f'{end.year-1}-01-01',end=f'{end.year-1}-06-30',printed_token='\u2014').items()):return False
        if isinstance(proof['value'],bool):return False
        headings=proof['statement_heading']
        if not isinstance(headings,list) or len(headings)!=3 or not TITLE.fullmatch(headings[1]):return False
        if headings[2]!=f'FOR THE SIX MONTHS ENDED JUNE 30, {end.year-1} AND {end.year}':return False
        if proof['row_label'] not in {'revenue','revenues','licensing and collaboration revenue'}:return False
        columns=proof['columns']
        if len(columns)!=2:return False
        for column,year in zip(columns,(end.year-1,end.year)):
            if column['start']!=f'{year}-01-01' or column['end']!=f'{year}-06-30':return False
            bounds=column['column']
            if len(bounds)!=2 or any(type(x) is not int for x in bounds) or not 0<=bounds[0]<bounds[1]<=128 or bounds[1]-bounds[0]!=1:return False
            facts=column['operating_facts']
            if [f['tag'] for f in facts]!=list(EXPENSES)+['OperatingIncomeLoss']:return False
            values=[column['revenue']]+[f['value'] for f in facts]
            if any(isinstance(x,bool) or not isinstance(x,(int,float)) or not math.isfinite(x) for x in values):return False
            if any(not isinstance(f['context_id'],str) or not f['context_id'] for f in facts):return False
            if len({f['context_id'] for f in facts})!=1:return False
            if any(f['value']<0 for f in facts[:2]) or facts[2]['value']>=0:return False
            if Decimal(str(values[0]))-sum(Decimal(str(v)) for v in values[1:3])!=Decimal(str(values[3])):return False
        return columns[0]['revenue']==0 and columns[1]['revenue']>0 and columns[0]['column']!=columns[1]['column']
    except (KeyError,TypeError,ValueError,IndexError):
        return False


def validate(supplement):
    proof=supplement.get('evidence',{})
    zeros=proof.get('primary_comparative_revenue_zeros',[])
    derived=[p for p in supplement.get('provenance',{}).values() if p.get('source_kind')=='sec_primary_statement_zero']
    if not zeros and not derived:return
    if len(zeros)!=1 or len(derived)!=1 or not proof_valid(zeros[0],proof.get('report_date')):
        raise ValueError('primary_revenue_zero_proof')
    zero=zeros[0]
    key=(REVENUE,'USD',zero['start'],zero['end'])
    point=supplement['provenance'].get(key,{})
    if (point.get('zero_reconciliation')!=zero or point.get('source_sha256')!=proof.get('sha256')
            or point.get('source_cik')!=proof.get('cik') or point.get('source_url')!=proof.get('url')):
        raise ValueError('primary_revenue_zero_original')
    facts=[p for p in supplement['facts'][REVENUE]['units']['USD'] if p.get('start')==zero['start'] and p['end']==zero['end']]
    if len(facts)!=1 or any(facts[0].get(k)!=v for k,v in dict(val=0,filed=proof['filed'],form=proof['form'],accn=proof['accession']).items()):
        raise ValueError('primary_revenue_zero_fact')


def cached_valid(evidence):
    source=evidence.get('inline_filing_supplement',{})
    if not isinstance(source,dict):return False
    zeros=source.get('primary_comparative_revenue_zeros',[])
    if not isinstance(zeros,list):return False
    def points(value):
        if isinstance(value,dict):
            if value.get('source_kind')=='sec_primary_statement_zero':yield value
            for child in value.values():yield from points(child)
        elif isinstance(value,list):
            for child in value:yield from points(child)
    selected=list(points(evidence.get('current',{})))+list(points(evidence.get('prior',{})))
    if not zeros and not selected:return True
    if len(zeros)!=1 or not proof_valid(zeros[0],source.get('report_date')):return False
    return all(p.get('zero_reconciliation')==zeros[0] and p.get('source_sha256')==source.get('sha256')
        and p.get('source_cik')==source.get('cik') and p.get('source_url')==source.get('url') for p in selected)
