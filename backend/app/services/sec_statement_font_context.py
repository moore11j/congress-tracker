"""Explicit adjacent issuer/title/units for otherwise empty HTML statements."""
import re

POLICY = 'explicit_adjacent_font_statement_v1'
RETRY_REASONS = {'table_accounting_basis_unproved', 'table_reporting_currency_unproved',
                 'table_current_statement_alignment', 'financial_hint_not_qualified'}


def context(table, issuer):
    from app.services import sec_tabular_financials as tab
    parent = table.getparent()
    if (parent is None or parent.tag != 'div' or len(parent.xpath('./table')) != 1
            or table.xpath('ancestor::blockquote')):
        return None
    siblings = list(table.itersiblings(preceding=True))
    if not 1 <= len(siblings) <= 4 or any(n.tag != 'font' or len(n) for n in siblings):
        return None
    blocks = [tab.visible_text(n) for n in reversed(siblings) if tab.visible_text(n)]
    if len(blocks) != 2 or blocks[0] != 'Unaudited':
        return None
    if not re.fullmatch(r'\(Expressed in (?:thousands|millions) of U\.S\. dollars'
                        r'(?:, except per share amounts and number of shares)?\)', blocks[1]):
        return None
    title = parent.getprevious()
    owner = title.getprevious() if title is not None else None
    if title is None or owner is None or any(
            n.tag != 'div' or n.xpath('.//table|.//div') for n in (title, owner)):
        return None
    heading, name = tab.visible_text(title), tab.visible_text(owner)
    if not re.fullmatch(r'Condensed Consolidated Interim Statements of '
                       r'(?:Financial Position|Loss and Comprehensive Loss|Cash Flows)', heading):
        return None
    if tab.company_key(name) != tab.company_key(issuer):
        raise tab.TabularFinancialError('font_statement_owner_unproved')
    return [name, heading, *blocks]


def table_kind(text):
    from app.services.sec_tabular_financials import table_kind as original
    text = re.sub(r'(?i)statements of loss and comprehensive loss$',
                  'statements of income and comprehensive loss', text)
    return original(text, expanded=True)


def revenue_rows(rows, columns, kind):
    """Prove the sole printed revenue line against cost and monetary gross profit."""
    from decimal import Decimal
    from app.services import sec_tabular_financials as tab
    labels=[tab.clean(row[0]['text']).lower() if row else '' for row in rows]
    if kind!='income' or 'product and service revenues' not in labels:
        return {},None
    if labels.count('revenues:')!=1:
        raise tab.TabularFinancialError('font_revenue_group_unproved')
    index=labels.index('revenues:')
    if (labels[index:index+4]!=['revenues:','product and service revenues',
                              'cost of product and service revenues','gross margin']
            or [i for i,label in enumerate(labels) if 'revenue' in label]!=[index,index+1,index+2]
            or any(cell['text'] for cell in rows[index][1:])):
        raise tab.TabularFinancialError('font_revenue_group_unproved')
    proofs=[]
    for column in columns:
        revenue,cost,gross=[tab.amount(rows[index+i],column['column_start'],column['column_end'],column['scale'])
                            for i in (1,2,3)]
        if (None in (revenue,cost,gross) or revenue<0 or cost<0
                or Decimal(str(revenue))-Decimal(str(cost))!=Decimal(str(gross))):
            raise tab.TabularFinancialError('font_revenue_reconciliation')
        proofs.append(dict(start=column['start'],end=column['end'],currency=column['currency'],
                           revenue=revenue,cost=cost,gross_profit=gross))
    return {index+1:'revenue',index+3:'gross_profit'},dict(
        basis='single_revenue_line_exact_gross_profit_reconciliation',
        source_rows=list(range(index,index+4)),columns=proofs)


def explanatory_note(table, issuer):
    """Prove a legacy distant-prose caption belongs to an explicitly numbered note."""
    from app.services import sec_tabular_financials as tab
    parent = table.getparent()
    if parent is None or parent.tag != 'div' or list(table.itersiblings(preceding=True)):
        return False
    previous = parent.getprevious()
    if previous is None or previous.tag != 'div' or previous.xpath('.//table'):
        return False
    title = tab.visible_text(previous)
    if not re.fullmatch(r'\d{1,2}\. [A-Za-z][^:]{1,150}:', title) or tab.table_kind(title):
        return False
    preceding = table.xpath('preceding::table[1]')
    if not preceding or preceding[0].xpath('.//table'):
        return False
    heading = tab.visible_text(preceding[0])
    if len(heading) > 700 or ' Notes to ' not in heading:
        return False
    name, rest = heading.split(' Notes to ', 1)
    return (tab.company_key(name) == tab.company_key(issuer)
            and bool(re.fullmatch(r'Condensed Consolidated Interim Financial Statements '
                                  r'.{1,500}Unaudited \(Tabular amounts expressed in thousands '
                                  r'of U\.S\. dollars, except number of shares\)', rest)))


def recover(baseline, tree, raw, **args):
    from app.services import sec_tabular_financials as tab
    import json
    if baseline['facts'] or baseline['conflicts']:
        return None
    issuer = json.loads(args['company_raw'])['name']
    all_tables = tree.xpath('//table')
    try:
        if not any(context(t, issuer) for t in all_tables):
            return None
    except tab.TabularFinancialError:
        return None
    candidate = tab.extract(raw, **args, _expanded=True, _font=True)
    if (not candidate['facts'] or candidate['holds'] or candidate['conflicts']
            or any(t['holds'] for t in candidate['tables'])):
        return None
    recovered = {t['table'] for t in candidate['tables']}
    notes = []
    for hold in baseline['holds']:
        index = hold['table']
        if index in recovered and hold['reason'] in {'table_scale_unproved', 'table_issuer_unproved'}:
            continue
        if hold['reason'] == 'table_issuer_unproved' and explanatory_note(all_tables[index], issuer):
            notes.append(index)
            continue
        return None
    candidate['source']['statement_font_policy'] = POLICY
    totals=[dict(table=t['table'],**t['revenue_total_proof']) for t in candidate['tables'] if t.get('revenue_total_proof')]
    if totals:
        candidate['source']['statement_revenue_reconciliation']=totals
    if notes:
        candidate['source']['excluded_numbered_note_tables'] = notes
    return candidate
