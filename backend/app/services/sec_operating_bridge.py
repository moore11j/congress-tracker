"""Complete primary-statement SG&A bridge, without an EBITDA inference."""
from datetime import date
import math
from lxml import html

METHOD = 'sec_complete_sga_operating_bridge_v1'
BASIS = ('Calculated operating result: reported gross profit less total selling and administrative expense; '
         'every intervening expense is reconciled, and reported net interest and other nonoperating income reconcile to pretax income. Not EBITDA.')
TAGS = {
    'gross': 'GrossProfit', 'marketing': 'MarketingAndAdvertisingExpense',
    'administrative': 'GeneralAndAdministrativeExpense',
    'sga': 'SellingGeneralAndAdministrativeExpense',
    'interest': 'InterestIncomeExpenseNonoperatingNet',
    'other': 'OtherNonoperatingIncomeExpense',
    'pretax': 'IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest',
}
LABELS = {
    'gross': {'gross profit'},
    'marketing': {'demand creation expense', 'marketing and advertising expense'},
    'administrative': {'operating overhead expense', 'general and administrative expense'},
    'sga': {'total selling and administrative expense'},
    'interest': {'interest (income) expense, net', 'interest expense (income), net'},
    'other': {'other (income) expense, net'},
    'pretax': {'income before income taxes', 'loss before income taxes', 'income (loss) before income taxes'},
}
ORDER = tuple(TAGS)


def candidate_expenses(payload, gross, *, available_by):
    from app.services import sec_fundamentals as sec
    # Most issuers cannot qualify this exact bridge. Avoid scanning their full
    # historical facts when one required standard component is absent.
    if not gross or any(tag not in payload.get('facts', {}).get('us-gaap', {}) for tag in TAGS.values()):
        return {}
    series = {k: sec._records(payload, 'operating_income', available_by, concepts=(v,)) for k,v in TAGS.items() if k!='gross'}
    accepted = {}
    for period,left in gross.items():
        rows = {'gross': left, **{k:v.get(period) for k,v in series.items()}}
        if (not all(rows.values()) or any(any(r.get(k)!=left.get(k) for k in ('start','end','filed','accession','unit')) for r in rows.values())
            or any(type(r.get('value')) not in (int,float) or not math.isfinite(r['value']) for r in rows.values())):
            continue
        values = {k:v['value'] for k,v in rows.items()}
        if _reconciles(values): accepted[period] = rows['sga']
    return accepted


def _reconciles(v):
    return (all(v[k]>=0 for k in ('marketing','administrative','sga'))
            and v['marketing']+v['administrative']==v['sga']
            and v['gross']-v['sga']+v['interest']+v['other']==v['pretax'])


def statement_rows(raw, filing, *, issuer):
    from app.services import sec_inline_financials as inline,sec_tabular_financials as tables,sec_primary_income as primary
    parsed = inline.parse(raw, **filing)
    # Independently qualify the same statement's pretax/tax/total-income bridge.
    earnings = primary.statement_rows(raw, filing, issuer)
    if not earnings:return {}
    tree = html.fromstring(raw);contexts={}
    for n in inline.nodes(tree,'context'):
        ids,starts,ends=(inline.nodes(n,k) for k in ('identifier','startdate','enddate'))
        if (not inline.nodes(n,'segment') and not inline.nodes(n,'scenario') and len(ids)==len(starts)==len(ends)==1
            and ids[0].get('scheme')=='http://www.sec.gov/CIK' and ids[0].text_content().strip().zfill(10)==filing['cik']):
            contexts[n.get('id')]=(starts[0].text_content().strip(),ends[0].text_content().strip())
    units={n.get('id') for n in inline.nodes(tree,'unit') if [x.text_content().strip() for x in inline.nodes(n,'measure')]==['iso4217:USD'] and not inline.nodes(n,'divide')}
    out={}
    for table in tree.xpath('//table'):
        if table.xpath('.//table|ancestor::blockquote'):continue
        if not any(n.get('name')=='us-gaap:'+TAGS['sga'] for n in inline.nodes(table,'nonfraction')):continue
        try:
            panel=tables.parse_table(table,issuer=issuer,filed=date.fromisoformat(filing['filed']),
                currency_scope={'issuer':issuer,'currency':'USD','basis':'Individually verified entity-wide inline USD components'},
                _fiscal_periods=tuple(contexts.values()))
            geometry=tables.grid(table)
        except ValueError:continue
        if not panel or panel['kind']!='income':continue
        rows=table.xpath('./tr|./tbody/tr|./thead/tr|./tfoot/tr')
        for point in panel['observations']:
            if point['metric']!='gross_profit':continue
            period=(point['start'],point['end']);income=earnings.get(period+(filing['accession'],))
            if not income or income['heading']!=panel['heading']:continue
            values={};proofs={};sequence=[];invalid=False
            for index in range(point['row'],len(rows)):
                line=geometry[index]
                if not line:continue
                label=tables.clean(line[0]['text']).lower().rstrip(':')
                if not label and not any(c['text'].strip() for c in line):continue
                kind=next((k for k,labels in LABELS.items() if label in labels),None)
                if kind is None or kind in values:invalid=True;break
                cells=rows[index].xpath('./td|./th');positions=[c for c in line if c['row']==index]
                if len(cells)!=len(positions):invalid=True;break
                positions={id(c):p for c,p in zip(cells,positions)};accepted=[]
                for node in inline.nodes(rows[index],'nonfraction'):
                    if node.get('name')!='us-gaap:'+TAGS[kind] or contexts.get(node.get('contextref'))!=period or node.get('unitref') not in units or any(a.tag=='ix:hidden' for a in node.iterancestors()):continue
                    parents=node.xpath('ancestor::td|ancestor::th');pos=positions.get(id(parents[-1])) if parents else None
                    if not pos or not point['column_start']<=pos['start']<pos['end']<=point['column_end']:continue
                    value=inline._number(node,tree);displayed=tables.amount(line,point['column_start'],point['column_end'],point['scale'])
                    if displayed is None or value!=(-displayed if kind in ('interest','other') else displayed):continue
                    accepted.append((value,node.get('contextref')))
                if len(accepted)!=1:invalid=True;break
                value,context=accepted[0];values[kind]=value;sequence.append(kind)
                proofs[kind]={'value':value,'tag':TAGS[kind],'label':label,'context':context,'row':index}
                if kind=='pretax':break
            if (invalid or tuple(sequence)!=ORDER or not _reconciles(values) or values['gross']!=point['value'] or values['pretax']!=income['pretax']):continue
            value=values['gross']-values['sga']
            # Any printed operating subtotal must agree, even outside the bridge.
            reported=[]
            for line in geometry:
                if not line:continue
                label=tables.clean(line[0]['text']).lower().rstrip(':')
                if tables.metric_for(label,'income',issuer)=='operating_income' or label in {'operating income (loss)','operating profit','operating profit (loss)','operating (loss) profit'}:
                    reported.append(tables.amount(line,point['column_start'],point['column_end'],point['scale']))
            if reported and (len(reported)!=1 or reported[0]!=value):continue
            proof=dict(start=period[0],end=period[1],value=value,gross_profit=values['gross'],operating_expenses=values['sga'],unit='USD',
                filed=filing['filed'],accession=filing['accession'],source_cik=filing['cik'],source_url=filing['url'],source_sha256=parsed['evidence']['sha256'],
                row_labels=['Gross profit','Total selling and administrative expense'],statement_heading=panel['heading'],reported_operating_subtotal=reported[0] if reported else None,
                basis=BASIS,expense_bridge={'methodology':METHOD,'components':proofs,'pretax':income['pretax'],'income':income['value'],'tax':income['tax']})
            if period in out and out[period]!=proof:raise ValueError('operating_bridge_statement_conflict')
            out[period]=proof
    return out


def valid_bridge(row):
    bridge=row.get('expense_bridge')
    if row.get('row_labels')==['Gross profit','Total operating expenses']:return bridge is None
    try:
        if row['basis']!=BASIS or bridge['methodology']!=METHOD or set(bridge['components'])!=set(TAGS):return False
        components=bridge['components'];values={k:v['value'] for k,v in components.items()}
        if any(type(v) not in (int,float) or not math.isfinite(v) for v in values.values()):return False
        if any(p['tag']!=TAGS[k] or p['label'] not in LABELS[k] or not isinstance(p['context'],str) or not p['context'] or type(p['row']) is not int for k,p in components.items()):return False
        indices=[components[k]['row'] for k in ORDER]
        if indices!=sorted(set(indices)) or indices[0]<0:return False
        if any(type(bridge[k]) not in (int,float) or not math.isfinite(bridge[k]) for k in ('pretax','income','tax')):return False
        return (_reconciles(values) and row['gross_profit']==values['gross'] and row['operating_expenses']==values['sga']
            and values['pretax']==bridge['pretax'] and bridge['pretax']-bridge['tax']==bridge['income'])
    except (KeyError,TypeError,ValueError,AttributeError):return False
