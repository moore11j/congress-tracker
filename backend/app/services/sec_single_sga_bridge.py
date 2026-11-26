"""Complete single-SG&A operating bridge from one issuer-owned statement."""
from datetime import date
import hashlib,math
METHOD='sec_single_sga_operating_bridge_v1'
BASIS='Calculated operating result: gross profit less the sole reported SG&A expense, with every subsequent interest, other-income and tax row reconciled through net income. Not EBITDA.'
ROW_LABELS=['Gross income','Selling, general and administrative expense']
from lxml import html
from app.services import sec_inline_financials as inline,sec_tabular_financials as tables
from app.services.sec_revenue_statement_layout import _visible
TAGS=['GrossProfit','SellingGeneralAndAdministrativeExpense','OperatingIncomeLoss','InterestIncomeExpenseNonoperatingNet','OtherNonoperatingIncomeExpense','IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest','IncomeTaxExpenseBenefit','NetIncomeLoss']
LABELS={
 'GrossProfit':{'gross income'},
 'SellingGeneralAndAdministrativeExpense':{'selling, general and administrative expense','selling, general and administrative expenses'},
 'OperatingIncomeLoss':{'operating income','operating income (loss)'},
 'InterestIncomeExpenseNonoperatingNet':{'interest expense'},
 'OtherNonoperatingIncomeExpense':{'other income'},
 TAGS[5]:{'income before income taxes','income (loss) before income taxes','loss before income taxes'},
 'IncomeTaxExpenseBenefit':{'income tax (expense) benefit','income tax benefit','income tax expense'},
 'NetIncomeLoss':{'net income','net income (loss)','net loss'},
}

def _qualify_original(raw,source,company):
    cik=str(company['cik']).zfill(10)
    if hashlib.sha256(raw).hexdigest()!=source['sha256']:raise ValueError('source_hash')
    inline.parse(raw,cik=cik,accession=source['accession'],filed=source['filing_date'],report_date=source['period'],form=source['form'],url=source['url'])
    tree=html.fromstring(raw);contexts={};units=set()
    for n in inline.nodes(tree,'context'):
        ids,starts,ends=(inline.nodes(n,k) for k in ['identifier','startdate','enddate'])
        if (not inline.nodes(n,'segment') and not inline.nodes(n,'scenario') and len(ids)==len(starts)==len(ends)==1
            and ids[0].get('scheme')=='http://www.sec.gov/CIK' and ids[0].text_content().strip().zfill(10)==cik):
            contexts[n.get('id')]=(starts[0].text_content().strip(),ends[0].text_content().strip())
    for n in inline.nodes(tree,'unit'):
        if not inline.nodes(n,'divide') and [x.text_content().strip() for x in inline.nodes(n,'measure')]==['iso4217:USD']:units.add(n.get('id'))
    results=[]
    for table in tree.xpath('//table'):
        if table.xpath('.//table|ancestor::blockquote') or not _visible(table):continue
        if not any(n.get('name')=='us-gaap:GrossProfit' for n in inline.nodes(table,'nonfraction')):continue
        panel=tables.parse_table(table,issuer=company['name'],filed=date.fromisoformat(source['filing_date']),currency_scope=dict(issuer=company['name'],currency='USD',basis='Each monetary cell independently checked against visible entity USD facts'))
        if not panel or panel['kind']!='income':continue
        rows=table.xpath('./tr|./tbody/tr|./thead/tr|./tfoot/tr');geometry=tables.grid(table)
        columns={}
        for point in panel['observations']:
            columns.setdefault((point['start'],point['end']),point)
        for point in columns.values():
            period=(point['start'],point['end']);proof=[];active=False
            start,end=map(date.fromisoformat,period)
            if not 1 <= (end-start).days+1 <= 380 or end > date.fromisoformat(source['period']):
                raise ValueError('period_outside_filing')
            for ri,(row,line) in enumerate(zip(rows,geometry)):
                cells=row.xpath('./td|./th');positions=[c for c in line if c['row']==ri]
                if len(cells)!=len(positions):raise ValueError('row_geometry')
                inside=[(cell,pos) for cell,pos in zip(cells,positions) if point['column_start']<=pos['start']<point['column_end']]
                nodes=[n for cell,pos in inside for n in inline.nodes(cell,'nonfraction')]
                if not active:
                    if not any(n.get('name')=='us-gaap:GrossProfit' for n in nodes):continue
                    active=True
                if any(pos['end']>point['column_end'] for cell,pos in inside):raise ValueError('cross_column')
                text=''.join(pos['text'] for cell,pos in inside).replace('$','').strip()
                if not nodes and not text:continue
                if len(nodes)!=1:raise ValueError('unaccounted_amount')
                n=nodes[0];tag=n.get('name','').removeprefix('us-gaap:')
                if tag not in TAGS or contexts.get(n.get('contextref'))!=period or n.get('unitref') not in units or not _visible(n) or inline._nil_fact(n):raise ValueError('component_identity')
                label=tables.clean(line[0]['text']).lower().rstrip(':')
                # Exact observed printed labels, including their footnote markers.
                allowed=LABELS[tag]|{x+digit for x in LABELS[tag] for digit in ['1','2','3','4']}
                if label not in allowed:raise ValueError(('component_label',tag,label))
                value=inline._number(n,tree);display=tables.amount(line,point['column_start'],point['column_end'],point['scale'])
                if display is None and text in {'-','\u2014','\u2013'} and value==0:display=0
                if tag=='SellingGeneralAndAdministrativeExpense':
                    sign_ok=value>=0 and display in (value,-value)
                elif tag=='InterestIncomeExpenseNonoperatingNet':
                    sign_ok=value<=0 and display in (value,-value)
                elif tag=='IncomeTaxExpenseBenefit':
                    sign_ok=display==value*(-1 if 'benefit' in label else 1)
                else:sign_ok=display==value
                if not sign_ok:raise ValueError(('display_sign',tag,label,display,value))
                proof.append(dict(tag=tag,value=value,display=display,row=ri,label=label,context=n.get('contextref')))
                if tag=='NetIncomeLoss':break
            tags=[x['tag'] for x in proof]
            if tags not in [TAGS,TAGS[:2]+TAGS[3:]]:raise ValueError(('bridge_sequence',tags))
            values={x['tag']:x['value'] for x in proof};op=values[TAGS[0]]-values[TAGS[1]]
            if values[TAGS[1]]<0 or (TAGS[2] in values and values[TAGS[2]]!=op):raise ValueError('operating_subtotal')
            if op+values[TAGS[3]]+values[TAGS[4]]!=values[TAGS[5]] or values[TAGS[5]]-values[TAGS[6]]!=values[TAGS[7]]:raise ValueError('earnings_bridge')
            results.append(dict(start=period[0],end=period[1],operating_income=op,components=proof,heading=panel['heading'],sha256=source['sha256'],url=source['url']))
    if not results:raise ValueError('no_qualified_primary_statement')
    return results


def candidate_expenses(payload,gross,*,available_by):
    from app.services import sec_fundamentals as sec
    required=[TAGS[i] for i in (1,3,4,5,6,7)]
    if not gross or any(t not in payload.get('facts',{}).get('us-gaap',{}) for t in required):return {}
    series={t:sec._records(payload,'operating_income',available_by,concepts=(t,)) for t in required}
    accepted={}
    for period,left in gross.items():
        points={t:rows.get(period) for t,rows in series.items()}
        if not left or not all(points.values()):continue
        if any(any(r[k]!=left[k] for k in ('start','end','filed','accession','unit')) for r in points.values()):continue
        values={t:r['value'] for t,r in points.items()}
        if any(type(v) not in (int,float) or not math.isfinite(v) for v in [left['value'],*values.values()]):continue
        op=left['value']-values[TAGS[1]]
        if values[TAGS[1]]>=0 and values[TAGS[3]]<=0 and op+values[TAGS[3]]+values[TAGS[4]]==values[TAGS[5]] and values[TAGS[5]]-values[TAGS[6]]==values[TAGS[7]]:
            accepted[period]=points[TAGS[1]]
    return accepted


def statement_rows(raw,filing,*,issuer):
    source=dict(sha256=hashlib.sha256(raw).hexdigest(),accession=filing['accession'],filing_date=filing['filed'],period=filing['report_date'],form=filing['form'],url=filing['url'])
    points=_qualify_original(raw,source,dict(cik=filing['cik'],name=issuer));out={}
    for point in points:
        values={p['tag']:p['value'] for p in point['components']}
        row=dict(start=point['start'],end=point['end'],value=point['operating_income'],gross_profit=values[TAGS[0]],operating_expenses=values[TAGS[1]],
            unit='USD',filed=filing['filed'],accession=filing['accession'],source_cik=filing['cik'],source_url=filing['url'],source_sha256=source['sha256'],
            row_labels=ROW_LABELS,statement_heading=point['heading'],reported_operating_subtotal=values.get(TAGS[2]),basis=BASIS,
            expense_bridge=dict(methodology=METHOD,components=point['components']))
        key=(point['start'],point['end'])
        if key in out and out[key]!=row:raise ValueError('single_sga_statement_conflict')
        out[key]=row
    return out


def valid_bridge(row):
    try:
        if row['row_labels']!=ROW_LABELS or row['basis']!=BASIS or row['expense_bridge']['methodology']!=METHOD:return False
        parts=row['expense_bridge']['components'];tags=[p['tag'] for p in parts]
        if tags not in [TAGS,TAGS[:2]+TAGS[3:]]:return False
        indices=[p['row'] for p in parts]
        if any(type(v) is not int or v<0 for v in indices) or indices!=sorted(set(indices)):return False
        for p in parts:
            if any(type(p[k]) not in (int,float) or not math.isfinite(p[k]) for k in ('value','display')) or not isinstance(p['context'],str) or not p['context']:return False
            tag,label,value,display=p['tag'],p['label'],p['value'],p['display']
            allowed=LABELS[tag]|{x+digit for x in LABELS[tag] for digit in ['1','2','3','4']}
            if label not in allowed:return False
            if tag==TAGS[1]:valid=value>=0 and display in (value,-value)
            elif tag==TAGS[3]:valid=value<=0 and display in (value,-value)
            elif tag==TAGS[6]:valid=display==value*(-1 if 'benefit' in label else 1)
            else:valid=display==value
            if not valid:return False
        values={p['tag']:p['value'] for p in parts};op=values[TAGS[0]]-values[TAGS[1]]
        return (row['value']==op and row['gross_profit']==values[TAGS[0]] and row['operating_expenses']==values[TAGS[1]]
            and row['reported_operating_subtotal']==values.get(TAGS[2]) and (TAGS[2] not in values or values[TAGS[2]]==op)
            and op+values[TAGS[3]]+values[TAGS[4]]==values[TAGS[5]] and values[TAGS[5]]-values[TAGS[6]]==values[TAGS[7]])
    except (KeyError,ValueError,TypeError,AttributeError):return False
