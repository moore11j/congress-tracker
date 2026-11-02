"""Explicitly named IFRS statement sections, with source-column evidence.

This shared fallback preserves every already extracted result. It supports only
the fully proved calendar-quarter/EUR layout; other formats remain held.
"""
from datetime import date
import calendar,copy,json,re
from lxml import html
from app.services import sec_tabular_financials as tab

POLICY='explicit_named_statement_sections_v1'
RETRY_REASONS={'table_reporting_currency_unproved','table_issuer_unproved'}

def reject(reason):
    raise ValueError(reason)


def section_observations(raw,company):
    if len(raw)>20_000_000:reject('source_bound')
    tree=html.fromstring(raw)
    if tree.xpath('//base'):reject('base_element')
    for n in tree.xpath('//script|//style|//template'):n.drop_tree()
    if len(tree.xpath('//table'))>500:reject('table_bound')
    nodes=tree.xpath('//*[self::p or self::div or self::table][not(ancestor::table)][not(ancestor::blockquote)]'
        '[self::table or (not(.//table) and not(.//p) and not(.//div))]')
    texts=[tab.visible_text(n) for n in nodes]
    heading=re.compile(r'Primary Financial Statements of (.{1,140}) Group \(IFRS\)',re.I)
    starts=[(i,heading.fullmatch(t)) for i,(n,t) in enumerate(zip(nodes,texts)) if n.tag!='table' and heading.fullmatch(t)]
    if len(starts)!=1:reject('section_identity_ambiguous')
    start,match=starts[0]
    # A legal-form suffix can be omitted by an explicitly named consolidated
    # group heading. It never permits an arbitrary acronym or subsidiary alias.
    issuer=re.sub(r'\s+(?:SE|S\.A\.|plc)$','',company['name'],flags=re.I)
    if tab.company_key(issuer)!=tab.company_key(match[1]):reject('section_issuer_mismatch')
    ends=[i for i in range(start+1,len(nodes)) if re.fullmatch(r'Non-IFRS Numbers',texts[i],re.I)]
    if len(ends)!=1:reject('section_end_unproved')
    end=ends[0]
    scope=texts[start:end]
    if any(re.search(r'non.?ifrs|non.?gaap|adjusted|pro.?forma',s,re.I) for s in scope):reject('mixed_accounting_section')
    if company.get('fiscalYearEnd')!='1231':reject('calendar_quarter_unproved')
    candidates=[];kind=None;caption=None
    for i in range(start+1,end):
        node=nodes[i];text=texts[i]
        normalized=re.sub(r'^\([A-Z](?:\.\d+)?\)\s*','',text)
        detected=None
        if re.fullmatch(r'Consolidated Income Statements(?: [–-] (?:Quarter|Year-to-Date))?',normalized,re.I):detected='income'
        elif re.fullmatch(r'Consolidated Statements of Financial Position',normalized,re.I):detected='balance'
        elif re.fullmatch(r'Consolidated Statements of Cash Flows',normalized,re.I):detected='cashflow'
        if detected:
            kind=detected;caption=text
            continue
        if node.tag!='table':
            if not text or re.fullmatch(r'\d{1,3}/\d{1,3}',text):continue
            if re.match(r'^\d{1,2} For the (?:three months ended|first (?:three|six|nine|twelve) months of)\b',text):continue
            reject('unrecognized_section_context')
        rows=tab.grid(node)
        if len(rows)<=1:
            if re.fullmatch(r'Quarterly Statement Q[1-4] 20\d{2}',text):continue
            reject('unrecognized_layout_table')
        if kind is None:reject('unowned_numeric_table')
        candidates.append((i,kind,caption,rows))
    if {r[1] for r in candidates}!={'income','balance','cashflow'}:reject('statement_kinds_missing')
    balances=[r for r in candidates if r[1]=='balance']
    if len(balances)!=1:reject('balance_count')
    _,_,_,balance_rows=balances[0]
    date_caption=' '.join(c['text'] for c in balance_rows[0])
    dates_match=re.fullmatch(r'as at (\d{1,2})/(\d{1,2})/(20\d{2}) and (\d{1,2})/(\d{1,2})/(20\d{2})',date_caption,re.I)
    if not dates_match:reject('comparative_dates_unproved')
    nums=list(map(int,dates_match.groups()));balance_dates=[]
    for month,day,year in (nums[:3],nums[3:]):
        if day<=12:reject('numeric_date_ambiguous')
        value=date(year,month,day)
        if value.day!=calendar.monthrange(year,month)[1]:reject('balance_not_month_end')
        balance_dates.append(value)
    if len({d.year for d in balance_dates})!=2:reject('year_only_balance_columns_ambiguous')
    current=max(balance_dates)
    if current.month not in {3,6,9,12}:reject('current_quarter_unproved')
    # The quarter income footnote independently anchors month/day and both
    # comparative years; no filing date is substituted for a statement date.
    month_name=calendar.month_name[current.month]
    footnote=re.compile(r'For the three months ended '+month_name+r' '+str(current.day)+r', (20\d{2}) and (20\d{2}),',re.I)
    anchors=[m for text in scope if (m:=footnote.search(text))]
    if len(anchors)!=1 or set(map(int,anchors[0].groups()))!={current.year,current.year-1}:reject('quarter_date_corroboration_missing')
    facts=[];table_proofs=[]
    for index,kind,caption,rows in candidates:
        header_index=1 if kind=='balance' else 0
        header=rows[header_index]
        if not re.fullmatch(r'€ millions(?:, unless otherwise stated)?',header[0]['text']):reject('explicit_eur_scale_missing')
        columns=[]
        for cell in header[1:]:
            label=cell['text']
            if not label or label=='∆ in %':continue
            if kind=='balance':
                if not re.fullmatch(r'20\d{2}',label):reject('balance_header_unsupported')
                matches=[d for d in balance_dates if d.year==int(label)]
                if len(matches)!=1:reject('balance_column_date')
                begin=None;finish=matches[0];months=None
            else:
                period=re.fullmatch(r'Q(?:(1)[–-]Q)?([1-4]) (20\d{2})',label)
                if not period:reject('quarter_header_unsupported')
                ytd,q,year=period.groups();q=int(q);year=int(year)
                if year not in {current.year,current.year-1} or q*3!=current.month:reject('quarter_header_conflict')
                if kind=='cashflow' and not ytd:reject('cashflow_duration_conflict')
                if kind=='income' and bool(ytd)!=('Year-to-Date' in caption):reject('income_duration_conflict')
                finish=date(year,q*3,calendar.monthrange(year,q*3)[1]);months=q*3 if ytd else 3
                begin=date(year,1 if ytd else q*3-2,1)
            columns.append(dict(start=str(begin) if begin else None,end=str(finish),months=months,header=label,column_start=cell['start'],column_end=cell['end']))
        if len(columns)!=2:reject('comparative_column_count')
        table_proofs.append(dict(node=index,kind=kind,heading=caption,columns=columns))
        for row_number,row in enumerate(rows[header_index+1:],header_index+1):
            if not row:continue
            label=row[0]['text'];metric=tab.metric_for(label,kind,company['name'])
            if re.search(r'\b(?:millions|thousands)\b',label,re.I):
                if [c['text'] for c in row]!=[c['text'] for c in header]:reject('repeated_header_conflict')
                continue
            if kind=='income':metric=metric or {'Operating profit (loss)':'operating_income','Profit (loss) after tax':'consolidated_income'}.get(label)
            if kind=='cashflow' and label=='Net cash flows from operating activities':metric='operating_cash_flow'
            if not metric:continue
            for column in columns:
                # Unicode minus is accepted only immediately before digits.
                numeric=copy.deepcopy(row)
                for cell in numeric:
                    if re.fullmatch(r'[–−]\d[\d,.]*',cell['text']):cell['text']='-'+cell['text'][1:]
                value=tab.amount(numeric,column['column_start'],column['column_end'],1_000_000)
                if value is None:continue
                facts.append(dict(metric=metric,value=value,currency='EUR',scale=1_000_000,source_label=label,source_node=index,source_row=row_number,source_table=tree.xpath('//table').index(nodes[index]),**column))
    identities=[(f['metric'],f['start'],f['end'],f['currency']) for f in facts]
    if len(set(identities))!=len(identities):reject('duplicate_observations')
    return dict(issuer=company['name'],section_heading=texts[start],section_end=texts[end],
        calendar_year_evidence=company['fiscalYearEnd'],balance_date_caption=date_caption,
        quarter_date_corroboration=anchors[0].group(0),current_period=str(current),
        tables=table_proofs,facts=facts,publication_eligible=False)



def extend(prior,raw,company_raw):
    """Called only after existing filing, CIK, cover-link and size validation."""
    if prior['facts']:return prior
    try:section=section_observations(raw,json.loads(company_raw))
    except ValueError:return prior
    if section['current_period']>prior['source']['filed']:return prior
    rows=section['facts'];covered={r['source_table'] for r in rows}
    holds=[h for h in prior['holds'] if not (h['table'] in covered and h['reason']=='table_issuer_unproved')]
    if holds or prior['conflicts']:return prior
    result=copy.deepcopy(prior)
    result['facts']=[{k:r[k] for k in ('metric','start','end','months','currency','value')}|
        {'occurrences':[{'table':r['source_table'],'row':r['source_row'],'label':r['source_label'],**r}]} for r in rows]
    result['tables']=[{**t,'observations':[r for r in rows if r['source_node']==t['node']]} for t in section['tables']]
    result['holds']=holds
    result['source']['statement_section']={'policy':POLICY,**{k:v for k,v in section.items() if k not in {'facts','tables'}}}
    result['status']='prepared'
    return result
