"""Explicit shareholder income and printed common-share EPS from linked notes.

No consolidated-to-parent substitution, inferred share counts or derived EPS.
"""
import re
from lxml import html
from app.services import sec_tabular_financials as tab,sec_statement_layout as layout

POLICY='explicit_shareholder_note_v1'


def fail(reason):
    raise ValueError(reason)


def note_context(table,issuer):
    """The bounded local note owns its numbers, not the SEC filer alone."""
    texts=[];header=None
    for i,node in enumerate(table.itersiblings(preceding=True)):
        if i>=12:fail('note_context_bound')
        if node.xpath('ancestor::blockquote'):fail('quoted_note')
        text=tab.visible_text(node)
        if not text and node.tag in {'p','table'}:continue
        if node.tag=='table':
            rows=tab.grid(node)
            if len(rows)!=2 or len([c for c in rows[0] if c['text']])!=1:fail('note_header_unproved')
            owner=next(c['text'] for c in rows[0] if c['text'])
            if tab.company_key(owner)!=tab.company_key(issuer):fail('note_owner_mismatch')
            header=' '.join(c['text'] for c in rows[1] if c['text'])
            break
        if node.tag!='p' or len(text)>700:fail('note_context_unproved')
        texts.append(text)
    if header is None or len(texts)!=3:fail('note_context_unproved')
    paragraph,basic,heading=texts
    if not re.fullmatch(r'a\) Basic',basic,re.I):fail('note_kind_unproved')
    number=re.fullmatch(r'(\d{1,2})\. Earnings per share',heading,re.I)
    if number is None:fail('note_heading_unproved')
    match=re.fullmatch(r'Basic earnings per share is calculated by dividing net income attributable to equity holders of (.{1,140}) by the weighted average number of common shares issued and outstanding for the (.{1,130}):',paragraph,re.I)
    if not match or tab.company_key(match[1])!=tab.company_key(issuer):fail('parent_holder_identity_unproved')
    title=re.fullmatch(r'Notes to the unaudited condensed consolidated interim financial statements As of ([A-Za-z]+ \d{1,2}, 20\d{2}) and for the (.{1,130}) \(All amounts in (thousands|millions) of reais unless otherwise stated\)',header,re.I)
    if not title or title[2].lower()!=match[2].lower():fail('note_period_caption_unproved')
    if not re.fullmatch(r'(?:three|six|nine|twelve)(?:- and (?:three|six|nine|twelve))?-month periods? ended [A-Za-z]+ \d{1,2}, 20\d{2}(?: and 20\d{2})?',match[2],re.I):fail('note_period_caption_unproved')
    return dict(number=number[1],heading=heading,paragraph=paragraph,header=header,as_of=title[1],period_caption=title[2],
                scale=1000 if title[3].lower()=='thousands' else 1_000_000)


def columns(rows,filed,context):
    if len(rows)<3:fail('note_rows_unproved')
    anchors=[c for c in rows[1] if re.fullmatch(r'20\d{2}',c['text'])]
    if not 2<=len(anchors)<=4:fail('note_columns_unproved')
    result=[]
    for c in anchors:
        texts=[a['text'] for row in rows[:2] for a in row if a['start']<=c['start']<a['end'] and a['text']]
        caption=layout.period_text(context['period_caption'])
        try:
            start,end,months=tab.column_period(' '.join([caption]+texts),texts,[caption],'income',filed)
        except tab.TabularFinancialError as exc:
            raise ValueError('note_period_column_conflict') from exc
        result.append(dict(start=str(start),end=str(end),months=months,column_start=c['start'],column_end=c['end'],header=texts))
    if len({(c['start'],c['end']) for c in result})!=len(result):fail('duplicate_note_period')
    return result


def enrich(raw,observations,*,issuer,filed,currency):
    """Extend only supported layout statements using their complete original."""
    if observations['source'].get('statement_layout_policy')!=layout.POLICY or currency!='BRL':return None
    if observations['holds'] or observations['conflicts']:return None
    tree=html.fromstring(raw)
    for n in tree.xpath('//script|//style|//template'):n.drop_tree()
    tables=tree.xpath('//table')
    scope=layout.currency_scope(tree,issuer)
    if scope is None:return None
    candidates=[]
    for index,t in enumerate(tables):
        if t.xpath('ancestor::blockquote') or not re.search(r'Profit attributable to stockholders of the Company',tab.visible_text(t),re.I):continue
        rows=tab.grid(t)
        if any(re.fullmatch(r'Profit attributable to stockholders of the Company',r[0]['text'],re.I) for r in rows if r):candidates.append((index,t,rows))
    if not candidates:return None
    if len(candidates)!=1:fail('competing_parent_notes')
    index,t,rows=candidates[0];context=note_context(t,issuer)
    if context['scale']!=scope['scale']:fail('note_scale_conflict')
    cols=columns(rows,filed,context)
    try:as_of=str(tab.period_for(context['as_of'],'balance',filed)[1])
    except tab.TabularFinancialError as exc:raise ValueError('note_as_of_mismatch') from exc
    if as_of!=max(c['end'] for c in cols):fail('note_as_of_mismatch')
    primary=[t for t in observations['tables'] if t['kind']=='income'
             and not any(re.search(r'\bcomprehensive\b',h,re.I) for h in t['heading'])
             and any(r['metric']=='consolidated_income' for r in t['observations'])]
    if len(primary)!=1:fail('primary_income_ambiguous')
    primary=primary[0]
    periods={(c['start'],c['end'],c['months']) for c in primary['columns']}
    if {(c['start'],c['end'],c['months']) for c in cols}!=periods:fail('note_primary_period_mismatch')
    parent_rows=[(i,r) for i,r in enumerate(rows) if r and re.fullmatch(r'Profit attributable to stockholders of the Company',r[0]['text'],re.I)]
    basic_rows=[(i,r) for i,r in enumerate(rows) if r and re.fullmatch(r'Basic earnings per share - R\$',r[0]['text'],re.I)]
    if len(parent_rows)!=1 or len(basic_rows)!=1:fail('note_metric_ambiguous')
    primary_rows=tab.grid(tables[primary['table']]);printed={}
    for kind in ('Basic','Diluted'):
        matches=[(i,r) for i,r in enumerate(primary_rows) if r and re.fullmatch(kind+r' earnings per common share - R\$',r[0]['text'],re.I)]
        if len(matches)!=1:fail('primary_eps_ambiguous')
        ri,row=matches[0]
        refs=[c['text'] for c in row[1:] if c['end']<=min(c['column_start'] for c in primary['columns']) and c['text']]
        if refs!=[context['number']]:fail('primary_note_reference_mismatch')
        printed[kind]={(c['start'],c['end']):(tab.amount(row,c['column_start'],c['column_end'],1),ri,row[0]['text'],c) for c in primary['columns']}
    facts=[];basic_proof=[]
    for col in cols:
        key=col['start'],col['end'];pri=printed['Basic'][key];dil=printed['Diluted'][key]
        pi,pr=parent_rows[0];bi,br=basic_rows[0]
        parent=tab.amount(pr,col['column_start'],col['column_end'],scope['scale'])
        basic=tab.amount(br,col['column_start'],col['column_end'],1)
        if parent is None or basic is None or pri[0]!=basic or dil[0] is None:fail('note_primary_eps_mismatch')
        for metric,value,table,row,label,scale in [('parent_income',parent,index,pi,pr[0]['text'],scope['scale']),('eps',dil[0],primary['table'],dil[1],dil[2],1)]:
            occurrence=dict(table=table,row=row,label=label,**(col if metric=='parent_income' else dil[3]))
            occurrence['scale']=scale
            facts.append(dict(metric=metric,value=value,currency='BRL',start=col['start'],end=col['end'],months=col['months'],occurrences=[occurrence]))
        basic_proof.append(dict(start=col['start'],end=col['end'],value=basic,note_table=index,note_row=bi,primary_table=primary['table'],primary_row=pri[1]))
    return dict(policy=POLICY,context=context,facts=facts,basic_eps_corroboration=basic_proof,
                limitations=['No share-count units or shares inferred. EPS is the printed diluted common-share value, not income divided by ambiguous share counts.'])
