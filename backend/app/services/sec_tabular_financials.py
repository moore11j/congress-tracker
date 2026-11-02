"""Source-bound HTML statement observations. No provider selection or publication.

Column geometry, not ticker-specific positions, binds each number to its printed
period and currency. Consumers must separately prove accounting/TTM comparability.
"""
import calendar
from datetime import date
from decimal import Decimal, InvalidOperation
import hashlib
import json
import re
from urllib.parse import urljoin
from lxml import html

VERSION = 'sec_tabular_financial_evidence_v1'
MAX_BYTES = 20_000_000
MONTHS = {name.lower(): n for n, name in enumerate(calendar.month_name) if name}
MONTH_PATTERN = '|'.join(MONTHS)
from app.services.sec_statement_currency import ALIASES
CURRENCIES = {**ALIASES, 'USD': ALIASES['USD'] + r'|US\$', 'EUR': ALIASES['EUR'] + r'|€', 'GBP': ALIASES['GBP'] + r'|£'}
DURATIONS = {'three':3,'six':6,'nine':9,'twelve':12,'3':3,'6':6,'9':9,'12':12}


class TabularFinancialError(ValueError):
    pass


def clean(value):
    return ' '.join(value.replace('\xa0',' ').replace('\u200b','').split())


def visible_text(node):
    """Join adjacent inline runs without inventing spaces inside printed words."""
    blocks={'p','div','br','li','tr','td','th','h1','h2','h3','h4'}
    def pieces(current):
        if current.text:yield current.text
        for child in current:
            if isinstance(child.tag,str):
                if child.tag.lower() in blocks:yield ' '
                yield from pieces(child)
                if child.tag.lower() in blocks:yield ' '
            if child.tail:yield child.tail
    return clean(''.join(pieces(node)))


def company_key(value):
    value = re.sub(r'^the\s+', '', value, flags=re.I)
    value = re.sub(r'\s+and (?:its )?subsidiaries$', '', value, flags=re.I)
    value = re.sub(r'\b(?:corporation|corp\.?|incorporated|inc\.?|limited|ltd\.?)\b', '', value, flags=re.I)
    return re.sub(r'[^a-z0-9]', '', value.lower())


def grid(table):
    """Retain real cell spans. Never align values by removing blank cells."""
    if table.xpath('.//table'):
        raise TabularFinancialError('nested_table')
    rows=table.xpath('./tr|./tbody/tr|./thead/tr|./tfoot/tr')
    if len(rows)>2000:raise TabularFinancialError('table_row_bound')
    out=[];occupied={}
    for r,row in enumerate(rows):
        cells=[];column=0
        for cell in row.xpath('./td|./th'):
            while (r,column) in occupied:column+=1
            try:span=int(cell.get('colspan','1'));height=int(cell.get('rowspan','1'))
            except ValueError as exc:raise TabularFinancialError('table_span') from exc
            if not 1<=span<=128 or not 1<=height<=100 or column+span>128 or r+height>len(rows):
                raise TabularFinancialError('table_span')
            if any((y,x) in occupied for y in range(r,r+height) for x in range(column,column+span)):
                raise TabularFinancialError('table_overlap')
            record={'text':visible_text(cell),'start':column,'end':column+span,'row':r}
            for y in range(r,r+height):
                for x in range(column,column+span):occupied[y,x]=record
            cells.append(record);column+=span
        # Include cells inherited through rowspan once, preserving their origin.
        inherited={id(v):v for (y,x),v in occupied.items() if y==r and v['row']<r}
        out.append(sorted(cells+list(inherited.values()),key=lambda c:c['start']))
        for key in [key for key in occupied if key[0]==r]:del occupied[key]
    return out


def preceding_text(table, *, nonempty=False):
    # Only local text blocks, not a whole ancestor DIV containing other tables.
    # A bounded fallback ignores empty layout paragraphs without crossing an
    # intervening issuer caption; parse_table still checks the nearest owner.
    nodes=table.xpath('preceding::*[self::p or self::div][not(ancestor::table)][not(.//table)][not(.//p)][not(.//div)]')[-64 if nonempty else -8:]
    texts=[text for n in nodes if (text:=visible_text(n))]
    return texts[-8:] if nonempty else texts


def statement_layout_context(text):
    """Recognize exact page furniture without treating other prose as neutral."""
    return bool(re.fullmatch(r'\d{1,3}',text) or text in {
        'Table of Contents',
        'See accompanying Notes to Consolidated Financial Statements.',
        'See notes to unaudited condensed consolidated financial statements.',
    })


def statement_prefix(text):
    text=re.sub(r'\bSummary\s+(?=Consolidated (?:Balance Sheets?|Statements? of)\b)', '', text, flags=re.I)
    return re.split(r'\b(?:unaudited|audited|interim(?= (?:condensed|consolidated))|condensed|consolidated)\b',text,maxsplit=1,flags=re.I)[0].strip()


def neutral_statement_context(text):
    """Page numbers and numbered explanatory footnotes are not issuer captions."""
    return bool(re.fullmatch(r'\d{1,3}', text) or
                (len(text) > 100 and re.match(r'^\(\d{1,2}\)\s', text)
                 and table_kind(text) is None))


def document_statement_scope(tree, issuer):
    """An exact issuer earnings headline can own otherwise unnamed statements.

    This is never inferred from a company mention, URL, SEC filer identity alone,
    or a quote. Any competing financial headline/caption disables this fallback.
    """
    nodes=tree.xpath('//*[self::p or self::div or self::h1 or self::h2]'
                     '[not(ancestor::blockquote)][not(.//table)][not(.//p)]'
                     '[not(.//div)][not(.//h1)][not(.//h2)]')
    blocks=[(n,visible_text(n)) for n in nodes]
    blocks=[(n,t) for n,t in blocks if t]
    heading=re.compile(r'(?P<issuer>.{1,160}?) (?:Reports|Announces) '
                       r'(?:(?:First|Second|Third|Fourth) Quarter|(?:Full|Fiscal) Year) '
                       r'20\d{2} (?:Financial )?Results', re.I)
    top=[t for n,t in blocks if not n.xpath('ancestor::table')][:12]
    titles=[(t,heading.fullmatch(t)) for t in top]
    titles=[(t,m) for t,m in titles if m]
    if len(titles)!=1 or company_key(titles[0][1]['issuer'])!=company_key(issuer):
        from app.services.sec_statement_declarations import named_statement_scope, unnamed_heading
        # Visible nonquoted declaration only; independently checked local
        # captions still prevent a different entity from owning a table.
        scope=named_statement_scope(' '.join(t for n,t in blocks),issuer,company_key)
        if scope is None:return None
        # A different explicit statement owner anywhere in the document makes
        # unnamed tables ambiguous, even when local page furniture separates it.
        for node,text in blocks:
            match=heading.fullmatch(text)
            if match and company_key(match['issuer'])!=company_key(issuer):return None
            if len(text)<=240 and table_kind(text) is not None and not statement_prefix(text) and not unnamed_heading(text):
                return None
            if len(text)<=240 and table_kind(text) is not None and re.search(
                    r'(?:balance sheets?|statements? of (?:financial position|operations|income|profit or loss|comprehensive (?:loss|income)|cash flows?))(?: \(unaudited\))?$',text,re.I):
                prefix=statement_prefix(text)
                if prefix and company_key(prefix)!=company_key(issuer):return None
        return scope
    for i,(node,text) in enumerate(blocks):
        match=heading.fullmatch(text)
        if match and company_key(match['issuer'])!=company_key(issuer):return None
        if table_kind(text) is None:continue
        prefix=statement_prefix(text)
        if not prefix:
            # Inspect beyond page numbers, not just the last eight local blocks.
            current_table=node.xpath('ancestor::table')
            prefix=next((t for n,t in reversed(blocks[:i])
                         if (not n.xpath('ancestor::table') or n.xpath('ancestor::table')==current_table)
                         and not neutral_statement_context(t)
                         and not re.match(r'^\(?\s*(?:Amounts expressed in|In thousands|In millions|Stated in)\b',t,re.I)), '')
            if table_kind(prefix) is not None:continue
            # Long prose is not an entity caption; local admission below still
            # requires only neutral blocks immediately before an unnamed title.
            if len(prefix)>160:continue
        if prefix and company_key(prefix)!=company_key(issuer):return None
    return {'issuer':issuer,'headline':titles[0][0],'basis':'exact_issuer_earnings_headline'}


def currencies(text, *, expanded=False):
    if expanded:text=re.sub(r"\bUS\s+\$", "US$", text, flags=re.I)
    return {code for code,pattern in CURRENCIES.items() if re.search(r'(?<![A-Za-z])(?:' + pattern + r')(?![A-Za-z])',text,re.I)}


def statement_currency_scope(tree,issuer):
    """Explicit currency of these statements, never a derivative or bare dollar."""
    nodes=tree.xpath('//*[self::p or self::div][not(ancestor::blockquote)]'
                     '[not(.//table)][not(.//p)][not(.//div)]')
    blocks=[visible_text(n) for n in nodes]
    # A different entity's statement caption makes document-wide scope unsafe.
    for i,text in enumerate(blocks):
        if len(text)>240 or table_kind(text) is None:continue
        if not re.search(r'\b(?:balance sheets?|statements? of (?:operations|income|comprehensive (?:loss|income)|cash flows?))(?: \(unaudited\))?$',text,re.I):continue
        prefix=statement_prefix(text)
        if not prefix:
            if nodes[i].xpath('ancestor::table'):continue
            prior=next((v for v in reversed(blocks[:i]) if v and not neutral_statement_context(v)), '')
            if table_kind(prior) is not None:continue
            prefix=prior
        if prefix and company_key(prefix)!=company_key(issuer):return None
    subject=(r'\b(?:the(?: accompanying)?|these|our)\s+'
             r'(?:(?:unaudited|audited|interim|condensed|consolidated)\s+){0,5}'
             r'financial statements(?: of the Company| included in this Report)?\s+')
    bridge=(r'(?:(?:have been|are) prepared in accordance with '
            r'(?:(?![;]|\b(?:not|never|except|subsidiary|subsidiaries)\b).){1,220}? and )?')
    predicate=r'are (?:presented|denominated) in\s+'
    matches=[]
    for text in blocks:
        if not re.search(r'\bfinancial statements\b',text,re.I) or not re.search(predicate,text,re.I):continue
        for code,alias in ALIASES.items():
            for match in re.finditer(subject+bridge+predicate+r'('+alias+r')\b',text,re.I):
                prefix=text[max(0,match.start()-70):match.start()]
                if re.search(r'\b(?:not|never|unlike|if|hypothetical)\s+(?:\w+\s+){0,3}$',prefix,re.I):continue
                matches.append({'currency':code,'declaration':match.group(0)})
    units={m['currency'] for m in matches}
    if len(units)!=1:return None
    return {'currency':next(iter(units)),'issuer':issuer,'basis':'explicit_statement_currency',
            'declarations':[dict(t) for t in dict.fromkeys(tuple(m.items()) for m in matches)]}


def metric_for(label,kind,issuer, *, expanded=False):
    label=clean(label).lower().replace('\u2019',"'").rstrip(':')
    # Printed numeric note references do not change the financial line item.
    # Keep other parentheses (adjusted, per-share, currency, etc.) meaningful.
    label=re.sub(r'\s+\(\s*notes?\s+\d+(?:\s*(?:,|and|&|-)\s*\d+)*\s*\)$','',label)
    if re.search(r'non.?gaap|adjusted|per share|per ads|non.?controlling',label):return None
    exact={
        'total current assets':'current_assets','total current liabilities':'current_liabilities',
        'total assets':'total_assets','total liabilities':'total_liabilities',
        "total shareholders' equity":'total_equity',"total stockholders' equity":'total_equity',
        "total shareholders' (deficit) equity":'total_equity',
        'total equity':'total_equity','revenues':'revenue','revenue':'revenue','total revenue':'revenue',
        'total revenues':'revenue','net revenues':'revenue','total net revenues':'revenue','revenue, net':'revenue',
        'gross profit':'gross_profit','gross (loss) profit':'gross_profit',
        'operating income':'operating_income','operating loss':'operating_income',
        'income from operations':'operating_income','loss from operations':'operating_income',
        'income (loss) from operations':'operating_income','operating (loss) income':'operating_income',
        'net income':'consolidated_income','net loss':'consolidated_income',
        'net (loss) income':'consolidated_income','net income (loss)':'consolidated_income',
    }
    metric=exact.get(label)
    if metric and kind == ('balance' if metric in {'current_assets','current_liabilities','total_assets','total_liabilities','total_equity'} else 'income'):
        return metric
    parent=re.fullmatch(r'net (?:income|loss|\(loss\) income|income \(loss\)) attributable to (.+)',label)
    if parent and kind=='income' and company_key(parent[1])==company_key(issuer):return 'parent_income'
    if kind=='cashflow' and re.fullmatch(r'net cash (?:provided by|provided from|used in|used for|\(used in\) provided by|provided by \(used in\)|used in operating activities|from)(?: operating activities)?',label):
        return 'operating_cash_flow'
    if expanded:
        from app.services.sec_statement_heading_order import additional_metric
        return additional_metric(label,kind)
    return None


def table_kind(text, *, expanded=False):
    if expanded:
        if re.search(r'non.?ifrs',text,re.I):return None
        from app.services.sec_statement_heading_order import normalized_heading
        text=normalized_heading(text)
    if re.search(r'non.?gaap|reconciliation|adjusted|pro.?forma|notes to',text,re.I):return None
    if re.search(r'consolidated (?:(?:interim|condensed) )?(?:balance sheets?|statements? of financial position)',text,re.I):return 'balance'
    if re.search(r'consolidated (?:(?:interim|condensed) )?statements? of cash flows?',text,re.I):return 'cashflow'
    if re.search(r'consolidated (?:(?:interim|condensed) )?statements? of (?:operations|income|profit or loss|comprehensive (?:loss|income))',text,re.I):return 'income'
    return None


def period_for(text,kind,filed):
    years=set(re.findall(r'\b(?:19|20)\d{2}\b',text))
    dates=re.findall(r'\b('+MONTH_PATTERN+r')\s*,?\s*(\d{1,2})\b',text,re.I)
    dates={(MONTHS[m.lower()],int(d)) for m,d in dates}
    if len(years)!=1 or len(dates)!=1:raise TabularFinancialError('column_date_ambiguous')
    month,day=next(iter(dates))
    try:end=date(int(next(iter(years))),month,day)
    except ValueError as exc:raise TabularFinancialError('column_date_invalid') from exc
    if end>filed:raise TabularFinancialError('column_period_after_filing')
    if kind=='balance':return None,end,None
    matches={DURATIONS[n.lower()] for n in re.findall(r'\b(three|six|nine|twelve|3|6|9|12)\s+months?\s+ended',text,re.I)}
    if re.search(r'\byears?\s+ended\b',text,re.I):
        if re.search(r'\b(?:[0-9]+|one|two|three|four|five|six|seven|eight|nine|ten|twelve)\s+years?\s+ended\b',text,re.I):
            raise TabularFinancialError('column_duration_unsupported')
        matches.add(12)
    if len(matches)!=1 or end.day!=calendar.monthrange(end.year,end.month)[1]:
        raise TabularFinancialError('column_duration_unsupported')
    months=next(iter(matches));serial=end.year*12+end.month-months
    start=date(serial//12,serial%12+1,1)
    return start,end,months


def column_period(header,texts,shared_periods,kind,filed):
    """A comparative caption constrains, but cannot merge, explicit columns."""
    try:
        return period_for(header,kind,filed)
    except TabularFinancialError as original:
        if not shared_periods or str(original) not in {'column_date_ambiguous','column_duration_unsupported'}:
            raise
        # Only a self-contained column can resolve a comparative caption.
        # Year-only columns still require the original shared-header path.
        result=period_for(' '.join(texts),kind,filed)
        _,end,months=result
        for context in shared_periods:
            dates={(MONTHS[m.lower()],int(d)) for m,d in re.findall(
                r'\b('+MONTH_PATTERN+r')\s*,?\s*(\d{1,2})\b',context,re.I)}
            years={int(y) for y in re.findall(r'\b(?:19|20)\d{2}\b',context)}
            if dates!={(end.month,end.day)} or (years and end.year not in years):
                raise original
            durations=set()
            clauses=re.findall(r'\b((?:[a-z]+|\d+)(?:\s*(?:,|\band\b|\bor\b)\s*(?:[a-z]+|\d+))*)\s+months?\s+ended\b',context,re.I)
            for clause in clauses:
                numbers=re.split(r'\s*(?:,|\band\b|\bor\b)\s*',clause.lower())
                if any(n not in DURATIONS for n in numbers):raise original
                durations.update(DURATIONS[n] for n in numbers)
            if re.search(r'\byears?\s+ended\b',context,re.I):
                if re.search(r'\b(?:[0-9]+|one|two|three|four|five|six|seven|eight|nine|ten|twelve)\s+years?\s+ended\b',context,re.I):raise original
                durations.add(12)
            if months is not None and months not in durations:raise original
        return result


def amount(cells,start,end,scale):
    chosen=[c for c in cells if c['start']>=start and c['start']<end]
    if any(c['end']>end for c in chosen):raise TabularFinancialError('value_crosses_column')
    value=''.join(c['text'] for c in chosen).replace('$','').replace('US','').strip()
    if not value or value in {'-','\u2014','\u2013'}:return None
    if not re.fullmatch(r'(?:-?(?:\d+|\d{1,3}(?:,\d{3})+)(?:\.\d+)?|\((?:\d+|\d{1,3}(?:,\d{3})+)(?:\.\d+)?\))',value):
        raise TabularFinancialError('column_numeric_unsupported')
    try:value=Decimal(value.replace(',','').replace('(','-').replace(')',''))*scale
    except InvalidOperation as exc:raise TabularFinancialError('column_numeric_invalid') from exc
    if not value.is_finite() or abs(value)>Decimal('1e16'):raise TabularFinancialError('column_numeric_bound')
    return float(value)


def parse_table(table,*,issuer,filed,document_scope=None,currency_scope=None,periods_only=False,_nonempty_context=False,_expanded=False):
    rows=grid(table)
    if not rows:return None
    local=preceding_text(table, nonempty=_nonempty_context)
    # Find the first monetary row before treating later years as header labels.
    def data_row(row):
        if not row or row[0]['start']!=0 or not re.search(r'[A-Za-z]',row[0]['text']):return False
        if re.search(r'in thousands|in millions|months? ended|as of|as at',row[0]['text'],re.I):return False
        return any(re.fullmatch(r'\(?-?[0-9][0-9,.]*\)?',c['text']) for c in row[1:])
    boundary=next((i for i,row in enumerate(rows) if data_row(row)),None)
    if boundary is None:return None
    header_rows=rows[:boundary]
    intro=' '.join(local+[' '.join(c['text'] for c in r) for r in header_rows])
    identity_blocks=local+[c['text'] for row in header_rows for c in row if c['start']==0]
    from app.services.sec_statement_heading_order import CAPTION
    caption_pattern=CAPTION if _expanded else r'consolidated (?:(?:interim|condensed) )?(?:balance sheets?|statements? of)'
    captions=[(i,b) for i,b in enumerate(identity_blocks) if re.search(caption_pattern,b,re.I)]
    if not captions:return None
    caption_index,caption=captions[-1];kind=table_kind(caption,expanded=_expanded)
    if (kind is None and re.search(r'consolidated (?:(?:interim|condensed) )?statements? of comprehensive$',caption,re.I)
            and caption_index+1<len(identity_blocks)
            and re.fullmatch(r'(?:income|loss)',identity_blocks[caption_index+1],re.I)):
        # A printed title can wrap into two adjacent blocks. Join only its
        # exact missing final word, retaining the original blocks in evidence.
        caption=caption+' '+identity_blocks[caption_index+1]
        identity_blocks[caption_index:caption_index+2]=[caption]
        kind=table_kind(caption,expanded=_expanded)
    if kind is None:return None
    issuer_key=company_key(issuer)
    if any(company_key(b)==issuer_key for b in identity_blocks[caption_index+1:]):
        raise TabularFinancialError('table_issuer_unproved')
    identified=[];scope_used=False
    def scoped_context(value):
        if neutral_statement_context(value):return True
        if not document_scope or document_scope.get('basis')!='explicit_named_statement_preparation':return False
        from app.services.sec_statement_declarations import neutral_context
        return neutral_context(value,table_kind,statement_prefix,statement_layout_context)

    for index,block in enumerate(identity_blocks):
        if table_kind(block,expanded=_expanded) != kind:
            continue
        prefix=statement_prefix(block)
        if not prefix and document_scope and company_key(document_scope.get('issuer',''))==issuer_key and all(scoped_context(v) for v in identity_blocks[:index] if v.strip()):
            identified.append(True);scope_used=True
        else:
            if not prefix:
                prefix=next((v for v in reversed(identity_blocks[:index])
                             if v.strip() and not statement_layout_context(v)), '')
            identified.append(company_key(prefix)==issuer_key)
    # The closest statement heading owns this table. An earlier parent-company
    # mention cannot authorize an intervening subsidiary/acquisition statement.
    if not issuer_key or not identified or not identified[-1]:
        if not _nonempty_context:
            return parse_table(table,issuer=issuer,filed=filed,document_scope=document_scope,
                currency_scope=currency_scope,periods_only=periods_only,_nonempty_context=True,_expanded=_expanded)
        raise TabularFinancialError('table_issuer_unproved')
    scales={Decimal('1000') for _ in re.finditer(r'\bin thousands\b',intro,re.I)}
    scales.update(Decimal('1000000') for _ in re.finditer(r'\bin (?:[€£¥]\s+|USD |EUR |CHF )?millions\b',intro,re.I))
    if not scales and re.search(r'\bstated in (?:U\.?S\.? dollars?|USD)\b',intro,re.I):scales={Decimal(1)}
    if len(scales)!=1:raise TabularFinancialError('table_scale_unproved')
    scale=next(iter(scales))
    candidates=[]
    for index,row in enumerate(header_rows):
        # A lone footnote asterisk does not change an explicitly printed year.
        # Keep the original header in provenance; words, extra dates and other
        # suffixes are not silently stripped into a valid date anchor.
        anchors=[c for c in row if re.fullmatch(r'(?:19|20)\d{2}(?:\s*\*)?',c['text']) or re.fullmatch(r'(?:As (?:of|at)\s+)?(?:'+MONTH_PATTERN+r')\s+\d{1,2},?\s+(?:19|20)\d{2}(?:\s*\*)?',c['text'],re.I)]
        if len(anchors)>=2:candidates.append((index,anchors))
    if not candidates:raise TabularFinancialError('table_date_header_unproved')
    last,anchors=candidates[-1]
    for index,row in enumerate(header_rows[last+1:],last+1):
        monetary=[c for c in row if re.fullmatch(r'(?:[A-Z]{3}|US\$|[$€£¥])',c['text'])]
        if len(monetary)>=len(anchors):
            if any(not currencies(c['text'],expanded=_expanded) and c['text']!='$' for c in monetary):
                raise TabularFinancialError('column_currency_unproved')
            anchors=monetary;last=index
    shared=currencies(' '.join(local+[c['text'] for row in header_rows for c in row if c['start']==0]),expanded=_expanded)
    currency_used=False
    columns=[]
    for i,anchor in enumerate(anchors):
        texts=[]
        for row in header_rows:
            for cell in row:
                if cell['start']<=anchor['start']<cell['end'] and cell['text'] and cell['text'] not in texts:texts.append(cell['text'])
        # A statement may print one month/day above year-only columns. Keep
        # each column's own year and duration; competing dates still fail the
        # normal period parser instead of being silently replaced.
        shared_periods=[s for s in local+[c['text'] for row in header_rows for c in row if c['start']==0]
                        if re.search(r'\b(?:months?|years?|periods?)\s+ended\b',s,re.I)
                        and re.search(r'\b(?:'+MONTH_PATTERN+r')\s+\d{1,2}\b',s,re.I)]
        header=' '.join(shared_periods+texts)
        if shared_periods:
            header=re.sub(r'\b(three|six|nine|twelve|3|6|9|12) months?\b(?!\s+ended)',r'\1 months ended',header,flags=re.I)
        # A duration above the whole table applies only if columns have none.
        if not re.search(r'months?\s+ended',header,re.I):
            duration=[s for s in local if re.fullmatch(r'(?:For the )?(?:Three|Six|Nine|Twelve|3|6|9|12) months? ended',s,re.I)]
            if len(duration)==1:header+=' '+duration[0]
        start,finish,months=column_period(header,texts,shared_periods,kind,filed)
        if periods_only:
            columns.append({'start':str(start) if start else None,'end':str(finish),'months':months,'header':texts})
            continue
        unit=currencies(anchor['text'],expanded=_expanded) or currencies(header,expanded=_expanded)
        if not unit:unit=shared
        if not unit and currency_scope and company_key(currency_scope.get('issuer',''))==issuer_key:
            unit={currency_scope['currency']};currency_used=True
        if len(unit)!=1:raise TabularFinancialError('column_currency_unproved')
        declared=shared
        if currency_scope and unit!={currency_scope['currency']}:raise TabularFinancialError('column_currency_conflict')
        if len(declared)==1 and unit!=declared:raise TabularFinancialError('column_currency_conflict')
        columns.append({'column_start':anchor['start'],'column_end':anchors[i+1]['start'] if i+1<len(anchors) else max(c['end'] for r in rows for c in r),
                        'start':str(start) if start else None,'end':str(finish),'months':months,'currency':next(iter(unit)),
                        'scale':int(scale),'header':texts})
    if periods_only:return {'kind':kind,'columns':columns,'heading':local}
    observations=[];holds=[]
    for row_number,row in enumerate(rows[boundary:],boundary):
        if not row:continue
        label=row[0]['text'];metric=metric_for(label,kind,issuer,expanded=_expanded)
        if not metric:continue
        for col in columns:
            try:value=amount(row,col['column_start'],col['column_end'],scale)
            except TabularFinancialError as exc:
                holds.append({'row':row_number,'label':label,'column':col['column_start'],'reason':str(exc)});continue
            if value is None:continue
            observations.append({'metric':metric,'value':value,'label':label,'row':row_number,**col})
    if not observations:return None
    result={'kind':kind,'heading':local,'columns':columns,'observations':observations,'holds':holds}
    if scope_used:result['document_scope']=document_scope
    if currency_used:result['currency_scope']=currency_scope
    return result


def inspect_statement_period(raw,*,issuer,filed):
    """Period evidence only: cannot authorize values, currency or accounting."""
    if not isinstance(raw,bytes) or not raw or len(raw)>MAX_BYTES:return None
    tree=html.fromstring(raw)
    if tree.xpath("//base") or len(tree.xpath("//table"))>500:return None
    for node in tree.xpath('//script|//style|//template|//blockquote'):node.drop_tree()
    rows=[]
    for index,table in enumerate(tree.xpath('//table')):
        try:result=parse_table(table,issuer=issuer,filed=filed,periods_only=True)
        except TabularFinancialError as exc:
            if str(exc)!='table_issuer_unproved':return None
            continue
        if result:rows.append({'table':index,**result})
    kinds={r['kind'] for r in rows}
    if not {'balance','income'}<=kinds:return None
    ends={kind:max(c['end'] for r in rows if r['kind']==kind for c in r['columns']) for kind in kinds}
    if len(set(ends.values()))!=1:return None
    return {'end':ends['balance'],'statements':rows,'basis':'owned_statement_headers_only',
            'source_sha256':hashlib.sha256(raw).hexdigest(),'financial_values_qualified':False}


def extract(raw,*,source_url,company_raw,cover_raw,cover_url,symbol,cik,available_by,_expanded=False):
    from app.services.sec_document_sets import candidates
    from app.services.sec_statement_currency import source_identity
    if not isinstance(raw,bytes) or not raw or len(raw)>MAX_BYTES:raise TabularFinancialError('source_size')
    metadata=[m for m in candidates(company_raw,symbol=symbol,cik=cik,available_by=available_by) if m['cover_url']==cover_url]
    if len(metadata)!=1:raise TabularFinancialError('source_directory_identity')
    metadata=metadata[0]
    if source_identity(source_url)!=(metadata['cik'],metadata['accession']):raise TabularFinancialError('source_accession_identity')
    if not isinstance(cover_raw,bytes) or not cover_raw or len(cover_raw)>(MAX_BYTES if source_url==cover_url else 2_000_000):raise TabularFinancialError('cover_size')
    try:cover=html.fromstring(cover_raw);tree=html.fromstring(raw)
    except (ValueError,html.etree.ParserError) as exc:raise TabularFinancialError('source_html') from exc
    if cover.xpath('//base') or tree.xpath('//base'):raise TabularFinancialError('source_base')
    if source_url==cover_url and raw!=cover_raw:raise TabularFinancialError('primary_bytes_mismatch')
    if source_url!=cover_url and source_url not in {urljoin(cover_url,n.get('href')) for n in cover.xpath('//a[@href]')}:
        raise TabularFinancialError('source_not_linked')
    for n in tree.xpath('//script|//style|//template'):n.drop_tree()
    tables=tree.xpath('//table')
    if len(tables)>500:raise TabularFinancialError('table_count_bound')
    company=json.loads(company_raw);issuer=company['name'];filed=date.fromisoformat(metadata['filed']);out=[];holds=[]
    scope=None;scope_checked=False;currency_scope=None;currency_checked=False
    for index,table in enumerate(tables):
        if _expanded and table.xpath('ancestor::blockquote'):continue
        try:
            for attempt in range(3):
                try:
                    quoted=bool(table.xpath('ancestor::blockquote'))
                    result=parse_table(table,issuer=issuer,filed=filed,document_scope=None if quoted else scope,currency_scope=None if quoted else currency_scope,_expanded=_expanded)
                except TabularFinancialError as exc:
                    # New grammar requires the explicit local company and
                    # currency context. Legacy document-wide declarations
                    # were not designed to reconcile these added headings.
                    if _expanded:raise
                    if str(exc)=='table_issuer_unproved' and not scope_checked:
                        scope=document_statement_scope(tree,issuer);scope_checked=True
                        if scope is None or table.xpath('ancestor::blockquote'):raise
                    elif str(exc)=='column_currency_unproved' and not currency_checked:
                        currency_scope=statement_currency_scope(tree,issuer);currency_checked=True
                        if currency_scope is None or table.xpath('ancestor::blockquote'):raise
                    else:raise
                else:break
        except TabularFinancialError as exc:holds.append({'table':index,'reason':str(exc)});continue
        if result is not None:out.append({'table':index,**result})
    grouped={}
    for table in out:
        for observation in table['observations']:
            key=tuple(observation[k] for k in ('metric','start','end','currency'))
            grouped.setdefault(key,[]).append({'table':table['table'],**observation})
    facts=[];conflicts=[]
    for key,points in grouped.items():
        if len({p['value'] for p in points})!=1:
            conflicts.append({'metric':key[0],'start':key[1],'end':key[2],'currency':key[3],
                'reason':'conflicting_statement_observations','occurrences':points})
        else:
            facts.append({k:points[0][k] for k in ('metric','start','end','months','currency','value')}|{'occurrences':points})
    ownership=[{'table':t['table'],**t['document_scope']} for t in out if 'document_scope' in t]
    scope_proof={'statement_ownership':ownership} if ownership else {}
    used_currency=[{'table':t['table'],**t['currency_scope']} for t in out if 'currency_scope' in t]
    if used_currency:scope_proof['statement_currency']=used_currency
    if source_url==cover_url:scope_proof['document_role']='primary_filing'
    result = {'version':VERSION,'status':'prepared' if facts else 'unavailable','publication_eligible':False,
            'source':{**metadata,'statement_url':source_url,'statement_sha256':hashlib.sha256(raw).hexdigest(),
                      'cover_sha256':hashlib.sha256(cover_raw).hexdigest(),**scope_proof},
            'tables':out,'holds':holds,'facts':facts,'conflicts':conflicts,
            'limitations':['Table observations are not admitted ratios or historical panels.',
                'Reporting currency versus convenience translation and accounting/period comparability require consumer qualification.',
                'Missing or ambiguous columns remain held; no values or zero amounts are inferred.']}

    from app.services.sec_statement_sections import extend
    if _expanded:return result
    result=extend(result,raw,company_raw)
    # Recover a previously empty, unambiguous document only. Existing facts,
    # numeric conflicts and qualification holds retain their exact output.
    if result['facts'] or result['holds'] or result['conflicts']:return result
    from app.services.sec_statement_heading_order import POLICY, has_heading
    if not has_heading(tree):return result
    recovered=extract(raw,source_url=source_url,company_raw=company_raw,
        cover_raw=cover_raw,cover_url=cover_url,symbol=symbol,cik=cik,
        available_by=available_by,_expanded=True)
    if (not recovered['facts'] or recovered['holds'] or recovered['conflicts']
            or any(t['holds'] for t in recovered['tables'])):return result
    recovered['source']['statement_heading_policy']=POLICY
    return recovered
