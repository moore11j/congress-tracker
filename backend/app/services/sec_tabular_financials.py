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
    return ' '.join(value.replace('\xa0',' ').split())


def company_key(value):
    value = re.sub(r'^the\s+', '', value, flags=re.I)
    value = re.sub(r'\s+and subsidiaries$', '', value, flags=re.I)
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
            record={'text':clean(' '.join(cell.itertext())),'start':column,'end':column+span,'row':r}
            for y in range(r,r+height):
                for x in range(column,column+span):occupied[y,x]=record
            cells.append(record);column+=span
        # Include cells inherited through rowspan once, preserving their origin.
        inherited={id(v):v for (y,x),v in occupied.items() if y==r and v['row']<r}
        out.append(sorted(cells+list(inherited.values()),key=lambda c:c['start']))
        for key in [key for key in occupied if key[0]==r]:del occupied[key]
    return out


def preceding_text(table):
    # Only local text blocks, not a whole ancestor DIV containing other tables.
    nodes=table.xpath('preceding::*[self::p or self::div][not(ancestor::table)][not(.//table)][not(.//p)][not(.//div)]')[-8:]
    return [clean(' '.join(n.itertext())) for n in nodes if clean(' '.join(n.itertext()))]


def currencies(text):
    return {code for code,pattern in CURRENCIES.items() if re.search(r'(?<![A-Za-z])(?:' + pattern + r')(?![A-Za-z])',text,re.I)}


def metric_for(label,kind,issuer):
    label=clean(label).lower().replace('\u2019',"'").rstrip(':')
    if re.search(r'non.?gaap|adjusted|per share|per ads|non.?controlling',label):return None
    exact={
        'total current assets':'current_assets','total current liabilities':'current_liabilities',
        'total assets':'total_assets','total liabilities':'total_liabilities',
        "total shareholders' equity":'total_equity',"total stockholders' equity":'total_equity',
        "total shareholders' (deficit) equity":'total_equity',
        'total equity':'total_equity','revenues':'revenue','revenue':'revenue','total revenue':'revenue',
        'total revenues':'revenue','net revenues':'revenue','total net revenues':'revenue',
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
    return None


def table_kind(text):
    if re.search(r'non.?gaap|reconciliation|adjusted|pro.?forma|notes to',text,re.I):return None
    if re.search(r'consolidated balance sheets?',text,re.I):return 'balance'
    if re.search(r'consolidated statements? of cash flows?',text,re.I):return 'cashflow'
    if re.search(r'consolidated statements? of (?:operations|income|comprehensive (?:loss|income))',text,re.I):return 'income'
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
    if len(matches)!=1 or end.day!=calendar.monthrange(end.year,end.month)[1]:
        raise TabularFinancialError('column_duration_unsupported')
    months=next(iter(matches));serial=end.year*12+end.month-months
    start=date(serial//12,serial%12+1,1)
    return start,end,months


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


def parse_table(table,*,issuer,filed):
    rows=grid(table)
    if not rows:return None
    local=preceding_text(table)
    # Find the first monetary row before treating later years as header labels.
    def data_row(row):
        if not row or row[0]['start']!=0 or not re.search(r'[A-Za-z]',row[0]['text']):return False
        if re.search(r'in thousands|in millions|months? ended|as of|as at',row[0]['text'],re.I):return False
        return any(re.fullmatch(r'\(?-?[0-9][0-9,.]*\)?',c['text']) for c in row[1:])
    boundary=next((i for i,row in enumerate(rows) if data_row(row)),min(len(rows),12))
    header_rows=rows[:boundary]
    intro=' '.join(local+[' '.join(c['text'] for c in r) for r in header_rows])
    kind=table_kind(intro)
    if kind is None:return None
    identity_blocks=local+[c['text'] for row in header_rows for c in row if c['start']==0]
    issuer_key=company_key(issuer)
    identified=[]
    for index,block in enumerate(identity_blocks):
        if table_kind(block) != kind:
            continue
        prefix=re.split(r'\b(?:unaudited|audited|condensed|consolidated)\b',block,maxsplit=1,flags=re.I)[0].strip()
        if not prefix:
            prefix=next((v for v in reversed(identity_blocks[:index]) if v.strip()), '')
        identified.append(company_key(prefix)==issuer_key)
    # The closest statement heading owns this table. An earlier parent-company
    # mention cannot authorize an intervening subsidiary/acquisition statement.
    if not issuer_key or not identified or not identified[-1]:
        raise TabularFinancialError('table_issuer_unproved')
    scales={Decimal('1000') for _ in re.finditer(r'\bin thousands\b',intro,re.I)}
    scales.update(Decimal('1000000') for _ in re.finditer(r'\bin millions\b',intro,re.I))
    if not scales and re.search(r'\bstated in (?:U\.?S\.? dollars?|USD)\b',intro,re.I):scales={Decimal(1)}
    if len(scales)!=1:raise TabularFinancialError('table_scale_unproved')
    scale=next(iter(scales))
    candidates=[]
    for index,row in enumerate(header_rows):
        anchors=[c for c in row if re.fullmatch(r'(?:19|20)\d{2}',c['text']) or re.fullmatch(r'(?:As (?:of|at)\s+)?(?:'+MONTH_PATTERN+r')\s+\d{1,2},?\s+(?:19|20)\d{2}',c['text'],re.I)]
        if len(anchors)>=2:candidates.append((index,anchors))
    if not candidates:raise TabularFinancialError('table_date_header_unproved')
    last,anchors=candidates[-1]
    for index,row in enumerate(header_rows[last+1:],last+1):
        monetary=[c for c in row if re.fullmatch(r'(?:[A-Z]{3}|US\$|[$€£¥])',c['text'])]
        if len(monetary)>=len(anchors):
            if any(not currencies(c['text']) and c['text']!='$' for c in monetary):
                raise TabularFinancialError('column_currency_unproved')
            anchors=monetary;last=index
    columns=[]
    for i,anchor in enumerate(anchors):
        texts=[]
        for row in header_rows:
            for cell in row:
                if cell['start']<=anchor['start']<cell['end'] and cell['text'] and cell['text'] not in texts:texts.append(cell['text'])
        header=' '.join(texts)
        # A duration above the whole table applies only if columns have none.
        if not re.search(r'months?\s+ended',header,re.I):
            duration=[s for s in local if re.fullmatch(r'(?:For the )?(?:Three|Six|Nine|Twelve|3|6|9|12) months? ended',s,re.I)]
            if len(duration)==1:header+=' '+duration[0]
        start,finish,months=period_for(header,kind,filed)
        unit=currencies(anchor['text']) or currencies(header)
        if not unit:unit=currencies(' '.join(local))
        if len(unit)!=1:raise TabularFinancialError('column_currency_unproved')
        declared=currencies(' '.join(local+[c['text'] for row in header_rows for c in row if c['start']==0]))
        if len(declared)==1 and unit!=declared:raise TabularFinancialError('column_currency_conflict')
        columns.append({'column_start':anchor['start'],'column_end':anchors[i+1]['start'] if i+1<len(anchors) else max(c['end'] for r in rows for c in r),
                        'start':str(start) if start else None,'end':str(finish),'months':months,'currency':next(iter(unit)),
                        'scale':int(scale),'header':texts})
    observations=[];holds=[]
    for row_number,row in enumerate(rows[boundary:],boundary):
        if not row:continue
        label=row[0]['text'];metric=metric_for(label,kind,issuer)
        if not metric:continue
        for col in columns:
            try:value=amount(row,col['column_start'],col['column_end'],scale)
            except TabularFinancialError as exc:
                holds.append({'row':row_number,'label':label,'column':col['column_start'],'reason':str(exc)});continue
            if value is None:continue
            observations.append({'metric':metric,'value':value,'label':label,'row':row_number,**col})
    if not observations:return None
    return {'kind':kind,'heading':local,'columns':columns,'observations':observations,'holds':holds}


def extract(raw,*,source_url,company_raw,cover_raw,cover_url,symbol,cik,available_by):
    from app.services.sec_document_sets import candidates
    from app.services.sec_statement_currency import source_identity
    if not isinstance(raw,bytes) or not raw or len(raw)>MAX_BYTES:raise TabularFinancialError('source_size')
    metadata=[m for m in candidates(company_raw,symbol=symbol,cik=cik,available_by=available_by) if m['cover_url']==cover_url]
    if len(metadata)!=1:raise TabularFinancialError('source_directory_identity')
    metadata=metadata[0]
    if source_identity(source_url)!=(metadata['cik'],metadata['accession']):raise TabularFinancialError('source_accession_identity')
    if not isinstance(cover_raw,bytes) or not cover_raw or len(cover_raw)>2_000_000:raise TabularFinancialError('cover_size')
    try:cover=html.fromstring(cover_raw);tree=html.fromstring(raw)
    except (ValueError,html.etree.ParserError) as exc:raise TabularFinancialError('source_html') from exc
    if cover.xpath('//base') or tree.xpath('//base'):raise TabularFinancialError('source_base')
    if source_url not in {urljoin(cover_url,n.get('href')) for n in cover.xpath('//a[@href]')}:
        raise TabularFinancialError('source_not_linked')
    for n in tree.xpath('//script|//style|//template'):n.drop_tree()
    tables=tree.xpath('//table')
    if len(tables)>500:raise TabularFinancialError('table_count_bound')
    company=json.loads(company_raw);issuer=company['name'];filed=date.fromisoformat(metadata['filed']);out=[];holds=[]
    for index,table in enumerate(tables):
        try:result=parse_table(table,issuer=issuer,filed=filed)
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
    return {'version':VERSION,'status':'prepared' if facts else 'unavailable','publication_eligible':False,
            'source':{**metadata,'statement_url':source_url,'statement_sha256':hashlib.sha256(raw).hexdigest(),
                      'cover_sha256':hashlib.sha256(cover_raw).hexdigest()},
            'tables':out,'holds':holds,'facts':facts,'conflicts':conflicts,
            'limitations':['Table observations are not admitted ratios or historical panels.',
                'Reporting currency versus convenience translation and accounting/period comparability require consumer qualification.',
                'Missing or ambiguous columns remain held; no values or zero amounts are inferred.']}
