"""Common equity qualified from complete entity-owned balance sheets.

Inclusive equity is never a parent-equity alias. Every common component,
liability section, visible column and independent asset total must reconcile.
"""
from datetime import date, timedelta
from copy import deepcopy
from functools import lru_cache
import math
import re
from lxml import html

VERSION = 'sec_complete_common_equity_v1'
BASIS = ('Calculated common equity from all reported common-stock components, '
         'reconciled to total equity and the complete assets and liabilities balance.')
TAGS = {
    'common': 'CommonStockValue', 'paid': 'AdditionalPaidInCapitalCommonStock',
    'treasury': 'TreasuryStockCommonValue', 'retained': 'RetainedEarningsAccumulatedDeficit',
    'aoci': 'AccumulatedOtherComprehensiveIncomeLossNetOfTax',
    'total': 'StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest',
    'current': 'LiabilitiesCurrent', 'debt': 'LongTermDebtNoncurrent',
    'other': 'LiabilitiesOtherThanLongtermDebtNoncurrent',
    'balance': 'LiabilitiesAndStockholdersEquity', 'assets': 'Assets',
}
LABELS = {
    'paid': {'additional paid-in capital'}, 'treasury': {'treasury stock'},
    'retained': {'earnings reinvested', 'retained earnings', 'accumulated deficit'},
    'aoci': {'accumulated other comprehensive loss', 'accumulated other comprehensive income', 'accumulated other comprehensive income (loss)'},
    'total': {'total equity'}, 'current': {'total current liabilities'},
    'debt': {'long-term debt'}, 'other': {'total deferred credits and other noncurrent liabilities'},
    'balance': {'total liabilities and equity'}, 'assets': {'total assets'},
}
LIABILITY_ITEMS = {
    'current': {'ShortTermBorrowings', 'LongTermDebtCurrent', 'AccountsPayableCurrent',
                'TaxesPayableCurrent', 'InterestPayableCurrent', 'DividendsPayableCurrent',
                'RegulatoryLiabilityCurrent', 'OtherLiabilitiesCurrent'},
    'other': {'DeferredIncomeTaxLiabilitiesNet', 'DeferredInvestmentTaxCredit',
              'DefinedBenefitPensionPlanLiabilitiesNoncurrent', 'AssetRetirementObligationsNoncurrent',
              'RegulatoryLiabilityNoncurrent', 'OtherLiabilitiesNoncurrent'},
}


def _owned_table(table, issuer, filed):
    from app.services import sec_tabular_financials as tab
    if table.xpath('.//table|ancestor::blockquote') or any(a.tag=='ix:hidden' for a in table.iterancestors()):
        return None
    heading = tab.preceding_text(table, nonempty=True)
    captions = [i for i,s in enumerate(heading) if re.fullmatch(r'(?:CONDENSED )?CONSOLIDATED BALANCE SHEETS', s, re.I)]
    if not captions:
        return None
    suffix = heading[captions[-1]+1:]
    if (len(suffix)!=3 or tab.company_key(suffix[0])!=tab.company_key(issuer)
        or suffix[1].lower()!='(unaudited)'
        or suffix[2].lower()!='(millions of dollars, shares in thousands)'):
        return None
    geometry=tab.grid(table)
    nonempty=[(i,r) for i,r in enumerate(geometry) if any(c['text'] for c in r)]
    if not nonempty:return None
    first,header=nonempty[0]
    anchors=[c for c in header if c['text']]
    if len(anchors)!=2:return None
    columns=[]
    for a in anchors:
        if not re.fullmatch(r'(?:'+tab.MONTH_PATTERN+r')\s+\d{1,2},?\s+(?:19|20)\d{2}',a['text'],re.I):return None
        start,end,months=tab.column_period(a['text'],[a['text']],[],'balance',date.fromisoformat(filed))
        if start or months or str(end)>filed:return None
        columns.append(dict(end=str(end),column_start=a['start'],column_end=a['end'],scale=1000000,header=a['text']))
    if columns[0]['end']<=columns[1]['end']:return None
    return dict(heading=heading,geometry=geometry,columns=columns,first=first)


def statement_rows(raw, filing, *, issuer):
    # Two complete immutable originals at most. A different byte, filing identity
    # or owner causes full requalification. Return copies so callers cannot alter
    # the retained proof used by another consumer in the same finite worker.
    return deepcopy(_statement_rows(raw, tuple(sorted(filing.items())), issuer))


@lru_cache(maxsize=2)
def _statement_rows(raw, filing_items, issuer):
    filing=dict(filing_items)
    from app.services import sec_inline_financials as inline, sec_tabular_financials as tab
    from app.services.sec_parent_equity import GUARD_TAGS
    from app.services.sec_fundamentals import TAGS as CORE_TAGS
    basis=dict(namespace='us-gaap',currency='USD',tags={'revenue':CORE_TAGS['revenue'],**{k:(v,) for k,v in TAGS.items()}})
    basis['tags'].update({t:(t,) for t in (*GUARD_TAGS,'StockholdersEquity')})
    parsed=inline.parse(raw,**filing,basis=basis)
    tree=html.fromstring(raw);contexts={}
    for node in tree.iter('xbrli:context'):
        ids,instants=(inline.nodes(node,k) for k in ('identifier','instant'))
        if (len(ids)==len(instants)==1 and not inline.nodes(node,'segment') and not inline.nodes(node,'scenario')
            and ids[0].get('scheme')=='http://www.sec.gov/CIK' and ids[0].text_content().strip().zfill(10)==filing['cik']):
            contexts[node.get('id')]=instants[0].text_content().strip()
    units={n.get('id') for n in tree.iter('xbrli:unit') if [m.text_content().strip() for m in inline.nodes(n,'measure')]==['iso4217:USD'] and not inline.nodes(n,'divide')}
    panels=[]
    eligible=set()
    for n in tree.iter('ix:nonfraction'):
        if (n.get('name') in {'us-gaap:'+TAGS[k] for k in ('total','assets')}
            and n.get('contextref') in contexts and n.get('unitref') in units):
            ancestors=n.xpath('ancestor::table')
            if ancestors:eligible.add(ancestors[-1])
    for table in tree.xpath('//table'):
        if table not in eligible:continue
        panel=_owned_table(table,issuer,filing['filed'])
        if panel:panels.append((table,panel))
    def number(table,panel,index,column,tag=None,sign=1):
        rows=table.xpath('./tr|./tbody/tr|./thead/tr|./tfoot/tr');row=rows[index];line=panel['geometry'][index]
        cells=row.xpath('./td|./th');positions=[c for c in line if c['row']==index]
        if len(cells)!=len(positions):raise ValueError('common_equity_geometry')
        positions={id(c):p for c,p in zip(cells,positions)};matches=[]
        displayed=tab.amount(line,column['column_start'],column['column_end'],column['scale'])
        for node in row.iter('ix:nonfraction'):
            if tag and node.get('name')!='us-gaap:'+tag:continue
            parents=node.xpath('ancestor::td|ancestor::th');pos=positions.get(id(parents[-1])) if parents else None
            if not pos or not column['column_start']<=pos['start']<pos['end']<=column['column_end']:continue
            if (contexts.get(node.get('contextref'))!=column['end'] or node.get('unitref') not in units
                or any(a.tag in ('ix:hidden','script','style','template') for a in node.iterancestors())):continue
            value=inline._number(node,tree)
            if displayed is None or value*sign!=displayed:raise ValueError('common_equity_visible_value')
            matches.append(dict(value=value,context=node.get('contextref'),tag=node.get('name').split(':')[-1],label=line[0]['text'],row=index))
        if len(matches)!=1:raise ValueError('common_equity_visible_context')
        return matches[0]
    assets={};candidates={}
    for table,panel in panels:
        lines=[(i,r[0]['text'].lower()) for i,r in enumerate(panel['geometry']) if any(c['text'] for c in r) and i>panel['first']]
        for column in panel['columns']:
            for index,label in lines:
                if label in LABELS['assets']:
                    point=number(table,panel,index,column,TAGS['assets'])
                    if column['end'] in assets and assets[column['end']]['value']!=point['value']:raise ValueError('common_equity_asset_conflict')
                    assets[column['end']]={**point,'heading':panel['heading'],'column':column}
            if not lines or lines[0][1]!='liabilities and equity':continue
            components={};phase='start';subtotal=[];invalid=False
            for index,label in lines[1:]:
                if label=='current liabilities' and phase=='start':phase='current';continue
                if phase in ('current','other_items'):
                    kind='current' if phase=='current' else 'other'
                    if label in LABELS[kind]:
                        point=number(table,panel,index,column,TAGS[kind])
                        if not subtotal or sum(p['value'] for p in subtotal)!=point['value']:invalid=True;break
                        components[kind]={**point,'items':subtotal};subtotal=[]
                        phase='debt' if kind=='current' else 'equity_heading';continue
                    point=number(table,panel,index,column)
                    if point['value']<0 or point['tag'] not in LIABILITY_ITEMS[kind] or point['tag'] in {p['tag'] for p in subtotal}:invalid=True;break
                    subtotal.append(point);continue
                if phase=='debt' and label in LABELS['debt']:
                    components['debt']=number(table,panel,index,column,TAGS['debt']);phase='other_heading';continue
                if phase=='other_heading' and label=='deferred credits and other noncurrent liabilities':phase='other_items';continue
                if phase=='equity_heading' and re.fullmatch(r'commitments and contingent liabilities \(notes? [0-9 ,and]+\)',label):
                    if tab.amount(panel['geometry'][index],column['column_start'],column['column_end'],column['scale']) is not None:invalid=True;break
                    continue
                if phase=='equity_heading' and label=='equity':phase='common';continue
                if phase=='common':
                    kind=('common' if re.fullmatch(r'common stock(?: - \$[0-9.]+ par value(?: \([a-z]\))?)?',label)
                          else next((k for k in ('paid','treasury','retained','aoci','total') if label in LABELS[k]),None))
                    if kind is None or kind in components:invalid=True;break
                    components[kind]=number(table,panel,index,column,TAGS[kind],-1 if kind=='treasury' else 1)
                    if kind=='total':phase='balance'
                    continue
                if phase=='balance' and label in LABELS['balance']:
                    components['balance']=number(table,panel,index,column,TAGS['balance']);phase='complete';continue
                invalid=True;break
            if invalid or phase!='complete' or set(components)!=set(TAGS)-{'assets'}:continue
            values={k:p['value'] for k,p in components.items()}
            common=values['common']+values['paid']-values['treasury']+values['retained']+values['aoci']
            if (any(values[k]<0 for k in ('common','paid','treasury','current','debt','other'))
                or common!=values['total'] or common+values['current']+values['debt']+values['other']!=values['balance']):continue
            end=column['end']
            proof=dict(start=None,end=end,value=common,unit='USD',filed=filing['filed'],accession=filing['accession'],
                       source_cik=filing['cik'],source_url=filing['url'],source_sha256=parsed['evidence']['sha256'],
                       heading=panel['heading'],column=column,components=components,basis=BASIS)
            if end in candidates and candidates[end]!=proof:raise ValueError('common_equity_statement_conflict')
            candidates[end]=proof
    result={}
    for end,proof in candidates.items():
        if end not in assets or assets[end]['value']!=proof['components']['balance']['value']:continue
        if any(p.get('end')==end and p.get('tag') in {*GUARD_TAGS,'StockholdersEquity',*TAGS.values()} for p in parsed['evidence'].get('nil_facts',[])):continue
        # All same-period entity-wide tagged representations remain authoritative,
        # including nil/conflicting parent values and non-common interests.
        accepted=True
        for tag in (*GUARD_TAGS,'StockholdersEquity',*TAGS.values()):
            observations=parsed['facts'].get(tag,{}).get('units',{}).get('USD',[])
            for row in observations:
                if row.get('end')!=end or row.get('start') is not None:continue
                expected=(0 if tag in GUARD_TAGS else proof['value'] if tag=='StockholdersEquity'
                          else assets[end]['value'] if tag==TAGS['assets']
                          else proof['components'][next(k for k,v in TAGS.items() if v==tag)]['value'])
                if row.get('val')!=expected:accepted=False
        if accepted:result[end]={**proof,'assets':assets[end]}
    return result


def plan(payload, company, *, cik, available_by):
    from app.services import sec_fundamentals as sec
    if any(str(p.get('cik','')).zfill(10)!=cik for p in (payload,company)):
        raise ValueError('common_equity_identity')
    facts=payload.get('facts',{}).get('us-gaap',{})
    if any(tag not in facts for tag in TAGS.values()):return None
    recent=company.get('filings',{}).get('recent',{});keys=('accessionNumber','form','filingDate','reportDate','primaryDocument')
    arrays=[recent.get(k,[]) for k in keys]
    if len({len(a) for a in arrays})!=1:raise ValueError('common_equity_directory')
    all_filings=[r for r in zip(*arrays) if sec._day(r[2]) and sec._day(r[3]) and r[3]<=r[2]<=str(available_by)]
    filings=[r for r in all_filings if r[1] in {'10-K','10-Q','10-K/A','10-Q/A'}]
    if not filings:return None
    last=max(r[3] for r in filings);end=date.fromisoformat(last)
    if not 0<=(available_by-end).days<=180 or any(r[1] in {'6-K','20-F','40-F','6-K/A','20-F/A','40-F/A'} and r[3]>=last for r in all_filings):return None
    current=sec._ttm(sec._records(payload,'revenue',available_by),end)
    income=sec._ttm(sec._records(payload,'net_income',available_by),end)
    if not current or not income or current['start']!=income['start']:return None
    targets=[date.fromisoformat(current['start'])-timedelta(days=1),end]
    equity=sec._records(payload,'equity',available_by)
    targets=[d for d in targets if (None,d) not in equity]
    if not targets:return None
    selected=[]
    for target in targets:
        matches=[r for r in filings if r[3]==str(target)]
        if not matches:return None
        latest=max(r[2] for r in matches);matches=[r for r in matches if r[2]==latest]
        if len(matches)!=1:return None
        acc,form,filed,report,filename=matches[0]
        if not re.fullmatch(r'\d{10}-\d{2}-\d{6}',acc) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,180}\.html?',filename):raise ValueError('common_equity_directory_path')
        selected.append(dict(cik=cik,accession=acc,form=form,filed=filed,report_date=report,
            url=f'https://www.sec.gov/Archives/edgar/data/{int(cik)}/{acc.replace("-", "")}/{filename}'))
    return dict(cik=cik,period_end=last,filings=selected)


def qualify(expected, documents, payload, *, issuer, available_by):
    from app.services import sec_fundamentals as sec
    from app.services.sec_parent_equity import GUARD_TAGS
    records={tag:sec._records(payload,'equity',available_by,concepts=(tag,)) for tag in (*TAGS.values(),*GUARD_TAGS,'StockholdersEquity')}
    rows=[]
    for filing in expected['filings']:
        point=statement_rows(documents[filing['url']],filing,issuer=issuer).get(filing['report_date'])
        if point is None:continue
        period=(None,date.fromisoformat(point['end']));valid=True
        for tag,series in records.items():
            if period not in series:continue
            record=series[period]
            value=(0 if tag in GUARD_TAGS else point['value'] if tag=='StockholdersEquity'
                   else point['assets']['value'] if tag==TAGS['assets']
                   else point['components'][next(k for k,v in TAGS.items() if v==tag)]['value'])
            if record is None or record['value']!=value:valid=False
        if valid:rows.append(point)
    return dict(methodology=VERSION,cik=expected['cik'],period_end=expected['period_end'],rows=rows)


def acquire(payload, company, *, cik, available_by, client):
    expected=plan(payload,company,cik=cik,available_by=available_by)
    if expected is None:return None
    documents={f['url']:client.get(f['url']) for f in expected['filings']}
    proof=qualify(expected,documents,payload,issuer=company['name'],available_by=available_by)
    return dict(documents=documents,evidence=proof) if proof['rows'] else None


def apply(records,payload,company,*,cik,available_by,supplement):
    from app.services.sec_inline_financials import InlineFactsError
    item=(supplement or {}).get('common_equity')
    if not item or 'equity' not in records:return records
    expected=plan(payload,company,cik=cik,available_by=available_by)
    if (expected is None or qualify(expected,item['documents'],payload,issuer=company['name'],available_by=available_by)!=item['evidence']
        or supplement.get('evidence',{}).get('common_equity_reconciliation')!=item['evidence']):
        raise InlineFactsError('common_equity_proof_mismatch')
    result=dict(records);equity=dict(records['equity'])
    for p in item['evidence']['rows']:
        key=(None,date.fromisoformat(p['end']))
        if key not in equity:
            equity[key]={**{k:p[k] for k in ('start','end','filed','accession','unit','value','source_cik','source_url','source_sha256')},
                'tag':'derived_original_common_equity','priority':0,'derived':True,'derivation':VERSION,
                'source_kind':'derived_original_common_equity','original_statement':deepcopy(p)}
    result['equity']=equity
    return result


def cached_valid(proof, *, cik):
    from app.services.sec_statement_currency import source_identity
    try:
        if proof['methodology']!=VERSION or proof['cik']!=cik or not 1<=len(proof['rows'])<=2:return False
        seen=set()
        for row in proof['rows']:
            if (row['end'] in seen or row['start'] is not None or row['unit']!='USD' or row['basis']!=BASIS
                or not date.fromisoformat(row['end'])<=date.fromisoformat(proof['period_end'])
                or not date.fromisoformat(row['end'])<=date.fromisoformat(row['filed'])
                or source_identity(row['source_url'])!=(cik,row['accession']) or row['source_cik']!=cik
                or not re.fullmatch('[0-9a-f]{64}',row['source_sha256']) or not row['heading']
                or set(row['components'])!=set(TAGS)-{'assets'}):return False
            seen.add(row['end']);p=row['components'];v={k:r['value'] for k,r in p.items()}
            if any(type(n) not in (int,float) or not math.isfinite(n) for n in (*v.values(),row['value'],row['assets']['value'])):return False
            if (any(v[k]<0 for k in ('common','paid','treasury','current','debt','other'))
                or row['value']!=v['common']+v['paid']-v['treasury']+v['retained']+v['aoci']
                or row['value']!=v['total'] or v['balance']!=v['total']+v['current']+v['debt']+v['other']
                or row['assets']['value']!=v['balance']):return False
            for k,r in p.items():
                if r['tag']!=TAGS[k] or not r['context'] or type(r['row']) is not int or not r['label']:return False
                label=r['label'].lower()
                if k=='common':
                    if not re.fullmatch(r'common stock(?: - \$[0-9.]+ par value(?: \([a-z]\))?)?',label):return False
                elif label not in LABELS[k]:return False
            order=[p[k]['row'] for k in ('current','debt','other')]
            common_rows=[p[k]['row'] for k in ('common','paid','treasury','retained','aoci')]
            if (order!=sorted(set(order)) or min(order)<0 or min(common_rows)<=max(order)
                or len(set(common_rows))!=5 or max(common_rows)>=p['total']['row'] or p['total']['row']>=p['balance']['row']):return False
            col=row['column'];asset=row['assets']
            if (col['end']!=row['end'] or col['scale']!=1000000 or not col['header']
                or type(col['column_start']) is not int or type(col['column_end']) is not int
                or not 0<col['column_start']<col['column_end']<=128
                or asset['tag']!=TAGS['assets'] or asset['column']['end']!=row['end']
                or asset['label'].lower() not in LABELS['assets'] or not asset['context'] or not asset['heading']):return False
            for k in ('current','other'):
                if not p[k]['items'] or any(type(i['value']) not in (int,float) or not math.isfinite(i['value']) or i['value']<0 for i in p[k]['items']):return False
                if sum(i['value'] for i in p[k]['items'])!=v[k]:return False
                items=p[k]['items']
                if len({i['tag'] for i in items})!=len(items) or any(i['tag'] not in LIABILITY_ITEMS[k] or not i['context'] or not i['label'] or not 0<=i['row']<p[k]['row'] for i in items):return False
        return True
    except (KeyError,ValueError,TypeError,AttributeError):return False
