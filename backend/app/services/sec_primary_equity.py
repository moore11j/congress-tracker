"""Parent equity from a complete primary statement component inventory.

No inclusive-tag alias, absent component assumed zero, or existing value override.
Only the four-row common/APIC/retained/treasury layout is admitted.
"""
from copy import deepcopy
from functools import lru_cache
from datetime import date, timedelta
from decimal import Decimal
import calendar, hashlib, math, re
from lxml import html
from app.services.sec_inline_financials import parse, nodes, _number, _nil_fact
from app.services.sec_tabular_financials import grid, preceding_text, company_key
from app.services.sec_revenue_statement_layout import _visible
from app.services.sec_parent_equity import GUARD_TAGS
from app.services.sec_fundamentals import TAGS as CORE_TAGS
VERSION = 'sec_primary_parent_equity_v1'
KINDS = 'sec_primary_parent_equity_components'
BASIS = 'Complete primary common stock plus paid-in capital plus retained earnings minus treasury stock; equals total equity and assets less liabilities.'
coefficients = {'CommonStockValue': 1, 'AdditionalPaidInCapital': 1, 'RetainedEarningsAccumulatedDeficit': 1, 'TreasuryStockValue': -1}
totals = ('StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest', 'Assets', 'Liabilities')

@lru_cache(maxsize=2)
def _statement(raw, filing_items, issuer):
    filing=dict(filing_items)
    source = dict(cik=filing['cik'], accessionNumber=filing['accession'], filingDate=filing['filed'], reportDate=filing['report_date'], form=filing['form'], url=filing['url'])
    filing = dict(cik=source['cik'], accession=source['accessionNumber'], filed=source['filingDate'], report_date=source['reportDate'], form=source['form'], url=source['url'])
    parse(raw, **filing, basis=dict(namespace='us-gaap', currency='USD', tags={'revenue': CORE_TAGS['revenue'], **{t: (t,) for t in (*coefficients, *totals, *GUARD_TAGS, 'StockholdersEquity', 'AccumulatedOtherComprehensiveIncomeLossNetOfTax')}}))
    tree = html.fromstring(raw, parser=html.HTMLParser(encoding='utf-8'))
    contexts = {}
    units = set()
    for c in nodes(tree, 'context'):
        if nodes(c, 'segment') or nodes(c, 'scenario'):
            continue
        ids, instant = (nodes(c, 'identifier'), nodes(c, 'instant'))
        if len(ids) == len(instant) == 1 and ids[0].get('scheme') == 'http://www.sec.gov/CIK' and (ids[0].text_content().strip().zfill(10) == source['cik']) and (instant[0].text_content().strip() == source['reportDate']):
            contexts[c.get('id')] = c
    for u in nodes(tree, 'unit'):
        if not nodes(u, 'divide') and [m.text_content().strip() for m in nodes(u, 'measure')] == ['iso4217:USD']:
            units.add(u.get('id'))
    accepted = []
    for table in tree.xpath('//table'):
        if table.xpath('.//table|ancestor::blockquote') or not _visible(table):
            continue
        headings = preceding_text(table, nonempty=True)[-5:]
        titles = [i for i, h in enumerate(headings) if h.upper() in ('CONSOLIDATED BALANCE SHEETS', 'CONDENSED CONSOLIDATED BALANCE SHEETS')]
        if len(titles) != 1 or titles[0] == 0 or company_key(headings[titles[0] - 1]) != company_key(issuer):
            continue
        if not headings[-1].lower() == '(in thousands, except share data)':
            continue
        geometry = grid(table)
        rows = table.xpath('./tr|./tbody/tr|./thead/tr|./tfoot/tr')
        labels = [next((c['text'] for c in r if c['text']), '') for r in geometry]
        starts = [i for i, s in enumerate(labels) if s == 'EQUITY']
        ends = [i for i, s in enumerate(labels) if s == 'Total equity']
        if len(starts) != 1 or len(ends) != 1 or ends[0] - starts[0] != 5:
            continue
        section = labels[starts[0] + 1:ends[0]]
        if not (section[0].startswith('Common stock,') and section[1:3] == ['Additional paid-in capital', 'Retained earnings'] and section[3].startswith('Treasury stock, at cost')):
            continue
        facts = {}
        locations = {}
        for index, row in enumerate(rows):
            for n in nodes(row, 'nonfraction'):
                tag = n.get('name', '').removeprefix('us-gaap:')
                if tag not in (*coefficients, *totals) or n.get('contextref') not in contexts:
                    continue
                if not (n.get('unitref') in units and (not _nil_fact(n)) and _visible(n) and (n.get('scale') == '3')):
                    raise ValueError('primary_equity_original_mismatch')
                if not tag not in facts:
                    raise ValueError('primary_equity_original_mismatch')
                cell = next(n.iterancestors('td'))
                cells = row.xpath('./td|./th')
                direct = [g for g in geometry[index] if g['row'] == index]
                if not len(cells) == len(direct):
                    raise ValueError('primary_equity_original_mismatch')
                g = direct[cells.index(cell)]
                if not all((_visible(child) for child in cell.iter())):
                    raise ValueError('primary_equity_original_mismatch')
                facts[tag] = dict(value=_number(n, tree), context_id=n.get('contextref'), row_label=labels[index], column=[g['start'], g['end']])
                locations[tag] = index
        if set(facts) != set(coefficients) | set(totals):
            continue
        if [facts[t]['row_label'] for t in totals] != ['Total equity','Total assets','Total liabilities']:
            raise ValueError('primary_equity_total_labels')
        if not len({p['context_id'] for p in facts.values()}) == 1:
            raise ValueError('primary_equity_original_mismatch')
        for n in nodes(tree, 'nonfraction'):
            if n.get('contextref') not in contexts:
                continue
            tag = n.get('name', '').removeprefix('us-gaap:')
            if tag in (*GUARD_TAGS, 'StockholdersEquity', 'AccumulatedOtherComprehensiveIncomeLossNetOfTax'):
                if not (tag in GUARD_TAGS and n.get('unitref') in units and (not _nil_fact(n)) and (_number(n, tree) == 0)):
                    raise ValueError('primary_equity_original_mismatch')
            if tag in facts:
                if not (n.get('unitref') in units and (not _nil_fact(n)) and (_number(n, tree) == facts[tag]['value'])):
                    raise ValueError('primary_equity_original_mismatch')
        if not all((facts[t]['value'] >= 0 for t in (*coefficients, *totals) if t != 'RetainedEarningsAccumulatedDeficit')):
            raise ValueError('primary_equity_original_mismatch')
        first_amount = next((i for i, r in enumerate(rows) if nodes(r, 'nonfraction')))
        day = date.fromisoformat(source['reportDate'])
        header_groups = {(g['start'], g['end']) for line in geometry[:first_amount] for g in line if g['text'] == str(day.year) and all((g['start'] <= p['column'][0] < p['column'][1] <= g['end'] for p in facts.values()))}
        if not len(header_groups) == 1:
            raise ValueError('primary_equity_original_mismatch')
        group = next(iter(header_groups))
        if not any((g['text'].strip(',') == f'{calendar.month_name[day.month]} {day.day}' and g['start'] <= group[0] < group[1] <= g['end'] for line in geometry[:first_amount] for g in line)):
            raise ValueError('primary_equity_original_mismatch')
        if not [locations[t] for t in coefficients] == list(range(starts[0] + 1, ends[0])):
            raise ValueError('primary_equity_original_mismatch')
        calculated = sum((Decimal(str(facts[t]['value'])) * k for t, k in coefficients.items()))
        balance = Decimal(str(facts['Assets']['value'])) - Decimal(str(facts['Liabilities']['value']))
        if not calculated == balance == Decimal(str(facts[totals[0]]['value'])):
            raise ValueError('primary_equity_original_mismatch')
        accepted.append(dict(start=None, end=source['reportDate'], value=float(calculated), unit='USD', basis=BASIS, issuer=issuer, column=list(group), headers=[g['text'] for line in geometry[:first_amount] for g in line if g['start'] <= group[0] < group[1] <= g['end']], components=facts, heading=headings, rows=section, source_url=source['url'], source_sha256=hashlib.sha256(raw).hexdigest(), source_cik=source['cik'], accession=source['accessionNumber'], filed=source['filingDate']))
    if not len(accepted) == 1:
        raise ValueError('primary_equity_original_mismatch')
    return accepted[0]

def statement(raw, filing, *, issuer):
    try:
        return deepcopy(_statement(raw,tuple(sorted(filing.items())),issuer))
    except (KeyError, IndexError, StopIteration) as exc:
        raise ValueError('primary_equity_incomplete_original') from exc

def plan(payload, company, *, cik, available_by):
    from app.services import sec_fundamentals as sec
    if any(str(p.get('cik','')).zfill(10)!=cik for p in (payload,company)):
        raise ValueError('primary_equity_identity')
    facts=payload.get('facts',{}).get('us-gaap',{})
    if any(tag not in facts for tag in (*coefficients,*totals)):return None
    recent=company.get('filings',{}).get('recent',{});keys=('accessionNumber','form','filingDate','reportDate','primaryDocument')
    arrays=[recent.get(k,[]) for k in keys]
    if len({len(a) for a in arrays})!=1:raise ValueError('primary_equity_directory')
    all_filings=[r for r in zip(*arrays) if sec._day(r[2]) and sec._day(r[3]) and r[3]<=r[2]<=str(available_by)]
    filings=[r for r in all_filings if r[1] in {'10-K','10-Q','10-K/A','10-Q/A'}]
    if not filings:return None
    last=max(r[3] for r in filings);end=date.fromisoformat(last)
    if not 0<=(available_by-end).days<=180 or any(r[1] in {'6-K','20-F','40-F','6-K/A','20-F/A','40-F/A'} and r[3]>=last for r in all_filings):return None
    current=sec._ttm(sec._records(payload,'revenue',available_by),end)
    income=sec._ttm(sec._records(payload,'net_income',available_by),end)
    if not current or not income or current['start']!=income['start']:return None
    boundaries=[date.fromisoformat(current['start'])-timedelta(days=1),end]
    equity=sec._records(payload,'equity',available_by)
    targets=[d for d in boundaries if (None,d) not in equity]
    if not targets:return None
    # Historical presence of a concept does not establish that this issuer
    # still reports this complete common-stock balance. Require that inventory
    # at a needed denominator boundary before requesting further originals.
    # The other boundary may still need recovery from its newer whole filing.
    components=[sec._records(payload,'equity',available_by,concepts=(tag,)) for tag in (*coefficients,*totals)]
    if not any(all(series.get((None,target)) is not None for series in components) for target in boundaries):
        return None
    excluded=[sec._records(payload,'equity',available_by,concepts=(tag,)) for tag in (*GUARD_TAGS,'AccumulatedOtherComprehensiveIncomeLossNetOfTax')]
    if any((None,d) in series and (series[(None,d)] is None or series[(None,d)]['value']!=0) for d in targets for series in excluded):return None
    selected=[]
    for target in targets:
        matches=[r for r in filings if r[3]==str(target)]
        if not matches:return None
        if len(matches)!=1 or matches[0][1] not in {'10-Q','10-K'}:return None
        acc,form,filed,report,filename=matches[0]
        if not re.fullmatch(r'\d{10}-\d{2}-\d{6}',acc) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,180}\.html?',filename):raise ValueError('primary_equity_directory_path')
        selected.append(dict(cik=cik,accession=acc,form=form,filed=filed,report_date=report,
            url=f'https://www.sec.gov/Archives/edgar/data/{int(cik)}/{acc.replace("-", "")}/{filename}'))
    return dict(cik=cik,period_end=last,filings=selected)


def acquire(payload, company, *, cik, available_by, client):
    from app.services import sec_fundamentals as sec
    expected=plan(payload,company,cik=cik,available_by=available_by)
    if expected is None:return None
    documents={f['url']:client.get(f['url']) for f in expected['filings']}
    series={t:sec._records(payload,'equity',available_by,concepts=(t,))
            for t in (*coefficients,*totals,*GUARD_TAGS,'AccumulatedOtherComprehensiveIncomeLossNetOfTax')}
    rows=[]
    for filing in expected['filings']:
        row=statement(documents[filing['url']],filing,issuer=company['name'])
        key=(None,date.fromisoformat(row['end']))
        for tag,records in series.items():
            if key not in records:continue
            point=records[key]
            value=row['components'][tag]['value'] if tag in row['components'] else 0
            if point is None or point['value']!=value:
                raise ValueError('primary_equity_reported_conflict')
        rows.append(row)
    return dict(documents=documents,evidence=dict(methodology=VERSION,cik=cik,
        period_end=expected['period_end'],rows=rows))


def apply(records,payload,company,*,cik,available_by,supplement):
    item=(supplement or {}).get('primary_equity')
    if not item or 'equity' not in records:return records
    class Retained:
        def get(self,url):return item['documents'][url]
    verified=acquire(payload,company,cik=cik,available_by=available_by,client=Retained())
    if verified!=item or item['evidence']!=supplement['evidence'].get('primary_equity'):
        raise ValueError('primary_equity_proof_mismatch')
    result=dict(records);equity=dict(records['equity'])
    for p in item['evidence']['rows']:
        key=(None,date.fromisoformat(p['end']))
        if key in equity:continue
        equity[key]={k:p[k] for k in ('start','end','filed','accession','unit','value','source_cik','source_url','source_sha256')}
        equity[key].update(tag=KINDS,priority=0,derived=True,derivation=VERSION,
            source_kind=KINDS,original_statement=deepcopy(p))
    result['equity']=equity
    return result


def cached_valid(evidence):
    from app.services.sec_statement_currency import source_identity
    def points(value):
        if isinstance(value,dict):
            if value.get('source_kind')==KINDS:yield value
            for child in value.values():yield from points(child)
        elif isinstance(value,list):
            for child in value:yield from points(child)
    selected=list(points(evidence.get('current',{})))+list(points(evidence.get('prior',{})))
    proof=(evidence.get('inline_filing_supplement') or {}).get('primary_equity')
    if proof is None:return not selected
    try:
        if (proof['methodology']!=VERSION or proof['cik']!=evidence['cik']
                or proof['period_end']!=evidence['period_end'] or not 1<=len(proof['rows'])<=2):return False
        seen=set()
        for row in proof['rows']:
            end=date.fromisoformat(row['end']);c=row['components'];v={k:p['value'] for k,p in c.items()}
            if (row['end'] in seen or row['start'] is not None or row['unit']!='USD' or row['basis']!=BASIS
                or row['source_cik']!=proof['cik'] or source_identity(row['source_url'])!=(proof['cik'],row['accession'])
                or not row['end']<=row['filed']<=evidence['available_by'] or row['end']>proof['period_end']
                or not re.fullmatch('[0-9a-f]{64}',row['source_sha256'])
                or set(c)!=set(coefficients)|set(totals)):return False
            seen.add(row['end'])
            if any(type(x) not in (int,float) or not math.isfinite(x) for x in (*v.values(),row['value'])):return False
            if (any(v[t]<0 for t in v if t!='RetainedEarningsAccumulatedDeficit')
                or row['value']!=sum(v[t]*factor for t,factor in coefficients.items())
                or row['value']!=v[totals[0]] or row['value']!=v['Assets']-v['Liabilities']):return False
            heads=row['heading'];titles=[i for i,h in enumerate(heads) if h.upper() in ('CONSOLIDATED BALANCE SHEETS','CONDENSED CONSOLIDATED BALANCE SHEETS')]
            if (len(titles)!=1 or titles[0]==0 or company_key(heads[titles[0]-1])!=company_key(row['issuer'])
                or heads[-1].lower()!='(in thousands, except share data)'
                or str(end.year) not in row['headers'] or not any(h.strip(',')==f'{calendar.month_name[end.month]} {end.day}' for h in row['headers'])):return False
            labels=row['rows']
            if len(labels)!=4 or not labels[0].startswith('Common stock,') or labels[1:3]!=['Additional paid-in capital','Retained earnings'] or not labels[3].startswith('Treasury stock, at cost'):return False
            col=row['column']
            if len(col)!=2 or any(type(n) is not int for n in col) or not 0<=col[0]<col[1]<=128:return False
            if len({p['context_id'] for p in c.values()})!=1 or not all(p['context_id'] for p in c.values()):return False
            for p in c.values():
                if not col[0]<=p['column'][0]<p['column'][1]<=col[1]:return False
            if [c[t]['row_label'] for t in coefficients]!=labels:return False
            if [c[t]['row_label'] for t in totals]!=['Total equity','Total assets','Total liabilities']:return False
        for point in selected:
            matches=[r for r in proof['rows'] if r['end']==point['end']]
            if len(matches)!=1 or point.get('original_statement')!=matches[0]:return False
            if any(point.get(k)!=matches[0][k] for k in ('start','end','filed','accession','unit','value','source_cik','source_url','source_sha256')):return False
        return True
    except (KeyError,TypeError,ValueError,AttributeError,IndexError):return False

