"""Reviewed Alibaba CNY GAAP ratios, separate from convenience USD and non-GAAP."""
from datetime import date, timedelta
import hashlib
import json
from pathlib import Path
import re
from zoneinfo import ZoneInfo
from app.services.sec_fundamentals import SecFundamentalsCoverageError

VERSION = 'sec_baba_cny_statements_v1'
CIK = '0001577552'
END = date(2026, 6, 30)
MAX_BYTES = 15_000_000
REGISTRY = json.loads(Path(__file__).with_name('sec_baba_sources.json').read_text(encoding='utf-8'))
SOURCES = REGISTRY['sources']
TAGS = {'revenue':'Revenues','cost':'CostOfRevenue','operating_income':'OperatingIncomeLoss',
    'net_income':'NetIncomeLoss','equity':'StockholdersEquity','current_assets':'AssetsCurrent',
    'current_liabilities':'LiabilitiesCurrent'}
INCOME = {'revenue':'Revenue','cost':'Cost of revenue','research':'Product development expenses',
    'selling':'Sales and marketing expenses','administration':'General and administrative expenses',
    'other_gains':'Other gains, net','operating_income':'Income from operations',
    'finance_income':'Interest and investment income, net','finance_expense':'Interest expense',
    'other_income':'Other income, net','pretax':'Income before income tax and share of results of equity method investees',
    'tax':'Income tax expenses','investee':'Share of results of equity method investees',
    'consolidated_income':'Net income','net_income':'Net income attributable to Alibaba Group Holding Limited',
    'common_income':'Net income attributable to ordinary shareholders'}
BALANCE = {'equity':'Total shareholders’ equity','current_assets':'Total current assets','current_liabilities':'Total current liabilities',
    'assets':'Total assets','liabilities':'Total liabilities','mezzanine':'Mezzanine equity','nci':'Noncontrolling interests',
    'total_equity':'Total equity','total_balance':'Total liabilities, mezzanine equity and equity',
    'capital':'Ordinary shares','paid_in':'Additional paid-in capital','treasury':'Treasury shares at cost',
    'reserves':'Statutory reserves','retained':'Retained earnings'}


class BabaFinancialError(SecFundamentalsCoverageError):
    pass


def available_day(observed_at):
    if observed_at.tzinfo is None:
        raise BabaFinancialError('baba_observation_timezone')
    return observed_at.astimezone(ZoneInfo('America/New_York')).date() - timedelta(days=1)


def validate_company(company, *, symbol, cik, observed_at):
    if (symbol != 'BABA' or str(cik).zfill(10) != CIK or str(company.get('cik','')).zfill(10) != CIK
            or company.get('tickers',[]).count('BABA') != 1 or not company.get('name')):
        raise BabaFinancialError('baba_company_identity')
    available = available_day(observed_at)
    if not 0 <= (available-END).days <= 180:
        raise BabaFinancialError('baba_current_period_stale_or_future')
    fields = ('accessionNumber','form','filingDate','reportDate','primaryDocument')
    recent = company.get('filings',{}).get('recent',{})
    arrays = [recent.get(k,[]) for k in fields]
    if any(not isinstance(a,list) for a in arrays) or len({len(a) for a in arrays}) != 1:
        raise BabaFinancialError('baba_directory_shape')
    filings = list(zip(*arrays))
    required = {tuple(s[k] for k in fields) for s in SOURCES.values()}
    for row in required:
        if filings.count(row) != 1 or date.fromisoformat(row[2]) > available:
            raise BabaFinancialError('baba_original_unavailable')
    reviewed = required | {tuple(s[k] for k in fields) for s in REGISTRY['reviewed_later']}
    for row in filings:
        acc, form, filed, report, filename = row
        if form not in ('6-K','6-K/A','20-F','20-F/A') or filed > str(available) or row in reviewed:
            continue
        if filed >= SOURCES['current']['filingDate'] or form.endswith('/A') and filed >= SOURCES['prior']['filingDate']:
            raise BabaFinancialError('baba_new_or_amended_report_requires_review')


def verify_original(raw, key):
    if not isinstance(raw,bytes) or not raw or len(raw) > MAX_BYTES:
        raise BabaFinancialError('baba_document_size')
    if hashlib.sha256(raw).hexdigest() != SOURCES[key]['sha256']:
        raise BabaFinancialError('baba_original_changed')


def original_tree(raw, key):
    from lxml import html
    verify_original(raw,key)
    try:
        return html.fromstring(raw)
    except (ValueError,html.etree.ParserError) as exc:
        raise BabaFinancialError('baba_invalid_html') from exc


def proof(key):
    s = SOURCES[key]
    return {'source_url':s['url'],'source_sha256':s['sha256'],'accession':s['accessionNumber'],
        'filed':s['filingDate'],'currency':'CNY','accounting_basis':'US GAAP'}


def amount(value):
    if value == '–':
        return 0
    if not isinstance(value,str) or not re.fullmatch(r'(?:[0-9]+|[0-9]{1,3}(?:,[0-9]{3})+|\((?:[0-9]+|[0-9]{1,3}(?:,[0-9]{3})+)\))',value):
        raise BabaFinancialError('baba_numeric_format')
    result = int(value.strip('()').replace(',','')) * (-1 if value.startswith('(') else 1) * 1_000_000
    if abs(result) > 10**15:
        raise BabaFinancialError('baba_numeric_bound')
    return result


def rows_for(table):
    # Parentheses occupy their own layout cells in these complete originals.
    result=[]
    for tr in table.xpath('.//tr'):
        tokens=[' '.join(c.text_content().split()) for c in tr.xpath('./td|./th')]
        tokens=[t for t in tokens if t]
        row=[]
        for token in tokens:
            if token == ')':
                if not row or not re.fullmatch(r'\([0-9,]+',row[-1]):
                    raise BabaFinancialError('baba_unmatched_numeric_parenthesis')
                row[-1] += ')'
            else:
                row.append(token)
        if row:result.append(row)
    return result


def reconciles(total,*parts):
    return abs(total-sum(parts)) <= (len(parts)+1)*500_000


def parse_interim(raw, cover_raw, *, key):
    tree,cover=original_tree(raw,key),original_tree(cover_raw,key+'_cover')
    s=SOURCES[key];year=s['year']
    links=[a for a in cover.xpath('//a[@href]') if a.get('href')==s['filename'] and ' '.join(a.text_content().split()).startswith('Exhibit 99.1 ')]
    if len(links)!=1:
        raise BabaFinancialError('baba_financial_exhibit_link')
    found={'income':[],'assets':[],'equity':[]}
    for table in tree.xpath('//table[not(.//table)]'):
        # Restrict before normalizing: other tables contain percentages and
        # explanatory parentheses that are not monetary columns.
        labels={' '.join(r.xpath('./td|./th')[0].text_content().split()) for r in table.xpath('.//tr') if r.xpath('./td|./th')}
        for kind,markers in [('income',{'Net income attributable to ordinary shareholders','Income before income tax and share of results of equity method investees'}),
                ('assets',{'Total assets','Total current assets','Total current liabilities'}),
                ('equity',{'Total shareholders’ equity','Noncontrolling interests','Mezzanine equity'})]:
            if markers <= labels:
                prefix=' '.join(' '.join(table.xpath('preceding::text()')).split())
                if kind=='income' and not prefix.endswith('ALIBABA GROUP HOLDING LIMITED UNAUDITED CONSOLIDATED INCOME STATEMENTS'):
                    raise BabaFinancialError('baba_income_statement_title')
                found[kind].append(rows_for(table))
    if any(len(t)!=1 for t in found.values()):
        raise BabaFinancialError('baba_statements_missing_or_ambiguous')
    income,assets,equity=(found[k][0] for k in ('income','assets','equity'))
    if income[:4] != [['Three months ended June 30,'],[str(year-1),str(year)],['RMB','RMB','US$'],['(in millions, except per share data)']]:
        raise BabaFinancialError('baba_income_currency_or_period')
    for rows in (assets,equity):
        if rows[:4] != [['As of March 31,','As of June 30,'],[str(year),str(year)],['RMB','RMB','US$'],['(in millions)']]:
            raise BabaFinancialError('baba_balance_currency_or_period')
    def selected(rows,label):
        matches=[r for r in rows if r[0]==label]
        if len(matches)!=1 or len(matches[0])!=4:
            raise BabaFinancialError('baba_missing_or_ambiguous_amount:'+label)
        # Validate all three numeric cells, but only source RMB columns feed ratios.
        return [amount(v) for v in matches[0][1:]]
    labels=dict(INCOME)
    labels['amortization']='Amortization of intangible assets' if key=='current' else 'Amortization and impairment of intangible assets'
    labels['nci_adjustment']='Net (income) loss attributable to noncontrolling interests' if key=='current' else 'Net loss (income) attributable to noncontrolling interests'
    labels['accretion']='Reversal of accretion (Accretion) of mezzanine equity' if key=='current' else '(Accretion) Reversal of accretion of mezzanine equity'
    if key=='current':labels['impairment']='Impairment of goodwill'
    inc={k:selected(income,label) for k,label in labels.items()}
    balance={k:selected(assets if k in ('assets','current_assets','current_liabilities') else equity,label) for k,label in BALANCE.items()}
    balance['aoci']=selected(equity,'Accumulated other comprehensive loss' if key=='current' else 'Accumulated other comprehensive income (loss)')
    for i in range(2):
        v=lambda k:inc[k][i]
        expenses=['cost','research','selling','administration','amortization','other_gains']+(['impairment'] if key=='current' else [])
        if not all((reconciles(v('operating_income'),v('revenue'),*(v(k) for k in expenses)),
                reconciles(v('pretax'),v('operating_income'),v('finance_income'),v('finance_expense'),v('other_income')),
                reconciles(v('consolidated_income'),v('pretax'),v('tax'),v('investee')),
                reconciles(v('net_income'),v('consolidated_income'),v('nci_adjustment')),
                reconciles(v('common_income'),v('net_income'),v('accretion')),
                v('cost')<=0)):
            raise BabaFinancialError('baba_income_reconciliation')
        v=lambda k:balance[k][i]
        if not all((reconciles(v('equity'),*(v(k) for k in ('capital','paid_in','treasury','reserves','aoci','retained'))),
                reconciles(v('total_equity'),v('equity'),v('nci')),
                reconciles(v('assets'),v('liabilities'),v('mezzanine'),v('total_equity')),
                v('assets')==v('total_balance'),0<=v('current_assets')<=v('assets'),0<=v('current_liabilities')<=v('liabilities'))):
            raise BabaFinancialError('baba_equity_reconciliation')
    def fact(value,start,end,label):
        return {'value':value,'start':start,'end':end,'unit':'CNY','label':label,'source_kind':'reviewed_sec_html_table',
            **proof(key),'cover_url':SOURCES[key+'_cover']['url'],'cover_sha256':SOURCES[key+'_cover']['sha256']}
    facts={k:[fact(inc[k][i],f'{y}-04-01',f'{y}-06-30',labels[k]) for i,y in enumerate((year-1,year))]
        for k in ('revenue','operating_income','net_income')}
    facts['gross_profit']=[]
    for i,y in enumerate((year-1,year)):
        start,end=f'{y}-04-01',f'{y}-06-30'
        rev,cost=fact(inc['revenue'][i],start,end,'Revenue'),fact(inc['cost'][i],start,end,'Cost of revenue, signed')
        facts['gross_profit'].append({**fact(rev['value']+cost['value'],start,end,'Revenue plus signed cost of revenue'),
            'derived':True,'formula':'revenue plus signed cost of revenue','inputs':[rev,cost]})
    for k in ('equity','current_assets','current_liabilities'):
        facts[k]=[fact(balance[k][i],None,end,BALANCE[k]) for i,end in enumerate((f'{year}-03-31',f'{year}-06-30'))]
    return facts


def parse_annual(raw):
    from app.services import sec_inline_financials as inline
    verify_original(raw,'annual')
    s=SOURCES['annual']
    try:
        parsed=inline.parse(raw,cik=CIK,accession=s['accessionNumber'],filed=s['filingDate'],report_date=s['reportDate'],form='20-F',url=s['url'],
            basis={'namespace':'us-gaap','currency':'CNY','tags':{k:(v,) for k,v in TAGS.items()}})
    except ValueError as exc:
        raise BabaFinancialError('baba_invalid_annual:'+str(exc)) from exc
    facts={}
    for metric,tag in TAGS.items():
        facts[metric]=[{'value':r['val'],'start':r.get('start'),'end':r['end'],'unit':'CNY','tag':tag,
            **parsed['provenance'][tag,'CNY',r.get('start'),r['end']],**proof('annual')}
            for r in parsed['facts'].get(tag,{}).get('units',{}).get('CNY',[])]
    facts['gross_profit']=[]
    for rev in facts['revenue']:
        costs=[r for r in facts['cost'] if (r['start'],r['end'])==(rev['start'],rev['end'])]
        if len(costs)!=1 or costs[0]['value']<0:
            raise BabaFinancialError('baba_annual_gross_profit_inputs')
        facts['gross_profit'].append({**rev,'value':rev['value']-costs[0]['value'],'tag':'derived_gross_profit',
            'derived':True,'formula':'revenue minus reported positive cost of revenue','inputs':[rev,costs[0]]})
    return facts


def acquire(company_raw, *, symbol, cik, observed_at, client):
    validate_company(json.loads(company_raw),symbol=symbol,cik=cik,observed_at=observed_at)
    return {'baba_originals':{k:client.get(s['url']) for k,s in SOURCES.items()}}


def project(facts_raw, company_raw, *, symbol, cik, observed_at, supplement):
    company=json.loads(company_raw)
    validate_company(company,symbol=symbol,cik=cik,observed_at=observed_at)
    if str(json.loads(facts_raw).get('cik','')).zfill(10)!=CIK:
        raise BabaFinancialError('baba_companyfacts_identity')
    if supplement and supplement.get('baba_error'):
        raise BabaFinancialError(supplement['baba_error'])
    originals=supplement.get('baba_originals',{}) if supplement else {}
    if set(originals)!=set(SOURCES):
        raise BabaFinancialError('baba_source_bundle_incomplete')
    docs={'annual':parse_annual(originals['annual']),
        **{k:parse_interim(originals[k],originals[k+'_cover'],key=k) for k in ('prior','current')}}
    series={}
    for key,doc in docs.items():
        for metric in ('revenue','gross_profit','operating_income','net_income','equity','current_assets','current_liabilities'):
            for row in doc[metric]:
                period=(date.fromisoformat(row['start']) if row['start'] else None,date.fromisoformat(row['end']))
                old=series.setdefault(metric,{}).get(period)
                if old and old['value']!=row['value']:
                    raise BabaFinancialError('baba_comparative_restatement_requires_review:'+metric)
                if old is None or row['filed']>old['filed']:series[metric][period]=row
    from app.services.sec_fundamentals import _ttm
    metrics=('revenue','gross_profit','operating_income','net_income')
    current={k:_ttm(series[k],END) for k in metrics}
    prior={k:_ttm(series[k],date(2025,6,30)) for k in metrics}
    if any(p is None for group in (current,prior) for p in group.values()):
        raise BabaFinancialError('baba_incomplete_trailing_period')
    revenue,old_revenue=current['revenue']['value'],prior['revenue']['value']
    if min(revenue,old_revenue)<=0:
        raise BabaFinancialError('baba_nonpositive_revenue')
    instant={k:series[k][None,END] for k in ('equity','current_assets','current_liabilities')}
    beginning=series['equity'][None,date(2025,6,30)]
    values={'symbol':'BABA','provider':'sec_edgar','company_name':company['name'],'fetched_at':observed_at,'period_date':END,
        'status':'ok','error':None,'revenue_growth':(revenue/old_revenue-1)*100,
        'operating_margin_expansion':(current['operating_income']['value']/revenue-prior['operating_income']['value']/old_revenue)*100}
    for name,metric in [('gross_margin','gross_profit'),('operating_margin','operating_income'),('net_margin','net_income')]:
        values[name]=current[metric]['value']/revenue*100
    if instant['current_liabilities']['value']>0:
        values['current_ratio']=instant['current_assets']['value']/instant['current_liabilities']['value']
    if min(beginning['value'],instant['equity']['value'])>0:
        values['roe']=current['net_income']['value']/((beginning['value']+instant['equity']['value'])/2)*100
    exchanges=company.get('exchanges',[])
    values['exchange']=exchanges[company['tickers'].index('BABA')] if len(exchanges)==len(company['tickers']) else None
    calculations={'gross_margin':'TTM derived gross profit / TTM revenue * 100; CNY GAAP',
        'operating_margin':'TTM operating income / TTM revenue * 100; CNY GAAP',
        'net_margin':'TTM income attributable to Alibaba parent / TTM revenue * 100; CNY GAAP',
        'revenue_growth':'(TTM revenue / prior TTM revenue - 1) * 100',
        'operating_margin_expansion':'current minus prior TTM operating margin in percentage points',
        'current_ratio':'current assets / current liabilities; CNY GAAP',
        'roe':'TTM parent net income / average beginning and ending parent shareholders equity * 100; excludes NCI and mezzanine equity'}
    evidence={'methodology':VERSION,'cik':CIK,'reporting_currency':'CNY','accounting_basis':'US GAAP',
        'available_by':str(available_day(observed_at)),'availability_basis':'after completed New York filing day',
        'company':{'sha256':hashlib.sha256(company_raw).hexdigest()},'facts':{'sha256':hashlib.sha256(facts_raw).hexdigest()},
        'period_start':current['revenue']['start'],'period_end':str(END),'current':current,'prior':prior,
        'instant':instant,'beginning_equity':beginning,'sources':[proof(k) for k in SOURCES],
        'calculations':{k:v for k,v in calculations.items() if k in values},
        'limitations':['Native RMB/CNY GAAP columns only; convenience USD translations and non-GAAP adjustments are excluded.',
            'Parent earnings reconcile separately from consolidated income, noncontrolling interests and mezzanine accretion; parent equity excludes NCI/mezzanine.',
            'Issuer free cash flow excludes campus/land purchases and buyer-protection deposits and can include intangible purchases; it is not canonical CFO minus total PPE.',
            'Current total capital expenditure narrative does not independently establish PPE-only cash purchases for all trailing periods; cash-flow ratios remain unavailable.',
            'No FX conversion, USD cash or valuation, ADR EPS, adjusted EBITDA or combined depreciation/impairment substitution.',
            'New/amended filings require review. This adapter does not complete historical statement panels or all financial fields.']}
    values['source_evidence_json']=json.dumps(evidence,sort_keys=True)
    return values
