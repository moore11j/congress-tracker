"""Reviewed SAP EUR IFRS statements; no adjusted, FX or ADR substitutions."""
from datetime import date, timedelta
import hashlib
import json
import re
from zoneinfo import ZoneInfo
from app.services.sec_fundamentals import SecFundamentalsCoverageError

VERSION = 'sec_sap_eur_statements_v1'
CIK = '0001000184'
END = date(2026,6,30)
MAX_BYTES = 15_000_000
SOURCES = {'current': {'accession': '0001104659-26-087251', 'filed': '2026-07-28', 'report': '2026-07-28', 'year': 2026, 'end': '2026-06-30', 'form': '6-K', 'cover': 'tm2621275d1_6k.htm', 'filename': 'tm2621275d1_ex99-2.htm', 'sha256': 'f2afc72d4c46b140350ebf7e6476d00f1c393fba172451995972cb26eaf14329', 'cover_sha256': 'e4063183fe3a970fb5ef37b09acffff9fea121d63e380ec2511f59c9a1edc8b7'}, 'prior': {'accession': '0001104659-25-070485', 'filed': '2025-07-25', 'report': '2025-07-25', 'year': 2025, 'end': '2025-06-30', 'form': '6-K', 'cover': 'tm2521516d1_6k.htm', 'filename': 'tm2521516d1_ex99-2.htm', 'sha256': 'df76b849c9b76ad1eda631efb47a9d2d9d8aa54bb615789cfc098d37206dca9d', 'cover_sha256': 'aa7d096f54849eea5fca23089ebfca64a00f6f5f5f5dddd20b22bad5c9d24a43'}, 'annual': {'accession': '0001104659-26-020058', 'filed': '2026-02-26', 'report': '2025-12-31', 'year': 2025, 'end': '2025-12-31', 'form': '20-F', 'cover': 'sap-20251231x20f.htm', 'filename': 'sap-20251231x20f.htm', 'sha256': '4628dc2d12f68bfbbe7c07a2099a6916db30d9547b8f3d1424b451272790fc15'}}
TAGS = {'revenue':('Revenue',), 'gross_profit':('GrossProfit',),
    'operating_income':('ProfitLossFromOperatingActivities',),
    'net_income':('ProfitLossAttributableToOwnersOfParent',),
    'equity':('EquityAttributableToOwnersOfParent',),
    'current_assets':('CurrentAssets',), 'current_liabilities':('CurrentLiabilities',)}
INCOME = {'revenue':'Total revenue','gross_profit':'Gross profit','operating_income':'Operating profit (loss)',
    'net_income':'Attributable to owners of parent','cost':'Total cost of revenue',
    'expenses':'Total operating expenses','other_income':'Other non-operating income/expense, net',
    'finance_income':'Financial income, net','pretax':'Profit (loss) before tax',
    'tax':'Income tax expense','consolidated_income':'Profit (loss) after tax',
    'minority':'Attributable to non-controlling interests'}
BALANCE = {'equity':'Equity attributable to owners of parent','current_assets':'Total current assets',
    'current_liabilities':'Total current liabilities','assets':'Total assets','liabilities':'Total liabilities',
    'total_equity':'Total equity','total_balance':'Total equity and liabilities','minority':'Non-controlling interests'}


class SapFinancialError(SecFundamentalsCoverageError):
    pass


def url_for(source, *, cover=False):
    name=source['cover'] if cover else source['filename']
    return f"https://www.sec.gov/Archives/edgar/data/1000184/{source['accession'].replace('-', '')}/{name}"


def available_day(observed_at):
    if observed_at.tzinfo is None:raise SapFinancialError('sap_observation_timezone')
    return observed_at.astimezone(ZoneInfo('America/New_York')).date()-timedelta(days=1)


def validate_company(company, *, symbol, cik, observed_at):
    if (symbol!='SAP' or str(cik).zfill(10)!=CIK or str(company.get('cik','')).zfill(10)!=CIK
            or company.get('tickers',[]).count('SAP')!=1 or not company.get('name')):
        raise SapFinancialError('sap_company_identity')
    available=available_day(observed_at)
    if not 0 <= (available-END).days <= 180:raise SapFinancialError('sap_current_period_stale_or_future')
    recent=company.get('filings',{}).get('recent',{})
    arrays=[recent.get(k,[]) for k in ('accessionNumber','form','filingDate','reportDate','primaryDocument')]
    if len({len(a) for a in arrays})!=1:raise SapFinancialError('sap_filing_directory_shape')
    filings=list(zip(*arrays))
    for s in SOURCES.values():
        if filings.count((s['accession'],s['form'],s['filed'],s['report'],s['cover']))!=1 or date.fromisoformat(s['filed'])>available:
            raise SapFinancialError('sap_reviewed_filing_not_available')
    # 6-K reportDate is a publication date, not the half-year period.
    # A later or amended filing is held until its financial relevance is reviewed.
    for acc,form,filed,report,name in filings:
        if form in ('6-K','6-K/A','20-F','20-F/A') and filed<=str(available) and (
                filed>SOURCES['current']['filed'] or (form.endswith('/A') and filed>=SOURCES['annual']['filed'])
                or (filed==SOURCES['current']['filed'] and acc!=SOURCES['current']['accession'])):
            raise SapFinancialError('sap_newer_or_amended_report_requires_review')


def amount(text):
    if not isinstance(text,str) or not re.fullmatch(r'[–-]?(?:[0-9]+|[0-9]{1,3}(?:,[0-9]{3})+)',text):
        raise SapFinancialError('sap_numeric_format')
    value=int(text.replace('–','-').replace(',',''))*1_000_000
    if abs(value)>10**15:raise SapFinancialError('sap_numeric_bound')
    return value


def _reconciles(total, *components):
    # Each published EUR-million amount is independently rounded. Bound the
    # equality by the sum of its rounding intervals, never a percentage tolerance.
    return abs(total-sum(components)) <= (len(components)+1)*500_000


def parse(raw, *, source_key):
    source=SOURCES[source_key]
    if not isinstance(raw,bytes) or not raw or len(raw)>MAX_BYTES:raise SapFinancialError('sap_document_size')
    digest=hashlib.sha256(raw).hexdigest()
    if digest!=source['sha256']:raise SapFinancialError('sap_reviewed_original_changed')
    evidence={'url':url_for(source),'sha256':digest,'accession':source['accession'],'filed':source['filed'],
        'cover_url':url_for(source,cover=True),'accounting_basis':'IFRS','currency':'EUR','scale':1_000_000}
    if 'cover_sha256' in source:evidence['cover_sha256']=source['cover_sha256']
    if source_key=='annual':
        from app.services import sec_inline_financials as inline
        result=inline.parse(raw,cik=CIK,accession=source['accession'],filed=source['filed'],
            report_date=source['end'],form='20-F',url=url_for(source),
            basis={'namespace':'ifrs-full','currency':'EUR','tags':TAGS})
        facts={}
        for metric,tags in TAGS.items():
            facts[metric]=[]
            for tag in tags:
                for row in result['facts'].get(tag,{}).get('units',{}).get('EUR',[]):
                    proof=result['provenance'][tag,'EUR',row.get('start'),row['end']]
                    facts[metric].append({'value':row['val'],'start':row.get('start'),'end':row['end'],'unit':'EUR',
                        'filed':row['filed'],'accession':row['accn'],'tag':tag,**proof})
        return {'facts':facts,'evidence':evidence}
    from lxml import html
    try:tree=html.fromstring(raw)
    except (ValueError,html.etree.ParserError) as exc:raise SapFinancialError('sap_invalid_html') from exc
    text=' '.join(tree.text_content().split())
    if 'Due to rounding, numbers presented throughout these Consolidated Half-Year Financial Statements' not in text:
        raise SapFinancialError('sap_rounding_basis_missing')
    candidates={'income':[],'balance':[]}
    for table in tree.xpath('//table[not(.//table)]'):
        rows=[[' '.join(c.text_content().split()) for c in r.xpath('./td|./th')] for r in table.xpath('.//tr')]
        labels={r[0] for r in rows if r}
        for kind,markers in [('income',{'Total revenue','Gross profit','Attributable to owners of parent'}),
                             ('balance',{'Equity attributable to owners of parent','Total current assets','Total current liabilities'})]:
            if markers<=labels:
                prefix=' '.join(' '.join(table.xpath('preceding::text()')).split())
                title=('Consolidated Income Statement of SAP Group (IFRS) – Half Year' if kind=='income'
                    else 'Consolidated Statement of Financial Position of SAP Group (IFRS)')
                if prefix.endswith(title):candidates[kind].append(rows)
    if any(len(v)!=1 for v in candidates.values()):raise SapFinancialError('sap_statement_missing_or_ambiguous')
    year=source['year'];income,balance=candidates['income'][0],candidates['balance'][0]
    if income[0]!=['€ millions, unless otherwise stated','',f'Q1–Q2 {year}',f'Q1–Q2 {year-1}','∆ in %']:
        raise SapFinancialError('sap_income_columns')
    expected=[f'as at 6/30/{year} and 12/31/{year-1}']
    if [v for v in balance[0] if v]!=expected:raise SapFinancialError('sap_balance_dates')
    header=[r for r in balance if r and r[0]=='€ millions']
    if len(header)!=2 or any([v for v in r if v]!=['€ millions',str(year),str(year-1)] for r in header):
        raise SapFinancialError('sap_balance_columns')
    start=header[0].index(str(year))
    def selected(rows,label,index,width):
        found=[r for r in rows if r and r[0]==label]
        if len(found)!=1 or len(found[0])!=width:raise SapFinancialError('sap_row_missing_or_duplicate:'+label)
        return [amount(v) for v in found[0][index:index+2]]
    inc={k:selected(income,label,2,5) for k,label in INCOME.items()}
    bal={k:selected(balance,label,start,len(header[0])) for k,label in BALANCE.items()}
    for i in range(2):
        v=lambda k:inc[k][i]
        if not all((_reconciles(v('gross_profit'),v('revenue'),v('cost')),
                    _reconciles(v('operating_income'),v('revenue'),v('expenses')),
                    _reconciles(v('pretax'),v('operating_income'),v('other_income'),v('finance_income')),
                    _reconciles(v('consolidated_income'),v('pretax'),v('tax')),
                    _reconciles(v('consolidated_income'),v('net_income'),v('minority')))):
            raise SapFinancialError('sap_income_reconciliation')
        v=lambda k:bal[k][i]
        if (not _reconciles(v('total_equity'),v('equity'),v('minority'))
                or not _reconciles(v('assets'),v('liabilities'),v('total_equity'))
                or v('assets')!=v('total_balance') or not 0<=v('current_assets')<=v('assets')
                or not 0<=v('current_liabilities')<=v('liabilities')):
            raise SapFinancialError('sap_balance_reconciliation')
    def fact(value,start,end,label):
        return {'value':value,'start':start,'end':end,'unit':'EUR','filed':source['filed'],
            'accession':source['accession'],'source_url':url_for(source),'source_sha256':digest,
            'source_kind':'reviewed_sec_html_table','label':label}
    facts={k:[fact(inc[k][i],f'{y}-01-01',f'{y}-06-30',INCOME[k]) for i,y in enumerate((year,year-1))]
        for k in ('revenue','gross_profit','operating_income','net_income')}
    for k in ('equity','current_assets','current_liabilities'):
        facts[k]=[fact(bal[k][i],None,end,BALANCE[k]) for i,end in enumerate((f'{year}-06-30',f'{year-1}-12-31'))]
    return {'facts':facts,'evidence':evidence}


def acquire(company_raw, *, symbol, cik, observed_at, client):
    validate_company(json.loads(company_raw),symbol=symbol,cik=cik,observed_at=observed_at)
    return {'sap_originals':{k:client.get(url_for(s)) for k,s in SOURCES.items()}}


def project(facts_raw, company_raw, *, symbol, cik, observed_at, supplement):
    company=json.loads(company_raw);validate_company(company,symbol=symbol,cik=cik,observed_at=observed_at)
    if str(json.loads(facts_raw).get('cik','')).zfill(10)!=CIK:raise SapFinancialError('sap_companyfacts_identity')
    if supplement and supplement.get('sap_error'):raise SapFinancialError(supplement['sap_error'])
    originals=supplement.get('sap_originals',{}) if supplement else {}
    if set(originals)!=set(SOURCES):raise SapFinancialError('sap_incomplete_source_bundle')
    series={};proofs=[];unused_revisions=[]
    for key,raw in originals.items():
        doc=parse(raw,source_key=key);proofs.append(doc['evidence'])
        for metric,rows in doc['facts'].items():
            for row in rows:
                period=(date.fromisoformat(row['start']) if row['start'] else None,date.fromisoformat(row['end']))
                previous=series.setdefault(metric,{}).get(period)
                if previous and previous['value']!=row['value']:
                    # Prior-year closing balance is not a beginning/end point of
                    # either displayed ROE period. Retain the revision receipt;
                    # any conflict in a consumed period remains a hard hold.
                    if metric not in ('equity','current_assets','current_liabilities') or period!=(None,date(2024,12,31)):
                        raise SapFinancialError('sap_consumed_comparative_restatement_requires_review')
                    unused_revisions.append({'metric':metric,'period':str(period[1]),'observations':[previous,row],
                        'use':'not used in any published calculation'})
                if previous is None or row['filed']>previous['filed']:series[metric][period]=row
    from app.services.sec_fundamentals import _ttm
    metrics=('revenue','gross_profit','operating_income','net_income')
    current={k:_ttm(series[k],END) for k in metrics};prior={k:_ttm(series[k],date(2025,6,30)) for k in metrics}
    if any(p is None for group in (current,prior) for p in group.values()):raise SapFinancialError('sap_incomplete_trailing_period')
    revenue=current['revenue']['value'];old_revenue=prior['revenue']['value']
    if min(revenue,old_revenue)<=0:raise SapFinancialError('sap_nonpositive_revenue')
    instant={k:series[k][None,END] for k in ('equity','current_assets','current_liabilities')}
    beginning=series['equity'][None,date(2025,6,30)]
    values={'symbol':'SAP','provider':'sec_edgar','company_name':company['name'],'fetched_at':observed_at,
        'period_date':END,'status':'ok','error':None,'revenue_growth':(revenue/old_revenue-1)*100,
        'operating_margin_expansion':(current['operating_income']['value']/revenue-prior['operating_income']['value']/old_revenue)*100}
    calculations={'revenue_growth':'(TTM revenue / prior TTM revenue - 1) * 100',
        'operating_margin_expansion':'TTM operating margin minus prior TTM operating margin, percentage points'}
    for name,metric in [('gross_margin','gross_profit'),('operating_margin','operating_income'),('net_margin','net_income')]:
        values[name]=current[metric]['value']/revenue*100;calculations[name]='TTM '+metric+' / TTM revenue * 100; EUR IFRS'
    if instant['current_liabilities']['value']>0:
        values['current_ratio']=instant['current_assets']['value']/instant['current_liabilities']['value'];calculations['current_ratio']='current assets / current liabilities; EUR'
    if min(instant['equity']['value'],beginning['value'])>0:
        values['roe']=current['net_income']['value']/((instant['equity']['value']+beginning['value'])/2)*100
        calculations['roe']='TTM parent income / average beginning and ending parent equity * 100; EUR'
    exchanges=company.get('exchanges',[]);values['exchange']=exchanges[company['tickers'].index('SAP')] if len(exchanges)==len(company['tickers']) else None
    evidence={'methodology':VERSION,'cik':CIK,'reporting_currency':'EUR','accounting_basis':'IFRS',
        'available_by':str(available_day(observed_at)),'availability_basis':'filed date, available after completed New York filing day',
        'facts':{'sha256':hashlib.sha256(facts_raw).hexdigest()},'company':{'sha256':hashlib.sha256(company_raw).hexdigest()},
        'period_start':current['revenue']['start'],'period_end':str(END),'current':current,'prior':prior,
        'instant':instant,'beginning_equity':beginning,'sources':proofs,'calculations':calculations,
        'unused_balance_revisions':unused_revisions,
        'limitations':['Ratios use actual-currency EUR IFRS statements; no constant-currency or non-IFRS measures.',
            'No FX conversion, USD cash amount, ADR EPS or USD valuation inference.',
            'Cash purchases combine intangible assets with PPE; no canonical PPE-only FCF is inferred.',
            'Operating cash flow classification changed in January 2025; cash-flow growth is not substituted.',
            'Net margin and ROE use parent earnings and equity; no EBITDA or leverage proxy.',
            'Published EUR-million amounts are rounded; identities are checked within their explicit rounding intervals.',
            'New or amended filings require review before further preparation.']}
    values['source_evidence_json']=json.dumps(evidence,sort_keys=True);return values
