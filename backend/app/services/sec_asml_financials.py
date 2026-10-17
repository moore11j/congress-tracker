"""Reviewed ASML EUR US GAAP ratios; never a currency or per-share conversion."""
from datetime import date, datetime, timedelta
from decimal import Decimal
from copy import deepcopy
from zoneinfo import ZoneInfo
import hashlib
import math
import json
import re
from app.services.sec_fundamentals import SecFundamentalsCoverageError
from app.services.sec_original_integrity import verified_original

VERSION = 'sec_asml_eur_ratios_v2'
CIK = '0000937966'
SOURCES = {'2026-06-28': {'url': 'https://www.sec.gov/Archives/edgar/data/937966/000162828026048235/financialstatementsusgaa.htm', 'sha256': 'daa7cbe7b386c29820b61c4293592af55fa2afc97a25a8d5e778cb3fdcece163', 'accession': '0001628280-26-048235', 'filed': '2026-07-15', 'document_sha256': '082cbaf7ece5656692784d5b2824f37f8b973db9cf1d01734d5b36b9fae515de', 'document_integrity_policy': 'sec_terminal_delivery_script_path_v1'}, '2025-06-29': {'url': 'https://www.sec.gov/Archives/edgar/data/937966/000162828025034992/financialstatementsusgaa.htm', 'sha256': 'ba04996957cfcbb6f093e7b10e7b4a318db184794e1296aa47460af10b3320f8', 'accession': '0001628280-25-034992', 'filed': '2025-07-16', 'document_sha256': '79e5c301141bab3ddfc862cbd5886c0f1dec9a91650d276b045ba5eb806714ed', 'document_integrity_policy': 'sec_terminal_delivery_script_path_v1'}, '2026-09-27': {'url': 'https://www.sec.gov/Archives/edgar/data/937966/000162828026065813/financial_statementsxusx.htm', 'sha256': '6a0fa26a4bf3bf07da45bd64a124f34292ef00385f5d97f9b7ba255c017b6148', 'accession': '0001628280-26-065813', 'filed': '2026-10-14', 'document_sha256': '7b13cd56c67395e2d7273966d605948fafef47b41c52bc6d2fedc29dc35c4bbc', 'document_integrity_policy': 'sec_terminal_delivery_script_path_v1'}, '2025-09-28': {'url': 'https://www.sec.gov/Archives/edgar/data/937966/000162828025045043/financialstatementsusgaa.htm', 'sha256': 'c780fba12d84dcb34986da375ec66672137a8b8df38b0ad2426147bbe9db073c', 'accession': '0001628280-25-045043', 'filed': '2025-10-15', 'document_sha256': 'c2cd7b0d1e090ef95e3856a95a80875fe15ab85ace8f7aec3091dcfbad5d48f5', 'document_integrity_policy': 'sec_terminal_delivery_script_path_v1'}}
PAIRS = {'2026-06-28':'2025-06-29','2026-09-27':'2025-09-28'}
COMPARATIVES = {'2025-06-29':'2024-06-30','2026-06-28':'2025-06-29',
                '2025-09-28':'2024-09-29','2026-09-27':'2025-09-28'}
ANNUAL = {'accession':'0001628280-26-011378','filed':'2026-02-25','end':'2025-12-31',
 'filename':'asml-20251231.htm','url':'https://www.sec.gov/Archives/edgar/data/937966/000162828026011378/asml-20251231.htm',
 'sha256':'7a3902f17f4ea364b45172a5b7da443b033e25d01404a42c9169547a4d9b72b7'}
TAGS = {'revenue':'RevenueFromContractWithCustomerExcludingAssessedTax','gross_profit':'GrossProfit',
 'operating_income':'OperatingIncomeLoss','net_income':'NetIncomeLoss',
 'operating_cash_flow':'NetCashProvidedByUsedInOperatingActivities','capex':'PaymentsToAcquirePropertyPlantAndEquipment'}
INCOME = {'system_sales':'Net system sales','service_sales':'Net service and field option sales',
 'revenue':'Total net sales','cost':'Total cost of sales','gross_profit':'Gross profit',
 'rd':'Research and development costs','sga':'Selling, general and administrative costs',
 'operating_income':'Income from operations','pretax':'Income before income taxes',
 'tax':'Income tax expense','aftertax':'Income after income taxes',
 'equity_profit':'Profit related to equity method investments','net_income':'Net income'}
NUMBER = r'(?:\([0-9]{1,3}(?:,[0-9]{3})*\.[0-9]\)|[0-9]{1,3}(?:,[0-9]{3})*\.[0-9])'

class AsmlFinancialError(SecFundamentalsCoverageError):
    pass

def _available(observed_at):
    if observed_at.tzinfo is None:raise AsmlFinancialError('asml_observation_timezone')
    return observed_at.astimezone(ZoneInfo('America/New_York')).date()-timedelta(days=1)

def selected_period(company, *, symbol, cik, observed_at):
    if (symbol!='ASML' or str(cik).zfill(10)!=CIK or str(company.get('cik','')).zfill(10)!=CIK
        or company.get('tickers',[]).count('ASML')!=1 or not company.get('name')):
        raise AsmlFinancialError('asml_company_identity')
    available=_available(observed_at)
    recent=company.get('filings',{}).get('recent',{})
    arrays=[recent.get(k,[]) for k in ('accessionNumber','form','filingDate','reportDate','primaryDocument')]
    if len({len(a) for a in arrays})!=1:raise AsmlFinancialError('asml_directory_shape')
    rows=list(zip(*arrays))
    eligible=[r for r in rows if r[2]<=str(available) and
        (r[1] in ('20-F','20-F/A','6-K/A') or
         (r[1]=='6-K' and r[4].startswith('form6-kquarterly')))]
    latest=max((r[3] for r in eligible),default='')
    if latest not in PAIRS:raise AsmlFinancialError('asml_latest_financial_report_requires_review')
    if not 0<=(available-date.fromisoformat(latest)).days<=180:
        raise AsmlFinancialError('asml_current_period_stale_or_future')
    for end in (latest,PAIRS[latest]):
        s=SOURCES[end];expected=(s['accession'],'6-K',s['filed'],end,'form6-kquarterlyfilings.htm')
        if rows.count(expected)!=1 or s['filed']>str(available):
            raise AsmlFinancialError('asml_reviewed_source_unavailable')
        if any(r[3]==end and r[0]!=s['accession'] for r in eligible):
            raise AsmlFinancialError('asml_amendment_requires_review')
    annual=(ANNUAL['accession'],'20-F',ANNUAL['filed'],ANNUAL['end'],ANNUAL['filename'])
    if rows.count(annual)!=1 or any(r[1]=='20-F/A' and r[3]>=ANNUAL['end'] for r in eligible):
        raise AsmlFinancialError('asml_annual_source_unavailable_or_amended')
    return latest

def _row(text,label,count):
    matches=list(re.finditer(re.escape(label)+r' ((?:'+NUMBER+r'\s*){'+str(count)+r'})(?=[A-Za-z])',text))
    if len(matches)!=1:raise AsmlFinancialError('asml_missing_or_ambiguous_row:'+label)
    tokens=re.findall(NUMBER,matches[0][1])
    return [int(Decimal(t.replace(',','').replace('(','-').replace(')',''))*1_000_000) for t in tokens]

def parse(raw, *, end):
    if not raw or len(raw)>100_000:raise AsmlFinancialError('asml_document_size')
    from lxml import html
    try:tree=html.fromstring(raw.decode('utf-8'))
    except (UnicodeError,ValueError,html.etree.ParserError) as exc:raise AsmlFinancialError('asml_invalid_html') from exc
    pages=[' '.join(i.getparent().text_content().split()) for i in tree.xpath('//img')]
    if len(pages)!=12:raise AsmlFinancialError('asml_document_pages')
    year=date.fromisoformat(end).year;quarter=2 if '-06-' in end else 3
    if pages[0] not in (f'Financial Statements US GAAP Q{quarter} {year} Exhibit 99.3',
        f'Financial Statements US GAAP Q{quarter} {year} Powering technology forward with you Exhibit 99.3'):
        raise AsmlFinancialError('asml_document_identity')
    income,balance,cash=pages[2],pages[4],pages[5]
    period='Six' if quarter==2 else 'Nine';old=COMPARATIVES[end]
    month='Jun' if quarter==2 else 'Sep'
    dates=f'{month} {int(old[-2:])}, {month} {int(end[-2:])}, '*2
    prefix=f'Three months ended {period} months ended '+dates
    units=(f'(Unaudited, €, in millions, except per share data)' if year==2026 else
           '(unaudited, in millions €, except per share data)')
    header=prefix+units+f' {year-1} {year} {year-1} {year} '
    title=f'ASML Financial statements US GAAP Q{quarter} {year}'
    if not income.startswith(header) or title not in income or not income.endswith('Summary US GAAP Consolidated Statements of Operations'):
        raise AsmlFinancialError('asml_income_columns_or_basis')
    if not balance.startswith(f'Dec 31, {month} {int(end[-2:])}, ') or title not in balance or '€' not in balance.split('Assets',1)[0]:
        raise AsmlFinancialError('asml_balance_columns_or_basis')
    if not cash.startswith('Cash flows from operating activities ') or title not in cash or prefix not in cash or 'in millions' not in cash or '€' not in cash:
        raise AsmlFinancialError('asml_cashflow_columns_or_basis')
    nums={k:_row(income,label,4) for k,label in INCOME.items()}
    for i in range(4):
        v=lambda k:nums[k][i]
        checks=[(v('system_sales')+v('service_sales'),v('revenue')),
          (v('revenue')+v('cost'),v('gross_profit')),
          (v('gross_profit')+v('rd')+v('sga'),v('operating_income')),
          (v('pretax')+v('tax'),v('aftertax')),(v('aftertax')+v('equity_profit'),v('net_income'))]
        if any(abs(a-b)>100_000 for a,b in checks):raise AsmlFinancialError('asml_income_reconciliation')
        if any(v(k)>0 for k in ('cost','rd','sga')):raise AsmlFinancialError('asml_expense_sign')
    if _row(cash,'Net income',4)!=nums['net_income']:raise AsmlFinancialError('asml_cashflow_income_reconciliation')
    nums['operating_cash_flow']=_row(cash,'Net cash provided by (used in) operating activities',4)
    capex=_row(cash,'Purchase of property, plant and equipment',4)
    if any(v>0 for v in capex):raise AsmlFinancialError('asml_capex_sign')
    nums['capex']=[-v for v in capex]
    inst={k:_row(balance,label,2) for k,label in {
        'current_assets':'Total current assets','current_liabilities':'Total current liabilities',
        'equity':'Total shareholders’ equity','assets':'Total assets','liabilities':'Total liabilities'}.items()}
    if any(abs(inst['assets'][i]-inst['liabilities'][i]-inst['equity'][i])>100_000 for i in range(2)):
        raise AsmlFinancialError('asml_balance_reconciliation')
    source=SOURCES[end]
    try: receipt = verified_original(raw, source)
    except ValueError as exc: raise AsmlFinancialError('asml_reviewed_original_changed') from exc
    digest = receipt['source_sha256']
    def fact(value,start,end,label):
        return dict(value=value,start=start,end=end,unit='EUR',filed=source['filed'],
            accession=source['accession'],source_url=source['url'],source_sha256=digest,
            source_kind='reviewed_sec_exhibit_text',label=label)
    facts={k:[fact(nums[k][i],f'{y}-01-01',d,k) for i,y,d in [(2,year-1,old),(3,year,end)]] for k in TAGS}
    for k in ('current_assets','current_liabilities','equity'):
        facts[k]=[fact(v,None,d,k) for v,d in zip(inst[k],[f'{year-1}-12-31',end])]
    return {'facts':facts,'_original':raw,'source':dict(source,**{**receipt,'sha256':digest},end=end,accounting_basis='US GAAP',currency='EUR',scale=1_000_000)}

def acquire(company_raw, *, symbol, cik, observed_at, client):
    end=selected_period(json.loads(company_raw),symbol=symbol,cik=cik,observed_at=observed_at)
    return {'asml_documents':{d:parse(client.get(SOURCES[d]['url']),end=d) for d in [end,PAIRS[end]]}}

def project(facts_raw,company_raw,*,symbol,cik,observed_at,supplement):
    company=json.loads(company_raw);end=selected_period(company,symbol=symbol,cik=cik,observed_at=observed_at)
    payload=json.loads(facts_raw)
    if str(payload.get('cik','')).zfill(10)!=CIK:raise AsmlFinancialError('asml_facts_identity')
    if supplement and supplement.get('asml_error'):raise AsmlFinancialError(supplement['asml_error'])
    docs=supplement.get('asml_documents',{}) if supplement else {}
    if set(docs)!={end,PAIRS[end]}:raise AsmlFinancialError('asml_incomplete_bundle')
    from app.services.sec_fundamentals import _records,_ttm
    series={}
    for key,tag in TAGS.items():
        rows=_records(payload,key,_available(observed_at),concepts=(tag,),unit='EUR',forms={'20-F','20-F/A'})
        selected={}
        for year in (2024,2025):
            period=(date(year,1,1),date(year,12,31));row=rows.get(period)
            if not row or row['accession']!=ANNUAL['accession'] or row['filed']!=ANNUAL['filed']:
                raise AsmlFinancialError('asml_annual_facts_missing_or_revised:'+key)
            if abs(row['value'])>1e15:raise AsmlFinancialError('asml_annual_amount_bound')
            selected[period]={**row,'source_url':ANNUAL['url'],'reviewed_original_sha256':ANNUAL['sha256']}
        series[key]=selected
    for d,doc in docs.items():
        try: receipt = verified_original(doc.get('_original',b''), SOURCES[d])
        except ValueError as exc: raise AsmlFinancialError('asml_source_bundle_identity') from exc
        if doc['source']!=dict(SOURCES[d],**{**receipt,'sha256':receipt['source_sha256']},end=d,accounting_basis='US GAAP',currency='EUR',scale=1_000_000):
            raise AsmlFinancialError('asml_source_bundle_identity')
        if set(doc['facts'])!=set(TAGS)|{'current_assets','current_liabilities','equity'}:
            raise AsmlFinancialError('asml_bundle_metric_shape')
        for k,rows in doc['facts'].items():
            year=date.fromisoformat(d).year
            expected_periods=([(f'{year-1}-01-01',COMPARATIVES[d]),(f'{year}-01-01',d)] if k in TAGS else [(None,f'{year-1}-12-31'),(None,d)])
            if [(r.get('start'),r.get('end')) for r in rows]!=expected_periods:
                raise AsmlFinancialError('asml_bundle_period_shape')
            for r in rows:
                if (r.get('unit')!='EUR' or r.get('accession')!=SOURCES[d]['accession']
                    or r.get('filed')!=SOURCES[d]['filed'] or r.get('source_url')!=SOURCES[d]['url']
                    or r.get('source_sha256')!=receipt['source_sha256']
                    or type(r.get('value')) not in (int,float) or not math.isfinite(r['value']) or abs(r['value'])>1e15):
                    raise AsmlFinancialError('asml_bundle_fact_identity')
                period=(date.fromisoformat(r['start']) if r['start'] else None,date.fromisoformat(r['end']))
                prior=series.setdefault(k,{}).get(period)
                if prior and prior['value']!=r['value']:raise AsmlFinancialError('asml_comparative_restatement')
                if prior is None or r['filed']>prior['filed']:series[k][period]=deepcopy(r)
    current={k:_ttm(series[k],date.fromisoformat(end)) for k in TAGS}
    prior={k:_ttm(series[k],date.fromisoformat(PAIRS[end])) for k in TAGS}
    if any(v is None for group in (current,prior) for v in group.values()):raise AsmlFinancialError('asml_incomplete_trailing_period')
    revenue=current['revenue']['value'];old=prior['revenue']['value']
    if min(revenue,old)<=0:raise AsmlFinancialError('asml_nonpositive_revenue')
    result=dict(symbol='ASML',provider='sec_edgar',company_name=company['name'],
        exchange=company.get('exchanges',[])[company['tickers'].index('ASML')] if len(company.get('exchanges',[]))==len(company['tickers']) else None,
        fetched_at=observed_at,period_date=date.fromisoformat(end),status='ok',error=None)
    for name,key in [('gross_margin','gross_profit'),('operating_margin','operating_income'),('net_margin','net_income')]:
        result[name]=current[key]['value']/revenue*100
    result['revenue_growth']=(revenue/old-1)*100
    result['operating_margin_expansion']=result['operating_margin']-prior['operating_income']['value']/old*100
    instant={k:series[k][None,date.fromisoformat(end)] for k in ['current_assets','current_liabilities','equity']}
    beginning=series['equity'][None,date.fromisoformat(PAIRS[end])]
    if instant['current_liabilities']['value']>0:result['current_ratio']=instant['current_assets']['value']/instant['current_liabilities']['value']
    if min(beginning['value'],instant['equity']['value'])>0:result['roe']=current['net_income']['value']/((beginning['value']+instant['equity']['value'])/2)*100
    fcf=current['operating_cash_flow']['value']-current['capex']['value']
    old_fcf=prior['operating_cash_flow']['value']-prior['capex']['value']
    result['fcf_margin']=fcf/revenue*100
    if old_fcf>0:result['fcf_growth']=(fcf/old_fcf-1)*100
    evidence={'methodology':VERSION,'cik':CIK,'reporting_currency':'EUR','accounting_basis':'US GAAP',
        'available_by':str(_available(observed_at)),'availability_basis':'filed date, available after completed New York filing day',
        'company':{'sha256':hashlib.sha256(company_raw).hexdigest()},'facts':{'sha256':hashlib.sha256(facts_raw).hexdigest()},
        'period_start':current['revenue']['start'],'period_end':end,'current':current,'prior':prior,
        'instant':dict(instant,beginning_equity=beginning),'sources':[docs[d]['source'] for d in sorted(docs)],
        'annual_original':ANNUAL,'cash_flow_calculation':{'value':fcf,'prior_value':old_fcf,'currency':'EUR','basis':'Operating cash flow minus cash PP&E purchases; excludes intangible purchases.'},
        'calculations':{'margins':'TTM reported income / TTM revenue * 100','revenue_growth':'(TTM revenue / prior TTM revenue - 1) * 100',
            'operating_margin_expansion':'TTM operating margin minus prior TTM margin, percentage points',
            'current_ratio':'current assets / current liabilities','roe':'TTM net income / average beginning and ending shareholders equity * 100',
            'fcf_margin':'TTM arithmetic free cash flow / revenue * 100','fcf_growth':'(TTM arithmetic free cash flow / prior arithmetic free cash flow - 1) * 100'},
        'limitations':['Ratios only; EUR absolute values never populate USD fields. No FX or per-share conversion.',
            'Annual plus current YTD minus prior YTD uses reported fiscal dates and EUR US GAAP.',
            'Amounts are rounded to EUR0.1 million; reconciliations allow EUR0.1 million rounding residual.',
            'No canonical leverage, market-derived valuation or inferred missing amount. Unreviewed financial filings are held.']}
    result['source_evidence_json']=json.dumps(evidence,sort_keys=True)
    return result
