"""Unilever EUR statements with the issuer's explicit ice-cream restatement."""
from datetime import date, datetime, timedelta
from pathlib import Path
import hashlib
import io
import json
import re
from zoneinfo import ZoneInfo
from app.services.sec_fundamentals import SecFundamentalsCoverageError

VERSION = 'sec_ul_archived_restatement_v2'
RESTATEMENT_FILE = Path(__file__).resolve().parents[2]/'config/financial_continuity/ul-half-year-restatement-2026-03-12.pdf'
CIK = '0000217410'
END = date(2026, 6, 30)
SOURCES = {'current': {'period': '2026-06-30', 'form': '6-K', 'filed': '2026-07-28', 'accession': '0000217410-26-000057', 'document': 'ul-20260630.htm', 'url': 'https://www.sec.gov/Archives/edgar/data/217410/000021741026000057/ul-20260630.htm', 'sha256': '4f6be2d563061a80c34c691eee64a0874c4ca1a6ff034f17cde9e58194774188', 'bytes': 2092905, 'tables': 9, 'inline_facts': 0}, 'annual': {'period': '2025-12-31', 'form': '20-F', 'filed': '2026-03-12', 'accession': '0000217410-26-000007', 'document': 'ul-20251231.htm', 'url': 'https://www.sec.gov/Archives/edgar/data/217410/000021741026000007/ul-20251231.htm', 'sha256': '6b7a85d845cd8874ef8ddc674016ab8f693f7443d41fd19e0e4ddb2a01d5659c', 'bytes': 13935127, 'tables': 17, 'inline_facts': 0}}
NONFINANCIAL = [{'accessionNumber': '0001654954-26-008570', 'form': '6-K', 'filingDate': '2026-09-25', 'reportDate': '2026-09-25', 'primaryDocument': 'a4269w.htm', 'url': 'https://www.sec.gov/Archives/edgar/data/217410/000165495426008570/a4269w.htm', 'sha256': 'e68f513ed0f1d4a0e61bd7a9de4fec93f0e1ee6b6490c8c620d0ec1eccc7e650', 'bytes': 240094, 'reason': 'managerial shareholding notification'}, {'accessionNumber': '0001654954-26-008542', 'form': '6-K', 'filingDate': '2026-09-24', 'reportDate': '2026-09-24', 'primaryDocument': 'a1711w.htm', 'url': 'https://www.sec.gov/Archives/edgar/data/217410/000165495426008542/a1711w.htm', 'sha256': '7269b53aa8370e696833237f11d25fd3d31261dc65b61175f885d640ca4cf9a8', 'bytes': 35748, 'reason': 'managerial shareholding notification'}, {'accessionNumber': '0001654954-26-008017', 'form': '6-K', 'filingDate': '2026-09-01', 'reportDate': '2026-09-01', 'primaryDocument': 'a9452s.htm', 'url': 'https://www.sec.gov/Archives/edgar/data/217410/000165495426008017/a9452s.htm', 'sha256': '324794bf17fffc3941eb064392d41c36747ae0ad68cfb73e15ed3b7eff0c3dd0', 'bytes': 15054, 'reason': 'voting rights and capital notice'}, {'accessionNumber': '0001654954-26-007828', 'form': '6-K', 'filingDate': '2026-08-24', 'reportDate': '2026-08-24', 'primaryDocument': 'a9537r.htm', 'url': 'https://www.sec.gov/Archives/edgar/data/217410/000165495426007828/a9537r.htm', 'sha256': '9923ca6c148df5f819708debb72b0c7afa350f7bda1be57d4878fc651b4d43b3', 'bytes': 36916, 'reason': 'managerial shareholding notification'}, {'accessionNumber': '0001654954-26-007194', 'form': '6-K', 'filingDate': '2026-08-03', 'reportDate': '2026-08-03', 'primaryDocument': 'a0776p.htm', 'url': 'https://www.sec.gov/Archives/edgar/data/217410/000165495426007194/a0776p.htm', 'sha256': 'cef5f3dc562f2607d62c35b9b752bf4ff35d876ef28c2751dfbdd13bf6b700e0', 'bytes': 15777, 'reason': 'voting rights and capital notice'}, {'accessionNumber': '0001654954-26-007138', 'form': '6-K', 'filingDate': '2026-08-03', 'reportDate': '2026-08-03', 'primaryDocument': 'a9740o.htm', 'url': 'https://www.sec.gov/Archives/edgar/data/217410/000165495426007138/a9740o.htm', 'sha256': '65507a680da0093b0d8316d2f13c5564d73cb11ab14af6e42a4fe1d584bb7f2c', 'bytes': 35389, 'reason': 'managerial shareholding notification'}]
RESTATEMENT = {'url': 'https://www.unilever.com/files/unilever-half-year-historical-financials-excluding-ice-cream-pdf.pdf', 'sha256': 'edddb9fb8ac446e216ccf7dbcc193ca9484600999764f9651afc7917d1e4de93', 'bytes': 179697, 'observed_at': '2026-10-15T20:42:20.679876+00:00', 'pages': 7, 'published': '2026-03-12', 'source_kind': 'issuer_restatement_pdf'}
INCOME = {'revenue':'Turnover','operating_income':'Operating profit','net_income':'Shareholders’ equity',
    'total_income':'Total net profit','minority':'Non-controlling interests',
    'continuing_income':'Net profit from continuing operations','tax':'Taxation',
    'net_finance':'Net finance costs','pensions':'Pensions and similar obligations',
    'finance_income':'Finance income','finance_cost':'Finance costs',
    'monetary':'Net monetary loss arising from hyperinflationary economies',
    'associates':'Share of net profit of joint ventures and associates'}


class UnileverFinancialError(SecFundamentalsCoverageError):
    pass


def available_day(observed):
    if observed.tzinfo is None:raise UnileverFinancialError('ul_observation_timezone')
    return observed.astimezone(ZoneInfo('America/New_York')).date()-timedelta(days=1)


def validate_company(company, *, symbol, cik, observed_at):
    if (symbol!='UL' or str(cik).zfill(10)!=CIK or str(company.get('cik','')).zfill(10)!=CIK
            or company.get('tickers',[]).count('UL')!=1 or company.get('name')!='UNILEVER PLC'):
        raise UnileverFinancialError('ul_company_identity')
    available=available_day(observed_at)
    if not 0 <= (available-END).days <= 180:raise UnileverFinancialError('ul_period_stale_or_future')
    recent=company.get('filings',{}).get('recent',{})
    fields=('accessionNumber','form','filingDate','reportDate','primaryDocument')
    arrays=[recent.get(k,[]) for k in fields]
    if any(not isinstance(a,list) for a in arrays) or len({len(a) for a in arrays})!=1:
        raise UnileverFinancialError('ul_directory_shape')
    filings=list(zip(*arrays))
    for s in SOURCES.values():
        identity=(s['accession'],s['form'],s['filed'],s['period'],s['document'])
        if filings.count(identity)!=1 or date.fromisoformat(s['filed'])>available:
            raise UnileverFinancialError('ul_reviewed_filing_unavailable')
    permitted={tuple(row[k] for k in fields) for row in NONFINANCIAL}
    for row in filings:
        acc,form,filed,report,name=row
        if form not in ('6-K','6-K/A','20-F','20-F/A') or filed>str(available):continue
        if ((form.endswith('/A') and filed>=min(s['filed'] for s in SOURCES.values()))
                or (filed>=SOURCES['current']['filed'] and acc!=SOURCES['current']['accession'] and row not in permitted)):
            raise UnileverFinancialError('ul_newer_or_amended_report_requires_review')
    if date.fromisoformat(RESTATEMENT['published'])>available:
        raise UnileverFinancialError('ul_restatement_not_available')
    if observed_at < datetime.fromisoformat(RESTATEMENT['observed_at']):
        raise UnileverFinancialError('ul_historical_original_not_yet_observed')


def amount(value):
    if not isinstance(value,str) or not re.fullmatch(r'(?:[0-9]+|[0-9]{1,3}(?:,[0-9]{3})+|\((?:[0-9]+|[0-9]{1,3}(?:,[0-9]{3})+)\))',value):
        raise UnileverFinancialError('ul_numeric_format')
    negative=value.startswith('(')
    number=int(value.strip('()').replace(',',''))*1_000_000*(-1 if negative else 1)
    if abs(number)>10**15:raise UnileverFinancialError('ul_numeric_bound')
    return number


def reconciles(total,*parts):
    return abs(total-sum(parts)) <= (len(parts)+1)*500_000


def _selected(rows,label,start,count,*,width=None):
    matches=[r for r in rows if r and r[0]==label]
    if len(matches)!=1 or (width is not None and len(matches[0])!=width):
        raise UnileverFinancialError('ul_missing_or_duplicate_row:'+label)
    cells=matches[0][start:start+count]
    if len(cells)!=count:raise UnileverFinancialError('ul_numeric_columns')
    return [amount(v) for v in cells]


def _income(rows, *, source_key):
    annual=source_key=='annual';restated=source_key=='restated'
    count=3 if annual else 2;start=2 if annual else 1;width=5 if annual else (3 if restated else 4)
    labels={**INCOME,'pretax':'Profit before taxation' if source_key=='current' else 'Profit before taxation from continuing operations',
        'other':'Other income (loss) from non-current investments and associates' if source_key=='current' else 'Other income/(loss) from non-current investments and associates'}
    if restated:labels['finance_income']='Finance Income'
    values={k:_selected(rows,label,start,count,width=width) for k,label in labels.items()}
    for i in range(count):
        v=lambda k:values[k][i]
        if not all((reconciles(v('net_finance'),v('pensions'),v('finance_income'),v('finance_cost')),
                    reconciles(v('pretax'),v('operating_income'),v('net_finance'),v('monetary'),v('associates'),v('other')),
                    reconciles(v('continuing_income'),v('pretax'),v('tax')),
                    reconciles(v('total_income'),v('net_income'),v('minority')))):
            raise UnileverFinancialError('ul_income_reconciliation')
    return values


def _proof(source):
    return {'url':source['url'],'sha256':source['sha256'],'published':source.get('filed',source.get('published')),
            'source_kind':source.get('source_kind','sec_filing'),'currency':'EUR','scale':1_000_000,
            **({'accession':source['accession']} if 'accession' in source else {}),
            **({'retrieval':'reviewed_historical_original', 'source_observed_at':source['observed_at']}
               if source.get('source_kind')=='issuer_restatement_pdf' else {})}


def _facts(income,source,*,annual=False):
    years=(2025,2024,2023) if annual else ((2026,2025) if source.get('period')=='2026-06-30' else (2025,2024))
    proof=_proof(source)
    return {metric:[{'value':income[metric][i],'start':f'{year}-01-01','end':f'{year}-12-31' if annual else f'{year}-06-30',
                    'unit':'EUR','filed':proof['published'],'source_url':proof['url'],'source_sha256':proof['sha256'],
                    'source_kind':proof['source_kind'],'label':INCOME[metric]} for i,year in enumerate(years)]
            for metric in ('revenue','operating_income','net_income')}


def parse_html(raw,*,source_key):
    source=SOURCES[source_key]
    if not isinstance(raw,bytes) or not raw or len(raw)>15_000_000:raise UnileverFinancialError('ul_source_size')
    if hashlib.sha256(raw).hexdigest()!=source['sha256']:raise UnileverFinancialError('ul_reviewed_original_changed')
    from lxml import html
    try:tree=html.fromstring(raw)
    except (ValueError,html.etree.ParserError) as exc:raise UnileverFinancialError('ul_invalid_html') from exc
    tables=[]
    for table in tree.xpath('//table'):
        rows=[[' '.join(c.text_content().split()) for c in r.xpath('./td|./th')] for r in table.xpath('./tr|./tbody/tr')]
        if rows:tables.append(rows)
    def table_with(markers):
        found=[rows for rows in tables if markers<={r[0] for r in rows if r}]
        if len(found)!=1:raise UnileverFinancialError('ul_statement_missing_or_ambiguous')
        return found[0]
    income=table_with({'Turnover','Total net profit','Net profit from continuing operations'})
    annual=source_key=='annual'
    header=['','Notes','€ million2025','€ million2024(a)','€ million2023(a)'] if annual else ['(unaudited)','2026','2025(a)','Change']
    if income.count(header)!=1:raise UnileverFinancialError('ul_income_columns')
    inc=_income(income,source_key=source_key);facts=_facts(inc,source,annual=annual)
    cash=table_with({'Total cash flows from operating activities','Purchase of property, plant and equipment','Net cash flow from continuing operating activities'})
    if annual:
        if cash.count(header)!=1:raise UnileverFinancialError('ul_cash_columns')
    elif cash.count(['€ million','2026','2025(a)'])!=1:raise UnileverFinancialError('ul_cash_columns')
    start,count,width=(2,3,5) if annual else (1,2,3)
    cfo=_selected(cash,'Net cash flow from continuing operating activities',start,count,width=width)
    pretax=_selected(cash,'Cash flow from continuing operating activities',start,count,width=width)
    tax=_selected(cash,'Income tax paid on continuing operations',start,count,width=width)
    capex=_selected(cash,'Purchase of property, plant and equipment',start,count,width=width)
    if any(not reconciles(cfo[i],pretax[i],tax[i]) or capex[i]>0 for i in range(count)):
        raise UnileverFinancialError('ul_cash_reconciliation')
    for metric,values,label in [('operating_cash_flow',cfo,'Net cash flow from continuing operating activities'),('capex',[-v for v in capex],'Purchase of property, plant and equipment')]:
        facts[metric]=[{**facts['revenue'][i],'value':v,'label':label} for i,v in enumerate(values)]
    if not annual:
        balance=table_with({'Total assets','Total liabilities','Total liabilities and equity','Shareholders’ equity'})
        if balance.count(['€ million','As at 30 June 2026','As at 31 December 2025','As at 30 June 2025'])!=1:
            raise UnileverFinancialError('ul_balance_columns')
        selected={k:_selected(balance,label,1,3,width=4) for k,label in
                  {'assets':'Total assets','liabilities':'Total liabilities','total_equity':'Total equity','equity':'Shareholders’ equity',
                   'minority':'Non-controlling interests','total_balance':'Total liabilities and equity'}.items()}
        def subtotal(section,next_section):
            begins=[i for i,r in enumerate(balance) if r and r[0]==section]
            ends=[i for i,r in enumerate(balance) if r and r[0]==next_section]
            if len(begins)!=1 or len(ends)!=1 or begins[0]>=ends[0]:raise UnileverFinancialError('ul_balance_section')
            block=[r for r in balance[begins[0]+1:ends[0]] if any(r)]
            if not block or len(block[-1])!=4 or block[-1][0]!='':raise UnileverFinancialError('ul_balance_subtotal')
            totals=[amount(v) for v in block[-1][1:]]
            components=[[0 if v in ('—','–') else amount(v) for v in r[1:]] for r in block[:-1] if len(r)==4]
            if not components or any(not reconciles(totals[i],*(row[i] for row in components)) for i in range(3)):
                raise UnileverFinancialError('ul_balance_subtotal_reconciliation')
            return totals
        selected['current_assets']=subtotal('Current assets','Total assets')
        selected['current_liabilities']=subtotal('Current liabilities','Non-current liabilities')
        for i in range(3):
            v=lambda k:selected[k][i]
            if (not reconciles(v('assets'),v('liabilities'),v('total_equity')) or not reconciles(v('total_equity'),v('equity'),v('minority'))
                    or v('total_balance')!=v('assets') or not 0<=v('current_assets')<=v('assets') or not 0<=v('current_liabilities')<=v('liabilities')):
                raise UnileverFinancialError('ul_balance_reconciliation')
        for metric in ('equity','current_assets','current_liabilities'):
            facts[metric]=[{'value':selected[metric][i],'start':None,'end':end,'unit':'EUR','filed':source['filed'],
                'source_url':source['url'],'source_sha256':source['sha256'],'source_kind':'sec_filing','label':metric}
                for i,end in enumerate(('2026-06-30','2025-12-31','2025-06-30'))]
    return {'facts':facts,'evidence':_proof(source),'income':inc}


def parse_restated(raw):
    if not isinstance(raw,bytes) or not raw or len(raw)>2_000_000:raise UnileverFinancialError('ul_restatement_size')
    if hashlib.sha256(raw).hexdigest()!=RESTATEMENT['sha256']:raise UnileverFinancialError('ul_restatement_changed')
    from pypdf import PdfReader
    try:
        reader=PdfReader(io.BytesIO(raw),strict=True)
        if reader.is_encrypted or len(reader.pages)!=7:raise UnileverFinancialError('ul_restatement_pdf_structure')
        decoded=0;pages=[]
        for page in reader.pages:
            content=page.get_contents();decoded+=len(content.get_data()) if content else 0
            if decoded>5_000_000:raise UnileverFinancialError('ul_restatement_decoded_bound')
            pages.append(page.extract_text() or '')
    except UnileverFinancialError:raise
    except Exception as exc:raise UnileverFinancialError('ul_restatement_pdf_parse') from exc
    if ('12 March 2026' not in pages[0] or 'Unilever Historical Financials excluding Ice Cream' not in pages[0]
            or 'First Half of 2024 and the First Half of 2025' not in pages[0]):
        raise UnileverFinancialError('ul_restatement_basis')
    lines=[' '.join(line.split()) for line in pages[1].splitlines()]
    if 'Income statement line item (unaudited) € million 2025 2024' not in lines:
        raise UnileverFinancialError('ul_restatement_columns')
    rows=[]
    for line in lines:
        match=re.fullmatch(r'(.+?)\s+(\(?[0-9][0-9,]*\)?)\s+(\(?[0-9][0-9,]*\)?)',line)
        if match:rows.append(list(match.groups()))
    inc=_income(rows,source_key='restated')
    return {'facts':_facts(inc,RESTATEMENT),'evidence':_proof(RESTATEMENT),'income':inc}


def historical_restatement():
    """Retain the reviewed original, never present a local read as a new fetch."""
    try:
        raw = RESTATEMENT_FILE.read_bytes()
    except OSError as exc:
        raise UnileverFinancialError('ul_historical_original_missing') from exc
    if (len(raw) != RESTATEMENT['bytes']
            or hashlib.sha256(raw).hexdigest() != RESTATEMENT['sha256']):
        raise UnileverFinancialError('ul_historical_original_changed')
    return raw


def acquire(company_raw,*,symbol,cik,observed_at,client):
    validate_company(json.loads(company_raw),symbol=symbol,cik=cik,observed_at=observed_at)
    archived = historical_restatement()
    return {'ul_originals':{**{k:client.get(s['url']) for k,s in SOURCES.items()},'restated':archived}}


def project(facts_raw,company_raw,*,symbol,cik,observed_at,supplement):
    company=json.loads(company_raw);validate_company(company,symbol=symbol,cik=cik,observed_at=observed_at)
    if str(json.loads(facts_raw).get('cik','')).zfill(10)!=CIK:raise UnileverFinancialError('ul_companyfacts_identity')
    if supplement and supplement.get('ul_error'):raise UnileverFinancialError(supplement['ul_error'])
    originals=supplement.get('ul_originals',{}) if supplement else {}
    if set(originals)!={'current','annual','restated'}:raise UnileverFinancialError('ul_incomplete_source_bundle')
    docs={key:parse_restated(raw) if key=='restated' else parse_html(raw,source_key=key) for key,raw in originals.items()}
    # All common 2025 half-year income lines must match the explicitly restated
    # issuer disclosure. Never mix the old ice-cream-inclusive half-year release.
    if any(values[1]!=docs['restated']['income'][metric][0] for metric,values in docs['current']['income'].items()):
        raise UnileverFinancialError('ul_restatement_comparative_mismatch')
    series={}
    for doc in docs.values():
        for metric,rows in doc['facts'].items():
            for row in rows:
                key=(date.fromisoformat(row['start']) if row['start'] else None,date.fromisoformat(row['end']))
                prior=series.setdefault(metric,{}).get(key)
                if prior and prior['value']!=row['value']:raise UnileverFinancialError('ul_conflicting_period')
                if prior is None or row['filed']>prior['filed']:series[metric][key]=row
    from app.services.sec_fundamentals import _ttm
    current={k:_ttm(series[k],END) for k in ('revenue','operating_income','net_income','operating_cash_flow','capex')}
    prior={k:_ttm(series[k],date(2025,6,30)) for k in ('revenue','operating_income','net_income')}
    if any(v is None for group in (current,prior) for v in group.values()):raise UnileverFinancialError('ul_incomplete_ttm')
    revenue=current['revenue']['value'];old_revenue=prior['revenue']['value']
    if min(revenue,old_revenue)<=0:raise UnileverFinancialError('ul_nonpositive_revenue')
    instant={k:series[k][None,END] for k in ('equity','current_assets','current_liabilities')}
    beginning=series['equity'][None,date(2025,6,30)]
    values={'symbol':'UL','provider':'sec_edgar','company_name':company['name'],'fetched_at':observed_at,'period_date':END,'status':'ok','error':None,
        'revenue_growth':(revenue/old_revenue-1)*100,'operating_margin':current['operating_income']['value']/revenue*100,
        'operating_margin_expansion':(current['operating_income']['value']/revenue-prior['operating_income']['value']/old_revenue)*100,
        'net_margin':current['net_income']['value']/revenue*100,
        'fcf_margin':(current['operating_cash_flow']['value']-current['capex']['value'])/revenue*100}
    if instant['current_liabilities']['value']>0:values['current_ratio']=instant['current_assets']['value']/instant['current_liabilities']['value']
    if min(instant['equity']['value'],beginning['value'])>0:
        values['roe']=current['net_income']['value']/((instant['equity']['value']+beginning['value'])/2)*100
    exchanges=company.get('exchanges',[]);values['exchange']=exchanges[company['tickers'].index('UL')] if len(exchanges)==len(company['tickers']) else None
    evidence={'methodology':VERSION,'cik':CIK,'reporting_currency':'EUR','accounting_basis':'IFRS',
        'available_by':str(available_day(observed_at)),'availability_basis':'all source publication dates complete in New York; no intraday inference',
        'facts':{'sha256':hashlib.sha256(facts_raw).hexdigest()},'company':{'sha256':hashlib.sha256(company_raw).hexdigest()},
        'period_start':current['revenue']['start'],'period_end':str(END),'current':current,'prior':prior,'instant':instant,'beginning_equity':beginning,
        'sources':[doc['evidence'] for doc in docs.values()],'reviewed_nonfinancial_filings':NONFINANCIAL,
        'display_context':{'source_label':'SEC EDGAR + Unilever',
            'source_note':'EUR IFRS statements and issuer-restated continuing operations excluding Ice Cream. Net margin and ROE include discontinued operations and disposal gains. FCF margin uses continuing operating cash flow less gross PPE purchases; it excludes intangible purchases and is not issuer-adjusted FCF.'},
        'calculations':{'revenue_growth':'(current TTM continuing revenue / prior TTM continuing revenue - 1) * 100',
            'operating_margin':'TTM operating profit / TTM continuing revenue * 100',
            'operating_margin_expansion':'current minus prior TTM operating margin in percentage points',
            'net_margin':'TTM total parent earnings, including discontinued operations and disposal gains / TTM revenue * 100',
            'roe':'TTM total parent earnings / average beginning and ending parent equity * 100',
            'current_ratio':'current assets / current liabilities',
            'fcf_margin':'(TTM continuing operating cash flow after tax - gross PPE cash purchases) / TTM continuing revenue * 100'},
        'limitations':['EUR amounts are not converted to USD or combined with USD market values.',
            'Revenue and operating profit use the issuer-restated continuing perimeter excluding Ice Cream.',
            'Net margin and ROE retain total parent earnings including discontinued operations and disposal gains; not recurring profitability.',
            'FCF margin uses continuing cash flow with IFRS interest classified in financing, excludes intangible purchases, and is not issuer-adjusted FCF.',
            'No gross margin, prior FCF growth, EBITDA, net debt or ADR/EPS inference from incomplete definitions.',
            'Historical issuer restatement uses the reviewed original captured on '+RESTATEMENT['observed_at']+'; it is not a fresh issuer-site observation.',
            'Fresh SEC directory and report checks remain required. Any new filing or amended financial disclosure requires review.']}
    values['source_evidence_json']=json.dumps(evidence,sort_keys=True);return values
