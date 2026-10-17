"""Reviewed TSM Taiwan-IFRS ratios, without currency or ADR conversions.

Only reviewed consolidated 6-K originals may form a trailing period. The IASB
20-F and parent-company-only statements are deliberately not combined here.
"""
from datetime import date, datetime, timedelta
from decimal import Decimal
import hashlib
import json
import re
from zoneinfo import ZoneInfo

VERSION = 'sec_tsm_taiwan_ifrs_v2'
CIK = '0001046179'
END = date(2026, 6, 30)
MAX_BYTES = 8_000_000
SOURCES = {'current': {'accession': '0001046179-26-000541', 'filed': '2026-08-14', 'end': '2026-06-30', 'year': 2026, 'cover': 'tsm-fsx20260814x6k.htm', 'filename': 'a2026q2consolidatedreport-.htm', 'sha256': '87a317c976eae5fdffa37421e16f1d0d4248abc15a2ea7e0c3883cd0510a48c4', 'document_sha256': '1efacd09635cd31077ecf231e7c66906ad3807bd5fb6c4b90541e2b6319f423c', 'document_integrity_policy': 'sec_terminal_delivery_script_path_v1'}, 'prior': {'accession': '0001046179-25-000097', 'filed': '2025-08-14', 'end': '2025-06-30', 'year': 2025, 'cover': 'tsm-fsx20250814x6k.htm', 'filename': 'tsmc2025q2consolidatedfina.htm', 'sha256': '8510c495d472da69ff9cdf3d8a5d64212c6c296d3514cd5cce8dde5ed9a274f1', 'document_sha256': '11dc19569753d1128e9e03a26c1f86ab93561dd01bd3118799960634fbd7cec7', 'document_integrity_policy': 'sec_terminal_delivery_script_path_v1'}, 'annual': {'accession': '0001046179-26-000024', 'filed': '2026-02-26', 'end': '2025-12-31', 'year': 2025, 'cover': 'tsm-fsx20260226x6k.htm', 'filename': 'a2025q4consolidatedreport-.htm', 'sha256': 'ba24f40bf0bccb828bd54627d294b9e49c84d288cb0afb49f7cffeb0f7734b01', 'document_sha256': 'fc1b394c2abe29b22668cb3099c5265ed16cd62a046e344f032a88e2dc1ee130', 'document_integrity_policy': 'sec_terminal_delivery_script_path_v1'}}
INCOME = {
 'revenue':'NET REVENUE', 'cost':'COST OF REVENUE', 'gross_profit':'GROSS PROFIT',
 'expenses':'Total operating expenses', 'other_operating':'OTHER OPERATING INCOME AND EXPENSES, NET',
 'operating_income':'INCOME FROM OPERATIONS', 'nonoperating':'Total non-operating income and expenses',
 'pretax':'INCOME BEFORE INCOME TAX', 'tax':'INCOME TAX EXPENSE', 'consolidated_income':'NET INCOME',
}


from app.services.sec_fundamentals import SecFundamentalsCoverageError
from app.services.sec_original_integrity import verified_original


class TsmFinancialError(SecFundamentalsCoverageError):
    pass


def url_for(source):
    return f"https://www.sec.gov/Archives/edgar/data/1046179/{source['accession'].replace('-', '')}/{source['filename']}"


def available_day(observed_at):
    if observed_at.tzinfo is None:
        raise TsmFinancialError('tsm_observation_timezone')
    return observed_at.astimezone(ZoneInfo('America/New_York')).date()-timedelta(days=1)


def validate_company(company, *, symbol, cik, observed_at):
    if (symbol != 'TSM' or str(cik).zfill(10) != CIK
        or str(company.get('cik', '')).zfill(10) != CIK
        or company.get('tickers', []).count('TSM') != 1 or not company.get('name')):
        raise TsmFinancialError('tsm_company_identity')
    available=available_day(observed_at)
    if not 0 <= (available-END).days <= 180:
        raise TsmFinancialError('tsm_current_period_stale_or_future')
    recent=company.get('filings', {}).get('recent', {})
    arrays=[recent.get(k, []) for k in ('accessionNumber','form','filingDate','reportDate','primaryDocument')]
    if len({len(a) for a in arrays}) != 1:
        raise TsmFinancialError('tsm_filing_directory_shape')
    filings=list(zip(*arrays))
    for source in SOURCES.values():
        expected=(source['accession'],'6-K',source['filed'],source['end'],source['cover'])
        if filings.count(expected) != 1 or date.fromisoformat(source['filed']) > available:
            raise TsmFinancialError('tsm_reviewed_filing_not_available')
    for acc,form,filed,end,name in filings:
        if (form in ('6-K','6-K/A') and (name.startswith('tsm-fsx') or form=='6-K/A')
            and filed <= str(available)):
            if end > str(END) or (end == str(END) and acc != SOURCES['current']['accession']):
                raise TsmFinancialError('tsm_newer_or_amended_financial_report_requires_review')


def _label(value):
    return re.sub(r'\s*\(Notes? [^)]*\)$', '', value).strip()


def _amounts(cells, count):
    # Each reviewed row alternates an amount and its presentation percentage.
    # A dash is accepted in the percentage column only, never as an amount.
    text=' '.join(cells).replace('$','')
    tokens=re.findall(r'\(\s*[0-9,]+\s*\)|[0-9,]+|-',text)
    if re.sub(r'\(\s*[0-9,]+\s*\)|[0-9,]+|-','',text).strip() or len(tokens)!=count*2:
        raise TsmFinancialError('tsm_numeric_column_shape')
    result=[]
    for i,token in enumerate(tokens):
        clean=token.replace(' ','')
        if clean=='-':
            if i%2==0:raise TsmFinancialError('tsm_missing_amount')
            continue
        if not re.fullmatch(r'\(?[0-9]{1,3}(?:,[0-9]{3})*\)?|\(?[0-9]+\)?',clean):
            raise TsmFinancialError('tsm_numeric_format')
        if clean.startswith('(') != clean.endswith(')'):
            raise TsmFinancialError('tsm_numeric_sign')
        value=Decimal(clean.replace(',','').replace('(','-').replace(')',''))
        if i%2==0:
            if abs(value)>Decimal('1e13'):raise TsmFinancialError('tsm_numeric_bound')
            result.append(int(value)*1000)
        elif abs(value)>1000:
            raise TsmFinancialError('tsm_percentage_bound')
    return result


def parse(raw, *, source_key):
    source=SOURCES[source_key]
    if not raw or len(raw)>MAX_BYTES:
        raise TsmFinancialError('tsm_document_size')
    from lxml import html
    try:tree=html.fromstring(raw)
    except (ValueError, html.etree.ParserError) as exc:
        raise TsmFinancialError('tsm_invalid_html') from exc
    for node in tree.xpath('//script|//style'):node.drop_tree()
    candidates={'income':[],'balance':[]}
    for table in tree.xpath('//table[not(.//table)]'):
        rows=[[' '.join(c.text_content().split()) for c in row.xpath('./td|./th') if c.text_content().strip()]
              for row in table.xpath('.//tr')]
        rows=[r for r in rows if r]
        labels={_label(r[0]) for r in rows}
        for kind,marker in [('income','NET REVENUE'),('balance','Total current assets')]:
            if marker in labels:
                prefix=' '.join(' '.join(table.xpath('preceding::text()')).split())
                candidates[kind].append((rows,prefix))
    income_title='Taiwan Semiconductor Manufacturing Company Limited and Subsidiaries CONSOLIDATED STATEMENTS OF COMPREHENSIVE INCOME (In Thousands of New Taiwan Dollars, Except Earnings Per Share)'
    balance_title='Taiwan Semiconductor Manufacturing Company Limited and Subsidiaries CONSOLIDATED BALANCE SHEETS (In Thousands of New Taiwan Dollars)'
    if any(len(v)!=1 for v in candidates.values()):
        raise TsmFinancialError('tsm_missing_or_ambiguous_statement')
    income,prefix=candidates['income'][0];balance,bprefix=candidates['balance'][0]
    if not prefix.endswith(income_title) or not bprefix.endswith(balance_title):
        raise TsmFinancialError('tsm_statement_identity_or_units')
    year=source['year'];annual=source_key=='annual';count=2 if annual else 4
    expected_income=([[str(year),str(year-1)]] if annual else
        [['For the Three Months Ended June 30','For the Six Months Ended June 30'],[str(year),str(year-1)]*2])
    expected_income += [['Amount','%']*count]
    expected_balance=([f'December 31, {year}',f'December 31, {year-1}'] if annual else
        [f'June 30, {year}',f'December 31, {year-1}',f'June 30, {year-1}'])
    if income[:len(expected_income)]!=expected_income or balance[:2]!=[expected_balance,['Amount','%']*len(expected_balance)]:
        raise TsmFinancialError('tsm_period_column_order')
    def row_values(rows,label,n):
        found=[r[1:] for r in rows if _label(r[0])==label]
        if len(found)!=1:raise TsmFinancialError('tsm_missing_or_duplicate_row:'+label)
        return _amounts(found[0],n)
    values={key:row_values(income,label,count) for key,label in INCOME.items()}
    for i in range(count):
        v=lambda key:values[key][i]
        if (v('revenue')-v('cost')!=v('gross_profit') or
            v('gross_profit')-v('expenses')+v('other_operating')!=v('operating_income') or
            v('operating_income')+v('nonoperating')!=v('pretax') or
            v('pretax')-v('tax')!=v('consolidated_income')):
            raise TsmFinancialError('tsm_income_reconciliation')
    instant={key:row_values(balance,label,len(expected_balance)) for key,label in
        [('current_assets','Total current assets'),('current_liabilities','Total current liabilities')]}
    try: receipt = verified_original(raw, source)
    except ValueError as exc: raise TsmFinancialError('tsm_reviewed_original_changed') from exc
    digest = receipt['source_sha256']
    periods=([(f'{y}-01-01',f'{y}-12-31') for y in (year,year-1)] if annual else
        [(f'{y}-{month}-01',f'{y}-06-30') for month in ('04','01') for y in (year,year-1)])
    facts={key:[{'start':start,'end':end,'value':numbers[i],'unit':'TWD',
        'filed':source['filed'],'accession':source['accession'],'source_url':url_for(source),
        'source_sha256':digest,'source_kind':'reviewed_sec_html_table','label':INCOME[key]}
        for i,(start,end) in enumerate(periods)] for key,numbers in values.items()}
    instant_dates=[str(datetime.strptime(s,'%B %d, %Y').date()) for s in expected_balance]
    for key,numbers in instant.items():
        facts[key]=[{'start':None,'end':end,'value':numbers[i],'unit':'TWD','filed':source['filed'],
            'accession':source['accession'],'source_url':url_for(source),'source_sha256':digest,
            'source_kind':'reviewed_sec_html_table','label':key} for i,end in enumerate(instant_dates)]
    return {'facts':facts,'_original':raw,'evidence':{**receipt,'url':url_for(source),'sha256':digest,'accession':source['accession'],
        'filed':source['filed'],'accounting_basis':'Taiwan-IFRS','currency':'TWD','scale':1000}}


def acquire(company_raw, *, symbol, cik, observed_at, client):
    company=json.loads(company_raw);validate_company(company,symbol=symbol,cik=cik,observed_at=observed_at)
    return {'tsm_documents':{key:parse(client.get(url_for(source)),source_key=key) for key,source in SOURCES.items()}}


def project(facts_raw, company_raw, *, symbol, cik, observed_at, supplement):
    company=json.loads(company_raw);validate_company(company,symbol=symbol,cik=cik,observed_at=observed_at)
    if str(json.loads(facts_raw).get('cik','')).zfill(10)!=CIK:
        raise TsmFinancialError('tsm_companyfacts_identity')
    if supplement and supplement.get('tsm_error'):
        raise TsmFinancialError(supplement['tsm_error'])
    documents=supplement.get('tsm_documents',{}) if supplement else {}
    if set(documents)!=set(SOURCES):raise TsmFinancialError('tsm_incomplete_source_bundle')
    series={};sources=[]
    for key,document in documents.items():
        proof=document['evidence'];source=SOURCES[key]
        try: receipt = verified_original(document.get('_original',b''), source)
        except ValueError as exc: raise TsmFinancialError('tsm_source_bundle_identity') from exc
        if proof!={**receipt,'url':url_for(source),'sha256':receipt['source_sha256'],'accession':source['accession'],
            'filed':source['filed'],'accounting_basis':'Taiwan-IFRS','currency':'TWD','scale':1000}:
            raise TsmFinancialError('tsm_source_bundle_identity')
        sources.append(proof)
        for metric,rows in document['facts'].items():
            for row in rows:
                period=(date.fromisoformat(row['start']) if row['start'] else None,date.fromisoformat(row['end']))
                existing=series.setdefault(metric,{}).get(period)
                if existing and existing['value']!=row['value']:
                    raise TsmFinancialError('tsm_comparative_restatement_requires_review')
                if existing is None or row['filed']>existing['filed']:series[metric][period]=row
    from app.services.sec_fundamentals import _ttm
    current={k:_ttm(series[k],END) for k in INCOME};prior={k:_ttm(series[k],date(2025,6,30)) for k in INCOME}
    if any(v is None for group in (current,prior) for v in group.values()):
        raise TsmFinancialError('tsm_incomplete_trailing_period')
    revenue=current['revenue']['value'];old_revenue=prior['revenue']['value']
    if min(revenue,old_revenue)<=0:raise TsmFinancialError('tsm_nonpositive_revenue')
    values={'symbol':'TSM','provider':'sec_edgar','company_name':company['name'],
        'exchange':company['exchanges'][company['tickers'].index('TSM')] if len(company.get('exchanges',[]))==len(company['tickers']) else None,
        'fetched_at':observed_at,'period_date':END,'status':'ok','error':None,
        'gross_margin':current['gross_profit']['value']/revenue*100,
        'operating_margin':current['operating_income']['value']/revenue*100,
        'revenue_growth':(revenue/old_revenue-1)*100}
    values['operating_margin_expansion']=values['operating_margin']-prior['operating_income']['value']/old_revenue*100
    assets=series['current_assets'][None,END];liabilities=series['current_liabilities'][None,END]
    if liabilities['value']>0:values['current_ratio']=assets['value']/liabilities['value']
    evidence={'methodology':VERSION,'cik':CIK,'reporting_currency':'TWD','accounting_basis':'Taiwan-IFRS',
      'available_by':str(available_day(observed_at)),'availability_basis':'filed date, available after completed New York filing day',
      'company':{'sha256':hashlib.sha256(company_raw).hexdigest()},'facts':{'sha256':hashlib.sha256(facts_raw).hexdigest()},
      'period_start':current['revenue']['start'],'period_end':str(END),'current':current,'prior':prior,
      'instant':{'current_assets':assets,'current_liabilities':liabilities},'sources':sources,
      'calculations':{'gross_margin':'TTM gross profit / revenue * 100','operating_margin':'TTM operating income / revenue * 100',
        'revenue_growth':'(TTM revenue / prior TTM revenue - 1) * 100',
        'operating_margin_expansion':'TTM operating margin minus prior TTM operating margin, percentage points',
        'current_ratio':'current assets / current liabilities'},
      'limitations':['Ratios only: TWD amounts are not USD amounts. No ADR EPS or currency conversion.',
        'Taiwan-IFRS consolidated annual and interim reports; not mixed with IASB 20-F statements.',
        'Parent earnings, ROE, cash flow and EBITDA are not derived by this reader.']}
    values['source_evidence_json']=json.dumps(evidence,sort_keys=True)
    return values
