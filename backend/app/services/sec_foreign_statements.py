"""Reviewed foreign annual statements in their reporting currency.

Annual amounts never enter USD core caches, TTM figures or ADR EPS.
"""
from datetime import timedelta
import hashlib
import json
import re
from zoneinfo import ZoneInfo
from app.clients.direct_sources import DirectSourceError
from app.services import sec_fundamentals as sec, sec_inline_financials as inline

VERSION = 'sec_foreign_annual_v1'
IFRS = {
    'revenue': ('Revenue', 'RevenueFromContractsWithCustomers'),
    'gross_profit': ('GrossProfit',),
    'operating_income': ('ProfitLossFromOperatingActivities',),
    'net_income': ('ProfitLoss',),
    'operating_cash_flow': ('CashFlowsFromUsedInOperatingActivities',),
    'capex': ('PurchaseOfPropertyPlantAndEquipmentClassifiedAsInvestingActivities',),
}
# Reviewed reporting currencies, never listing currencies or convenience USD.
PROFILES = {
    'BULL': {'cik':'0001866364','currency':'USD','namespace':'us-gaap',
        'proof_url':'https://www.sec.gov/Archives/edgar/data/1866364/000121390026041653/ea0283691-20f_webull.htm',
        'proof_sha256':'9b64a0c7b4ee3a72cd8fb4acd3bb1e756eb0e238999af201fa45671b07c66890',
        'cash_flow_basis':'Operating cash flow includes customer account movements. Free cash flow subtracts reported purchases of property, equipment and intangible assets; it is not issuer-adjusted cash generation.',
        'income_basis':'Net income is attributable to the company before preferred-share adjustments.'},
    'NVO': {'cik':'0000353278','currency':'DKK','namespace':'ifrs-full',
        'proof_url':'https://www.sec.gov/Archives/edgar/data/353278/000035327826000012/nvo-20251231_d2.htm',
        'proof_sha256':'5f37750bbdf98737cdd06f5d71e9db4252b9c41493a59c32e2655e2a7407869e'},
    'TSM': {'cik':'0001046179','currency':'TWD','namespace':'ifrs-full',
        'proof_url':'https://www.sec.gov/Archives/edgar/data/1046179/000162828026025362/tsm-20251231.htm',
        'proof_sha256':'1971f40287b9b085f964e3acd60567fc35b792641ce5431d5fe5a65cae858bba'},
    'BABA': {'cik':'0001577552','currency':'CNY','namespace':'us-gaap',
        'proof_url':'https://www.sec.gov/Archives/edgar/data/1577552/000119312526231755/baba-20260331.htm',
        'proof_sha256':'16813cd58a7c7d42ea53c45b1db0f5c9a792fa1843ea5d0acd260bd6d6c59ad1'},
}
FORMS = {'20-F','20-F/A'}


def profile(symbol, cik):
    value = PROFILES.get(symbol)
    if value is None:
        return None
    if str(cik).zfill(10) != value['cik']:
        raise DirectSourceError('foreign_statement_cik_mismatch')
    return {**value, 'tags': IFRS if value['namespace']=='ifrs-full' else {k:sec.TAGS[k] for k in IFRS}}


def records(data, basis, available):
    return {name:sec._records(data,name,available,concepts=tags,unit=basis['currency'],
        namespace=basis['namespace'],forms=FORMS) for name,tags in basis['tags'].items()}


def validate(data, company, symbol, cik, observed_at):
    from app.services.sec_directory import _symbol_key
    if observed_at.tzinfo is None:
        raise DirectSourceError('foreign_observation_timezone')
    if (any(str(x.get('cik','')).zfill(10)!=str(cik).zfill(10) for x in [data,company])
        or not company.get('name') or sum(_symbol_key(s)==_symbol_key(symbol) for s in company.get('tickers',[]))!=1):
        raise DirectSourceError('foreign_company_identity')


def acquire(facts_raw, company_raw, *, symbol, cik, observed_at, client):
    basis=profile(symbol,cik)
    if basis is None:
        return None
    data,company=json.loads(facts_raw),json.loads(company_raw)
    validate(data,company,symbol,cik,observed_at)
    available=observed_at.astimezone(ZoneInfo('America/New_York')).date()-timedelta(days=1)
    revenue=records(data,basis,available)['revenue']
    ends=[end for (start,end),r in revenue.items() if r and start and 350<=(end-start).days+1<=380]
    recent=company.get('filings',{}).get('recent',{})
    keys=['accessionNumber','form','filingDate','reportDate','primaryDocument']
    arrays=[recent.get(k,[]) for k in keys]
    if len({len(a) for a in arrays})!=1:
        raise DirectSourceError('foreign_filing_directory_shape')
    candidates=[]
    for accn,form,filed,end,name in zip(*arrays):
        report,filing=sec._day(end),sec._day(filed)
        if form in FORMS and report and filing and report<=filing<=available and (not ends or report>max(ends)):
            candidates.append((end,filed,accn,form,name))
    if not candidates:
        return None
    latest=max(r[:2] for r in candidates)
    selected={r for r in candidates if r[:2]==latest}
    if len(selected)!=1:
        raise DirectSourceError('foreign_annual_filing_ambiguous')
    end,filed,accn,form,name=next(iter(selected))
    if not re.fullmatch(r'[0-9]{10}-[0-9]{2}-[0-9]{6}',accn) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,180}\.html?',name):
        raise DirectSourceError('foreign_filing_identity')
    url=f'https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accn.replace("-", "")}/{name}'
    return inline.parse(client.get(url),cik=str(cik).zfill(10),accession=accn,filed=filed,report_date=end,form=form,url=url,basis=basis)


def project(facts_raw, company_raw, *, symbol, cik, observed_at, supplement=None):
    from app.services.ticker_financials import _unavailable,_subsection
    basis=profile(symbol,cik)
    if basis is None:
        raise DirectSourceError('foreign_basis_not_reviewed')
    data,company=json.loads(facts_raw),json.loads(company_raw)
    validate(data,company,symbol,cik,observed_at)
    available=observed_at.astimezone(ZoneInfo('America/New_York')).date()-timedelta(days=1)
    if supplement and (supplement['evidence'].get('namespace')!=basis['namespace'] or supplement['evidence'].get('currency')!=basis['currency'] or supplement['evidence'].get('form') not in FORMS):
        raise DirectSourceError('foreign_supplement_basis')
    series=records(inline.merge(data,supplement,available_by=available),basis,available)
    inline.annotate(series,supplement)
    periods={end:{start for (start,e),r in series['revenue'].items() if r and start and e==end and 350<=(e-start).days+1<=380}
        for (start,end),r in series['revenue'].items() if r and start and 350<=(end-start).days+1<=380}
    rows=[];proof={}
    for end,starts in sorted(periods.items())[-6:]:
        if len(starts)!=1:
            continue
        start=next(iter(starts))
        source={k:v.get((start,end)) for k,v in series.items()}
        # A later partial restatement cannot reuse older same-period expenses.
        revenue=source['revenue']
        source={k:v if v and (v['accession'],v['filed'])==(revenue['accession'],revenue['filed']) else None for k,v in source.items()}
        value=lambda name:source[name]['value'] if source.get(name) else None
        sales=value('revenue');cash=value('operating_cash_flow');capex=value('capex')
        margin=lambda name:value(name)/sales*100 if value(name) is not None and sales>0 else None
        rows.append({'period':f'Year ended {end}','date':str(end),'periodStart':str(start),'currency':basis['currency'],
            'source':'sec_edgar','revenue':sales,'netIncome':value('net_income'),'eps':None,
            'grossMargin':margin('gross_profit'),'operatingMargin':margin('operating_income'),
            'operatingCashFlow':cash,'capex':capex,'freeCashFlow':cash-capex if cash is not None and capex is not None and capex>=0 else None})
        proof[str(end)]=source
    message=f"Reported annual financials in {basis['currency']}. Quarterly results, current TTM figures, earnings per share and estimates are unavailable."
    if basis.get('cash_flow_basis'):
        message += ' ' + basis['cash_flow_basis'] + ' ' + basis.get('income_basis', '')
    payload=_unavailable(symbol,message=message,reason='partial_foreign_annual_coverage')
    payload.update(source='sec_edgar',companyName=company['name'],annual=rows,quarterly=[],reportingCurrency=basis['currency'],
        reportingBasis=message,updatedAt=observed_at.isoformat(),status='partial' if rows else 'unavailable',unavailable=not bool(rows),
        preparationVersion=VERSION,sourceEvidence={'methodology':VERSION,'availableBy':str(available),'currency':basis['currency'],
            'taxonomy':basis['namespace'],'reviewedReportingBasis':{k:v for k,v in basis.items() if k!='tags'},
            'factsUrl':f'https://data.sec.gov/api/xbrl/companyfacts/CIK{str(cik).zfill(10)}.json',
            'factsSha256':hashlib.sha256(facts_raw).hexdigest(),'companySha256':hashlib.sha256(company_raw).hexdigest(),
            'periods':proof,'inlineFilingSupplement':supplement['evidence'] if supplement else None,
            'cashFlowBasis':basis.get('cash_flow_basis', 'Operating cash flow minus cash purchases of property, plant and equipment; not issuer-adjusted free cash flow')})
    if rows:
        payload['sections']['income']='partial'
        payload['subsections']['income']=_subsection(status='limited',reason_code='partial_foreign_annual_coverage',data={'annual':rows,'quarterly':[]})
        if any(r['operatingCashFlow'] is not None for r in rows):
            payload['sections']['cashFlow']='partial'
            payload['subsections']['cash_flow']=_subsection(status='limited',reason_code='partial_foreign_annual_coverage',data={'annual':rows,'quarterly':[]})
    return payload
