"""Actual reported table cells, condensed without changing financial values."""
from copy import deepcopy
from datetime import datetime,timezone
from pathlib import Path
from html import escape
import hashlib,json
import pytest
from app.services import sec_tsm_financials as tsm

FIXTURE=json.loads((Path(__file__).parent/'fixtures/tsm_reviewed_tables.json').read_text())
NOW=datetime(2026,10,14,19,30,tzinfo=timezone.utc)

def company():
    rows=[(s['accession'],'6-K',s['filed'],s['end'],s['cover']) for s in tsm.SOURCES.values()]
    return {'cik':1046179,'name':'TAIWAN SEMICONDUCTOR MANUFACTURING CO LTD','tickers':['TSM'],'exchanges':['NYSE'],
        'filings':{'recent':dict(zip(('accessionNumber','form','filingDate','reportDate','primaryDocument'),map(list,zip(*rows))))}}

def render(key,tables=None):
    tables=tables or FIXTURE[key]['tables']
    return ('<html><body>'+''.join('<p>'+escape(table['title'])+'</p><table>'+''.join('<tr>'+''.join('<td>'+escape(c)+'</td>' for c in row)+'</tr>' for row in table['rows'])+'</table>' for table in tables.values())+'</body></html>').encode()

def trust_fixture(monkeypatch,key,raw):
    profile=dict(tsm.SOURCES[key],sha256=hashlib.sha256(raw).hexdigest());monkeypatch.setitem(tsm.SOURCES,key,profile)

def bundle(monkeypatch):
    docs={}
    for key in tsm.SOURCES:
        raw=render(key);trust_fixture(monkeypatch,key,raw);docs[key]=tsm.parse(raw,source_key=key)
    return {'tsm_documents':docs}

def project(supplement,**kwargs):
    args=dict(symbol='TSM',cik=tsm.CIK,observed_at=NOW,supplement=supplement);args.update(kwargs)
    return tsm.project(json.dumps({'cik':1046179}).encode(),json.dumps(company()).encode(),**args)

def test_original_manifest_and_same_basis_ttm(monkeypatch):
    for key,source in tsm.SOURCES.items():assert source['sha256']==FIXTURE[key]['original_sha256']
    data=bundle(monkeypatch);before=deepcopy(data);result=project(data)
    assert result==project(data) and data==before
    e=json.loads(result['source_evidence_json'])
    assert e['reporting_currency']=='TWD' and e['accounting_basis']=='Taiwan-IFRS'
    assert e['current']['revenue']['value']==4_440_492_429_000
    assert e['current']['operating_income']['value']==2_491_156_024_000
    assert e['prior']['revenue']['value']==3_401_198_854_000
    assert e['prior']['operating_income']['value']==1_656_983_648_000
    assert result['operating_margin_expansion']==pytest.approx(7.3832605136448)
    assert result['current_ratio']==pytest.approx(2.457635139531409)
    assert not set(result)&{'free_cash_flow','roe','eps_ttm','price_to_sales','trailing_pe','net_debt_to_ebitda'}

@pytest.mark.parametrize('key',list(tsm.SOURCES))
def test_changed_original_requires_review(key):
    with pytest.raises(tsm.TsmFinancialError,match='original_changed'):tsm.parse(render(key),source_key=key)

@pytest.mark.parametrize('change',['wrong_currency','wrong_issuer','parent_only','wrong_year','swapped_columns','duplicate_statement','missing_operating','duplicate_revenue','alter_cost','missing_amount'])
def test_bad_table_never_becomes_a_fact(monkeypatch,change):
    tables=deepcopy(FIXTURE['current']['tables']);rows=tables['income']['rows']
    if change=='wrong_currency':tables['income']['title']=tables['income']['title'].replace('New Taiwan Dollars','U.S. Dollars')
    elif change=='wrong_issuer':tables['income']['title']=tables['income']['title'].replace('Taiwan Semiconductor','Other Semiconductor')
    elif change=='parent_only':tables['income']['title']=tables['income']['title'].replace('and Subsidiaries','Parent Company Only')
    elif change=='duplicate_statement':tables['duplicate']=deepcopy(tables['income'])
    elif change=='wrong_year':
        for row in rows:
            for i,c in enumerate(row):
                if c=='2026':row[i]='2027'
    elif change=='swapped_columns':
        for row in rows:
            if [c for c in row if c]==['2026','2025','2026','2025']:
                indexes=[i for i,c in enumerate(row) if c];row[indexes[0]],row[indexes[1]]=row[indexes[1]],row[indexes[0]]
    elif change=='missing_operating':rows[:]=[r for r in rows if not any(c.startswith('INCOME FROM OPERATIONS') for c in r)]
    elif change=='duplicate_revenue':rows.append(deepcopy(next(r for r in rows if any(c.startswith('NET REVENUE') for c in r))))
    elif change=='alter_cost':
        row=next(r for r in rows if any(c.startswith('COST OF REVENUE') for c in r));row[row.index('410,069,555')]='410,069,556'
    elif change=='missing_amount':
        row=next(r for r in rows if any(c.startswith('NET REVENUE') for c in r));row[row.index('$1,270,380,250')]='-'
    raw=render('current',tables);trust_fixture(monkeypatch,'current',raw)
    with pytest.raises(tsm.TsmFinancialError):tsm.parse(raw,source_key='current')

@pytest.mark.parametrize('cells,count',[( ['1','100','2'],2),(['NaN','100'],1),(['1e9','100'],1),(['-','100'],1),(['12,34','100'],1),(['(10','100'],1),(['1','99999'],1)])
def test_numeric_ambiguity_is_held(cells,count):
    with pytest.raises(tsm.TsmFinancialError):tsm._amounts(cells,count)

def test_explicit_zero_and_split_parentheses():
    assert tsm._amounts(['$','0','-','(','3,000',')','(2)'],2)==[0,-3000000]

@pytest.mark.parametrize('change',['wrong_cik','wrong_ticker','missing_source','new_report','amendment','filing_today','stale','naive','duplicate_source'])
def test_directory_identity_and_availability(change):
    data=company();now=NOW;recent=data['filings']['recent']
    if change=='wrong_cik':data['cik']=320193
    elif change=='wrong_ticker':data['tickers']=['OTHER']
    elif change=='missing_source':
        for a in recent.values():a.pop()
    elif change=='duplicate_source':
        for a in recent.values():a.append(a[0])
    elif change in ('new_report','amendment'):
        row=['0001046179-26-999999','6-K','2026-10-12','2026-09-30','tsm-fsx20261012x6k.htm']
        if change=='amendment':row[1]='6-K/A';row[3]='2026-06-30'
        for key,value in zip(recent,row):recent[key].append(value)
    elif change=='filing_today':now=datetime(2026,8,14,20,tzinfo=timezone.utc)
    elif change=='stale':now=datetime(2027,2,1,tzinfo=timezone.utc)
    elif change=='naive':now=NOW.replace(tzinfo=None)
    with pytest.raises(tsm.TsmFinancialError):tsm.validate_company(data,symbol='TSM',cik=tsm.CIK,observed_at=now)

def test_comparative_restatement_is_not_silently_spliced(monkeypatch):
    data=bundle(monkeypatch)
    data['tsm_documents']['prior']['facts']['revenue'][2]['value']+=1000
    with pytest.raises(tsm.TsmFinancialError,match='restatement'):project(data)

@pytest.mark.parametrize('change',['missing_document','currency','basis','hash'])
def test_bundle_source_identity(monkeypatch,change):
    data=bundle(monkeypatch)
    if change=='missing_document':data['tsm_documents'].pop('prior')
    else:data['tsm_documents']['prior']['evidence'][{'currency':'currency','basis':'accounting_basis','hash':'sha256'}[change]]='wrong'
    with pytest.raises(tsm.TsmFinancialError):project(data)

def test_integrated_sec_projector_and_version(monkeypatch):
    from app.services import sec_fundamentals as sec,sec_fundamentals_preparation as prep
    data=bundle(monkeypatch)
    assert sec.project_fundamentals(json.dumps({'cik':1046179}).encode(),json.dumps(company()).encode(),symbol='TSM',cik=tsm.CIK,observed_at=NOW,filing_supplement=data)==project(data)
    assert prep.version_for_symbol('TSM')==prep.VERSION+'_'+tsm.VERSION
    assert prep.version_for_symbol('AAPL')==prep.VERSION

def test_unreviewed_new_report_is_held_before_any_fetch(monkeypatch):
    from app.services import sec_inline_financials as inline
    class NoFetch:
        def get(self,url):raise AssertionError('Must hold before transport')
    data=company();data['filings']['recent']['reportDate'][0]='2026-09-30'
    result=inline.acquire(b'{}',json.dumps(data).encode(),symbol='TSM',cik=tsm.CIK,observed_at=NOW,client=NoFetch())
    assert result['tsm_documents']=={} and result['tsm_error']


@pytest.mark.parametrize('currency',['TWD','USD'])
def test_market_cap_never_uses_unconverted_foreign_amounts(monkeypatch,currency):
    from app.services import sec_fundamentals as sec
    from app.clients.direct_sources import DirectSourceClient
    from app.clients import massive_stocks
    from types import SimpleNamespace
    data=bundle(monkeypatch);financial=project(data)
    evidence=json.loads(financial['source_evidence_json']);evidence['reporting_currency']=currency
    # Synthetic absolute amounts isolate currency-sensitive formulas. Common
    # income now requires its own verified source; parent income is not a proxy.
    from app.services import sec_common_income
    monkeypatch.setattr(sec_common_income,'resolve',lambda *a,**k:
        {'status':'available','point':{'value':100}})
    evidence['current']['net_income']={'value':100}
    financial['free_cash_flow']=50
    financial['source_evidence_json']=json.dumps(evidence)
    monkeypatch.setattr(sec,'project_fundamentals',lambda *a,**k:deepcopy(financial))
    monkeypatch.setattr(sec,'company_directory',lambda day:{'TSM':{'cik':tsm.CIK}})
    monkeypatch.setattr(DirectSourceClient,'get',lambda self,url:json.dumps(company() if 'submissions' in url else {'cik':1046179}).encode())
    from app.services import sec_inline_financials as inline
    monkeypatch.setattr(inline,'acquire',lambda *a,**k:data)
    class Clock(datetime):
        @classmethod
        def now(cls,tz=None):return NOW
    monkeypatch.setattr(sec,'datetime',Clock)
    monkeypatch.setattr(massive_stocks,'selected',lambda:True)
    monkeypatch.setattr(massive_stocks,'MassiveStocksClient',lambda:SimpleNamespace(details=lambda symbol:{'cik':tsm.CIK,'market_cap':1000},bars=lambda *a:[]))
    result=sec.fetch_fundamentals('TSM')
    assert result['market_cap']==1000
    if currency=='TWD':assert not set(result)&{'price_to_sales','trailing_pe','earnings_yield','fcf_yield'}
    else:
        assert result['trailing_pe']==10 and result['earnings_yield']==10 and result['fcf_yield']==5
        assert result['price_to_sales']>0
