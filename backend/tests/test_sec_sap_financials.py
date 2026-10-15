from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import pytest
from app.services import sec_sap_financials as sap,sec_fundamentals as sec,sec_inline_financials as inline

ROOT=Path(__file__).parent/'fixtures/sec_sap'
META=json.loads((ROOT/'observations.json').read_text(encoding='utf-8'))
NOW=datetime(2026,10,15,18,tzinfo=timezone.utc)
FIELDS={'gross_margin','operating_margin','net_margin','revenue_growth','operating_margin_expansion','current_ratio','roe'}


def bundle(monkeypatch):
    docs={k:(ROOT/(k+'.htm')).read_bytes() for k in sap.SOURCES}
    for k,raw in docs.items():monkeypatch.setitem(sap.SOURCES,k,{**sap.SOURCES[k],'sha256':hashlib.sha256(raw).hexdigest()})
    return {'sap_originals':docs}


def project(data,company=None,**kwargs):
    options=dict(symbol='SAP',cik=sap.CIK,observed_at=NOW,supplement=data);options.update(kwargs)
    return sap.project(b'{"cik":1000184}',json.dumps(company or META['company']).encode(),**options)


def test_original_sources_and_all_seven_ratios_with_exact_repeat(monkeypatch):
    for k,s in sap.SOURCES.items():assert s['sha256']==META[k]['original_sha256']
    data=bundle(monkeypatch);before=deepcopy(data);r=project(data)
    assert r==project(data) and data==before and set(r)&FIELDS==FIELDS
    revenue=36800+19432-18040;prior=34176+18040-16329
    operating=9617+5383-4789;old_operating=4665+4789-434
    income=7161+4112-3477
    assert r['revenue_growth']==pytest.approx((revenue/prior-1)*100)
    assert r['gross_margin']==pytest.approx((26814+14201-13226)/revenue*100)
    assert r['operating_margin']==pytest.approx(operating/revenue*100)
    assert r['net_margin']==pytest.approx(income/revenue*100)
    assert r['operating_margin_expansion']==pytest.approx((operating/revenue-old_operating/prior)*100)
    assert r['current_ratio']==pytest.approx(22649/19734)
    assert r['roe']==pytest.approx(income/((44742+40998)/2)*100)
    assert not set(r)&{'free_cash_flow','fcf_margin','fcf_growth','eps_ttm','net_debt_to_ebitda','ev_to_ebitda'}
    e=json.loads(r['source_evidence_json']);assert e['reporting_currency']=='EUR' and e['accounting_basis']=='IFRS'
    assert len(e['sources'])==3 and len(e['unused_balance_revisions'])==2
    assert all(p['period']=='2024-12-31' and p['use']=='not used in any published calculation' for p in e['unused_balance_revisions'])
    for group in ('current','prior'):
        for point in e[group].values():
            assert len(point['inputs'])==3
            assert all(p['unit']=='EUR' and p['source_sha256'] for p in point['inputs'])
    assert any('Cash purchases combine' in s for s in e['limitations'])


@pytest.mark.parametrize('key',['current','prior','annual'])
def test_original_change_requires_review(key):
    with pytest.raises(sap.SapFinancialError,match='original_changed'):sap.parse((ROOT/(key+'.htm')).read_bytes(),source_key=key)


@pytest.mark.parametrize('change',['currency','title','period','year','missing','duplicate','sign','parent_reconciliation','balance_reconciliation','rounding','notes_as_amount','adjusted','duplicate_statement'])
def test_invalid_statement_rejected_after_test_hash_review(monkeypatch,change):
    raw=(ROOT/'current.htm').read_bytes()
    if change=='currency':raw=raw.replace('€ millions'.encode(),b'USD millions')
    elif change=='title':raw=raw.replace(b'SAP Group',b'Other Group')
    elif change=='period':raw=raw.replace(b'6/30/2026',b'9/30/2026')
    elif change=='year':raw=raw.replace('Q1\u2013Q2 2026'.encode(),'Q1\u2013Q2 2027'.encode())
    elif change=='missing':raw=raw.replace(b'Total revenue',b'Absent revenue')
    elif change=='duplicate':raw=raw.replace(b'</table>',b'<tr><td>Total revenue</td><td></td><td>19,432</td><td>18,040</td><td>8</td></tr></table>',1)
    elif change=='sign':raw=raw.replace('–5,232'.encode(),b'5,232')
    elif change=='parent_reconciliation':raw=raw.replace(b'>4,112<',b'>4,122<')
    elif change=='balance_reconciliation':raw=raw.replace(b'>44,742<',b'>44,752<')
    elif change=='rounding':raw=raw.replace(b'Due to rounding, numbers presented',b'No rounding basis presented')
    elif change=='notes_as_amount':raw=raw.replace(b'>19,432<',b'>(A.1)<')
    elif change=='adjusted':raw=raw.replace(b'(IFRS)',b'(non-IFRS)')
    elif change=='duplicate_statement':raw=raw.replace(b'</body>',raw+b'</body>')
    monkeypatch.setitem(sap.SOURCES,'current',{**sap.SOURCES['current'],'sha256':hashlib.sha256(raw).hexdigest()})
    with pytest.raises(sap.SapFinancialError):sap.parse(raw,source_key='current')


@pytest.mark.parametrize('value',['','—','NaN','1e9','12,34','1.5','(100)','1,000 EUR','0 note'])
def test_ambiguous_amount_is_never_zero(value):
    with pytest.raises(sap.SapFinancialError):sap.amount(value)


def test_rounding_interval_is_bounded_not_percentage_based():
    assert sap.amount('0')==0 and sap.amount('–1,234')==-1234000000
    assert sap._reconciles(4155000000,4112000000,44000000)
    assert not sap._reconciles(4154000000,4112000000,44000000)
    assert not sap._reconciles(1000000000000,999998000000,0)


@pytest.mark.parametrize('change',['wrong_cik','wrong_ticker','missing_source','duplicate_source','new_report','amendment','future','stale','naive','arrays'])
def test_directory_holds_before_source_requests(change):
    c=deepcopy(META['company']);r=c['filings']['recent'];now=NOW;cik=sap.CIK
    if change=='wrong_cik':cik='1'
    elif change=='wrong_ticker':c['tickers']=['OTHER']
    elif change=='missing_source':
        for k in r:r[k].pop()
    elif change=='duplicate_source':
        for k in r:r[k].append(r[k][0])
    elif change in ('new_report','amendment'):
        row=dict(zip(('accessionNumber','form','filingDate','reportDate','primaryDocument'),('0001000184-26-999999','6-K/A' if change=='amendment' else '6-K','2026-10-14','2026-09-30','new.htm')))
        for k in r:r[k].append(row.get(k,''))
    elif change=='future':now=NOW.replace(month=7,day=28)
    elif change=='stale':now=NOW.replace(year=2027)
    elif change=='naive':now=NOW.replace(tzinfo=None)
    elif change=='arrays':r['form'].pop()
    class Client:
        def get(self,url):raise AssertionError('Transport reached before identity checks')
    with pytest.raises(sap.SapFinancialError):sap.acquire(json.dumps(c).encode(),symbol='SAP',cik=cik,observed_at=now,client=Client())


def test_new_period_adapter_hold_does_not_fall_back_to_old_companyfacts():
    c=deepcopy(META['company']);r=c['filings']['recent']
    row=dict(zip(('accessionNumber','form','filingDate','reportDate','primaryDocument'),('0001000184-26-999999','6-K','2026-10-14','2026-10-14','new.htm')))
    for k in r:r[k].append(row.get(k,''))
    class Client:
        def get(self,url):raise AssertionError('Unexpected request')
    args=dict(symbol='SAP',cik=sap.CIK,observed_at=NOW)
    s=inline.acquire(b'{"cik":1000184}',json.dumps(c).encode(),client=Client(),**args)
    assert s=={'sap_error':'sap_newer_or_amended_report_requires_review'}
    with pytest.raises(sap.SapFinancialError):sec.project_fundamentals(b'{"cik":1000184}',json.dumps(c).encode(),filing_supplement=s,**args)


def test_conflict_in_consumed_comparative_period_is_a_hold(monkeypatch):
    data=bundle(monkeypatch);parse=sap.parse
    def changed(raw,source_key):
        result=parse(raw,source_key=source_key)
        if source_key=='prior':result['facts']['revenue'][0]['value']+=1000000
        return result
    monkeypatch.setattr(sap,'parse',changed)
    with pytest.raises(sap.SapFinancialError,match='consumed_comparative'):project(data)


def test_bounded_acquisition_and_cache_version(monkeypatch):
    data=bundle(monkeypatch);urls={sap.url_for(s):data['sap_originals'][k] for k,s in sap.SOURCES.items()};calls=[]
    class Client:
        def get(self,url):calls.append(url);return urls[url]
    result=sap.acquire(json.dumps(META['company']).encode(),symbol='SAP',cik=sap.CIK,observed_at=NOW,client=Client())
    assert result==data and len(calls)==3
    from app.services import sec_fundamentals_preparation as prep
    assert prep.version_for_symbol('SAP')==prep.VERSION+'_'+sap.VERSION
    assert prep.version_for_symbol('AAPL')==prep.VERSION


def test_usd_market_cap_never_consumes_eur_financial_amounts(monkeypatch):
    from app.clients.direct_sources import DirectSourceClient
    from app.clients.massive_stocks import MassiveStocksClient
    data=bundle(monkeypatch)
    class Clock(datetime):
        @classmethod
        def now(cls,tz=None):return NOW
    monkeypatch.setattr(sec,'datetime',Clock)
    monkeypatch.setattr(sec,'company_directory',lambda day:{'SAP':{'cik':sap.CIK}})
    monkeypatch.setattr(DirectSourceClient,'get',lambda self,url:b'{"cik":1000184}' if 'companyfacts' in url else json.dumps(META['company']).encode())
    monkeypatch.setattr(inline,'acquire',lambda *a,**k:data)
    monkeypatch.setenv('STOCK_PRICE_PROVIDER','massive');monkeypatch.setenv('MASSIVE_API_KEY','test-only')
    monkeypatch.setattr(MassiveStocksClient,'details',lambda *a:{'cik':sap.CIK,'market_cap':10000000000})
    monkeypatch.setattr(MassiveStocksClient,'bars',lambda *a:[])
    result=sec.fetch_fundamentals('SAP')
    assert result['market_cap']==10000000000 and set(result)&FIELDS==FIELDS
    assert not set(result)&{'price_to_sales','trailing_pe','earnings_yield','free_cash_flow','fcf_yield','eps_ttm'}
    assert json.loads(result['source_evidence_json'])['market_income']['reason']=='common_income_currency_not_usd'


def test_prepared_cache_persists_eur_provenance_and_repeat_without_fetch(monkeypatch):
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from app.models import InsightsSnapshot
    from app.services import sec_fundamentals_preparation as prep,sec_directory
    data=bundle(monkeypatch);calls=[];urls={sap.url_for(s):data['sap_originals'][k] for k,s in sap.SOURCES.items()}
    class Clock(datetime):
        @classmethod
        def now(cls,tz=None):return NOW
    class Client:
        def get(self,url):
            calls.append(url)
            if '/Archives/' in url:return urls[url]
            return b'{"cik":1000184}' if 'companyfacts' in url else json.dumps(META['company']).encode()
    engine=create_engine('sqlite:///:memory:');InsightsSnapshot.__table__.create(engine);factory=sessionmaker(bind=engine)
    monkeypatch.setenv('SEC_FUNDAMENTALS_WARMING_ENABLED','1');monkeypatch.setenv('FUNDAMENTALS_PROVIDER','fmp')
    monkeypatch.setattr(prep,'SessionLocal',factory);monkeypatch.setattr(prep,'datetime',Clock)
    monkeypatch.setattr(prep,'DirectSourceClient',Client);monkeypatch.setattr(sec_directory,'directory',lambda:{'SAP':{'cik':sap.CIK}})
    try:
        first=prep.prepare('SAP');assert set(first['values'])==FIELDS and len(calls)==5
        assert first['calculation_evidence']['reporting_currency']=='EUR'
        assert prep.prepare('SAP')==first and len(calls)==5
        with factory() as db:assert json.loads(db.get(InsightsSnapshot,'sec-current-fundamentals:SAP:v1').payload_json)==first
    finally:engine.dispose()


def test_prior_period_amendment_before_annual_report_requires_review():
    c=deepcopy(META['company']);r=c['filings']['recent']
    row=dict(zip(('accessionNumber','form','filingDate','reportDate','primaryDocument'),('0000000000-25-999999','6-K/A','2025-10-14','2025-06-30','amendment.htm')))
    for k in r:r[k].append(row.get(k,''))
    with pytest.raises(sap.SapFinancialError,match='amended_report_requires_review'):
        sap.validate_company(c,symbol='SAP',cik=sap.CIK,observed_at=NOW)
