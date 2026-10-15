from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import pytest
from app.services import sec_cnq_financials as cnq, sec_fundamentals as sec, sec_inline_financials as inline

ROOT = Path(__file__).parent/'fixtures/sec_cnq'
META = json.loads((ROOT/'observations.json').read_text(encoding='utf-8'))
NOW = datetime(2026,10,15,18,tzinfo=timezone.utc)
FIELDS = {'revenue_growth','net_margin','roe','current_ratio'}


def bundle(monkeypatch):
    docs = {k:(ROOT/(k+'.htm')).read_bytes() for k in cnq.SOURCES}
    for k,raw in docs.items():
        assert cnq.SOURCES[k]['sha256'] == META[k]['original_sha256']
        monkeypatch.setitem(cnq.SOURCES,k,{**cnq.SOURCES[k],'sha256':hashlib.sha256(raw).hexdigest()})
    return {'cnq_originals':docs}


def project(data,company=None,**kwargs):
    options = dict(symbol='CNQ',cik=cnq.CIK,observed_at=NOW,supplement=data);options.update(kwargs)
    return cnq.project(b'{"cik":1017413}',json.dumps(company or META['company']).encode(),**options)


def test_all_four_ratios_original_provenance_and_exact_repeat(monkeypatch):
    data=bundle(monkeypatch);before=deepcopy(data);r=project(data)
    assert r==project(data) and data==before and set(r)&FIELDS==FIELDS
    revenue=38762+25551-19637;prior=35656+19637-17295;income=10820+5851-4917
    assert r['revenue_growth']==pytest.approx((revenue/prior-1)*100)
    assert r['net_margin']==pytest.approx(income/revenue*100)
    assert r['roe']==pytest.approx(income/((46796+41298)/2)*100)
    assert r['current_ratio']==pytest.approx(11325/11317)
    assert not set(r)&{'operating_margin','operating_margin_expansion','gross_margin','free_cash_flow','fcf_margin','fcf_growth','eps_ttm','net_debt_to_ebitda','ev_to_ebitda'}
    e=json.loads(r['source_evidence_json'])
    assert e['reporting_currency']=='CAD' and e['accounting_basis']=='IFRS' and len(e['sources'])==3
    assert e['reviewed_nonfinancial_filing']==cnq.REVIEWED_DIVIDEND
    for point in [*e['current'].values(),e['prior_revenue']]:
        assert len(point['inputs'])==3 and all(p['unit']=='CAD' and p['source_sha256'] for p in point['inputs'])
    assert any('royalties' in v for v in e['limitations'])
    assert any('IFRS 9' in v for v in e['limitations'])


@pytest.mark.parametrize('key',['current','prior','annual'])
def test_changed_originals_are_held(key):
    with pytest.raises(cnq.CnqFinancialError,match='original_changed'):
        cnq.parse((ROOT/(key+'.htm')).read_bytes(),source_key=key)


@pytest.mark.parametrize('change',['currency','title','dates','year','duplicate','royalty_sign','tax','equity','subtotal','blank','ambiguous_statement'])
def test_statement_defects_held_after_test_hash_review(monkeypatch,change):
    raw=(ROOT/'current.htm').read_bytes()
    if change=='currency':raw=raw.replace(b'Canadian dollars',b'US dollars')
    elif change=='title':raw=raw.replace(b'CONSOLIDATED STATEMENTS OF EARNINGS',b'ADJUSTED EARNINGS')
    elif change=='dates':raw=raw.replace(b'Jun 302026',b'Sep 302026')
    elif change=='year':raw=raw.replace(b'Jun 302025',b'Jun 302024')
    elif change=='duplicate':raw=raw.replace(b'</table>',b'<tr><td>Revenue</td><td></td><td>14,741</td><td>8,698</td><td>25,551</td><td>19,637</td></tr></table>')
    elif change=='royalty_sign':raw=raw.replace(b'(4,067)',b'4,067')
    elif change=='tax':raw=raw.replace(b'>139<',b'>140<')
    elif change=='equity':raw=raw.replace(b'>34,684<',b'>34,685<')
    elif change=='subtotal':raw=raw.replace(b'>11,325<',b'>11,326<')
    elif change=='blank':raw=raw.replace(b'>25,551<',b'><')
    elif change=='ambiguous_statement':raw=raw.replace(b'</body>',raw+b'</body>')
    assert raw!=(ROOT/'current.htm').read_bytes()
    monkeypatch.setitem(cnq.SOURCES,'current',{**cnq.SOURCES['current'],'sha256':hashlib.sha256(raw).hexdigest()})
    with pytest.raises(cnq.CnqFinancialError):cnq.parse(raw,source_key='current')


@pytest.mark.parametrize('value',['','—','NaN','1e9','12,34','1.5','(100','1,000 CAD','0 note'])
def test_missing_and_ambiguous_values_never_become_zero(value):
    with pytest.raises(cnq.CnqFinancialError):cnq.amount(value)


def test_parentheses_and_zero_are_explicit():
    assert cnq.amount('(1,234)')==-1234000000 and cnq.amount('0')==0


@pytest.mark.parametrize('change',['wrong_cik','wrong_ticker','missing_source','duplicate_source','new_report','amendment','changed_dividend','future','stale','naive','arrays'])
def test_directory_identity_and_unreviewed_rollover_hold_before_transport(change):
    c=deepcopy(META['company']);r=c['filings']['recent'];now=NOW;cik=cnq.CIK
    if change=='wrong_cik':cik='1'
    elif change=='wrong_ticker':c['tickers']=['OTHER']
    elif change=='missing_source':
        for k in r:r[k].pop()
    elif change=='duplicate_source':
        i=r['accessionNumber'].index(cnq.SOURCES['current']['accession'])
        for k in r:r[k].append(r[k][i])
    elif change in ('new_report','amendment'):
        row=dict(zip(('accessionNumber','form','filingDate','reportDate','primaryDocument'),('0001017413-26-999999','6-K/A' if change=='amendment' else '6-K','2026-10-14','2026-09-30','new.htm')))
        for k in r:r[k].append(row.get(k,''))
    elif change=='changed_dividend':r['primaryDocument'][r['accessionNumber'].index(cnq.REVIEWED_DIVIDEND['accessionNumber'])]='new.htm'
    elif change=='future':now=NOW.replace(month=8,day=6)
    elif change=='stale':now=NOW.replace(year=2027)
    elif change=='naive':now=NOW.replace(tzinfo=None)
    elif change=='arrays':r['form'].pop()
    class Client:
        def get(self,url):raise AssertionError('Transport reached before identity check')
    with pytest.raises(cnq.CnqFinancialError):cnq.acquire(json.dumps(c).encode(),symbol='CNQ',cik=cik,observed_at=now,client=Client())


def test_changed_report_does_not_fall_back_to_annual_companyfacts():
    c=deepcopy(META['company']);r=c['filings']['recent']
    row=dict(zip(('accessionNumber','form','filingDate','reportDate','primaryDocument'),('0001017413-26-999999','6-K','2026-10-14','2026-09-30','new.htm')))
    for k in r:r[k].append(row.get(k,''))
    class Client:
        def get(self,url):raise AssertionError('Unexpected transport')
    args=dict(symbol='CNQ',cik=cnq.CIK,observed_at=NOW)
    supplement=inline.acquire(b'{"cik":1017413}',json.dumps(c).encode(),client=Client(),**args)
    assert supplement=={'cnq_error':'cnq_unreviewed_new_or_amended_report'}
    with pytest.raises(cnq.CnqFinancialError):sec.project_fundamentals(b'{"cik":1017413}',json.dumps(c).encode(),filing_supplement=supplement,**args)


def test_comparative_conflict_is_never_silently_reconciled(monkeypatch):
    data=bundle(monkeypatch);parse=cnq.parse
    def changed(raw,source_key):
        result=parse(raw,source_key=source_key)
        if source_key=='prior':result['facts']['revenue'][0]['value']+=1000000
        return result
    monkeypatch.setattr(cnq,'parse',changed)
    with pytest.raises(cnq.CnqFinancialError,match='comparative_conflict'):project(data)


def test_three_original_request_budget_and_selective_cache_version(monkeypatch):
    data=bundle(monkeypatch);urls={s['url']:data['cnq_originals'][k] for k,s in cnq.SOURCES.items()};calls=[]
    class Client:
        def get(self,url):calls.append(url);return urls[url]
    result=cnq.acquire(json.dumps(META['company']).encode(),symbol='CNQ',cik=cnq.CIK,observed_at=NOW,client=Client())
    assert result==data and len(calls)==3
    from app.services import sec_fundamentals_preparation as prep
    assert prep.version_for_symbol('CNQ')==prep.VERSION+'_'+cnq.VERSION
    assert prep.version_for_symbol('AAPL')==prep.VERSION


def test_usd_market_cap_never_consumes_cad_financial_amounts(monkeypatch):
    from app.clients.direct_sources import DirectSourceClient
    from app.clients.massive_stocks import MassiveStocksClient
    data=bundle(monkeypatch)
    class Clock(datetime):
        @classmethod
        def now(cls,tz=None):return NOW
    monkeypatch.setattr(sec,'datetime',Clock)
    monkeypatch.setattr(sec,'company_directory',lambda day:{'CNQ':{'cik':cnq.CIK}})
    monkeypatch.setattr(DirectSourceClient,'get',lambda self,url:b'{"cik":1017413}' if 'companyfacts' in url else json.dumps(META['company']).encode())
    monkeypatch.setattr(inline,'acquire',lambda *a,**k:data)
    monkeypatch.setenv('STOCK_PRICE_PROVIDER','massive');monkeypatch.setenv('MASSIVE_API_KEY','test-only')
    monkeypatch.setattr(MassiveStocksClient,'details',lambda *a:{'cik':cnq.CIK,'market_cap':10000000000})
    monkeypatch.setattr(MassiveStocksClient,'bars',lambda *a:[])
    result=sec.fetch_fundamentals('CNQ')
    assert result['market_cap']==10000000000 and set(result)&FIELDS==FIELDS
    assert not set(result)&{'price_to_sales','trailing_pe','earnings_yield','free_cash_flow','fcf_yield','eps_ttm'}
    assert json.loads(result['source_evidence_json'])['market_income']['reason']=='common_income_currency_not_usd'


def test_prepared_cache_persists_cad_provenance_and_repeat_without_fetch(monkeypatch):
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from app.models import InsightsSnapshot
    from app.services import sec_fundamentals_preparation as prep,sec_directory
    data=bundle(monkeypatch);calls=[];urls={s['url']:data['cnq_originals'][k] for k,s in cnq.SOURCES.items()}
    class Clock(datetime):
        @classmethod
        def now(cls,tz=None):return NOW
    class Client:
        def get(self,url):
            calls.append(url)
            if '/Archives/' in url:return urls[url]
            return b'{"cik":1017413}' if 'companyfacts' in url else json.dumps(META['company']).encode()
    engine=create_engine('sqlite:///:memory:');InsightsSnapshot.__table__.create(engine);factory=sessionmaker(bind=engine)
    monkeypatch.setenv('SEC_FUNDAMENTALS_WARMING_ENABLED','1');monkeypatch.setenv('FUNDAMENTALS_PROVIDER','fmp')
    monkeypatch.setattr(prep,'SessionLocal',factory);monkeypatch.setattr(prep,'datetime',Clock)
    monkeypatch.setattr(prep,'DirectSourceClient',Client);monkeypatch.setattr(sec_directory,'directory',lambda:{'CNQ':{'cik':cnq.CIK}})
    try:
        first=prep.prepare('CNQ');assert set(first['values'])==FIELDS and len(calls)==5
        assert first['calculation_evidence']['reporting_currency']=='CAD'
        assert prep.prepare('CNQ')==first and len(calls)==5
        with factory() as db:assert json.loads(db.get(InsightsSnapshot,'sec-current-fundamentals:CNQ:v1').payload_json)==first
    finally:engine.dispose()


def test_prior_period_amendment_before_annual_report_requires_review():
    c=deepcopy(META['company']);r=c['filings']['recent']
    row=dict(zip(('accessionNumber','form','filingDate','reportDate','primaryDocument'),('0001017413-25-999999','6-K/A','2025-10-14','2025-06-30','amendment.htm')))
    for k in r:r[k].append(row.get(k,''))
    with pytest.raises(cnq.CnqFinancialError,match='unreviewed_new_or_amended'):
        cnq.validate_company(c,symbol='CNQ',cik=cnq.CIK,observed_at=NOW)
