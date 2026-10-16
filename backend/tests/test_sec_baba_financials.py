from copy import deepcopy
from datetime import datetime,timezone
import gzip,hashlib,json
from pathlib import Path
import pytest
from lxml import html,etree
from app.services import sec_baba_financials as baba,sec_fundamentals as sec,sec_inline_financials as inline

ROOT=Path(__file__).parent/'fixtures/sec_baba'
NOW=datetime(2026,10,16,2,45,tzinfo=timezone.utc)
FIELDS={'gross_margin','operating_margin','net_margin','revenue_growth','operating_margin_expansion','current_ratio','roe'}


@pytest.fixture(scope='module')
def originals():
    return {k:gzip.decompress((ROOT/(k+'.htm.gz')).read_bytes()) for k in baba.SOURCES}


@pytest.fixture
def company():
    return json.loads(gzip.decompress((ROOT/'company.json.gz').read_bytes()))


def project(originals,company,**kwargs):
    options=dict(symbol='BABA',cik=baba.CIK,observed_at=NOW,supplement={'baba_originals':originals});options.update(kwargs)
    return baba.project(b'{"cik":1577552}',json.dumps(company).encode(),**options)


def test_complete_originals_reported_cny_parent_ratios_and_repeat(originals,company):
    manifest=json.loads((ROOT/'manifest.json').read_text())
    for k,raw in originals.items():
        assert hashlib.sha256(raw).hexdigest()==manifest[k]['original_sha256']==baba.SOURCES[k]['sha256']
        assert len(raw)==manifest[k]['original_bytes'] and manifest[k]['complete_original']
    before=deepcopy(originals);r=project(originals,company)
    assert r==project(originals,company) and originals==before and FIELDS<=r.keys()
    revenue=1023670+268953-247652;old_revenue=996347+247652-243236
    gross=(1023670-616136)+(268953-166096)-(247652-136429)
    operating=50150+15161-34988;old_operating=140905+34988-35989
    parent=103592+10614-40649
    assert r['gross_margin']==pytest.approx(gross/revenue*100)
    assert r['operating_margin']==pytest.approx(operating/revenue*100)
    assert r['net_margin']==pytest.approx(parent/revenue*100)
    assert r['revenue_growth']==pytest.approx((revenue/old_revenue-1)*100)
    assert r['operating_margin_expansion']==pytest.approx((operating/revenue-old_operating/old_revenue)*100)
    assert r['current_ratio']==pytest.approx(724854/532257)
    assert r['roe']==pytest.approx(parent/((1049038+1013672)/2)*100)
    assert not set(r)&{'free_cash_flow','fcf_margin','fcf_growth','eps_growth','eps_ttm','net_debt_to_ebitda','ev_to_ebitda'}
    e=json.loads(r['source_evidence_json'])
    assert e['reporting_currency']=='CNY' and e['accounting_basis']=='US GAAP' and len(e['sources'])==5
    assert set(e['calculations'])==FIELDS
    for group in ('current','prior'):
        for metric,point in e[group].items():
            assert len(point['inputs'])==3
            assert all(p['source_sha256'] and p['source_url'] and p['unit']=='CNY' for p in point['inputs'])
            if metric=='gross_profit':assert all(p['derived'] and len(p['inputs'])==2 for p in point['inputs'])


@pytest.mark.parametrize('key',list(baba.SOURCES))
def test_original_tamper_never_accepted(originals,company,key):
    with pytest.raises(baba.BabaFinancialError,match='original_changed'):
        project({**originals,key:originals[key]+b' '},company)


@pytest.mark.parametrize('change',['currency','period','title','missing','duplicate','sign','parent','mezzanine','duplicate_statement'])
def test_bad_current_statement_rejected_after_test_hash_review(originals,monkeypatch,change):
    tree=html.fromstring(originals['current'])
    table=next(t for t in tree.xpath('//table[not(.//table)]') if 'Income before income tax and share of results' in t.text_content() and 'Net income attributable to ordinary' in t.text_content())
    row=next(r for r in table.xpath('.//tr') if r.xpath('./td')[0].text_content().strip()=='Net income attributable to Alibaba Group Holding Limited')
    if change=='missing':row.getparent().remove(row)
    elif change=='duplicate':row.addnext(deepcopy(row))
    elif change=='duplicate_statement':table.addnext(deepcopy(table))
    elif change=='period':
        for cell in table.xpath('.//td'):
            if ' '.join(cell.text_content().split())=='Three months ended June 30,':
                cell.clear();cell.text='Three months ended September 30,'
    raw=etree.tostring(tree,method='html')
    replacements={'currency':(b'RMB',b'USD'),
        'title':(b'UNAUDITED CONSOLIDATED INCOME STATEMENTS',b'ADJUSTED CONSOLIDATED INCOME STATEMENTS'),
        'sign':(b'(166,096',b'166,096'),'parent':(b'10,614',b'10,634'),'mezzanine':(b'7,584',b'7,590')}
    if change in replacements:
        old,new=replacements[change];assert old in raw;raw=raw.replace(old,new)
    monkeypatch.setitem(baba.SOURCES,'current',{**baba.SOURCES['current'],'sha256':hashlib.sha256(raw).hexdigest()})
    with pytest.raises(baba.BabaFinancialError):baba.parse_interim(raw,originals['current_cover'],key='current')


@pytest.mark.parametrize('change',['missing','foreign','duplicate'])
def test_exact_exhibit_link_required(originals,monkeypatch,change):
    raw=originals['current_cover'];filename=baba.SOURCES['current']['filename'].encode()
    if change=='missing':raw=raw.replace(filename,b'other.htm')
    elif change=='foreign':raw=raw.replace(filename,b'https://other.example/'+filename)
    else:
        tree=html.fromstring(raw);link=next(a for a in tree.xpath('//a[@href]') if a.get('href')==filename.decode());link.addnext(deepcopy(link));raw=etree.tostring(tree,method='html')
    assert raw!=originals['current_cover']
    monkeypatch.setitem(baba.SOURCES,'current_cover',{**baba.SOURCES['current_cover'],'sha256':hashlib.sha256(raw).hexdigest()})
    with pytest.raises(baba.BabaFinancialError,match='exhibit_link'):baba.parse_interim(originals['current'],raw,key='current')


@pytest.mark.parametrize('change',['cik','currency','period'])
def test_annual_financial_identity_is_not_overridden(originals,monkeypatch,change):
    tree=html.fromstring(originals['annual'])
    if change=='cik':
        for n in tree.iter('xbrli:identifier'):n.text='0000000001'
    elif change=='currency':
        for n in tree.iter('xbrli:measure'):
            if n.text=='iso4217:CNY':n.text='iso4217:USD'
    else:
        node=next(n for n in tree.iter('ix:nonnumeric') if n.get('name')=='dei:DocumentPeriodEndDate')
        for child in list(node):node.remove(child)
        node.text='March 31, 2025'
    raw=etree.tostring(tree,method='html')
    monkeypatch.setitem(baba.SOURCES,'annual',{**baba.SOURCES['annual'],'sha256':hashlib.sha256(raw).hexdigest()})
    with pytest.raises(baba.BabaFinancialError):baba.parse_annual(raw)


@pytest.mark.parametrize('value',['','NaN','1e9','12,34','1.5','-100','1,000 RMB','0 note'])
def test_missing_or_ambiguous_number_is_not_zero(value):
    with pytest.raises(baba.BabaFinancialError):baba.amount(value)


@pytest.mark.parametrize('change',['cik','symbol','duplicate_symbol','new_report','amendment','missing_filing','shape','stale','naive','future'])
def test_scope_and_availability(company,change):
    args=dict(symbol='BABA',cik=baba.CIK,observed_at=NOW)
    if change=='cik':args['cik']='1'
    elif change=='symbol':args['symbol']='BABAF'
    elif change=='duplicate_symbol':company['tickers'].append('BABA')
    elif change=='stale':args['observed_at']=datetime(2027,2,1,tzinfo=timezone.utc)
    elif change=='naive':args['observed_at']=NOW.replace(tzinfo=None)
    elif change=='future':args['observed_at']=datetime(2026,8,20,12,tzinfo=timezone.utc)
    else:
        recent=company['filings']['recent']
        if change=='shape':recent['form'].pop()
        elif change=='missing_filing':recent['accessionNumber'][recent['accessionNumber'].index(baba.SOURCES['current']['accessionNumber'])]='absent'
        else:
            for k,v in {'accessionNumber':'0001577552-26-999999','form':'6-K/A' if change=='amendment' else '6-K','filingDate':'2026-10-14','reportDate':'2026-06-30','primaryDocument':'new.htm'}.items():recent[k].insert(0,v)
    with pytest.raises(baba.BabaFinancialError):baba.validate_company(company,**args)


def test_acquisition_project_dispatch_cache_version_and_no_fetch_on_hold(originals,company):
    from app.services.sec_fundamentals_preparation import version_for_symbol
    calls=[]
    class Client:
        def get(self,url):
            calls.append(url);return next(originals[k] for k,s in baba.SOURCES.items() if s['url']==url)
    meta=json.dumps(company).encode();facts=b'{"cik":1577552}'
    data=inline.acquire(facts,meta,symbol='BABA',cik=baba.CIK,observed_at=NOW,client=Client())
    assert data=={'baba_originals':originals} and len(calls)==5
    assert sec.project_fundamentals(facts,meta,symbol='BABA',cik=baba.CIK,observed_at=NOW,filing_supplement=data)==project(originals,company)
    assert baba.VERSION in version_for_symbol('BABA') and baba.VERSION not in version_for_symbol('AAPL')
    company['cik']=1
    held=inline.acquire(facts,json.dumps(company).encode(),symbol='BABA',cik=baba.CIK,observed_at=NOW,client=Client())
    assert held=={'baba_error':'baba_company_identity'} and len(calls)==5


def test_comparative_conflict_is_never_silently_replaced(originals,company,monkeypatch):
    real=baba.parse_interim
    def changed(raw,cover_raw,*,key):
        result=real(raw,cover_raw,key=key)
        if key=='current':result['revenue'][0]['value']+=1_000_000
        return result
    monkeypatch.setattr(baba,'parse_interim',changed)
    with pytest.raises(baba.BabaFinancialError,match='comparative_restatement'):project(originals,company)


def test_cache_round_trip_preserves_cny_and_repeat_without_network(originals,company,monkeypatch):
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from app.models import InsightsSnapshot
    from app.services import sec_fundamentals_preparation as prep,sec_directory
    calls=[];urls={s['url']:originals[k] for k,s in baba.SOURCES.items()}
    class Clock(datetime):
        @classmethod
        def now(cls,tz=None):return NOW
    class Client:
        def get(self,url):
            calls.append(url)
            return urls[url] if '/Archives/' in url else b'{"cik":1577552}' if 'companyfacts' in url else json.dumps(company).encode()
    engine=create_engine('sqlite:///:memory:');InsightsSnapshot.__table__.create(engine);factory=sessionmaker(bind=engine)
    monkeypatch.setenv('SEC_FUNDAMENTALS_WARMING_ENABLED','1');monkeypatch.setenv('FUNDAMENTALS_PROVIDER','fmp')
    monkeypatch.setattr(prep,'SessionLocal',factory);monkeypatch.setattr(prep,'datetime',Clock)
    monkeypatch.setattr(prep,'DirectSourceClient',Client);monkeypatch.setattr(sec_directory,'directory',lambda:{'BABA':{'cik':baba.CIK}})
    try:
        first=prep.prepare('BABA');assert set(first['values'])==FIELDS and len(calls)==7
        assert first['calculation_evidence']['reporting_currency']=='CNY'
        assert prep.prepare('BABA')==first and len(calls)==7
        with factory() as db:assert json.loads(db.get(InsightsSnapshot,'sec-current-fundamentals:BABA:v1').payload_json)==first
    finally:engine.dispose()


def test_usd_market_cap_never_divides_by_native_cny_earnings(originals,company,monkeypatch):
    from app.clients.direct_sources import DirectSourceClient
    from app.clients.massive_stocks import MassiveStocksClient
    class Clock(datetime):
        @classmethod
        def now(cls,tz=None):return NOW
    monkeypatch.setattr(sec,'datetime',Clock)
    monkeypatch.setattr(sec,'company_directory',lambda day:{'BABA':{'cik':baba.CIK}})
    monkeypatch.setattr(DirectSourceClient,'get',lambda self,url:b'{"cik":1577552}' if 'companyfacts' in url else json.dumps(company).encode())
    monkeypatch.setattr(inline,'acquire',lambda *a,**k:{'baba_originals':originals})
    monkeypatch.setenv('STOCK_PRICE_PROVIDER','massive');monkeypatch.setenv('MASSIVE_API_KEY','test-only')
    monkeypatch.setattr(MassiveStocksClient,'details',lambda *a:{'cik':baba.CIK,'market_cap':10000000000})
    monkeypatch.setattr(MassiveStocksClient,'bars',lambda *a:[])
    result=sec.fetch_fundamentals('BABA')
    assert result['market_cap']==10000000000 and set(result)&FIELDS==FIELDS
    assert not set(result)&{'price_to_sales','trailing_pe','earnings_yield','free_cash_flow','fcf_yield','eps_ttm'}
    assert json.loads(result['source_evidence_json'])['market_income']['reason']=='common_income_currency_not_usd'
