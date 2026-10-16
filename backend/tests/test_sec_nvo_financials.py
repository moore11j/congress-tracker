from copy import deepcopy
from datetime import datetime, timezone
import gzip
import hashlib
import json
from pathlib import Path

import pytest
from lxml import html, etree
from app.services import sec_nvo_financials as nvo, sec_fundamentals as sec, sec_inline_financials as inline

ROOT = Path(__file__).parent/'fixtures/sec_nvo'
NOW = datetime(2026,10,16,2,30,tzinfo=timezone.utc)
FIELDS = {'gross_margin','operating_margin','net_margin','revenue_growth','operating_margin_expansion','current_ratio','roe','fcf_margin','fcf_growth'}


@pytest.fixture(scope='module')
def originals():
    return {k:gzip.decompress((ROOT/(k+'.htm.gz')).read_bytes()) for k in nvo.SOURCES}


@pytest.fixture
def company():
    return json.loads(gzip.decompress((ROOT/'company.json.gz').read_bytes()))


def project(originals, company, **kwargs):
    options = dict(symbol='NVO',cik=nvo.CIK,observed_at=NOW,supplement={'nvo_originals':originals})
    options.update(kwargs)
    return nvo.project(b'{"cik":353278}',json.dumps(company).encode(),**options)


def test_complete_originals_all_nine_ratios_repeat_and_reclassified_cash(originals,company):
    meta=json.loads((ROOT/'manifest.json').read_text())
    for k,raw in originals.items():
        assert hashlib.sha256(raw).hexdigest()==meta[k]['original_sha256']==nvo.SOURCES[k]['sha256']
        assert len(raw)==meta[k]['original_bytes'] and meta[k]['complete_original']
    before=deepcopy(originals);r=project(originals,company)
    assert r==project(originals,company) and originals==before and FIELDS<=r.keys()
    revenue=309064+175311-154944;prior_revenue=290403+154944-133409
    operating=127658+86679-72240;prior_operating=128339+72240-57780
    profit=102434+69546-55537
    assert r['gross_margin']==pytest.approx((250276+144613-129208)/revenue*100)
    assert r['operating_margin']==pytest.approx(operating/revenue*100)
    assert r['net_margin']==pytest.approx(profit/revenue*100)
    assert r['revenue_growth']==pytest.approx((revenue/prior_revenue-1)*100)
    assert r['operating_margin_expansion']==pytest.approx((operating/revenue-prior_operating/prior_revenue)*100)
    assert r['current_ratio']==pytest.approx(208696/239083)
    assert r['roe']==pytest.approx(profit/((221277+168066)/2)*100)
    cfo=119102-1398+3419+79283-66504
    old_cfo=120968-1884+612+66504-(64817-547+217)
    cash=cfo-(60140+23986-28083)
    old_cash=old_cfo-(47164+28083-18944)
    assert cfo==133902 and old_cfo==121713
    assert r['fcf_margin']==pytest.approx(cash/revenue*100)
    assert r['fcf_growth']==pytest.approx((cash/old_cash-1)*100)
    assert not set(r)&{'free_cash_flow','eps_growth','eps_ttm','net_debt_to_ebitda','ev_to_ebitda'}
    e=json.loads(r['source_evidence_json'])
    assert e['native_cash_flow']['value']==cash*1_000_000
    assert e['reporting_currency']=='DKK' and e['period_end']=='2026-06-30'
    assert len(e['sources'])==4 and len(e['unused_balance_revisions'])==2
    assert len(e['reclassifications'])==5
    comparable=[p for p in e['reclassifications'] if p['start']=='2025-01-01' and p['end']=='2025-06-30']
    assert len(comparable)==1 and comparable[0]['value']==66_504_000_000
    for group in ('current','prior'):
        for point in e[group].values():
            assert len(point['inputs'])==3
            assert all(p['source_sha256'] and p['source_url'] and p['unit']=='DKK' for p in point['inputs'])


@pytest.mark.parametrize('key',list(nvo.SOURCES))
def test_changed_original_is_never_silently_accepted(originals,company,key):
    changed={**originals,key:originals[key]+b' '}
    with pytest.raises(nvo.NvoFinancialError,match='original_changed'):project(changed,company)


@pytest.mark.parametrize('change',['currency','header','title','missing','duplicate','sign','cash_basis','cash_value','equity','duplicate_statement'])
def test_invalid_current_statement_after_test_hash_review(originals,monkeypatch,change):
    tree=html.fromstring(originals['current'])
    def replace_cell(label, column, value):
        rows=[r for r in tree.xpath('//tr') if r.xpath('./td|./th') and ' '.join(r.xpath('./td|./th')[0].text_content().split())==label]
        for row in rows:
            cells=[c for c in row.xpath('./td|./th') if c.text_content().strip()]
            if len(cells)==5:
                cells[column].clear();cells[column].text=value
                return
        raise AssertionError(label)
    if change=='sign':replace_cell('Cost of goods sold',3,'30,698')
    elif change=='cash_value':
        for c in tree.xpath('//td'):
            if c.text_content().strip()=='79,283':c.clear();c.text='79,300'
    elif change=='equity':
        for c in tree.xpath('//td'):
            if c.text_content().strip()=='221,277':c.clear();c.text='221,300'
    elif change in ('missing','duplicate','duplicate_statement'):
        table=next(t for t in tree.xpath('//table[not(.//table)]') if 'NET PROFIT' in t.text_content() and 'Gross profit' in t.text_content())
        row=next(r for r in table.xpath('.//tr') if r.xpath('./td')[0].text_content().strip()=='NET PROFIT')
        if change=='missing':row.getparent().remove(row)
        elif change=='duplicate':row.addnext(deepcopy(row))
        else:table.addnext(deepcopy(table))
    raw=etree.tostring(tree,method='html')
    if change=='currency':raw=raw.replace(b'DKK million',b'USD million')
    elif change=='header':raw=raw.replace(b'H1 2026',b'H2 2026')
    elif change=='title':raw=raw.replace(b'Condensed income statement',b'Adjusted income statement')
    elif change=='cash_basis':raw=raw.replace(b'Effective 1 January 2026',b'Effective 1 January 2027')
    monkeypatch.setitem(nvo.SOURCES,'current',{**nvo.SOURCES['current'],'sha256':hashlib.sha256(raw).hexdigest()})
    with pytest.raises(nvo.NvoFinancialError):nvo.parse_interim(raw,key='current')


def test_exact_prior_scale_duplicate_is_required(originals,monkeypatch):
    raw=originals['prior'].replace(b'>57780000000<',b'>57781000000<')
    assert raw!=originals['prior']
    monkeypatch.setitem(nvo.SOURCES,'prior',{**nvo.SOURCES['prior'],'sha256':hashlib.sha256(raw).hexdigest()})
    with pytest.raises(nvo.NvoFinancialError,match='duplicate_scale_cell'):nvo.parse_interim(raw,key='prior')


@pytest.mark.parametrize('change',['namespace','exhibit','cik','period'])
def test_annual_cover_exhibit_identity(originals,monkeypatch,change):
    raw=originals['cover']
    if change=='namespace':raw=raw.replace(b'http://www.xbrl.org/2003/instance',b'https://wrong.example/instance')
    elif change=='exhibit':raw=raw.replace(b'nvo-20251231_d2.htm',b'other.htm')
    elif change=='cik':raw=raw.replace(b'0000353278',b'0000000001')
    else:
        tree=html.fromstring(raw)
        node=next(n for n in tree.iter('ix:nonnumeric') if n.get('name')=='dei:DocumentPeriodEndDate')
        node.text='December 31, 2024'
        raw=etree.tostring(tree,method='html')
    assert raw!=originals['cover']
    monkeypatch.setitem(nvo.SOURCES,'cover',{**nvo.SOURCES['cover'],'sha256':hashlib.sha256(raw).hexdigest()})
    with pytest.raises(ValueError):nvo.parse_annual(originals['annual'],raw)


@pytest.mark.parametrize('value',['','NaN','1e9','12,34','1.5','-100','1,000 DKK','0 note'])
def test_ambiguous_numeric_cell_is_not_zero(value):
    with pytest.raises(nvo.NvoFinancialError):nvo.amount(value)


@pytest.mark.parametrize('change',['wrong_cik','wrong_symbol','duplicate_symbol','new_report','amendment','missing_filing','shape','stale','naive','future'])
def test_company_scope_and_availability(company,change):
    args=dict(symbol='NVO',cik=nvo.CIK,observed_at=NOW)
    if change=='wrong_cik':args['cik']='1'
    elif change=='wrong_symbol':args['symbol']='NONOF'
    elif change=='duplicate_symbol':company['tickers'].append('NVO')
    elif change=='stale':args['observed_at']=datetime(2027,2,1,tzinfo=timezone.utc)
    elif change=='naive':args['observed_at']=NOW.replace(tzinfo=None)
    elif change=='future':args['observed_at']=datetime(2026,8,4,12,tzinfo=timezone.utc)
    else:
        recent=company['filings']['recent']
        if change=='shape':recent['form'].pop()
        elif change=='missing_filing':recent['accessionNumber'][recent['accessionNumber'].index(nvo.SOURCES['current']['accessionNumber'])]='absent'
        else:
            for k,value in {'accessionNumber':'0000353278-26-999999','form':'6-K/A' if change=='amendment' else '6-K',
                'filingDate':'2026-09-30','reportDate':'2026-06-30','primaryDocument':'new.htm'}.items():recent[k].insert(0,value)
    with pytest.raises(nvo.NvoFinancialError):nvo.validate_company(company,**args)


def test_acquisition_dispatch_and_scoped_cache_version(originals,company):
    from app.services.sec_fundamentals_preparation import version_for_symbol, VERSION
    calls=[]
    class Client:
        def get(self,url):
            calls.append(url)
            return next(originals[k] for k,s in nvo.SOURCES.items() if s['url']==url)
    raw=json.dumps(company).encode()
    data=inline.acquire(b'{"cik":353278}',raw,symbol='NVO',cik=nvo.CIK,observed_at=NOW,client=Client())
    assert data=={'nvo_originals':originals} and len(calls)==4
    assert sec.project_fundamentals(b'{"cik":353278}',raw,symbol='NVO',cik=nvo.CIK,observed_at=NOW,filing_supplement=data)==project(originals,company)
    assert nvo.VERSION in version_for_symbol('NVO') and nvo.VERSION not in version_for_symbol('AAPL')


def test_cash_comparative_conflict_is_held_even_after_internal_reconciliation(originals,company,monkeypatch):
    real=nvo.parse_interim
    def changed(raw,*,key):
        result=real(raw,key=key)
        if key=='current':result['operating_cash_flow'][1]['value']+=1_000_000
        return result
    monkeypatch.setattr(nvo,'parse_interim',changed)
    with pytest.raises(nvo.NvoFinancialError,match='comparative_conflict:operating_cash_flow'):project(originals,company)


def test_usd_market_cap_does_not_consume_dkk_amounts(originals,company,monkeypatch):
    from app.clients.direct_sources import DirectSourceClient
    from app.clients.massive_stocks import MassiveStocksClient
    class Clock(datetime):
        @classmethod
        def now(cls,tz=None):return NOW
    monkeypatch.setattr(sec,'datetime',Clock)
    monkeypatch.setattr(sec,'company_directory',lambda day:{'NVO':{'cik':nvo.CIK}})
    monkeypatch.setattr(DirectSourceClient,'get',lambda self,url:b'{"cik":353278}' if 'companyfacts' in url else json.dumps(company).encode())
    monkeypatch.setattr(inline,'acquire',lambda *a,**k:{'nvo_originals':originals})
    monkeypatch.setenv('STOCK_PRICE_PROVIDER','massive');monkeypatch.setenv('MASSIVE_API_KEY','test-only')
    monkeypatch.setattr(MassiveStocksClient,'details',lambda *a:{'cik':nvo.CIK,'market_cap':10000000000})
    monkeypatch.setattr(MassiveStocksClient,'bars',lambda *a:[])
    result=sec.fetch_fundamentals('NVO')
    assert result['market_cap']==10000000000 and set(result)&FIELDS==FIELDS
    assert not set(result)&{'price_to_sales','trailing_pe','earnings_yield','free_cash_flow','fcf_yield','eps_ttm'}
    assert json.loads(result['source_evidence_json'])['market_income']['reason']=='common_income_currency_not_usd'


def test_prepared_cache_retains_native_provenance_and_repeat_without_fetch(originals,company,monkeypatch):
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from app.models import InsightsSnapshot
    from app.services import sec_fundamentals_preparation as prep,sec_directory
    calls=[];urls={s['url']:originals[k] for k,s in nvo.SOURCES.items()}
    class Clock(datetime):
        @classmethod
        def now(cls,tz=None):return NOW
    class Client:
        def get(self,url):
            calls.append(url)
            if '/Archives/' in url:return urls[url]
            return b'{"cik":353278}' if 'companyfacts' in url else json.dumps(company).encode()
    engine=create_engine('sqlite:///:memory:');InsightsSnapshot.__table__.create(engine);factory=sessionmaker(bind=engine)
    monkeypatch.setenv('SEC_FUNDAMENTALS_WARMING_ENABLED','1');monkeypatch.setenv('FUNDAMENTALS_PROVIDER','fmp')
    monkeypatch.setattr(prep,'SessionLocal',factory);monkeypatch.setattr(prep,'datetime',Clock)
    monkeypatch.setattr(prep,'DirectSourceClient',Client);monkeypatch.setattr(sec_directory,'directory',lambda:{'NVO':{'cik':nvo.CIK}})
    try:
        first=prep.prepare('NVO');assert set(first['values'])==FIELDS and len(calls)==6
        assert first['calculation_evidence']['reporting_currency']=='DKK'
        assert first['calculation_evidence']['native_cash_flow']['value']==77_859_000_000
        assert prep.prepare('NVO')==first and len(calls)==6
        with factory() as db:assert json.loads(db.get(InsightsSnapshot,'sec-current-fundamentals:NVO:v1').payload_json)==first
    finally:engine.dispose()
