from copy import deepcopy
from datetime import datetime,timezone
from pathlib import Path
import hashlib,json,socket,types
import pytest
from app.services import sec_ul_financials as ul,sec_fundamentals as sec,sec_inline_financials as inline

ROOT=Path(__file__).parent/'fixtures/sec_ul'
COMPANY=json.loads((ROOT/'company.json').read_bytes())
NOW=datetime(2026,10,15,21,tzinfo=timezone.utc)
FIELDS={'revenue_growth','operating_margin','operating_margin_expansion','net_margin','current_ratio','roe','fcf_margin'}


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    monkeypatch.setattr(socket.socket,'connect',lambda *a,**k:(_ for _ in ()).throw(AssertionError('Network forbidden')))


def bundle(monkeypatch,*,pages=None):
    docs={k:(ROOT/(k+'.htm')).read_bytes() for k in ul.SOURCES}
    for k,raw in docs.items():monkeypatch.setitem(ul.SOURCES,k,{**ul.SOURCES[k],'sha256':hashlib.sha256(raw).hexdigest()})
    docs['restated']=b'%PDF-synthetic-extraction-fixture'
    monkeypatch.setitem(ul.RESTATEMENT,'sha256',hashlib.sha256(docs['restated']).hexdigest())
    monkeypatch.setitem(ul.RESTATEMENT,'bytes',len(docs['restated']))
    monkeypatch.setattr(ul,'RESTATEMENT_FILE',types.SimpleNamespace(read_bytes=lambda:docs['restated']))
    import pypdf
    texts=pages if pages is not None else json.loads((ROOT/'restated-extracted.json').read_bytes())
    class Reader:
        is_encrypted=False
        def __init__(self,*args,**kwargs):
            self.pages=[types.SimpleNamespace(extract_text=lambda value=value:value,get_contents=lambda:None) for value in texts]
    monkeypatch.setattr(pypdf,'PdfReader',Reader)
    return {'ul_originals':docs}


def project(data,company=None,**kwargs):
    options=dict(symbol='UL',cik=ul.CIK,observed_at=NOW,supplement=data);options.update(kwargs)
    return ul.project(b'{"cik":217410}',json.dumps(company or COMPANY).encode(),**options)


def test_seven_ratios_use_restated_perimeter_and_total_parent_income(monkeypatch):
    data=bundle(monkeypatch);before=deepcopy(data);r=project(data)
    assert r==project(data) and data==before and set(r)&FIELDS==FIELDS
    revenue=50503+25623-25506;prior=52479+25506-26507
    operating=9037+4885-4762;prior_op=8829+4762-5351;parent=9469+3316-3512
    assert r['revenue_growth']==pytest.approx((revenue/prior-1)*100)
    assert r['operating_margin']==pytest.approx(operating/revenue*100)
    assert r['operating_margin_expansion']==pytest.approx((operating/revenue-prior_op/prior)*100)
    assert r['net_margin']==pytest.approx(parent/revenue*100)
    assert r['roe']==pytest.approx(parent/((16194+17795)/2)*100)
    assert r['current_ratio']==pytest.approx(21389/28891)
    assert r['fcf_margin']==pytest.approx(((8052+2577-2019)-(1417+532-531))/revenue*100)
    assert not set(r)&{'free_cash_flow','fcf_growth','gross_margin','net_debt_to_ebitda','eps_ttm','ev_to_ebitda'}
    e=json.loads(r['source_evidence_json']);assert e['reporting_currency']=='EUR'
    assert len(e['sources'])==3 and len(e['reviewed_nonfinancial_filings'])==6
    assert any(s['source_kind']=='issuer_restatement_pdf' for s in e['sources'])
    assert 'discontinued operations and disposal gains' in e['calculations']['net_margin']
    for group in ('current','prior'):
        for point in e[group].values():
            assert len(point['inputs'])==3
            assert all(p['unit']=='EUR' and p['source_sha256'] for p in point['inputs'])


@pytest.mark.parametrize('key',['current','annual','restated'])
def test_original_changes_are_held(monkeypatch,key):
    data=bundle(monkeypatch);data['ul_originals'][key]+=b'changed'
    with pytest.raises(ul.UnileverFinancialError,match='changed'):project(data)


@pytest.mark.parametrize('value',['','—','NaN','1e9','12,34','1.5','-100','(1,00)','1,000 EUR'])
def test_ambiguous_amounts_are_not_zero(value):
    with pytest.raises(ul.UnileverFinancialError):ul.amount(value)


@pytest.mark.parametrize('change',['wrong_cik','wrong_name','wrong_ticker','duplicate_source','missing_source','new_report','amendment','altered_exclusion','future','stale','naive','arrays'])
def test_directory_guards_precede_transport(change):
    c=deepcopy(COMPANY);r=c['filings']['recent'];now=NOW;cik=ul.CIK
    if change=='wrong_cik':cik='1'
    elif change=='wrong_name':c['name']='OTHER'
    elif change=='wrong_ticker':c['tickers']=['OTHER']
    elif change=='duplicate_source':
        for k in r:r[k].append(r[k][-1])
    elif change=='missing_source':
        for k in r:r[k].pop()
    elif change in ('new_report','amendment'):
        row=dict(zip(('accessionNumber','form','filingDate','reportDate','primaryDocument'),('0000000000-26-999999','6-K/A' if change=='amendment' else '6-K','2026-10-14','2026-09-30','new.htm')))
        for k in r:r[k].append(row[k])
    elif change=='altered_exclusion':r['primaryDocument'][0]='changed.htm'
    elif change=='future':now=NOW.replace(month=7,day=28)
    elif change=='stale':now=NOW.replace(year=2027)
    elif change=='naive':now=NOW.replace(tzinfo=None)
    elif change=='arrays':r['form'].pop()
    class Client:
        def get(self,url):raise AssertionError('Transport before source validation')
    with pytest.raises(ul.UnileverFinancialError):ul.acquire(json.dumps(c).encode(),symbol='UL',cik=cik,observed_at=now,client=Client())


@pytest.mark.parametrize('change',['period','currency','wrong_value','duplicate','restated_mismatch','balance','cash_sign'])
def test_semantic_guards_survive_matching_hash_fixture(monkeypatch,change):
    data=bundle(monkeypatch);raw=data['ul_originals']['current']
    if change=='period':raw=raw.replace(b'2026',b'2027')
    elif change=='currency':raw=raw.replace('€ million'.encode(),b'USD million')
    elif change=='wrong_value':raw=raw.replace(b'4,885',b'9,885')
    elif change=='duplicate':raw=raw.replace(b'</table>',b'<tr><td>Turnover</td><td>25,623</td><td>25,506</td><td>0.5%</td></tr></table>',1)
    elif change=='restated_mismatch':raw=raw.replace(b'25,506',b'30,127')
    elif change=='balance':raw=raw.replace(b'21,389',b'22,389')
    elif change=='cash_sign':raw=raw.replace(b'(532)',b'532')
    data['ul_originals']['current']=raw;monkeypatch.setitem(ul.SOURCES['current'],'sha256',hashlib.sha256(raw).hexdigest())
    with pytest.raises(ul.UnileverFinancialError):project(data)


@pytest.mark.parametrize('change',['date','basis','columns','duplicate','missing','pages'])
def test_restatement_extraction_must_match_reviewed_basis(monkeypatch,change):
    pages=json.loads((ROOT/'restated-extracted.json').read_bytes())
    if change=='date':pages[0]=pages[0].replace('12 March 2026','12 March 2027')
    elif change=='basis':pages[0]=pages[0].replace('excluding Ice Cream','including Ice Cream')
    elif change=='columns':pages[1]=pages[1].replace('2025 2024','2024 2025')
    elif change=='duplicate':pages[1]+='\nTurnover 25,506 26,507'
    elif change=='missing':pages[1]=pages[1].replace('Turnover','Other row')
    elif change=='pages':pages.pop()
    with pytest.raises(ul.UnileverFinancialError):project(bundle(monkeypatch,pages=pages))


def test_archived_restatement_needs_only_current_sec_sources(monkeypatch):
    data=bundle(monkeypatch);calls=[];permitted=frozenset(['https://example.test/already-reviewed'])
    urls={s['url']:data['ul_originals'][k] for k,s in ul.SOURCES.items()}
    class Client:
        issuer_document_urls=permitted
        def get(self,url):
            assert self.issuer_document_urls==permitted
            assert url.startswith('https://www.sec.gov/Archives/')
            calls.append(url);return urls[url]
    client=Client();assert ul.acquire(json.dumps(COMPANY).encode(),symbol='UL',cik=ul.CIK,observed_at=NOW,client=client)==data
    assert len(calls)==2 and client.issuer_document_urls==permitted
    def fail(url):raise RuntimeError('SEC unavailable')
    client.get=fail
    with pytest.raises(RuntimeError,match='SEC unavailable'):ul.acquire(json.dumps(COMPANY).encode(),symbol='UL',cik=ul.CIK,observed_at=NOW,client=client)
    assert client.issuer_document_urls==permitted


def test_new_report_hold_is_not_replaced_with_old_facts():
    c=deepcopy(COMPANY);r=c['filings']['recent'];r['primaryDocument'][0]='new.htm'
    class Client:
        def get(self,url):raise AssertionError('Unexpected transport')
    args=dict(symbol='UL',cik=ul.CIK,observed_at=NOW)
    result=inline.acquire(b'{"cik":217410}',json.dumps(c).encode(),client=Client(),**args)
    assert result=={'ul_error':'ul_newer_or_amended_report_requires_review'}
    with pytest.raises(ul.UnileverFinancialError):sec.project_fundamentals(b'{"cik":217410}',json.dumps(c).encode(),filing_supplement=result,**args)


def test_prepared_cache_persists_restatement_and_repeats_without_fetch(monkeypatch):
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from app.models import InsightsSnapshot
    from app.services import sec_fundamentals_preparation as prep,sec_directory
    data=bundle(monkeypatch);calls=[]
    urls={**{s['url']:data['ul_originals'][k] for k,s in ul.SOURCES.items()},ul.RESTATEMENT['url']:data['ul_originals']['restated']}
    class Clock(datetime):
        @classmethod
        def now(cls,tz=None):return NOW
    class Client:
        issuer_document_urls=frozenset()
        def get(self,url):
            calls.append(url)
            if url in urls:return urls[url]
            return b'{"cik":217410}' if 'companyfacts' in url else json.dumps(COMPANY).encode()
    engine=create_engine('sqlite:///:memory:');InsightsSnapshot.__table__.create(engine);factory=sessionmaker(bind=engine)
    monkeypatch.setenv('SEC_FUNDAMENTALS_WARMING_ENABLED','1');monkeypatch.setenv('FUNDAMENTALS_PROVIDER','fmp')
    monkeypatch.setattr(prep,'SessionLocal',factory);monkeypatch.setattr(prep,'datetime',Clock)
    monkeypatch.setattr(prep,'DirectSourceClient',Client);monkeypatch.setattr(sec_directory,'directory',lambda:{'UL':{'cik':ul.CIK}})
    try:
        first=prep.prepare('UL');assert set(first['values'])==FIELDS and len(calls)==4
        assert first['calculation_evidence']['reporting_currency']=='EUR'
        assert prep.prepare('UL')==first and len(calls)==4
        assert prep.version_for_symbol('UL')==prep.VERSION+'_'+ul.VERSION
        assert prep.version_for_symbol('AAPL')==prep.VERSION
        with factory() as db:assert json.loads(db.get(InsightsSnapshot,'sec-current-fundamentals:UL:v1').payload_json)==first
    finally:engine.dispose()


def test_usd_market_cap_never_consumes_eur_financial_amounts(monkeypatch):
    from app.clients.direct_sources import DirectSourceClient
    from app.clients.massive_stocks import MassiveStocksClient
    data=bundle(monkeypatch)
    class Clock(datetime):
        @classmethod
        def now(cls,tz=None):return NOW
    monkeypatch.setattr(sec,'datetime',Clock)
    monkeypatch.setattr(sec,'company_directory',lambda day:{'UL':{'cik':ul.CIK}})
    monkeypatch.setattr(DirectSourceClient,'get',lambda self,url:b'{"cik":217410}' if 'companyfacts' in url else json.dumps(COMPANY).encode())
    monkeypatch.setattr(inline,'acquire',lambda *a,**k:data)
    monkeypatch.setenv('STOCK_PRICE_PROVIDER','massive');monkeypatch.setenv('MASSIVE_API_KEY','test-only')
    monkeypatch.setattr(MassiveStocksClient,'details',lambda *a:{'cik':ul.CIK,'market_cap':10000000000})
    monkeypatch.setattr(MassiveStocksClient,'bars',lambda *a:[])
    result=sec.fetch_fundamentals('UL')
    assert result['market_cap']==10000000000 and set(result)&FIELDS==FIELDS
    assert not set(result)&{'price_to_sales','trailing_pe','earnings_yield','free_cash_flow','fcf_yield','eps_ttm'}
    assert json.loads(result['source_evidence_json'])['market_income']['reason']=='common_income_currency_not_usd'



@pytest.mark.parametrize('provider', ['sec_edgar', 'free_direct'])
def test_summary_discloses_issuer_restatement_and_disposal_basis(monkeypatch,provider):
    from app.services.fundamentals_cache import fundamentals_summary_from_cache_row
    r=project(bundle(monkeypatch));e=json.loads(r['source_evidence_json'])
    if provider=='free_direct':
        e['supplement']={'used_fields':['beta'],'fields':{'beta':{'source':'Finnhub'}}}
    r.update(provider=provider,source_evidence_json=json.dumps(e))
    summary=fundamentals_summary_from_cache_row(types.SimpleNamespace(**r),now=NOW)
    context=summary['context']
    assert context['source_label']=='SEC EDGAR + Unilever'+(' + Finnhub' if provider=='free_direct' else '')
    assert 'disposal gains' in context['source_note'] and 'excludes intangible purchases' in context['source_note']
    assert ('provider observations' in context['source_note'])==(provider=='free_direct')
    assert summary['metrics']['return_on_equity']['value']==pytest.approx(r['roe']/100)
    assert summary['data_quality']['scored_metric_count']==3


@pytest.mark.parametrize('display',[None,[],{'source_label':[],'source_note':{}},{'source_label':'x'*121,'source_note':'x'*1001}])
def test_invalid_display_metadata_does_not_break_summary(monkeypatch,display):
    from app.services.fundamentals_cache import fundamentals_summary_from_cache_row
    r=project(bundle(monkeypatch));e=json.loads(r['source_evidence_json']);e['display_context']=display
    r['source_evidence_json']=json.dumps(e)
    context=fundamentals_summary_from_cache_row(types.SimpleNamespace(**r),now=NOW)['context']
    assert context['source_label']=='SEC EDGAR' and context['source_note'] is None


def test_other_sources_keep_existing_labels(monkeypatch):
    from app.services.fundamentals_cache import fundamentals_summary_from_cache_row
    r=project(bundle(monkeypatch));r.update(provider='fmp')
    context=fundamentals_summary_from_cache_row(types.SimpleNamespace(**r),now=NOW)['context']
    assert context['source_label']=='FMP' and context['source_note'] is None
    e=json.loads(r['source_evidence_json']);e.pop('display_context');r.update(provider='sec_edgar',source_evidence_json=json.dumps(e))
    context=fundamentals_summary_from_cache_row(types.SimpleNamespace(**r),now=NOW)['context']
    assert context['source_label']=='SEC EDGAR' and context['source_note'] is None


def test_packaged_complete_original_and_capture_provenance():
    raw=ul.historical_restatement()
    assert len(raw)==179697 and hashlib.sha256(raw).hexdigest()==ul.RESTATEMENT['sha256']
    result=ul.parse_restated(raw)
    assert result['income']['revenue']==[25506000000,26507000000]
    assert result['evidence']['retrieval']=='reviewed_historical_original'
    assert result['evidence']['source_observed_at']=='2026-10-15T20:42:20.679876+00:00'
    assert result['evidence']['published']=='2026-03-12'


@pytest.mark.parametrize('change',['missing','truncated','same_size_tamper'])
def test_missing_or_tampered_archive_fails_before_transport(monkeypatch,tmp_path,change):
    raw=ul.historical_restatement();path=tmp_path/'original.pdf'
    if change=='truncated':path.write_bytes(raw[:-1])
    elif change=='same_size_tamper':path.write_bytes(raw[:-1]+bytes([raw[-1]^1]))
    monkeypatch.setattr(ul,'RESTATEMENT_FILE',path)
    class Client:
        def get(self,url):raise AssertionError('Transport before archive validation')
    with pytest.raises(ul.UnileverFinancialError,match='ul_historical_original_'):
        ul.acquire(json.dumps(COMPANY).encode(),symbol='UL',cik=ul.CIK,observed_at=NOW,client=Client())


def test_archive_does_not_claim_observation_before_capture(monkeypatch):
    data=bundle(monkeypatch)
    with pytest.raises(ul.UnileverFinancialError,match='not_yet_observed'):
        project(data,observed_at=NOW.replace(hour=20,minute=41))
    evidence=json.loads(project(data)['source_evidence_json'])
    original=next(s for s in evidence['sources'] if s['source_kind']=='issuer_restatement_pdf')
    assert original['source_observed_at']!=NOW.isoformat()
    assert any('not a fresh issuer-site observation' in line for line in evidence['limitations'])
