"""Complete SEC document-set sources and cross-document identity boundaries."""
from pathlib import Path
from copy import deepcopy
import gzip,hashlib,json,socket
import pytest
from lxml import html,etree
from app.services import sec_inline_financials as inline
from app.services.sec_foreign_statements import IFRS

ROOT=Path(__file__).parent/'fixtures/sec_document_sets'
SOURCES=json.loads((ROOT/'sources.json').read_text())


def originals(symbol):
    cover_name,cover=next((n,m) for n,m in SOURCES.items() if m['symbol']==symbol and 'parent_url' not in m)
    name,exhibit=next((n,m) for n,m in SOURCES.items() if m['symbol']==symbol and m.get('parent_url')==cover['url'])
    def read(n,m):
        raw=gzip.decompress((ROOT/(n+'.gz')).read_bytes())
        assert hashlib.sha256(raw).hexdigest()==m['sha256']
        return raw
    basis=None if symbol=='AER' else {'namespace':'ifrs-full','currency':'EUR' if symbol=='ADSE' else 'USD','tags':{**IFRS,'equity':('EquityAttributableToOwnersOfParent',),'current_assets':('CurrentAssets',),'current_liabilities':('CurrentLiabilities',)}}
    return dict(raw=read(name,exhibit),cover_raw=read(cover_name,cover),url=exhibit['url'],cover_url=cover['url'],cik=cover['cik'],accession=cover['accession'],filed=cover['filed'],report_date=cover['end'],form=cover['form'],basis=basis,document_set=True)


@pytest.mark.parametrize('symbol,count,currency,tag,start,value',[
    ('ADSE',23,'EUR','Revenue','2026-01-01',7352000),
    ('AER',37,'USD','Revenues','2026-04-01',2167213000),
    ('ALC',30,'USD','Revenue','2026-04-01',2823000000),
])
def test_complete_original_document_sets(symbol,count,currency,tag,start,value,monkeypatch):
    monkeypatch.setattr(socket.socket,'connect',lambda *a,**k:pytest.fail('Unexpected network'))
    kwargs=originals(symbol);before=deepcopy(kwargs);p=inline.parse(**kwargs)
    assert kwargs==before and p==inline.parse(**kwargs)
    proof=p['evidence'];assert proof['reported_facts']==count and proof['currency']==currency
    assert proof['sha256']==hashlib.sha256(kwargs['raw']).hexdigest()
    assert proof['cover_evidence']['source_sha256']==hashlib.sha256(kwargs['cover_raw']).hexdigest()
    assert proof['cover_evidence']['methodology']=='sec_inline_document_set_v1'
    rows=p['facts'][tag]['units'][currency]
    assert next(r for r in rows if r['end']=='2026-06-30' and r.get('start')==start)['val']==value
    assert all(r['accn']==kwargs['accession'] and r['filed']==kwargs['filed'] for concept in p['facts'].values() for rows in concept['units'].values() for r in rows)
    # Financial numeric provenance identifies the actual exhibit, while the
    # companion receipt retains the cover's identity/context/unit source.
    assert all(v['source_sha256']==proof['sha256'] for v in p['provenance'].values())
    with pytest.raises(inline.InlineFactsError):inline.parse(**{**kwargs,'document_set':False,'cover_raw':None,'cover_url':None})


@pytest.mark.parametrize('defect',['wrong_cik','wrong_accession','wrong_form','wrong_date','external_url','query_url','no_link','namespace_conflict','base_url','wrong_context_cik','cover_numeric','duplicate_context','cover_missing','duplicate_document'])
def test_cross_document_validation_holds(defect):
    kw=originals('AER')
    if defect=='wrong_cik':kw['cik']='0000000001'
    elif defect=='wrong_accession':kw['accession']='0000000001-26-000001'
    elif defect=='wrong_form':kw['form']='10-Q'
    elif defect=='wrong_date':kw['report_date']='2026-03-31'
    elif defect=='external_url':kw['url']=kw['url'].replace('www.sec.gov','example.com')
    elif defect=='query_url':kw['url']+='?other=1'
    elif defect=='no_link':kw['cover_raw']=kw['cover_raw'].replace(b'aer-20260630_d2.htm',b'unlinked.htm')
    elif defect=='namespace_conflict':kw['raw']=kw['raw'].replace(b'http://fasb.org/us-gaap/2026',b'http://fasb.org/us-gaap/2025')
    elif defect=='base_url':kw['raw']=kw['raw'].replace(b'<head>',b'<head><base href="https://example.com/">')
    elif defect=='wrong_context_cik':kw['cover_raw']=kw['cover_raw'].replace(b'0001378789',b'0000000001')
    elif defect=='cover_numeric':kw['cover_raw']=kw['cover_raw'].replace(b'</body>',b'<ix:nonFraction name="us-gaap:Revenues">1</ix:nonFraction></body>')
    elif defect=='duplicate_context':
        tree=html.fromstring(kw['cover_raw']);tree.append(deepcopy(inline.nodes(tree,'context')[0]));kw['cover_raw']=etree.tostring(tree,method='html')
    elif defect=='cover_missing':kw['cover_raw']=None
    else:kw['cover_url']=kw['url']
    with pytest.raises(inline.InlineFactsError):inline.parse(**kw)


def test_unverified_currency_is_not_converted():
    kwargs=originals('ALC');kwargs['basis']['currency']='EUR'
    with pytest.raises(inline.InlineFactsError):inline.parse(**kwargs)



def directory(symbol):
    manifest=json.loads((ROOT/'directories.json').read_text());name=symbol+'-company.json'
    raw=gzip.decompress((ROOT/(name+'.gz')).read_bytes());assert hashlib.sha256(raw).hexdigest()==manifest[name]['sha256']
    return raw


@pytest.mark.parametrize('symbol',['ADSE','AER','ALC'])
def test_directory_to_cover_to_financial_exhibit(symbol):
    from datetime import date
    from app.services import sec_document_sets as sets
    kw=originals(symbol);raw=directory(symbol);available=date(2026,10,15)
    candidates=sets.candidates(raw,symbol=symbol,cik=kw['cik'],available_by=available)
    metadata=next(c for c in candidates if c['cover_url']==kw['cover_url'])
    class Client:
        def __init__(self):self.calls=[]
        def get(self,url):self.calls.append(url);return {kw['url']:kw['raw'],kw['cover_url']:kw['cover_raw']}[url]
    client=Client();result=sets.acquire_candidate(metadata,raw,symbol=symbol,cik=kw['cik'],available_by=available,client=client,basis=kw['basis'])
    assert client.calls==[kw['cover_url'],kw['url']]
    assert result['facts']==inline.parse(**kw)['facts']
    proof=result['evidence']['document_set_discovery']
    assert proof['directory_sha256']==hashlib.sha256(raw).hexdigest()
    assert proof['statement_url']==kw['url'] and proof['accession']==kw['accession']


@pytest.mark.parametrize('defect',['url','accession','report_date','directory_sha256'])
def test_tampered_candidates_never_fetch(defect):
    from datetime import date
    from app.services import sec_document_sets as sets
    kw=originals('AER');raw=directory('AER');available=date(2026,10,15)
    meta=next(c for c in sets.candidates(raw,symbol='AER',cik=kw['cik'],available_by=available) if c['cover_url']==kw['cover_url'])
    meta['cover_url' if defect=='url' else defect]='https://example.com/forged'
    class Client:
        def get(self,url):pytest.fail('Tampered metadata reached transport')
    with pytest.raises(inline.InlineFactsError,match='candidate_not_in_directory'):
        sets.acquire_candidate(meta,raw,symbol='AER',cik=kw['cik'],available_by=available,client=Client())


@pytest.mark.parametrize('defect',['symbol','cik','shape','not_object','unsafe_file'])
def test_invalid_directory_holds(defect):
    from datetime import date
    from app.services import sec_document_sets as sets
    kw=originals('AER');value=json.loads(directory('AER'))
    if defect=='symbol':value['tickers']=['WRONG']
    elif defect=='cik':value['cik']=1
    elif defect=='shape':value['filings']['recent']['form'].pop()
    elif defect=='not_object':value=[]
    else:value['filings']['recent']['primaryDocument']=['../outside.htm']*len(value['filings']['recent']['form'])
    with pytest.raises(inline.InlineFactsError):sets.candidates(json.dumps(value).encode(),symbol='AER',cik=kw['cik'],available_by=date(2026,10,15))



def integration_inputs():
    from datetime import datetime,timezone
    kw=originals('AER');manifest=json.loads((ROOT/'directories.json').read_text())
    raw=gzip.decompress((ROOT/'AER-facts.json.gz').read_bytes());assert hashlib.sha256(raw).hexdigest()==manifest['AER-facts.json']['sha256']
    company=directory('AER');urls={kw['url']:kw['raw'],kw['cover_url']:kw['cover_raw'],manifest['AER-facts.json']['url']:raw,manifest['AER-company.json']['url']:company}
    class Client:
        issuer_document_urls=frozenset()
        def __init__(self):self.calls=[]
        def get(self,url):self.calls.append(url);return urls[url]
    return raw,company,kw['cik'],Client,datetime(2026,10,17,2,20,tzinfo=timezone.utc)


EXPECTED_METRICS={'net_margin':37.85381190257698,'revenue_growth':12.244553145307705,'roe':18.661845073534135}


def test_shared_acquisition_projection_cache_and_historical_repeat(monkeypatch):
    from unittest.mock import patch
    from sqlalchemy import create_engine,select,func
    from sqlalchemy.orm import sessionmaker
    from app.models import InsightsSnapshot
    from app.services import sec_fundamentals as sec,sec_financial_statements as statements,sec_fundamentals_preparation as prep
    facts,company,cik,Client,now=integration_inputs();client=Client()
    monkeypatch.setattr(socket.socket,'connect',lambda *a,**k:pytest.fail('Unexpected network'))
    supplement=inline.acquire(facts,company,symbol='AER',cik=cik,observed_at=now,client=client)
    assert len(client.calls)==2
    value=sec.project_fundamentals(facts,company,symbol='AER',cik=cik,observed_at=now,filing_supplement=supplement)
    assert {k:value[k] for k in json.loads(value['source_evidence_json'])['calculations']}==EXPECTED_METRICS
    assert value==sec.project_fundamentals(facts,company,symbol='AER',cik=cik,observed_at=now,filing_supplement=supplement)
    panel=statements.project(facts,company,symbol='AER',cik=cik,observed_at=now,filing_supplement=supplement)
    assert len(panel['annual'])==6 and len(panel['quarterly'])==9
    assert all(row['eps'] is None for row in panel['annual']+panel['quarterly'])
    class Clock(type(now)):
        @classmethod
        def now(cls,tz=None):return now
    engine=create_engine('sqlite://');InsightsSnapshot.__table__.create(engine);factory=sessionmaker(bind=engine)
    monkeypatch.setenv('SEC_FUNDAMENTALS_WARMING_ENABLED','1');client=Client()
    with patch.object(prep,'SessionLocal',factory),patch.object(prep,'datetime',Clock),patch.object(prep,'DirectSourceClient',lambda:client),patch.object(prep.sec_directory,'directory',lambda:{'AER':{'cik':cik}}):
        first=prep.prepare('AER');assert first['values']==EXPECTED_METRICS
        count=len(client.calls);assert prep.prepare('AER')==first and len(client.calls)==count
        with factory() as db:assert db.scalar(select(func.count()).select_from(InsightsSnapshot))==1
    engine.dispose()


@pytest.mark.parametrize('field',['source_url','source_sha256','exhibit_url','exhibit_sha256'])
def test_foreign_authorization_binds_both_originals(field):
    from app.services import sec_fundamentals as sec,sec_tagged_foreign as tagged
    facts,company,cik,Client,now=integration_inputs()
    supplement=inline.acquire(facts,company,symbol='AER',cik=cik,observed_at=now,client=Client())
    supplement['evidence']['cover_evidence'][field]='invalid'
    with pytest.raises(tagged.TaggedForeignError):
        sec.project_fundamentals(facts,company,symbol='AER',cik=cik,observed_at=now,filing_supplement=supplement)


def test_domestic_path_never_enters_foreign_discovery(monkeypatch):
    from app.services import sec_document_sets as sets
    # The complete existing domestic-source fixtures are covered separately;
    # this checks the fallback's admission before any directory reparse/fetch.
    from app.services.sec_tagged_foreign import _document_set
    monkeypatch.setattr(sets,'candidates',lambda *a,**k:pytest.fail('Domestic source entered foreign discovery'))
    assert _document_set({}, {'filings':{'recent':{'form':['10-Q']}}},b'{}',symbol='ANY',cik='1',available_by=None,client=None) is None



@pytest.mark.parametrize('metric',['roe','revenue_growth'])
def test_document_set_source_score_watchlist_lifecycle(metric,monkeypatch):
    import test_sec_tagged_foreign_consumers as consumer
    facts,company,cik,Client,now=integration_inputs();kw=originals('AER')
    urls={kw['url']:kw['raw'],kw['cover_url']:kw['cover_raw']}
    monkeypatch.setattr(consumer,'originals',lambda symbol:(facts,company,cik,urls))
    monkeypatch.setattr(consumer,'EXPECTED',{'AER':{'metrics':EXPECTED_METRICS}})
    monkeypatch.setattr(consumer,'NOW',now)
    consumer.test_original_source_score_and_alert_recovery_repeat_crossing_rollback('AER',metric,monkeypatch)
