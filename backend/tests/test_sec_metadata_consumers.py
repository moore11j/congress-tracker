from datetime import datetime, timedelta, timezone
import hashlib
import json
import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session
from app.db import Base
from app.models import InsightsSnapshot, TickerMeta, CikMeta
from app.services import sec_directory, sec_metadata, ticker_meta

CIK='0000123456'
DIRECTORY={'fields':['cik','name','ticker','exchange'],'data':[[123456,'Zebra Example Inc','ZBEX','Nasdaq'],[123456,'Zebra Example Inc','ZBEX.A','NYSE']]}
COMPANY={'cik':123456,'name':'Zebra Example Inc','tickers':['ZBEX','ZBEX.A'],'exchanges':['Nasdaq','NYSE'],'sicDescription':'EXAMPLE SIC INDUSTRY'}

@pytest.fixture
def db(monkeypatch):
    engine=create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine,tables=[InsightsSnapshot.__table__,TickerMeta.__table__,CikMeta.__table__])
    monkeypatch.setenv('COMPANY_METADATA_PROVIDER','sec_edgar')
    monkeypatch.setattr('requests.sessions.Session.request',lambda *a,**k:pytest.fail('Unexpected HTTP'))
    monkeypatch.setattr(ticker_meta,'_is_public_request_context',lambda:True)
    with Session(engine) as session:
        yield session
    engine.dispose()

def cache(db,kind,url,data,*,age=0):
    stamp=datetime.now(timezone.utc)-timedelta(days=age)
    db.add(InsightsSnapshot(kind=kind,source='sec_edgar',fetched_at=stamp,payload_json=json.dumps({
        'source':'sec_edgar','source_url':url,'sha256':hashlib.sha256(json.dumps(data).encode()).hexdigest(),
        'observed_at':stamp.isoformat(),'data':data})))
    db.commit()

def seed(db,company=True,age=0):
    cache(db,'sec-directory:exchange:v1',sec_directory.URL,sec_directory.parse_directory(DIRECTORY),age=age)
    if company:cache(db,f'sec-company:{CIK}:v1',f'https://data.sec.gov/submissions/CIK{CIK}.json',COMPANY,age=age)

def test_selected_identity_preserves_legacy_fields_and_rollback(db,monkeypatch):
    seed(db)
    old=datetime(2026,1,1)
    legacy=TickerMeta(symbol='ZBEX',company_name='Legacy Company',exchange='Old',sector='Old sector',industry='Old industry',country='Old country',updated_at=old)
    cik=CikMeta(cik=CIK,company_name='Legacy Holder',updated_at=old)
    db.add_all([legacy,cik]);db.commit()
    before=[{c.name:getattr(row,c.name) for c in row.__table__.columns} for row in (legacy,cik)]
    selected=ticker_meta.get_ticker_meta(db,['ZBEX','ZBEX.A'],allow_refresh=False,enqueue_refresh=False)
    assert selected['ZBEX']['company_name']=='Zebra Example Inc' and selected['ZBEX']['sector'] is None
    assert selected['ZBEX']['industry']=='EXAMPLE SIC INDUSTRY' and selected['ZBEX']['classification']=='SEC SIC'
    assert selected['ZBEX']['exchange']=='Nasdaq' and selected['ZBEX.A']['exchange']=='NYSE'
    assert ticker_meta.get_cik_meta(db,[CIK],allow_refresh=False,enqueue_refresh=False)=={CIK:'Zebra Example Inc'}
    assert [{c.name:getattr(row,c.name) for c in row.__table__.columns} for row in (legacy,cik)]==before
    monkeypatch.setenv('COMPANY_METADATA_PROVIDER','fmp')
    assert ticker_meta.get_ticker_meta(db,['ZBEX'],allow_refresh=False,enqueue_refresh=False)['ZBEX']['company_name']=='Legacy Company'
    assert ticker_meta.get_cik_meta(db,[CIK],allow_refresh=False,enqueue_refresh=False)=={CIK:'Legacy Holder'}

def test_prepared_directory_gives_cold_name_without_implying_missing_classification(db):
    seed(db,company=False)
    result=sec_metadata.ticker_metadata(db,['ZBEX','ZBEX.A','UNKNOWN'])
    assert set(result)=={'ZBEX','ZBEX.A'}
    assert result['ZBEX']['company_name']=='Zebra Example Inc'
    assert result['ZBEX']['industry'] is result['ZBEX']['classification'] is None
    assert result['ZBEX']['source']=='sec_edgar'

def test_stale_source_does_not_relabel_legacy_as_current_and_public_only_enqueues(db,monkeypatch):
    seed(db,age=2)
    db.add(TickerMeta(symbol='ZBEX',company_name='Legacy',updated_at=datetime.now(timezone.utc)));db.commit()
    queued=[]
    monkeypatch.setattr('app.services.data_enrichment_queue.enqueue_data_enrichment_job',lambda **k:queued.append(k))
    assert ticker_meta.get_ticker_meta(db,['ZBEX'],allow_public_live_refresh=True)=={}
    assert len(queued)==1 and queued[0]['job_type']=='ticker_meta'
    assert db.get(TickerMeta,'ZBEX').company_name=='Legacy'

@pytest.mark.parametrize('field,value',[('source_url','https://example.com/wrong'),('sha256','bad'),('source','fmp')])
def test_source_identity_mismatch_fails_closed(db,field,value):
    seed(db)
    row=db.get(InsightsSnapshot,'sec-directory:exchange:v1');payload=json.loads(row.payload_json);payload[field]=value;row.payload_json=json.dumps(payload);db.commit()
    assert sec_metadata.ticker_metadata(db,['ZBEX'])=={}

def test_wrong_company_listing_cannot_supply_industry_or_exchange(db):
    seed(db)
    row=db.get(InsightsSnapshot,f'sec-company:{CIK}:v1');payload=json.loads(row.payload_json);payload['data']['tickers']=['WRONG'];row.payload_json=json.dumps(payload);db.commit()
    result=sec_metadata.ticker_metadata(db,['ZBEX'])['ZBEX']
    assert result['company_name']=='Zebra Example Inc' and result['exchange']=='Nasdaq' and result['industry'] is None

def test_selected_cold_search_and_index_use_same_directory_identity(db):
    seed(db)
    from app.services.search_suggest import _exact_ticker_suggestion,_ticker_suggestions
    from app.services.universal_search import _stock_entities
    exact=_exact_ticker_suggestion(db,'ZBEX.A')
    assert exact['symbol']=='ZBEX-A'
    results=_ticker_suggestions(db,'Zebra Example',10)
    assert {r['symbol'] for r in results}>={'ZBEX','ZBEX-A'}
    assert len(results)==len({r['symbol'] for r in results})
    entities={r.entity_id:r for r in _stock_entities(db)}
    assert entities['stock:ZBEX-A'].source_table=='sec_directory'
    assert 'stock:ZBEX.A' not in entities


def test_selected_debug_reader_cannot_contact_fmp(db):
    assert ticker_meta.debug_stable_search_row('ZBEX')=={'error':'fmp_identity_not_selected'}


def test_worker_refresh_uses_separate_cache_and_releases_transaction(db,monkeypatch):
    seed(db,company=False)
    db.add(TickerMeta(symbol='ZBEX',company_name='Preserved',updated_at=datetime(2026,1,1)));db.commit()
    monkeypatch.setattr(ticker_meta,'_is_public_request_context',lambda:False)
    calls=[]
    def refresh(symbol):
        assert not db.in_transaction()
        calls.append(symbol)
        return 'Unused tuple','Nasdaq',None,None,None
    monkeypatch.setattr(sec_directory,'symbol_metadata',refresh)
    assert ticker_meta.get_ticker_meta(db,['ZBEX'],enqueue_refresh=False)['ZBEX']['company_name']=='Zebra Example Inc'
    assert calls==['ZBEX'] and db.get(TickerMeta,'ZBEX').company_name=='Preserved'


def test_global_shutdown_also_blocks_legacy_debug_reader(db,monkeypatch):
    monkeypatch.setenv('COMPANY_METADATA_PROVIDER','fmp')
    monkeypatch.setenv('FMP_PROVIDER_DISABLED','1')
    assert ticker_meta.debug_stable_search_row('ZBEX').get('error')


def test_stale_directory_cannot_replace_existing_search_index(db):
    from app.models import SearchEntity,SearchEntityTerm
    from app.clients.direct_sources import DirectSourceError
    from app.services.universal_search import rebuild_search_entities
    SearchEntity.__table__.create(db.get_bind(),checkfirst=True)
    SearchEntityTerm.__table__.create(db.get_bind(),checkfirst=True)
    seed(db,age=2)
    row=SearchEntity(entity_id='stock:LEG',entity_type='stock',display_name='Retained',canonical_url='/ticker/LEG',search_text='Retained LEG',normalized_search_text='retained leg',compact_search_text='retainedleg')
    db.add(row);db.commit()
    with pytest.raises(DirectSourceError,match='directory_unavailable'):
        rebuild_search_entities(db)
    assert db.scalar(select(SearchEntity.entity_id))=='stock:LEG'
