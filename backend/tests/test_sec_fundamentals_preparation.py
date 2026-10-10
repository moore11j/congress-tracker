import json
from datetime import datetime, timedelta, timezone
from sqlalchemy import create_engine, select, func
from sqlalchemy.orm import sessionmaker
import pytest
from app.db import Base
from app.models import InsightsSnapshot, FundamentalsCache, Event
from app.services import sec_fundamentals_preparation as prep, sec_directory
from app.clients.direct_sources import DirectSourceClient, DirectSourceError
from test_sec_fundamentals import sources


@pytest.fixture
def fixture(monkeypatch):
    engine=create_engine('sqlite:///:memory:');Base.metadata.create_all(engine)
    factory=sessionmaker(bind=engine)
    for module in [prep,sec_directory]:monkeypatch.setattr(module,'SessionLocal',factory)
    monkeypatch.setenv('SEC_FUNDAMENTALS_WARMING_ENABLED','1')
    monkeypatch.setenv('FUNDAMENTALS_PROVIDER','fmp')
    facts,company=sources();calls=[]
    def get(client,url):
        calls.append(url)
        if url==sec_directory.URL:
            data={'fields':['cik','name','ticker','exchange'],'data':[[1,'Sample','ABC','NYSE']]}
        else:data=facts if 'companyfacts' in url else company
        return json.dumps(data).encode()
    monkeypatch.setattr(DirectSourceClient,'get',get)
    yield factory,facts,company,calls
    engine.dispose()


def test_current_ratios_preserve_provenance_and_never_fetch_market_data(fixture):
    factory,facts,company,calls=fixture
    result=prep.prepare('ABC')
    assert result['status']=='partial' and result['values']['current_ratio']==2
    assert result['values']['operating_margin']==20
    assert set(result['values'])<=set(prep.METRICS)
    assert not result['coverage']['market_data_included']
    assert result['calculation_evidence']['current']['revenue']['value']==130
    assert result['source_evidence']['facts_sha256']==result['calculation_evidence']['facts']['sha256']
    assert len(calls)==3 and all('sec.gov' in url for url in calls)
    with factory() as db:
        before=db.get(InsightsSnapshot,'sec-current-fundamentals:ABC:v1').payload_json
        assert db.scalar(select(func.count()).select_from(FundamentalsCache))==0
        assert db.scalar(select(func.count()).select_from(Event))==0
    assert prep.prepare('ABC')==result and len(calls)==3
    with factory() as db:assert db.get(InsightsSnapshot,'sec-current-fundamentals:ABC:v1').payload_json==before


@pytest.mark.parametrize('reason',['absent','coverage','404'])
def test_known_absence_is_explicit_and_cached(fixture,monkeypatch,reason):
    factory,facts,company,calls=fixture
    original=DirectSourceClient.get
    if reason=='coverage':facts['facts']['us-gaap']={}
    if reason=='404':
        def missing(client,url):
            if 'companyfacts' in url:
                calls.append(url);raise DirectSourceError('Source HTTP 404: '+url)
            return original(client,url)
        monkeypatch.setattr(DirectSourceClient,'get',missing)
    symbol='ZZZX' if reason=='absent' else 'ABC'
    result=prep.prepare(symbol);assert result['status']=='unavailable' and result['values']=={}
    assert result['reason'] in ['symbol_absent_from_sec_directory','unsupported_current_financial_coverage','sec_company_facts_not_found']
    before=len(calls);assert prep.prepare(symbol)==result and len(calls)==before


@pytest.mark.parametrize('failure',['identity','refusal'])
def test_identity_failure_and_transient_refusal_do_not_write_ratio_cache(fixture,monkeypatch,failure):
    factory,facts,company,calls=fixture
    if failure=='identity':company['cik']=2
    else:
        original=DirectSourceClient.get
        def denied(client,url):
            if 'companyfacts' in url:raise DirectSourceError('Source HTTP 429: '+url)
            return original(client,url)
        monkeypatch.setattr(DirectSourceClient,'get',denied)
    with pytest.raises((DirectSourceError,ValueError)):prep.prepare('ABC')
    with factory() as db:assert db.get(InsightsSnapshot,'sec-current-fundamentals:ABC:v1') is None


def test_disabled_preparation_does_not_open_database(monkeypatch):
    monkeypatch.setenv('SEC_FUNDAMENTALS_WARMING_ENABLED','0')
    monkeypatch.setattr(prep,'SessionLocal',lambda:pytest.fail('database opened'))
    with pytest.raises(ValueError,match='disabled'):prep.prepare('ABC')


def test_failed_refresh_preserves_expired_evidence_and_preparation_change_is_guarded(fixture,monkeypatch):
    factory,facts,company,calls=fixture
    prep.prepare('ABC')
    with factory() as db:
        row=db.get(InsightsSnapshot,'sec-current-fundamentals:ABC:v1')
        row.fetched_at=datetime.now(timezone.utc)-timedelta(days=2);db.commit()
        before=(row.payload_json,str(row.fetched_at))
    original=DirectSourceClient.get
    def changed(client,url):
        data=original(client,url)
        if 'companyfacts' in url:monkeypatch.setenv('SEC_FUNDAMENTALS_WARMING_ENABLED','0')
        return data
    monkeypatch.setattr(DirectSourceClient,'get',changed)
    with pytest.raises(ValueError,match='changed during refresh'):prep.prepare('ABC')
    with factory() as db:
        row=db.get(InsightsSnapshot,'sec-current-fundamentals:ABC:v1')
        assert (row.payload_json,str(row.fetched_at))==before
