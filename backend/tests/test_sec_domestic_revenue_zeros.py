from copy import deepcopy
from datetime import date, datetime, timezone
from pathlib import Path
import gzip
import hashlib
import json
import socket
import pytest
from lxml import html
from app.services import sec_domestic_revenue_zeros as zero
from app.services import sec_fundamentals as sec

ROOT = Path(__file__).parent/'fixtures/sec_domestic_revenue_zeros'
DAY = date(2026, 11, 17)
NOW = datetime(2026, 11, 18, 19, 45, tzinfo=timezone.utc)


def originals(symbol):
    manifest = json.loads((ROOT/'sources.json').read_bytes())
    sources = {k.split(':')[1]:v for k,v in manifest.items() if k.startswith(symbol+':')}
    raw = {k:gzip.decompress((ROOT/v['file']).read_bytes()) for k,v in sources.items()}
    assert all(len(raw[k]) == v['bytes'] and hashlib.sha256(raw[k]).hexdigest() == v['sha256'] for k,v in sources.items())
    return sources,raw


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    monkeypatch.setattr(socket.socket, 'connect', lambda *a,**k:pytest.fail('Network forbidden'))
    monkeypatch.setenv('SEC_FINANCIAL_DISCOVERY_READ_ENABLED','0')


@pytest.mark.parametrize('symbol', ['MNOV'])
def test_complete_originals_qualify_two_zeros_and_recover_prior_ttm(symbol):
    sources,raw = originals(symbol)
    payload,company = json.loads(raw['facts']),json.loads(raw['company'])
    cik = str(company['cik']).zfill(10)
    expected = zero.plan(payload,company,cik=cik,available_by=DAY)
    assert expected is not None
    calls=[]
    class Client:
        def get(self,url):
            calls.append(url)
            return next(raw[k] for k,v in sources.items() if v['url']==url)
    item = zero.acquire(payload,company,cik=cik,available_by=DAY,client=Client())
    assert len(calls)==2 and len(set(calls))==2
    assert len(item['evidence']['rows'])==2
    supplement={'domestic_revenue_zeros':item,'evidence':{'domestic_revenue_zeros':item['evidence']}}
    before={'revenue':sec._records(payload,'revenue',DAY)}
    assert sec._ttm(before['revenue'],date(2025,9,30)) is None
    after=zero.apply(before,payload,company,cik=cik,available_by=DAY,supplement=supplement)
    assert all(after['revenue'][k]==v for k,v in before['revenue'].items())
    old=sec._ttm(after['revenue'],date(2025,9,30))
    assert old['value']=={'BFRG':116670,'MNOV':257918}[symbol]
    assert sec._ttm(after['revenue'],date(2026,9,30))==sec._ttm(before['revenue'],date(2026,9,30))
    tampered=deepcopy(supplement);tampered['domestic_revenue_zeros']['evidence']['rows'][0]['value']=1
    with pytest.raises(ValueError,match='proof_mismatch'):
        zero.apply(before,payload,company,cik=cik,available_by=DAY,supplement=tampered)


@pytest.mark.parametrize('symbol',['MNOV'])
@pytest.mark.parametrize('role',['annual','prior'])
@pytest.mark.parametrize('mutation',['blank','year','issuer','unit','context','expense','hidden','nil'])
def test_original_mutations_cannot_supply_a_zero(symbol,role,mutation):
    from app.services.sec_inline_financials import nodes
    sources,raw=originals(symbol)
    payload,company=json.loads(raw['facts']),json.loads(raw['company'])
    cik=str(company['cik']).zfill(10)
    expected=zero.plan(payload,company,cik=cik,available_by=DAY)
    item=next(r for r in expected['rows'] if r['filing']['url']==sources[role]['url'])
    tree=html.fromstring(raw[role],parser=html.HTMLParser(encoding='utf-8'))
    issuer=company['name']
    if mutation=='issuer':issuer='Unrelated Owner'
    elif mutation=='unit':
        for n in nodes(tree,'measure'):
            if n.text=='iso4217:USD':n.text='iso4217:EUR'
    elif mutation=='context':
        for n in nodes(tree,'startdate'):
            if n.text==item['start']:n.text='2024-02-01'
    elif mutation in {'blank','year','hidden','nil'}:
        for n in tree.iter():
            if mutation=='year' and n.text and n.text.strip()=='2024':n.text='2023'
            if n.text and n.text.strip() in {'-','\u2014'}:
                if mutation=='blank':n.text=''
                if mutation=='hidden':n.set('style','display:none')
                if mutation=='nil':n.append(html.fromstring('<ix:nonfraction xsi:nil="true"></ix:nonfraction>'))
    else:
        for n in nodes(tree,'nonfraction'):
            if n.get('name')=='us-gaap:OperatingExpenses':n.set('sign','-')
    with pytest.raises(ValueError):
        zero.qualify(html.tostring(tree,encoding='utf-8'),item,issuer=issuer)


@pytest.mark.parametrize('symbol',['BFRG','MNOV'])
def test_wrong_cik_stale_and_reported_conflict_do_not_request(symbol):
    _,raw=originals(symbol);payload,company=json.loads(raw['facts']),json.loads(raw['company'])
    cik=str(company['cik']).zfill(10)
    with pytest.raises(ValueError,match='identity'):zero.plan(payload,company,cik='0000000001',available_by=DAY)
    assert zero.plan(payload,company,cik=cik,available_by=date(2028,1,1)) is None
    tag=next(iter({p['tag'] for p in sec._records(payload,'revenue',DAY).values() if p}))
    changed=deepcopy(payload)
    changed['facts']['us-gaap'][tag]['units']['USD'].append(dict(start='2024-01-01',end='2024-12-31',val=99,filed='2026-03-20',accn='0000000001-26-000001',form='10-K'))
    assert zero.plan(changed,company,cik=cik,available_by=DAY) is None


def test_bfrg_hidden_nil_keeps_hold_despite_visible_dash_and_equation():
    sources,raw=originals('BFRG');payload,company=json.loads(raw['facts']),json.loads(raw['company'])
    expected=zero.plan(payload,company,cik=str(company['cik']).zfill(10),available_by=DAY)
    for item in expected['rows']:
        source=next(raw[k] for k,v in sources.items() if v['url']==item['filing']['url'])
        with pytest.raises(ValueError,match='reported_or_nil'):
            zero.qualify(source,item,issuer=company['name'])


def test_acquisition_projection_cache_and_repeated_consumer_values():
    from app.services import sec_inline_financials as inline
    from app.services.sec_tagged_foreign import cached_currency_valid
    sources,raw=originals('MNOV');company=json.loads(raw['company']);cik=str(company['cik']).zfill(10)
    calls=[]
    class Client:
        issuer_document_urls=frozenset()
        def get(self,url):
            calls.append(url)
            return next(raw[k] for k,v in sources.items() if v['url']==url)
    before=sec.project_fundamentals(raw['facts'],raw['company'],symbol='MNOV',cik=cik,observed_at=NOW)
    supplement=inline.acquire(raw['facts'],raw['company'],symbol='MNOV',cik=cik,observed_at=NOW,client=Client())
    assert set(calls)=={sources[k]['url'] for k in ('annual','prior')}
    after=sec.project_fundamentals(raw['facts'],raw['company'],symbol='MNOV',cik=cik,observed_at=NOW,filing_supplement=supplement)
    assert all(after[k]==v for k,v in before.items() if k!='source_evidence_json')
    assert after['operating_margin_expansion']==pytest.approx(3953.321074957527)
    evidence=json.loads(after['source_evidence_json'])
    assert zero.cached_valid(evidence) and cached_currency_valid(evidence)
    assert sec.project_fundamentals(raw['facts'],raw['company'],symbol='MNOV',cik=cik,observed_at=NOW,filing_supplement=supplement)==after
    from types import SimpleNamespace
    from app.services.fundamentals_cache import fundamentals_summary_from_cache_row
    assert fundamentals_summary_from_cache_row(SimpleNamespace(**before),now=NOW)['status']=='unavailable'
    assert fundamentals_summary_from_cache_row(SimpleNamespace(**after),now=NOW)['status']=='mixed'
    for key in ('source_sha256','value','source_cik','filed','header_cells','column'):
        changed=deepcopy(evidence)
        changed['inline_filing_supplement']['domestic_revenue_zeros']['rows'][0][key]=None
        assert not cached_currency_valid(changed)
    missing=deepcopy(evidence);del missing['inline_filing_supplement']['domestic_revenue_zeros']
    assert not cached_currency_valid(missing)


def test_real_preparation_repeat_and_alert_rebaseline(monkeypatch):
    from datetime import timedelta
    from unittest.mock import patch
    from sqlalchemy import create_engine,select,func
    from sqlalchemy.orm import sessionmaker
    from app.db import Base
    from app.models import (FundamentalsCache,Event,EmailDelivery,MonitoringAlert,Security,
        UserAccount,Watchlist,WatchlistItem,WatchlistAlertRule)
    from app.services import sec_fundamentals_preparation as prep
    from app.services.custom_alert_rules import evaluate_watchlist_custom_alerts,validate_conditions
    sources,raw=originals('MNOV');company=json.loads(raw['company']);cik=str(company['cik']).zfill(10)
    calls=[]
    class Client:
        issuer_document_urls=frozenset()
        def get(self,url):
            calls.append(url)
            return next(raw[k] for k,v in sources.items() if v['url']==url)
    class Clock(datetime):
        @classmethod
        def now(cls,tz=None):return NOW
    engine=create_engine('sqlite://');Base.metadata.create_all(engine);factory=sessionmaker(bind=engine)
    monkeypatch.setenv('SEC_FUNDAMENTALS_WARMING_ENABLED','1')
    with patch.object(prep,'SessionLocal',factory),patch.object(prep,'datetime',Clock),patch.object(prep,'DirectSourceClient',Client),patch.object(prep.sec_directory,'directory',return_value={'MNOV':{'cik':cik}}):
        payload=prep.prepare('MNOV');first=list(calls)
        assert payload['values']['operating_margin_expansion']==pytest.approx(3953.321074957527)
        assert prep.prepare('MNOV')==payload and calls==first
        assert set(first)=={sources[k]['url'] for k in ('facts','company','annual','prior')}
        with factory() as db:
            assert db.scalar(select(func.count()).select_from(FundamentalsCache))==0
            assert db.scalar(select(func.count()).select_from(Event))==0
    monkeypatch.setenv('FUNDAMENTALS_PROVIDER','sec_edgar')
    with factory() as db:
        user=UserAccount(email='zero-proof@example.test');security=Security(symbol='MNOV',name='Zero proof',asset_class='equity')
        db.add_all([user,security]);db.flush();watch=Watchlist(name='Proof',owner_user_id=user.id);db.add(watch);db.flush()
        db.add(WatchlistItem(watchlist_id=watch.id,security_id=security.id,target_type='ticker'))
        # Margin expansion is a scoring input; revenue growth is the supported
        # custom-alert input recovered by the same verified prior revenue.
        metric='revenue_growth';threshold=payload['values'][metric]-1
        rule=WatchlistAlertRule(user_id=user.id,watchlist_id=watch.id,name='Margin',enabled=True,scope_type='any_watchlist_ticker',match_type='all',delivery='both',
            conditions_json=json.dumps(validate_conditions([dict(metric=metric,operator='gt',comparison_type='value',comparison_value=threshold)])))
        row=FundamentalsCache(symbol='MNOV',provider='sec_edgar',status='ok',fetched_at=NOW,
            source_evidence_json=json.dumps({'financial_status':'unavailable','calculations':{}}))
        db.add_all([rule,row]);db.commit()
        def run(minutes):
            result=evaluate_watchlist_custom_alerts(db,user_id=user.id,watchlist_id=watch.id,now=NOW+timedelta(minutes=minutes));db.commit();return result['triggered']
        assert run(0)==0
        for k,v in payload['values'].items():setattr(row,k,v)
        row.source_evidence_json=json.dumps(payload['calculation_evidence'])
        assert run(5)==run(10)==0
        setattr(row,metric,threshold-1);assert run(15)==0
        setattr(row,metric,payload['values'][metric]);assert run(20)==1 and run(25)==0
        assert db.scalar(select(func.count()).select_from(MonitoringAlert))==1
        assert db.scalar(select(func.count()).select_from(EmailDelivery))==0
        assert db.scalar(select(func.count()).select_from(Event))==0
    engine.dispose()
