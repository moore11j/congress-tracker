from datetime import datetime, date, timezone
import json
import socket

import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker
from app.db import Base
from app.models import PriceCache, InsightsSnapshot, AppSetting
from app.services import sector_etf_snapshot as sectors, price_lookup, insights_snapshots as snapshots
from app.services import fmp_market_snapshot as fmp
from app.request_priority import set_request_context, reset_request_context

NOW=datetime(2026,10,15,3,tzinfo=timezone.utc)
DATES=('2026-10-13','2026-10-14')


@pytest.fixture
def db(monkeypatch):
    network=[]
    def forbidden(*args,**kwargs):
        network.append(True)
        raise AssertionError('Unexpected external connection')
    monkeypatch.setattr(socket.socket,'connect',forbidden)
    engine=create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    monkeypatch.setenv('INSIGHTS_SECTOR_PROVIDER','etf_proxy')
    monkeypatch.setenv('STOCK_PRICE_PROVIDER','massive')
    monkeypatch.setenv('FMP_PROVIDER_DISABLED','true')
    monkeypatch.setenv('FMP_PERSIST_USAGE_EVENTS','false')
    monkeypatch.delenv('INSIGHTS_SECTOR_WARMING_ENABLED',raising=False)
    monkeypatch.setattr(price_lookup,'get_expected_latest_market_date',lambda now=None:date(2026,10,14))
    monkeypatch.setattr(fmp,'_request_payload',lambda *a,**kw:pytest.fail('FMP request attempted'))
    with sessionmaker(bind=engine)() as session:
        yield session
    engine.dispose()
    assert not network


def seed(db, symbol, *, source='massive+corporate_actions'):
    for day,value in zip(DATES,(100,102)):
        db.add(PriceCache(symbol=symbol,date=day,close=value,adjusted_close=value,
            price_source=source,provider_symbol=symbol,adjustment_status='split_adjusted_price_return'))
    db.commit()


def test_complete_eleven_sectors_one_bounded_query_explicit_evidence_and_repeat(db):
    for _,symbol in sectors.TARGETS:seed(db,symbol)
    queries=[]
    def counted(*args):queries.append(args[2])
    event.listen(db.get_bind(),'before_cursor_execute',counted)
    try: items,coverage=sectors.build(db,now=NOW)
    finally:event.remove(db.get_bind(),'before_cursor_execute',counted)
    assert len(queries)==1 and 'upper(' not in queries[0].lower()
    assert len(items)==11 and len({item['symbol'] for item in items})==11
    assert coverage['status']=='ok' and coverage['missing_count']==0
    assert all(item['change_pct']==pytest.approx(2) and item['date']==DATES[1] and item['previous_date']==DATES[0] for item in items)
    assert all(item['is_proxy'] and 'ETF proxy' in item['sector'] and item['price_source']=='massive+corporate_actions' for item in items)
    assert sectors.build(db,now=NOW)==(items,coverage)


@pytest.mark.parametrize('change',['gap','future','raw','mixed_source','zero','negative','nan','wrong_symbol','missing_source'])
def test_unsafe_price_inputs_are_explicitly_missing_not_zero(db,change):
    seed(db,'XLK')
    row=db.get(PriceCache,('XLK',DATES[1]))
    if change=='gap':db.get(PriceCache,('XLK',DATES[0])).date='2026-10-09'
    elif change=='future':row.date='2026-10-15'
    elif change=='raw':row.adjustment_status='raw'
    elif change=='mixed_source':row.price_source='fmp'
    elif change=='zero':row.close=0
    elif change=='negative':row.close=-102
    elif change=='nan':row.close=float('inf')
    elif change=='wrong_symbol':row.provider_symbol='XLF'
    elif change=='missing_source':row.price_source=None
    db.commit()
    items,coverage=sectors.build(db,now=NOW)
    assert items[0]['change_pct'] is None and items[0]['status']=='unavailable'
    assert coverage['status']=='unavailable' and coverage['missing_count']==11


def test_partial_coverage_keeps_all_eleven_sectors_and_zero_return(db):
    seed(db,'XLK')
    db.get(PriceCache,('XLK',DATES[1])).close=100
    db.commit()
    items,coverage=sectors.build(db,now=NOW)
    assert items[0]['change_pct']==0 and items[0]['status']=='ok'
    assert len(items)==11 and coverage['status']=='partial' and coverage['available_count']==1


def test_selected_sector_path_never_requests_fmp_even_with_key(db,monkeypatch):
    monkeypatch.setenv('FMP_API_KEY','dummy')
    assert fmp.get_sector_performance_snapshot()==[]
    assert fmp._build_sector_performance()==[]


def test_cache_switch_and_rollback_preserve_independent_snapshots(db,monkeypatch):
    original={'status':'ok','sector_performance':[{'sector':'Technology','change_pct':99,'source':'sector_performance_snapshot'}]}
    db.add(InsightsSnapshot(kind='macro-snapshot',source='fmp',fetched_at=NOW,payload_json=json.dumps(original)))
    db.commit()
    assert snapshots.get_insights_snapshot(db)['status']=='warming'
    seed(db,'XLK')
    current=snapshots.refresh_insights_snapshot(db)
    assert current['sector_performance'][0]['change_pct']==pytest.approx(2)
    assert current['block_status']['us_sectors']['status']=='partial'
    stored=snapshots.get_insights_snapshot(db)
    assert stored['sector_performance']==current['sector_performance']
    assert json.loads(db.get(InsightsSnapshot,'macro-snapshot').payload_json)==original
    assert db.query(InsightsSnapshot).filter(InsightsSnapshot.kind.like('macro-snapshot%')).count()==2
    again=snapshots.refresh_insights_snapshot(db)
    assert again['sector_performance']==stored['sector_performance']
    assert db.query(InsightsSnapshot).filter(InsightsSnapshot.kind.like('macro-snapshot%')).count()==2
    monkeypatch.setenv('INSIGHTS_SECTOR_PROVIDER','fmp')
    assert snapshots.get_insights_snapshot(db)['sector_performance']==original['sector_performance']


def test_non_builder_path_uses_same_selected_sector_contract(db,monkeypatch):
    seed(db,'XLK')
    monkeypatch.setenv('INSIGHTS_DATA_MODE','legacy')
    monkeypatch.setattr(snapshots,'get_macro_snapshot',lambda:{'status':'unavailable'})
    current=snapshots.refresh_insights_snapshot(db)
    assert current['status']=='partial'
    assert current['sector_performance'][0]['change_pct']==pytest.approx(2)
    assert current['block_status']['us_sectors']['missing_count']==10


def test_warming_disabled_and_public_context_never_hydrate(db,monkeypatch):
    monkeypatch.setattr(sectors,'_hydrate',lambda *a:pytest.fail('Unexpected price fetch'))
    assert sectors.warm(db)['attempted']==0
    monkeypatch.setenv('INSIGHTS_SECTOR_WARMING_ENABLED','true')
    token=set_request_context({'path':'/api/insights/refresh'})
    try:assert sectors.warm(db)['attempted']==0
    finally:reset_request_context(token)


def test_bounded_background_rotation_does_not_starve_after_failures(db,monkeypatch):
    monkeypatch.setenv('INSIGHTS_SECTOR_WARMING_ENABLED','true')
    calls=[]
    def fail(db,symbol,start,end):
        calls.append((symbol,start,end))
        raise RuntimeError('provider unavailable')
    monkeypatch.setattr(sectors,'_hydrate',fail)
    for _ in range(6):
        result=sectors.warm(db,now=NOW)
        assert result['attempted']==2 and result['failed']==2
    assert len(calls)==12 and len({value[0] for value in calls[:11]})==11
    assert all(value[1:]==DATES for value in calls)
    assert db.query(AppSetting).filter_by(key=sectors.CURSOR_SETTING).count()==1


def test_complete_cache_requires_no_warming_calls(db,monkeypatch):
    for _,symbol in sectors.TARGETS:seed(db,symbol)
    monkeypatch.setenv('INSIGHTS_SECTOR_WARMING_ENABLED','true')
    monkeypatch.setattr(sectors,'_hydrate',lambda *a:pytest.fail('Fresh prices fetched again'))
    assert sectors.warm(db,now=NOW)['attempted']==0


def test_bounded_warming_completes_all_sectors_then_stops_and_persists_repeat(db,monkeypatch):
    monkeypatch.setenv('INSIGHTS_SECTOR_WARMING_ENABLED','true')
    calls=[]
    def hydrate(db,symbol,start,end):
        calls.append(symbol)
        assert (start,end)==DATES
        seed(db,symbol)
        return 2
    monkeypatch.setattr(sectors,'_hydrate',hydrate)
    results=[sectors.warm(db,now=NOW) for _ in range(6)]
    assert [r['attempted'] for r in results]==[2,2,2,2,2,1]
    assert len(calls)==len(set(calls))==11
    assert sectors.warm(db,now=NOW)['attempted']==0
    payload=snapshots.refresh_insights_snapshot(db)
    assert payload['block_status']['us_sectors']['status']=='ok'
    assert len(payload['sector_performance'])==11
    assert snapshots.get_insights_snapshot(db)['sector_performance']==payload['sector_performance']
    assert db.query(PriceCache).count()==22
    assert len(calls)==11


def bars():
    from zoneinfo import ZoneInfo
    return {'ticker':'XLK','adjusted':True,'status':'OK','results':[
        {'t':int(datetime.fromisoformat(day).replace(tzinfo=ZoneInfo('America/New_York')).timestamp()*1000),
         'o':value,'h':value+1,'l':value-1,'c':value,'v':100}
        for day,value in zip(DATES,(100,102))]}


def test_actual_massive_adapter_validates_writes_and_repeats_without_fmp(db,monkeypatch):
    from app.clients.massive_stocks import MassiveStocksClient
    calls=[]
    def fetch(self,path,params):
        calls.append((path,params));return bars()
    monkeypatch.setattr(MassiveStocksClient,'get',fetch)
    seed(db,'XLK',source='fmp:historical-price-eod/full')
    db.get(PriceCache,('XLK',DATES[0])).raw_close=200
    db.commit()
    assert sectors.build(db)[0][0]['change_pct'] is None
    for _ in range(2):assert sectors._hydrate(db,'XLK',*DATES)==2
    assert db.query(PriceCache).count()==2
    assert db.get(PriceCache,('XLK',DATES[0])).raw_close is None
    assert sectors.build(db)[0][0]['change_pct']==pytest.approx(2)
    assert len(calls)==2 and all(call[1]['adjusted']=='true' for call in calls)


@pytest.mark.parametrize('change',['wrong_ticker','raw','partial','duplicate','extra_page','future','intraday','negative','invalid_ohlc','dollar_volume_overflow'])
def test_massive_adapter_holds_entire_invalid_pair_without_writes(db,monkeypatch,change):
    from app.clients.massive_stocks import MassiveStocksClient
    payload=bars()
    if change=='wrong_ticker':payload['ticker']='XLF'
    elif change=='raw':payload['adjusted']=False
    elif change=='partial':payload['results'].pop()
    elif change=='duplicate':payload['results'][1]=payload['results'][0].copy()
    elif change=='extra_page':payload['next_url']='https://api.massive.com/page2'
    elif change=='future':payload['results'][1]['t']+=86400000
    elif change=='intraday':payload['results'][1]['t']+=60000
    elif change=='negative':payload['results'][1]['c']=-1
    elif change=='invalid_ohlc':payload['results'][1]['l']=200
    elif change=='dollar_volume_overflow':payload['results'][1]['v']=1e308
    monkeypatch.setattr(MassiveStocksClient,'get',lambda *a:payload)
    with pytest.raises(ValueError):sectors._hydrate(db,'XLK',*DATES)
    assert db.query(PriceCache).count()==0


def test_warming_waits_for_explicit_massive_selection(db,monkeypatch):
    monkeypatch.setenv('INSIGHTS_SECTOR_WARMING_ENABLED','true')
    monkeypatch.setenv('STOCK_PRICE_PROVIDER','fmp')
    monkeypatch.setattr(sectors,'_hydrate',lambda *a:pytest.fail('FMP selection must not warm sectors'))
    assert sectors.warm(db)=={'status':'awaiting_massive_selection','attempted':0}


def test_warmer_transport_is_massive_only_even_when_fmp_is_enabled(db,monkeypatch):
    from types import SimpleNamespace
    import requests
    monkeypatch.setenv('FMP_PROVIDER_DISABLED','false')
    monkeypatch.setenv('FMP_API_KEY','dummy-fmp')
    monkeypatch.setenv('MASSIVE_API_KEY','dummy-massive')
    calls=[]
    def get(url,**kwargs):
        calls.append((url,kwargs))
        return SimpleNamespace(status_code=200,json=bars)
    monkeypatch.setattr(requests,'get',get)
    assert sectors._hydrate(db,'XLK',*DATES)==2
    assert len(calls)==1
    assert calls[0][0]=='https://api.massive.com/v2/aggs/ticker/XLK/range/1/day/2026-10-13/2026-10-14'
    assert calls[0][1]['params']['adjusted']=='true'
    assert calls[0][1]['allow_redirects'] is False
    assert calls[0][1]['timeout']==15
