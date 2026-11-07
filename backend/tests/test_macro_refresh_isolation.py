"""Actual database durability and request isolation during optional AI failures."""
from datetime import date,datetime,timezone
from types import SimpleNamespace
import json,socket
import pytest
from sqlalchemy import create_engine,select,func,event
from sqlalchemy.orm import Session
from app.db import Base
from app.models import MacroPositioningAsset,MacroPositioningCache,MacroPositioningFeedEvent,Security,TickerMeta,FundamentalsCache
from app.services import macro_positioning as m

DAY=date(2026,11,3)


@pytest.fixture
def database(tmp_path,monkeypatch):
    def denied(*a,**k):pytest.fail('External network forbidden')
    monkeypatch.setattr(socket.socket,'connect',denied)
    engine=create_engine('sqlite:///'+str(tmp_path/'macro.sqlite'),pool_size=1,max_overflow=0,pool_timeout=.1)
    Base.metadata.create_all(engine)
    with Session(engine) as db:
        db.add_all([MacroPositioningAsset(asset_key=s.asset_key,display_name=s.display_name,
            bias='bullish',rating=4,positioning_date=DAY,fetched_at=datetime(2026,11,6,22,tzinfo=timezone.utc),
            payload_json=json.dumps({'percentile':72,'trend':'increasing','trend_weeks':2})) for s in m._CFTC_MARKET_SPECS])
        db.add_all([Security(symbol='T'+str(i).zfill(3),name='Example '+str(i),asset_class='stock',sector='Technology') for i in range(64)])
        db.commit()
        yield db,engine
    engine.dispose()


def snapshots(engine):
    with Session(engine) as independent:
        return {r.symbol:(r.updated,r.overall,r.rating,r.source_refresh_at,json.loads(r.drivers_json))
                for r in independent.scalars(select(MacroPositioningCache))}


def test_actual_rate_limit_cannot_rollback_data_or_repeat_across_tickers(database,monkeypatch):
    db,engine=database
    from app.services import ai_marketing
    monkeypatch.setattr(ai_marketing,'resolved_setting_value',lambda *a,**k:'offline-fixture')
    calls=[]
    def rejected(**kw):
        calls.append(len(snapshots(engine)))
        return SimpleNamespace(status_code=429)
    monkeypatch.setattr(m,'audited_openai_request',rejected)
    result=m.refresh_macro_positioning_cache(db)
    assert calls==[64]
    assert result['refreshed']==64 and result['commentary_attempted']==1 and result['commentary_enriched']==0
    first=snapshots(engine)
    assert len(first)==64 and {v[0] for v in first.values()}=={DAY}
    assert all(f['interpretation_source']=='derived' for v in first.values() for f in v[-1])
    assert m.refresh_macro_positioning_cache(db)['refreshed']==64
    assert len(calls)==2 and snapshots(engine)==first


def test_successful_commentary_is_bounded_and_preserves_all_numeric_facts(database,monkeypatch):
    db,engine=database;calls=[];before=[]
    def generated(session,*,symbol,profile,factors):
        calls.append(symbol)
        if not before:before.append(snapshots(engine))
        return {'summary':'Current positioning supports this company.',
                'factors':[{'factor':f['factor'],'confidence':'HIGH','why_macro':'Observed positioning.',
                'ticker_readthrough':'Current evidence supports this exposure.','watch_condition':'Changes in positioning.'} for f in factors]}
    monkeypatch.setattr(m,'_generate_ticker_interpretation',generated)
    result=m.refresh_macro_positioning_cache(db)
    assert result['refreshed']==64 and result['commentary_attempted']==result['commentary_enriched']==len(calls)==3
    after=snapshots(engine);assert len(before[0])==len(after)==64
    text={'why_macro','ticker_readthrough','watch_condition','confidence','interpretation_source'}
    for symbol,old in before[0].items():
        assert old[:4]==after[symbol][:4]
        strip=lambda rows:[{k:v for k,v in f.items() if k not in text} for f in rows]
        assert strip(old[-1])==strip(after[symbol][-1])


def test_interrupted_data_pass_preserves_completed_batch_without_model_calls(database,monkeypatch):
    db,engine=database;original=m._interpret_mapping;seen=[]
    def interrupted(*a,**k):
        seen.append(1)
        if len(seen)==52:raise RuntimeError('Simulated interruption')
        return original(*a,**k)
    monkeypatch.setattr(m,'_interpret_mapping',interrupted)
    monkeypatch.setattr(m,'_generate_ticker_interpretation',lambda *a,**k:pytest.fail('Data pass must finish first'))
    with pytest.raises(RuntimeError,match='Simulated interruption'):m.refresh_macro_positioning_cache(db)
    db.rollback()
    assert len(snapshots(engine))==50


def test_insights_cache_miss_never_generates_or_writes_on_request(database,monkeypatch):
    db,engine=database;writes=[];calls=[]
    monkeypatch.setattr(m,'macro_positioning_feature_enabled',lambda db:True)
    def forbidden(*a,**k):calls.append(1);raise AssertionError('Generation on request')
    monkeypatch.setattr(m,'_generate_market_picture_summary',forbidden)
    from app.services import ai_marketing
    monkeypatch.setattr(ai_marketing,'resolved_setting_value',lambda *a,**k:'offline-fixture')
    def sql(conn,cursor,statement,parameters,context,executemany):
        if statement.lstrip().upper().startswith(('INSERT','UPDATE','DELETE')):writes.append(statement)
    event.listen(engine,'before_cursor_execute',sql)
    try:
        result=m.get_insights_macro_positioning(db)
        assert result['summary'] and result['summary_source']=='derived' and result['markets']
        assert not writes and not calls
    finally:event.remove(engine,'before_cursor_execute',sql)


def test_prepared_summary_stays_available_without_generation(database,monkeypatch):
    db,engine=database;calls=[]
    monkeypatch.setattr(m,'macro_positioning_feature_enabled',lambda db:True)
    from app.services import ai_marketing
    monkeypatch.setattr(ai_marketing,'resolved_setting_value',lambda *a,**k:'offline-fixture')
    def generated(*a,**k):calls.append(1);return 'Positioning supports equities while currency exposure remains mixed.'
    monkeypatch.setattr(m,'_generate_market_picture_summary',generated)
    first=m.get_insights_macro_positioning(db,prepare_summary=True)
    assert first['summary_source']=='openai'
    assert m.get_insights_macro_positioning(db)==first and calls==[1]


def test_weekly_feed_survives_later_ticker_interruption(database,monkeypatch):
    db,engine=database
    from app.jobs import refresh_macro_positioning as job
    monkeypatch.setattr(job,'ensure_macro_positioning_schema',lambda:None)
    monkeypatch.setattr(job,'SessionLocal',lambda:Session(engine))
    monkeypatch.setattr(job,'ingest_macro_positioning_assets',lambda db:{'status':'ok','refreshed':11})
    def interrupted(db):raise RuntimeError('Ticker interruption')
    monkeypatch.setattr(job,'refresh_macro_positioning_cache',interrupted)
    with pytest.raises(RuntimeError,match='Ticker interruption'):job.run_macro_positioning_weekly_refresh()
    with Session(engine) as other:
        rows=list(other.scalars(select(MacroPositioningFeedEvent)))
        assert rows and {r.report_date for r in rows}=={DAY}
        ids={r.event_id for r in rows}
        m.refresh_macro_positioning_feed_events(other)
        assert {r.event_id for r in other.scalars(select(MacroPositioningFeedEvent))}==ids


def test_weekly_job_stops_optional_requests_after_first_failure(database,monkeypatch):
    db,engine=database;calls=[]
    from app.jobs import refresh_macro_positioning as job
    from app.services import ai_marketing
    monkeypatch.setattr(job,'ensure_macro_positioning_schema',lambda:None)
    monkeypatch.setattr(job,'SessionLocal',lambda:Session(engine))
    monkeypatch.setattr(job,'ingest_macro_positioning_assets',lambda db:{'status':'ok','refreshed':11})
    monkeypatch.setattr(m,'macro_positioning_feature_enabled',lambda db:True)
    monkeypatch.setattr(ai_marketing,'resolved_setting_value',lambda *a,**k:'offline-fixture')
    def rejected(**kw):calls.append(kw['feature']);return SimpleNamespace(status_code=429)
    monkeypatch.setattr(m,'audited_openai_request',rejected)
    result=job.run_macro_positioning_weekly_refresh()
    assert result['status']=='ok' and result['cache']['refreshed']==64 and result['feed']['generated']>0
    assert result['summary_source']=='derived' and calls==['ticker_macro_positioning']
    assert len(snapshots(engine))==64


@pytest.mark.parametrize('provider',['fmp','sec_edgar','free_direct'])
def test_bulk_profiles_preserve_selection_precedence_and_case_ambiguity(database,monkeypatch,provider):
    db,engine=database;monkeypatch.setenv('FUNDAMENTALS_PROVIDER',provider)
    db.add(Security(symbol='t000',name='Case duplicate',asset_class='bond',sector='Utilities'))
    db.add_all([TickerMeta(symbol='T001',company_name='Metadata name',industry='First industry'),
                TickerMeta(symbol='t001',company_name='Other case',industry='Second industry')])
    for i,p in enumerate(('fmp','sec_edgar','free_direct')):
        db.add(FundamentalsCache(symbol='T002',provider=p,status='ok',fetched_at=datetime.now(timezone.utc),
            company_name=p,country='CA',industry='Prepared industry',price_to_sales=i+1,market_cap=100+i))
    db.commit()
    symbols=['T'+str(i).zfill(3) for i in range(50)]
    before={s:m._profile_for_symbol(db,s) for s in symbols}
    statements=[]
    def sql(conn,cursor,statement,parameters,context,executemany):
        if statement.lstrip().upper().startswith('SELECT'):statements.append(statement)
    event.listen(engine,'before_cursor_execute',sql)
    try:after=m._profiles_for_symbols(db,symbols)
    finally:event.remove(engine,'before_cursor_execute',sql)
    assert after==before and len(statements)<=9
    assert after['T002']['company_name']==provider
    assert after['T002']['price_to_sales']==('fmp','sec_edgar','free_direct').index(provider)+1
    with pytest.raises(ValueError,match='batch exceeds'):m._profiles_for_symbols(db,symbols+['T050'])


@pytest.mark.parametrize('target',['ticker','market'])
def test_optional_http_does_not_hold_database_slot(database,monkeypatch,target):
    db,engine=database;occupied=[]
    from app.services import ai_marketing
    def key(session,*a,**k):
        assert session.scalar(select(func.count()).select_from(Security))==64
        return 'offline-fixture'
    monkeypatch.setattr(ai_marketing,'resolved_setting_value',key)
    if target=='ticker':
        def rejected(**kw):occupied.append(engine.pool.checkedout());return SimpleNamespace(status_code=429)
        monkeypatch.setattr(m,'audited_openai_request',rejected)
        assert m._generate_ticker_interpretation(db,symbol='T000',profile={},factors=[]) is None
    else:
        monkeypatch.setattr(m,'macro_positioning_feature_enabled',lambda db:True)
        def generated(*a,**k):
            occupied.append(engine.pool.checkedout())
            return 'Positioning supports equities while currency exposure remains mixed.'
        monkeypatch.setattr(m,'_generate_market_picture_summary',generated)
        assert m.get_insights_macro_positioning(db,prepare_summary=True)['summary_source']=='openai'
    assert occupied==[0]
