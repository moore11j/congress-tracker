"""Complete-original score and supported watchlist lifecycle across foreign issuers."""
from datetime import timedelta
from types import SimpleNamespace
import json,socket
import pytest
from test_sec_tagged_foreign import originals,Client,NOW,EXPECTED


@pytest.mark.parametrize('symbol',list(EXPECTED))
@pytest.mark.parametrize('metric',['current_ratio','revenue_growth'])
def test_original_source_score_and_alert_recovery_repeat_crossing_rollback(symbol,metric,monkeypatch):
    from sqlalchemy import create_engine,select,func
    from sqlalchemy.orm import sessionmaker
    from app.db import Base
    from app.models import (FundamentalsCache,Event,EmailDelivery,MonitoringAlert,
        Security,UserAccount,Watchlist,WatchlistItem,WatchlistAlertRule)
    from app.services import sec_fundamentals as sec,sec_inline_financials as inline
    from app.services.fundamentals_cache import fundamentals_summary_from_cache_row
    from app.services.confirmation_score import _fundamentals_context_source
    from app.services.custom_alert_rules import evaluate_watchlist_custom_alerts,validate_conditions
    monkeypatch.setattr(socket.socket,'connect',lambda *a,**k:pytest.fail('Unexpected network'))
    facts,company,cik,urls=originals(symbol)
    supplement=inline.acquire(facts,company,symbol=symbol,cik=cik,observed_at=NOW,client=Client(urls))
    value=sec.project_fundamentals(facts,company,symbol=symbol,cik=cik,observed_at=NOW,filing_supplement=supplement)
    assert value[metric]==EXPECTED[symbol]['metrics'][metric]
    assert _fundamentals_context_source(fundamentals_summary_from_cache_row(SimpleNamespace(**value),now=NOW))
    engine=create_engine('sqlite://');Base.metadata.create_all(engine);factory=sessionmaker(bind=engine)
    with factory() as db:
        user=UserAccount(email=symbol+'@example.test');security=Security(symbol=symbol,name=symbol,asset_class='equity')
        db.add_all([user,security]);db.flush()
        watch=Watchlist(name='Foreign statements',owner_user_id=user.id);db.add(watch);db.flush()
        db.add(WatchlistItem(watchlist_id=watch.id,security_id=security.id,target_type='ticker'))
        threshold=value[metric]-1
        rule=WatchlistAlertRule(user_id=user.id,watchlist_id=watch.id,name='Financial threshold',enabled=True,scope_type='any_watchlist_ticker',match_type='all',delivery='both',conditions_json=json.dumps(validate_conditions([dict(metric=metric,operator='gt',comparison_type='value',comparison_value=threshold)])))
        row=FundamentalsCache(symbol=symbol,provider='sec_edgar',status='ok',fetched_at=NOW,source_evidence_json=json.dumps({'financial_status':'unavailable','calculations':{}}))
        db.add_all([rule,row]);db.commit();monkeypatch.setenv('FUNDAMENTALS_PROVIDER','sec_edgar')
        def run(minutes):
            result=evaluate_watchlist_custom_alerts(db,user_id=user.id,watchlist_id=watch.id,now=NOW+timedelta(minutes=minutes));db.commit();return result
        assert run(0)['triggered']==0
        for key in EXPECTED[symbol]['metrics']:setattr(row,key,value[key])
        row.source_evidence_json=value['source_evidence_json']
        assert run(5)['triggered']==0 and run(10)['triggered']==0
        setattr(row,metric,threshold-1);assert run(15)['triggered']==0
        setattr(row,metric,value[metric]);assert run(20)['triggered']==1 and run(25)['triggered']==0
        setattr(row,metric,None);row.source_evidence_json=json.dumps({'financial_status':'unavailable','calculations':{}})
        assert run(30)['triggered']==0
        assert db.scalar(select(func.count()).select_from(MonitoringAlert))==1
        assert db.scalar(select(func.count()).select_from(EmailDelivery))==0
        assert db.scalar(select(func.count()).select_from(Event))==0
    engine.dispose()
