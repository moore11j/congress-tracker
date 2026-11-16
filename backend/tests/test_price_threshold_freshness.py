from datetime import datetime, timedelta, timezone
import json

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from app.db import Base
from app.models import (QuoteCache, PriceCache, UserAccount, Watchlist, WatchlistItem,
                        Security, WatchlistAlertRule, WatchlistAlertRuleState, MonitoringAlert)
from app.services.custom_alert_rules import _metric_value, evaluate_watchlist_custom_alerts, validate_conditions

NOW = datetime(2026, 11, 6, 20, tzinfo=timezone.utc)


def test_recent_quote_from_previous_new_york_date_is_unavailable():
    engine=create_engine('sqlite://');Base.metadata.create_all(engine)
    now=datetime(2026,11,9,5,5,tzinfo=timezone.utc)
    with Session(engine) as db:
        quote=QuoteCache(symbol='TEST',price=120,asof_ts=now-timedelta(minutes=10))
        db.add(quote);db.flush()
        assert _metric_value(db,'TEST',{'metric':'price'},now)==(None,[])
        quote.asof_ts=now;db.flush()
        assert _metric_value(db,'TEST',{'metric':'price'},now)==(120,[])
    engine.dispose()


@pytest.mark.parametrize('offset', [timedelta(days=-5), timedelta(minutes=-31), timedelta(minutes=1)])
def test_price_threshold_rejects_stale_or_future_quote_and_prior_close(offset):
    engine = create_engine('sqlite://')
    Base.metadata.create_all(engine)
    with Session(engine) as db:
        db.add_all([QuoteCache(symbol='TEST', price=120, asof_ts=NOW+offset),
                    PriceCache(symbol='TEST', date='2026-11-05', close=110)])
        db.flush()
        assert _metric_value(db, 'TEST', {'metric':'price'}, NOW) == (None, [])
        db.add(PriceCache(symbol='TEST', date='2026-11-06', close=95))
        db.flush()
        assert _metric_value(db, 'TEST', {'metric':'price'}, NOW) == (95, [])
    engine.dispose()


def test_missing_current_price_preserves_state_and_recovery_does_not_replay_crossing():
    engine = create_engine('sqlite://')
    Base.metadata.create_all(engine)
    with Session(engine) as db:
        user=UserAccount(email='freshness@example.test')
        db.add(user);db.flush()
        watchlist=Watchlist(name='Freshness',owner_user_id=user.id)
        security=Security(symbol='TEST',name='Test',asset_class='equity')
        db.add_all([watchlist,security]);db.flush()
        db.add(WatchlistItem(watchlist_id=watchlist.id,security_id=security.id,target_type='ticker'))
        rule=WatchlistAlertRule(user_id=user.id,watchlist_id=watchlist.id,name='Threshold',enabled=True,
            scope_type='any_watchlist_ticker',match_type='all',delivery='both',
            conditions_json=json.dumps(validate_conditions([dict(metric='price',operator='gt',comparison_type='value',comparison_value=100)])))
        db.add(rule);db.flush()
        state=WatchlistAlertRuleState(rule_id=rule.id,ticker='TEST',current_result=False,previous_result=False,
            last_evaluated_at=NOW-timedelta(days=5),values_json='{"0":90}')
        quote=QuoteCache(symbol='TEST',price=120,asof_ts=NOW-timedelta(days=5))
        db.add_all([state,quote]);db.commit()
        before=(state.current_result,state.previous_result,state.last_evaluated_at,state.values_json)
        result=evaluate_watchlist_custom_alerts(db,user_id=user.id,watchlist_id=watchlist.id,now=NOW)
        assert result['unavailable']==1 and result['triggered']==0
        assert (state.current_result,state.previous_result,state.last_evaluated_at,state.values_json)==before
        quote.asof_ts=NOW;db.commit()
        result=evaluate_watchlist_custom_alerts(db,user_id=user.id,watchlist_id=watchlist.id,now=NOW)
        assert result['initialized']==1 and result['triggered']==0
        assert db.scalar(select(MonitoringAlert.id)) is None
    engine.dispose()
