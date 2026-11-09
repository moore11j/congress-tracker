from datetime import datetime, timedelta, timezone
import json

import pytest
from sqlalchemy import select

from test_massive_price_core import db, isolated
from app.models import (QuoteCache, PriceCache, UserAccount, Watchlist, WatchlistItem,
                        Security, WatchlistAlertRule, WatchlistAlertRuleState, MonitoringAlert)
from app.services.custom_alert_rules import _metric_value, evaluate_watchlist_custom_alerts, validate_conditions
from app.services.quote_lookup import quote_cache_upsert_many

NOW = datetime(2026, 11, 6, 20, tzinfo=timezone.utc)


def verified_quote(db, value, observed=NOW-timedelta(minutes=16)):
    naive = observed.replace(tzinfo=None)
    quote_cache_upsert_many(db, {'TEST': value}, asof_by_symbol={'TEST': naive}, metadata_by_symbol={'TEST': {
        'symbol':'TEST', 'source':'massive_minute_snapshot', 'price_kind':'minute_close',
        'is_delayed':True, 'delay_minutes':15, 'cached_at':NOW.replace(tzinfo=None)}})


def test_absolute_threshold_holds_unverified_legacy_quote_and_daily_bar(db):
    db.add_all([QuoteCache(symbol='TEST',price=120,asof_ts=NOW), PriceCache(symbol='TEST',date='2026-11-06',close=115)])
    db.commit()
    assert _metric_value(db,'TEST',{'metric':'price'},NOW)==(None,[])


@pytest.mark.parametrize('offset', [timedelta(minutes=-31),timedelta(minutes=1),timedelta(days=-1)])
def test_absolute_threshold_rejects_stale_future_and_previous_session_verified_quote(db, offset):
    verified_quote(db,120,NOW+offset)
    assert _metric_value(db,'TEST',{'metric':'price'},NOW)==(None,[])


def test_current_verified_bar_only_available_after_completed_session(db):
    db.add(PriceCache(symbol='TEST',date='2026-11-06',close=115,price_source='massive',adjustment_status='split_adjusted_price_return'))
    db.commit()
    assert _metric_value(db,'TEST',{'metric':'price'},NOW)==(None,[])
    assert _metric_value(db,'TEST',{'metric':'price'},NOW+timedelta(hours=5))==(115,[])


def setup_rule(db, *, comparison=False):
    user=UserAccount(email='absolute-migration@example.test');db.add(user);db.flush()
    watchlist=Watchlist(name='Isolated',owner_user_id=user.id);security=Security(symbol='TEST',name='Test',asset_class='equity')
    db.add_all([watchlist,security]);db.flush();db.add(WatchlistItem(watchlist_id=watchlist.id,security_id=security.id,target_type='ticker'))
    condition=dict(metric='price',operator='gt',comparison_type='value',comparison_value=100)
    if comparison:condition=dict(metric='sma',metric_params={'period':2},operator='gt',comparison_type='metric',comparison_metric='price')
    rule=WatchlistAlertRule(user_id=user.id,watchlist_id=watchlist.id,name='Isolated threshold',enabled=True,
        scope_type='any_watchlist_ticker',match_type='all',delivery='both',conditions_json=json.dumps(validate_conditions([condition])))
    db.add(rule);db.flush()
    state=WatchlistAlertRuleState(rule_id=rule.id,ticker='TEST',current_result=False,previous_result=False,
        last_evaluated_at=NOW-timedelta(minutes=5),values_json='{"0":90}')
    db.add(state);db.commit()
    return user,watchlist,state


def test_source_transition_rebaselines_then_real_crossing_occurs_once(db):
    user,watchlist,state=setup_rule(db)
    def run(minutes):
        result=evaluate_watchlist_custom_alerts(db,user_id=user.id,watchlist_id=watchlist.id,now=NOW+timedelta(minutes=minutes));db.commit();return result
    before=(state.current_result,state.previous_result,state.last_evaluated_at,state.values_json)
    assert run(0)['unavailable']==1
    assert before==(state.current_result,state.previous_result,state.last_evaluated_at,state.values_json)
    verified_quote(db,120)
    result=run(0);assert result['initialized']==1 and result['triggered']==0
    stored=json.loads(state.values_json)
    assert stored['__price_basis']=='massive_stock_price_v1'
    assert stored['__price_observation']['observed_at']==(NOW-timedelta(minutes=16)).isoformat()
    verified_quote(db,90,NOW-timedelta(minutes=15));assert run(1)['triggered']==0
    verified_quote(db,120,NOW-timedelta(minutes=14));assert run(2)['triggered']==1
    assert run(3)['triggered']==0
    alert=db.scalars(select(MonitoringAlert)).one()
    evidence=json.loads(alert.payload_json)['price_observation']
    assert evidence['observed_at']==(NOW-timedelta(minutes=14)).isoformat()
    assert evidence['current_price']==120 and evidence['is_delayed'] is True
    from app.services.email_intraday import _signal_alert_candidate
    candidate=_signal_alert_candidate(user,alert,{'TEST'})
    assert 'Price delayed 15 minutes' in candidate.context['alert_intro']
    assert evidence['observed_at'] in candidate.context['why_notable']
    assert candidate.skip_reason is None


def test_unavailable_comparison_price_preserves_existing_state(db):
    user,watchlist,state=setup_rule(db,comparison=True)
    db.add_all([PriceCache(symbol='TEST',date='2026-11-04',close=110),PriceCache(symbol='TEST',date='2026-11-05',close=120)])
    db.commit();before=(state.current_result,state.previous_result,state.last_evaluated_at,state.values_json)
    result=evaluate_watchlist_custom_alerts(db,user_id=user.id,watchlist_id=watchlist.id,now=NOW)
    assert result['unavailable']==1 and result['triggered']==0
    assert before==(state.current_result,state.previous_result,state.last_evaluated_at,state.values_json)


def test_daily_close_evidence_does_not_invent_an_observation_timestamp(db):
    from app.services.price_alert_reference import daily_price_observation,valid_daily_price_evidence
    from app.services.email_intraday import _signal_alert_candidate
    for day,price in [('2026-11-05',100),('2026-11-06',110)]:
        db.add(PriceCache(symbol='TEST',date=day,close=price,adjusted_close=price,
            price_source='massive',adjustment_status='split_adjusted_price_return'))
    db.commit()
    now=NOW+timedelta(hours=5)
    evidence=daily_price_observation(db,'TEST',now)
    assert evidence['status']=='ok' and valid_daily_price_evidence(evidence,now)
    assert evidence['observed_at'] is None and evidence['price_date']=='2026-11-06'
    user=UserAccount(id=1,email='closed-price@example.test')
    alert=MonitoringAlert(id=1,user_id=1,source_id='1',source_type='watchlist',source_name='Test',
        alert_type='custom_alert',symbol='TEST',event_created_at=now,title='Test',body='Test',
        payload_json=json.dumps(dict(custom_alert=True,price_alert=True,trigger_price=110,
            daily_price_rule=True,price_observation=evidence)))
    candidate=_signal_alert_candidate(user,alert,{'TEST'})
    assert '2026-11-06 daily close' in candidate.context['alert_intro']
    assert candidate.skip_reason is None
