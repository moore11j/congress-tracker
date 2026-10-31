"""Selected quote to durable alert replay with all delivery/transport isolated."""
import json
from datetime import datetime,timedelta,timezone
from sqlalchemy import select
from app.models import (UserAccount,Watchlist,WatchlistItem,Security,WatchlistAlertRule,
                        PriceCache,QuoteCache,MonitoringAlert,EmailDelivery,Event)
from app.services.custom_alert_rules import evaluate_watchlist_custom_alerts,validate_conditions
from app.services.price_alert_reference import valid_daily_price_evidence
from app.services import quote_lookup as quotes
from test_massive_price_core import db,isolated,network,response


def test_selected_quote_to_daily_alert_preserves_evidence_and_deduplicates(monkeypatch,db):
    now=datetime(2026,10,6,15,tzinfo=timezone.utc);observed=now-timedelta(minutes=16)
    user=UserAccount(email='massive-replay@example.test')
    watchlist=Watchlist(name='Isolated replay',owner_user_id=1)
    security=Security(symbol='AAPL',name='Apple',asset_class='equity')
    db.add_all([user,watchlist,security]);db.flush();watchlist.owner_user_id=user.id
    db.add(WatchlistItem(watchlist_id=watchlist.id,security_id=security.id,target_type='ticker'))
    reference=PriceCache(symbol='AAPL',date='2026-10-05',close=100)
    db.add_all([reference,QuoteCache(symbol='AAPL',price=110,asof_ts=observed.replace(tzinfo=None))])
    db.add(WatchlistAlertRule(user_id=user.id,watchlist_id=watchlist.id,name='5% increase',enabled=True,
        scope_type='any_watchlist_ticker',match_type='all',delivery='immediate',
        conditions_json=json.dumps(validate_conditions([{'metric':'price_change_pct','operator':'increases_by',
            'comparison_type':'value','comparison_value':5,'time_window':{'value':1,'unit':'day'}}]))))
    db.commit()
    def evaluate(at):
        result=evaluate_watchlist_custom_alerts(db,user_id=user.id,watchlist_id=watchlist.id,now=at)
        db.commit();return result
    held=evaluate(now)
    assert held['triggered']==0 and held['unavailable']==1
    reference.price_source='massive';reference.adjusted_close=100;reference.adjustment_status='split_adjusted_price_return';db.commit()
    network(monkeypatch,lambda u,p:response({'status':'DELAYED','tickers':[{'ticker':'AAPL',
        'min':{'t':observed.timestamp()*1000,'c':110},'prevDay':{'c':100}}]}))
    quotes.get_current_prices_meta_db(db,['AAPL'],force_quote_endpoint=True)
    assert evaluate(now)['triggered']==1
    alert=db.scalars(select(MonitoringAlert)).one();payload=json.loads(alert.payload_json)
    evidence=payload['price_observation']
    assert payload['trigger_price']==110 and evidence['reference_price']==100
    assert evidence['observed_at']==observed.isoformat()
    assert evidence['price_provider']=='massive' and evidence['is_delayed'] and evidence['delay_minutes']==15
    assert valid_daily_price_evidence(evidence,now)
    assert evaluate(now)['triggered']==0
    # Real recrossing within the same session cannot duplicate the durable alert.
    for offset,price in [(1,99),(2,111)]:
        at=observed+timedelta(minutes=offset)
        quotes.quote_cache_upsert_many(db,{'AAPL':price},asof_by_symbol={'AAPL':at},metadata_by_symbol={
            'AAPL':{'source':'massive_minute_snapshot','cached_at':now,'is_delayed':True,'delay_minutes':15,'price_kind':'minute_close'}})
        assert evaluate(now+timedelta(minutes=offset))['triggered']==0
    assert db.query(MonitoringAlert).count()==1
    assert db.query(Event).count()==0 and db.query(EmailDelivery).count()==0
    assert json.loads(db.get(MonitoringAlert,alert.id).payload_json)==payload
