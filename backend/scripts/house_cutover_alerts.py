"""No-send downstream comparison for the isolated House cutover rehearsal."""
import copy
from datetime import datetime, timezone
import json
from unittest.mock import patch

from sqlalchemy import select, func
from app.models import Event, QuoteCache, PriceCache, LeaderboardSnapshot, Security, WatchlistItem, MonitoringAlert, EmailDelivery
from app.services import email_digests as digests, top_stocks, top_ideas_digest
from app.services.monitoring_alerts import _ensure_alert_for_event
from app.services.price_alert_reference import daily_price_observation
from app.services.confirmation_score import _activity_context_source, confirmation_score_bundle_from_source_payloads
from app.services.direct_congress_repair import _record, _digest
from app import main as api


def prepare(db,capture,load):
    import sys
    from pathlib import Path
    sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'tests'))
    from test_email_digests import _user,_watchlist
    # Use the later snapshot for overlapping event records and retain the full
    # member history from the canonical export for cross-document collision checks.
    for record in capture['events']:
        existing=db.get(Event,record['id'])
        if existing is None:load(db,Event,record)
        else:
            for key,value in record.items():
                if key in {'ts','created_at','event_date'} and value:value=datetime.fromisoformat(value)
                setattr(existing,key,value)
    for key,model in [('quotes',QuoteCache),('prices',PriceCache)]:
        for record in capture[key]:load(db,model,record)
    load(db,LeaderboardSnapshot,capture['leaderboard']);db.commit()
    user=_user(db,'house-cutover@example.test');user.email_verified_at=datetime(2026,10,1)
    watch=_watchlist(db,user,alert_triggers=['congress_activity'])
    existing=set(db.scalars(select(WatchlistItem.security_id).where(WatchlistItem.watchlist_id==watch.id)))
    for symbol in capture['symbols']:
        security=db.scalar(select(Security).where(Security.symbol==symbol))
        assert security is not None
        if security.id not in existing:db.add(WatchlistItem(watchlist_id=watch.id,security_id=security.id))
    db.commit()
    for identifier in capture['expected_event_ids']:
        assert _ensure_alert_for_event(db,user_id=user.id,watchlist=watch,event=db.get(Event,identifier))
    db.commit()
    return user,watch


def preview(db,capture,user,watch):
    now=datetime.fromisoformat(capture['captured_at'])
    class FrozenDatetime(datetime):
        @classmethod
        def now(cls,tz=None):return now.astimezone(tz) if tz else now.replace(tzinfo=None)
    bundles={};cards={}
    with patch.object(api,'datetime',FrozenDatetime):
        for symbol,original in capture['bundles'].items():
            card=api._ticker_trade_activity_summary(db,symbol,'congress_trade',lookback_days=30,side='all')
            cards[symbol]=card
            sources=copy.deepcopy(original['sources'])
            sources['congress']=_activity_context_source(card,label='Congress').as_dict()
            bundles[symbol]={**original,**confirmation_score_bundle_from_source_payloads(symbol,sources_payload=sources)}
    board=db.scalar(select(LeaderboardSnapshot).where(LeaderboardSnapshot.leaderboard_key=='top_stocks'))
    payload=json.loads(capture['leaderboard']['payload_json']);rows=payload['candidate_rows']
    for row in rows:
        if row['symbol'] in bundles:row['confirmation']=row['confirmation_bundle']=bundles[row['symbol']]
    ranked=top_stocks._ranked_payload(rows,generated_at=payload['generated_at'])
    board.payload_json=json.dumps({**ranked,'candidate_rows':rows,'score_context_version':payload['score_context_version']});db.commit()
    start=datetime(2026,10,1,tzinfo=timezone.utc);end=datetime(2026,10,9,tzinfo=timezone.utc)
    activity=digests.build_watchlist_activity_digest(db,user,watch,start)
    monitoring=digests.build_monitoring_digest(db,user,watch,start,window_end=end)
    with patch.object(digests,'_upcoming_calendar_events_for_digest',return_value=([],'Calendar outside House correction replay')):
        daily=digests.build_signal_alert_digest(db,user,start,window_end=end)
    ideas=top_ideas_digest.build_top_ideas_digest(db,user)
    return dict(cards=cards,bundles=bundles,activity=activity.items_count,monitoring=monitoring.items_count,
        daily=daily.items_count,top_items=ideas.items,
        monitoring_ids=list(db.scalars(select(MonitoringAlert.id).order_by(MonitoringAlert.id))),
        monitoring_state=_digest([_record(r) for r in db.scalars(select(MonitoringAlert).order_by(MonitoringAlert.id))]),
        price_inputs={s:daily_price_observation(db,s,now) for s in capture['symbols']})


def verify(db,capture,user,watch,before,after):
    assert before['cards']==capture['cards'],'Exported full activity population did not reproduce live cards'
    assert {s:b['score'] for s,b in before['bundles'].items()}=={s:b['score'] for s,b in capture['bundles'].items()}
    for key in ['activity','monitoring','daily','top_items','price_inputs','monitoring_ids','monitoring_state']:assert before[key]==after[key],key
    for identifier in capture['expected_event_ids']:
        assert not _ensure_alert_for_event(db,user_id=user.id,watchlist=watch,event=db.get(Event,identifier))
    assert db.scalar(select(func.count()).select_from(MonitoringAlert))==len(before['monitoring_ids'])
    cadences={}
    for cadence in ['daily','weekly']:
        user.top_stock_ideas_frequency=cadence;db.commit()
        result=top_ideas_digest.run_top_ideas_digest(db,dry_run=True,now=datetime(2026,10,9,20,tzinfo=timezone.utc))
        assert len(result)==1 and result[0]['status']=='would_send';cadences[cadence]=result[0]['status']
    assert db.scalar(select(func.count()).select_from(EmailDelivery))==0
    return dict(counts={k:after[k] for k in ['activity','monitoring','daily']},
        scores={s:dict(before=before['bundles'][s]['score'],after=after['bundles'][s]['score']) for s in capture['symbols']},
        changed_evidence_dates={s:dict(before=before['cards'][s]['latest_date'],after=after['cards'][s]['latest_date']) for s in capture['symbols'] if before['cards'][s]!=after['cards'][s]},
        top_stocks_items=len(after['top_items']),top_stocks_items_unchanged=True,price_inputs_unchanged=True,
        monitoring_alerts=len(after['monitoring_ids']),corrected_source_events=len(capture['expected_event_ids']),
        monitoring_state_unchanged=True,repeat_new_alerts=0,cadence_previews=cadences,emails=0,
        scope='Fresh captured House evidence correction; other source and price inputs retained, calendar excluded')
