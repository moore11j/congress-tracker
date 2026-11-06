"""Historical corrections must not create customer-facing new activity."""
from test_direct_13f_publication import db, count, _user, _watchlist
from datetime import date, datetime, timezone
import json
import pytest
from sqlalchemy import select
from app.models import InstitutionalPositionChange, InstitutionalSymbolSummary, InstitutionalActivityEvent, Event, MonitoringAlert, EmailDelivery
from app.services import institutional_activity as activity
from app.services.institutional_historical_rebaseline import rebaseline_historical_activity
from app.services.monitoring_alerts import _ensure_alert_for_event, event_freshness_at

BOUNDARY=date(2026,10,1)

def seed(db, value=500_000_000):
    change=InstitutionalPositionChange(cik='0000000123',holder_name='Example Manager',symbol='NVDA',normalized_symbol='NVDA',
        cusip='67066G104',report_year=2026,report_quarter=2,filing_date=date(2026,8,15),change_type='new_position',
        direction='bullish',curr_shares=1000,prev_shares=0,curr_value_usd=value,prev_value_usd=0,
        value_delta_usd=value,shares_delta_pct=100,value_delta_pct=100,holder_quality_weight=1,
        materiality_score=96,passive_adjusted_score=96,is_material=True)
    summary=InstitutionalSymbolSummary(symbol='NVDA',normalized_symbol='NVDA',report_year=2026,report_quarter=2,
        latest_filing_date=change.filing_date,direction='bullish',total_holders=1,new_positions=1,
        holders_increased=0,holders_reduced=0,exits=0,net_value_delta_usd=value,total_value_usd=value,
        materiality_score=96,accumulation_score=96,distribution_score=0)
    db.add_all([change,summary]);db.commit()
    return change,summary

def snapshot(row):
    return {c.name:getattr(row,c.name) for c in row.__table__.columns if c.name!='updated_at'}

def test_newly_eligible_historical_holder_and_cluster_create_no_feed_events(db):
    change,summary=seed(db,value=500_000)
    activity.generate_activity_events_for_symbol(db,summary);db.commit()
    change.curr_value_usd=change.value_delta_usd=summary.net_value_delta_usd=summary.total_value_usd=500_000_000
    db.commit()
    result=rebaseline_historical_activity(db,summary,publish_since=BOUNDARY);db.commit()
    assert result['inserted_feed_events']==0 and count(db,Event)==0
    activities=list(db.scalars(select(InstitutionalActivityEvent).order_by(InstitutionalActivityEvent.id)))
    assert any(a.feed_visible and a.cik for a in activities)
    before=[snapshot(a) for a in activities]
    again=rebaseline_historical_activity(db,summary,publish_since=BOUNDARY);db.commit()
    assert again['historical_activity_created']==0
    assert [snapshot(a) for a in db.scalars(select(InstitutionalActivityEvent).order_by(InstitutionalActivityEvent.id))]==before
    assert count(db,Event)==count(db,MonitoringAlert)==count(db,EmailDelivery)==0

def test_existing_legacy_feed_corrects_amount_without_retiming_or_duplicate_alert(db):
    change,summary=seed(db)
    activity.generate_activity_events_for_symbol(db,summary);db.flush()
    assert activity.materialize_feed_events_for_symbol(db,summary)>0;db.commit()
    event=db.scalar(select(Event).where(Event.member_bioguide_id==change.cik))
    event.ts=datetime(2026,8,20,15,12,13,tzinfo=timezone.utc)
    event.event_date=datetime(2026,8,18,tzinfo=timezone.utc)
    event.source_document_url='https://www.sec.gov/Archives/retained-source.txt'
    payload=json.loads(event.payload_json)
    payload['source_availability']={'basis':'retained_legacy_report_date','date':'2026-08-20'}
    payload['retained_note']='must survive'
    event.payload_json=json.dumps(payload);db.commit();db.refresh(event)
    user=_user(db,'historical-pro@example.test',tier='pro');watchlist=_watchlist(db,user,alert_triggers=['institutional_activity'])
    assert _ensure_alert_for_event(db,user_id=user.id,watchlist=watchlist,event=event);db.commit()
    alerts=[snapshot(a) for a in db.scalars(select(MonitoringAlert))]
    before=(event.id,event.ts,event.event_date,event.created_at,event.source_filing_id,event.source_document_url,event_freshness_at(event))
    change.curr_value_usd=change.value_delta_usd=summary.total_value_usd=summary.net_value_delta_usd=700_000_000;db.commit()
    result=rebaseline_historical_activity(db,summary,publish_since=BOUNDARY);db.commit();db.refresh(event)
    assert result['inserted_feed_events']==0 and event.amount_min==event.amount_max==700_000_000
    assert before==(event.id,event.ts,event.event_date,event.created_at,event.source_filing_id,event.source_document_url,event_freshness_at(event))
    assert json.loads(event.payload_json)['source_availability']==payload['source_availability']
    assert json.loads(event.payload_json)['retained_note']=='must survive'
    assert not _ensure_alert_for_event(db,user_id=user.id,watchlist=watchlist,event=event)
    assert [snapshot(a) for a in db.scalars(select(MonitoringAlert))]==alerts
    corrected=snapshot(event)
    rebaseline_historical_activity(db,summary,publish_since=BOUNDARY);db.commit();db.refresh(event)
    assert snapshot(event)==corrected and count(db,EmailDelivery)==0

@pytest.mark.parametrize('withdraw',[False,True])
def test_published_sec_evidence_remains_immutable(db,withdraw):
    change,summary=seed(db)
    activity.generate_activity_events_for_symbol(db,summary);db.flush()
    activity.materialize_feed_events_for_symbol(db,summary);db.commit()
    event=db.scalar(select(Event).where(Event.member_bioguide_id==change.cik))
    payload=json.loads(event.payload_json);payload['sec_verification']={'feed':'sec_13f','sha256':'abc'}
    event.payload_json=json.dumps(payload);event.ts=datetime(2026,9,10,12,30);db.commit();db.refresh(event)
    before=snapshot(event)
    change.curr_value_usd=change.value_delta_usd=800_000_000
    if withdraw:change.is_material=False
    db.commit()
    rebaseline_historical_activity(db,summary,publish_since=BOUNDARY);db.commit();db.refresh(event)
    assert snapshot(event)==before and count(db,Event)==1

@pytest.mark.parametrize('invalid',['boundary','missing_summary_date','summary_current','change_current','activity_current'])
def test_current_or_unbounded_scopes_fail_before_changes(db,invalid):
    change,summary=seed(db)
    activity.generate_activity_events_for_symbol(db,summary);db.commit()
    boundary=BOUNDARY
    if invalid=='boundary':boundary=datetime(2026,10,1)
    elif invalid=='missing_summary_date':summary.latest_filing_date=None
    elif invalid=='summary_current':summary.latest_filing_date=BOUNDARY
    elif invalid=='change_current':change.filing_date=BOUNDARY
    else:db.scalar(select(InstitutionalActivityEvent).where(InstitutionalActivityEvent.cik==change.cik)).filing_date=BOUNDARY
    db.commit()
    before=[snapshot(r) for r in db.scalars(select(InstitutionalActivityEvent))]
    with pytest.raises(ValueError):rebaseline_historical_activity(db,summary,publish_since=boundary)
    assert [snapshot(r) for r in db.scalars(select(InstitutionalActivityEvent))]==before

def test_normal_current_publication_still_inserts(db):
    change,summary=seed(db)
    activity.generate_activity_events_for_symbol(db,summary);db.flush()
    assert activity.materialize_feed_events_for_symbol(db,summary)>0
    assert count(db,Event)>0


@pytest.mark.parametrize('legacy_date', ['2026-08-10', None])
def test_legacy_payload_date_and_absence_keep_python_and_sql_activity_time(db,legacy_date):
    from app.routers.events import _event_effective_activity_ts, _event_effective_activity_ts_expr
    change,summary=seed(db)
    activity.generate_activity_events_for_symbol(db,summary);db.flush()
    activity.materialize_feed_events_for_symbol(db,summary);db.commit()
    event=db.scalar(select(Event).where(Event.member_bioguide_id==change.cik))
    payload=json.loads(event.payload_json)
    if legacy_date is None:payload.pop('filing_date')
    else:payload['filing_date']=legacy_date
    event.payload_json=json.dumps(payload);db.commit();db.refresh(event)
    before_python=_event_effective_activity_ts(event)
    before_sql=db.scalar(select(_event_effective_activity_ts_expr(db)).where(Event.id==event.id))
    change.curr_value_usd=change.value_delta_usd=700_000_000;db.commit()
    rebaseline_historical_activity(db,summary,publish_since=BOUNDARY);db.commit();db.refresh(event)
    assert _event_effective_activity_ts(event)==before_python
    assert db.scalar(select(_event_effective_activity_ts_expr(db)).where(Event.id==event.id))==before_sql
    corrected=json.loads(event.payload_json)
    assert corrected.get('filing_date')==legacy_date
    assert corrected['current_activity_filing_date']=='2026-08-15'


@pytest.mark.parametrize('invalid', ['{', '[]', 'null'])
def test_malformed_existing_event_is_held_before_event_mutation(db,invalid):
    change,summary=seed(db)
    activity.generate_activity_events_for_symbol(db,summary);db.flush()
    activity.materialize_feed_events_for_symbol(db,summary);db.commit()
    event=db.scalar(select(Event).where(Event.member_bioguide_id==change.cik))
    row=db.scalar(select(InstitutionalActivityEvent).where(InstitutionalActivityEvent.cik==change.cik))
    event.payload_json=invalid;db.commit();before=snapshot(event)
    with pytest.raises(ValueError):activity._upsert_feed_event(db,row,existing_only=True)
    assert snapshot(event)==before


def test_complete_summary_rebaseline_repeats_without_new_generation_time(db):
    from app.services.institutional_historical_rebaseline import rebaseline_historical_symbol
    change,summary=seed(db)
    rebaseline_historical_symbol(db,'NVDA',2026,2,publish_since=BOUNDARY);db.commit();db.refresh(summary)
    before=snapshot(summary)
    rebaseline_historical_symbol(db,'NVDA',2026,2,publish_since=BOUNDARY);db.commit();db.refresh(summary)
    assert snapshot(summary)==before and count(db,Event)==0
