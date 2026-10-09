from datetime import date, datetime, timedelta, timezone
import json
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session

from app.db import Base
from app.models import Event, Security, TickerContentCache, WatchlistItem, MonitoringAlert, EmailDelivery
from app.services.direct_feed_worker import DirectFeedPublication
from app.services.feed_source_control import FeedSourceMismatch, select_feed_source
from app.services.sec_earnings_store import stage_earnings_material
from app.services.sec_press_events import FEED, publish_sec_release
from app.services.watchlist_content_events import _content_event, sync_watchlist_content_events
from test_sec_earnings_materials import company, submission
from test_sec_earnings_store import index
from test_email_digests import _user, _watchlist


@pytest.fixture
def db(monkeypatch):
    monkeypatch.setenv('FMP_PROVIDER_DISABLED', '1')
    monkeypatch.setenv('PRESS_RELEASE_PROVIDER', 'sec_edgar')
    monkeypatch.setenv('SEC_EARNINGS_PUBLISH_AFTER', '2000-01-01T00:00:00+00:00')
    monkeypatch.setattr('requests.sessions.Session.request', lambda *a, **kw: pytest.fail('Unexpected network'))
    engine = create_engine('sqlite:///:memory:'); Base.metadata.create_all(engine)
    with Session(engine) as db:
        db.add(Security(id=1, symbol='TEST', name='Test', asset_class='stock')); db.commit()
        yield db
    engine.dispose()


def stage(db, *, day=None, submissions_accepted_at=None):
    day = day or datetime.now(timezone.utc).date()-timedelta(days=1)
    local = datetime.combine(day, datetime.min.time()).replace(hour=16, tzinfo=ZoneInfo('America/New_York'))
    metadata = company(filingDate=[day.isoformat()], acceptanceDateTime=[submissions_accepted_at or local.astimezone(timezone.utc).isoformat()])
    raw = submission().replace(b'20260730', day.strftime('%Y%m%d').encode())
    page = index().replace(b'2026-07-30', day.isoformat().encode())
    doc = stage_earnings_material(db, company_raw=metadata, submission_raw=raw, index_raw=page,
        symbol='TEST', cik=1234567, accession='0001234567-26-000001')
    db.commit()
    return doc, day


def activate(db, day, *, provider='sec_edgar', generation=0):
    select_feed_source(db, feed=FEED, provider=provider, publish_since=day,
                       expected_generation=generation, reason='isolated test')
    db.commit()


@pytest.mark.parametrize('value', [None, 'invalid', '2026-10-09T12:00:00'])
def test_missing_or_naive_activation_timestamp_cannot_publish(db, monkeypatch, value):
    doc, day = stage(db); activate(db, day)
    if value is None:
        monkeypatch.delenv('SEC_EARNINGS_PUBLISH_AFTER')
    else:
        monkeypatch.setenv('SEC_EARNINGS_PUBLISH_AFTER', value)
    with pytest.raises(ValueError, match='activation timestamp'):
        publish_sec_release(db, doc.id)
    assert db.scalar(select(func.count()).select_from(Event)) == 0


def test_same_day_release_before_activation_is_not_backfilled(db, monkeypatch):
    doc, day = stage(db); activate(db, day)
    accepted = datetime.fromisoformat(json.loads(doc.parsed_json)['sec_accepted_at'])
    monkeypatch.setenv('SEC_EARNINGS_PUBLISH_AFTER', (accepted+timedelta(seconds=1)).isoformat())
    assert publish_sec_release(db, doc.id)['reason'] == 'before_activation_timestamp'
    db.commit()
    assert db.scalar(select(func.count()).select_from(Event)) == 0


def test_changed_activation_timestamp_cannot_reinterpret_published_receipt(db, monkeypatch):
    doc, day = stage(db); activate(db, day)
    first = publish_sec_release(db, doc.id); db.commit()
    monkeypatch.setenv('SEC_EARNINGS_PUBLISH_AFTER', '2000-01-02T00:00:00+00:00')
    with pytest.raises(ValueError, match='state changed'):
        publish_sec_release(db, doc.id)
    db.rollback()
    assert db.get(Event, first['event_id']) is not None


def test_equivalent_database_timezone_does_not_change_event_identity(db):
    from app.services.sec_press_events import _event_hash, _aware
    doc, day = stage(db); activate(db, day)
    result = publish_sec_release(db, doc.id)
    event = db.get(Event, result['event_id'])
    before = _event_hash(event)
    event.event_date = _aware(event.event_date).astimezone(ZoneInfo('America/Los_Angeles'))
    event.ts = _aware(event.ts).astimezone(ZoneInfo('America/Los_Angeles'))
    assert _event_hash(event) == before
    event.ts += timedelta(seconds=1)
    assert _event_hash(event) != before


def test_disabled_by_default_and_pause_blocks(db):
    doc, day = stage(db)
    with pytest.raises(FeedSourceMismatch): publish_sec_release(db, doc.id)
    db.rollback(); activate(db, day, provider='paused')
    with pytest.raises(FeedSourceMismatch): publish_sec_release(db, doc.id)
    db.rollback()
    assert db.scalar(select(func.count()).select_from(Event)) == 0


def test_publication_repeat_retains_identity_dates_and_neutrality(db):
    doc, day = stage(db); activate(db, day)
    result = publish_sec_release(db, doc.id); db.commit()
    event = db.get(Event, result['event_id']); payload = json.loads(event.payload_json)
    assert event.event_date.date() == day
    assert event.ts.date() == datetime.now(timezone.utc).date()
    assert payload['published_at'] is None and payload['can_confirm'] is False
    assert payload['source_availability']['basis'] == 'direct_publication'
    assert payload['filing_date'] == day.isoformat() and event.impact_score == 0
    assert publish_sec_release(db, doc.id)['inserted_events'] == 0
    db.commit()
    assert db.scalar(select(func.count()).select_from(Event)) == 1
    assert db.scalar(select(func.count()).select_from(DirectFeedPublication)) == 1


def test_rollback_removes_event_and_receipt_atomically(db):
    doc, day = stage(db); activate(db, day)
    publish_sec_release(db, doc.id); db.rollback()
    assert db.scalar(select(func.count()).select_from(Event)) == 0
    assert db.scalar(select(func.count()).select_from(DirectFeedPublication)) == 0


@pytest.mark.parametrize('age,after', [(1, True), (10, False)])
def test_historical_or_preboundary_releases_do_not_become_new_alerts(db, age, after):
    doc, day = stage(db, day=datetime.now(timezone.utc).date()-timedelta(days=age))
    activate(db, day+timedelta(days=1) if after else day)
    assert publish_sec_release(db, doc.id)['reason'] == 'outside_recent_publication_boundary'
    db.commit()
    assert db.scalar(select(func.count()).select_from(Event)) == 0


def test_existing_legacy_press_event_is_not_duplicated(db):
    doc, day = stage(db); activate(db, day)
    db.add(Event(event_type='press_release', symbol='TEST', ts=datetime.now(timezone.utc),
                 event_date=datetime.combine(day,datetime.min.time()), source='fmp', payload_json='{}')); db.commit()
    assert publish_sec_release(db, doc.id)['reason'] == 'existing_press_event_requires_reconciliation'
    assert db.scalar(select(func.count()).select_from(Event)) == 1


def test_later_api_clock_cannot_publish_before_it_is_observed(db):
    future = datetime.now(timezone.utc) + timedelta(days=1)
    doc, day = stage(db, submissions_accepted_at=future.isoformat()); activate(db, day)
    result = publish_sec_release(db, doc.id)
    assert result['status'] == 'held' and result['reason'] == 'invalid_observation_order'
    assert db.scalar(select(func.count()).select_from(Event)) == 0


def test_conflicting_api_time_is_retained_without_backdating_alert_arrival(db):
    # Header/index agree at 16:00 Eastern; API has an extra four-hour offset.
    day = datetime.now(timezone.utc).date() - timedelta(days=2)
    api = datetime.combine(day + timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc)
    doc, _ = stage(db, day=day, submissions_accepted_at=api.isoformat()); activate(db, day)
    result = publish_sec_release(db, doc.id); db.commit()
    event = db.get(Event, result['event_id']); payload = json.loads(event.payload_json)
    assert result['inserted_events'] == 1
    assert event.ts.date() == datetime.now(timezone.utc).date()
    assert event.event_date.date() == day
    assert payload['acceptance_evidence']['submissions_raw'] == api.isoformat()
    assert payload['acceptance_evidence']['submissions_comparison'] == 'conflict'
    assert payload['published_at'] is None
    assert payload['source_availability']['observed_at'] == payload['first_observed_at']


def test_changed_published_evidence_is_not_rewritten(db):
    doc, day = stage(db); activate(db, day)
    result = publish_sec_release(db, doc.id); db.commit()
    event = db.get(Event,result['event_id']); event.payload_json='{}'; db.commit()
    with pytest.raises(ValueError, match='Published SEC release state changed'):
        publish_sec_release(db, doc.id)
    db.rollback()


@pytest.mark.parametrize('changed', ['bytes', 'observation', 'parsed'])
def test_repeat_checks_saved_source_bytes_clock_and_parsed_evidence(db, changed):
    from app.services.direct_feed_store import DirectFeedRevision
    doc, day = stage(db); activate(db, day)
    publish_sec_release(db, doc.id); db.commit()
    revision = db.scalar(select(DirectFeedRevision).where(DirectFeedRevision.document_id == doc.id))
    if changed == 'bytes':
        revision.source_bytes = b'changed'
    elif changed == 'observation':
        revision.fetched_at += timedelta(seconds=1)
    else:
        doc.parsed_json = '{}'
    db.commit()
    with pytest.raises(ValueError, match='Published SEC release'):
        publish_sec_release(db, doc.id)
    db.rollback()
    assert db.scalar(select(func.count()).select_from(Event)) == 1


def test_missing_legacy_publication_date_is_not_cache_fetch_date():
    assert _content_event('press_releases','TEST','fmp',datetime.now(timezone.utc),{'title':'Old release','url':'https://issuer.test'}) is None


def test_watchlist_materialization_and_monitoring_repeat_without_delivery(db):
    from app.services.monitoring_alerts import _ensure_alert_for_event
    doc, day = stage(db); activate(db, day)
    user = _user(db,'sec-release@example.test'); watchlist = _watchlist(db,user,alert_triggers=['press_releases'])
    db.add(WatchlistItem(watchlist_id=watchlist.id,security_id=1,target_type='ticker')); db.commit()
    assert sync_watchlist_content_events(db, watchlist.id) == 1
    db.commit()
    event = db.scalar(select(Event).where(Event.symbol=='TEST'))
    assert _ensure_alert_for_event(db,user_id=user.id,watchlist=watchlist,event=event)
    assert not _ensure_alert_for_event(db,user_id=user.id,watchlist=watchlist,event=event)
    db.commit()
    assert sync_watchlist_content_events(db,watchlist.id) == 0
    assert db.scalar(select(func.count()).select_from(MonitoringAlert)) == 1
    assert db.scalar(select(func.count()).select_from(EmailDelivery)) == 0


@pytest.mark.parametrize("news_provider,fmp_disabled", [("fmp", "1"), ("finnhub", "0")])
def test_fmp_off_does_not_materialize_old_news_or_press_cache(db, monkeypatch, news_provider, fmp_disabled):
    monkeypatch.setenv("NEWS_PROVIDER", news_provider)
    monkeypatch.setenv("FMP_PROVIDER_DISABLED", fmp_disabled)
    user = _user(db,'legacy-cache@example.test'); watchlist = _watchlist(db,user)
    for kind in ('news','press_releases'):
        db.add(TickerContentCache(content_type=kind,symbol='NVDA',window_key='latest',cache_key=kind,
            source='fmp',status='ok',item_count=1,payload_json=json.dumps({'items':[{
                'title':'old cached content','url':'https://issuer.test', 'published_at':datetime.now(timezone.utc).isoformat()}]}),
            fetched_at=datetime.now(timezone.utc)))
    db.commit()
    assert sync_watchlist_content_events(db,watchlist.id) == 0
    assert db.scalar(select(func.count()).select_from(Event)) == 0


def test_sec_release_reaches_complete_digest_builders_with_filing_labels(db, monkeypatch):
    from app.services import email_digests as digests
    from app.services.email_intraday import _watchlist_candidate, _intraday_key
    from app.services.monitoring_alerts import _ensure_alert_for_event
    doc, day = stage(db); activate(db, day)
    user = _user(db, 'sec-digest@example.test')
    watchlist = _watchlist(db, user, alert_triggers=['press_releases'])
    db.add(WatchlistItem(watchlist_id=watchlist.id, security_id=1, target_type='ticker')); db.commit()
    assert sync_watchlist_content_events(db, watchlist.id) == 1
    event = db.scalar(select(Event).where(Event.symbol == 'TEST'))
    assert _ensure_alert_for_event(db, user_id=user.id, watchlist=watchlist, event=event)
    db.commit()
    start = datetime.combine(datetime.now(timezone.utc).date(), datetime.min.time(), tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    monkeypatch.setattr(digests, '_upcoming_calendar_events_for_digest', lambda *a, **kw: ([], 'Outside fixture', None))
    monkeypatch.setattr(digests, '_send_digest', lambda *a, **kw: pytest.fail('Unexpected delivery'))
    first_key = _intraday_key(_watchlist_candidate(db, user, watchlist, event))
    for _ in range(2):
        monitor = digests.build_monitoring_digest(db, user, watchlist, start, window_end=end)
        daily = digests.build_signal_alert_digest(db, user, start, window_end=end)
        activity = digests.build_watchlist_activity_digest(db, user, watchlist, start)
        assert monitor.items_count == daily.items_count == activity.items_count == 1
        label = f'Filed {digests._friendly_date(day)}'
        assert label in monitor.context['items_html']
        assert label in daily.context['press_releases_text']
        assert label in activity.context['items_text']
        assert '>Published<' not in daily.context['press_releases_html']
        candidate = _watchlist_candidate(db, user, watchlist, event)
        assert candidate.context['event_date'] == f'Filed {day.isoformat()}'
        assert candidate.skip_reason is None
        assert _intraday_key(candidate) == first_key
    assert db.scalar(select(func.count()).select_from(MonitoringAlert)) == 1
    assert db.scalar(select(func.count()).select_from(EmailDelivery)) == 0
