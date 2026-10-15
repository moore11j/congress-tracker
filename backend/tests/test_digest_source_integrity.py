"""Actual daily digest projection over prepared sources, without network or mail."""
from datetime import datetime, timedelta, timezone
import json

import pytest
from sqlalchemy import create_engine, select, func
from sqlalchemy.orm import Session

from app.db import Base
from app.models import (UserAccount, Watchlist, WatchlistItem, Security,
    NotificationSubscription, Event, MonitoringAlert, EmailDelivery, InsightsSnapshot)
from app.services import email_digests as service
from app.services.monitoring_alerts import _ensure_alert_for_event
from app.services.watchlist_delivery import CATEGORY_TRIGGER


@pytest.fixture
def fixture(monkeypatch):
    attempted = []
    def forbidden(*args, **kwargs):
        attempted.append(True)
        raise AssertionError('Network or mail attempted in isolated digest replay')
    monkeypatch.setattr('requests.sessions.Session.request', forbidden)
    monkeypatch.setattr('httpx.Client.send', forbidden)
    monkeypatch.setattr(service, '_send_digest', forbidden)
    monkeypatch.setattr(service, '_upcoming_calendar_events_for_digest', lambda *a, **k: ([], '', ''))
    monkeypatch.setenv('FMP_PROVIDER_ENABLED', 'false')
    monkeypatch.setenv('FMP_PROVIDER_DISABLED', 'true')
    monkeypatch.setenv('NEWS_PROVIDER', 'finnhub')
    monkeypatch.setenv('FINNHUB_NEWS_PUBLICATION_ENABLED', '1')
    now = datetime.now(timezone.utc)
    monkeypatch.setenv('NEWS_PUBLISH_SINCE', (now - timedelta(days=2)).date().isoformat())
    monkeypatch.setenv('NEWS_PUBLISH_AFTER', (now - timedelta(days=2)).isoformat())
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    with Session(engine) as db:
        user = UserAccount(email='digest@example.test', entitlement_tier='pro')
        db.add(user)
        db.flush()
        yield db, user, now
        assert not db.scalar(select(func.count()).select_from(EmailDelivery))
    engine.dispose()
    assert not attempted


def watchlist(db, user, modes, *, active=True, daily=True, legacy=False):
    security = db.scalar(select(Security).where(Security.symbol == 'NVDA'))
    if security is None:
        security = Security(symbol='NVDA', name='NVIDIA', asset_class='stock')
        db.add(security)
    row = Watchlist(name=f'Source test {db.scalar(select(func.count()).select_from(Watchlist))}', owner_user_id=user.id)
    db.add(row)
    db.flush()
    db.add(WatchlistItem(watchlist_id=row.id, security_id=security.id, target_type='ticker'))
    payload = {'daily_digest_enabled': daily, 'intraday_alerts_enabled': True}
    if not legacy:
        payload['alert_delivery_modes'] = modes
    subscription = NotificationSubscription(email=user.email, source_type='watchlist',
        source_id=str(row.id), source_name=row.name, active=active, frequency='daily',
        # These retained legacy chips MUST NOT override an explicit matrix Off.
        alert_triggers_json=json.dumps(list(CATEGORY_TRIGGER.values())),
        source_payload_json=json.dumps(payload))
    db.add(subscription)
    db.commit()
    return row, subscription


def event(db, now, kind, *, title='Verified source item', url=None):
    payload = {'title': title, 'publisher': 'Issuer', 'url': url,
        'filing_date': now.date().isoformat(), 'holder_name': 'Source Holder',
        'reporting_owner_name': 'Source Insider', 'agency': 'Source Agency'}
    row = Event(event_type=kind, symbol='NVDA', source='direct_fixture',
        source_provider='sec_edgar', ts=now - timedelta(minutes=10),
        event_date=now - timedelta(minutes=15), member_name='Source Member',
        source_document_url=url, payload_json=json.dumps(payload))
    db.add(row)
    db.commit()
    return row


def digest(db, user, now):
    return service.build_signal_alert_digest(db, user, now - timedelta(days=1), window_end=now + timedelta(seconds=1))


@pytest.mark.parametrize('kind,category,section', [
    ('congress_trade', 'congress', 'congress_trades_text'),
    ('insider_trade', 'insiders', 'insider_trades_text'),
    ('institutional_accumulation', 'institutional_activity', 'institutional_activity_text'),
    ('government_contract', 'government_contracts', 'government_contracts_text'),
])
@pytest.mark.parametrize('mode,expected', [('off', 0), ('intraday', 0), ('daily', 1), ('both', 1)])
def test_activity_sections_obey_daily_matrix(fixture, kind, category, section, mode, expected):
    db, user, now = fixture
    row, _ = watchlist(db, user, {category: mode})
    item = event(db, now, kind)
    assert _ensure_alert_for_event(db, user_id=user.id, watchlist=row, event=item)
    result = digest(db, user, now)
    assert result.items_count == expected
    assert bool(result.context[section]) == bool(expected)
    assert digest(db, user, now).context == result.context
    assert db.scalar(select(func.count()).select_from(MonitoringAlert)) == 1


@pytest.mark.parametrize('state', ['inactive', 'daily_disabled', 'deleted', 'legacy_disabled'])
def test_existing_activity_respects_current_subscription(fixture, state):
    db, user, now = fixture
    row, subscription = watchlist(db, user, {'congress': 'daily'}, legacy=state == 'legacy_disabled')
    item = event(db, now, 'congress_trade')
    _ensure_alert_for_event(db, user_id=user.id, watchlist=row, event=item)
    if state == 'inactive':
        subscription.active = False
    elif state in {'daily_disabled', 'legacy_disabled'}:
        payload = json.loads(subscription.source_payload_json)
        payload['daily_digest_enabled'] = False
        subscription.source_payload_json = json.dumps(payload)
    else:
        db.delete(subscription)
    db.commit()
    assert digest(db, user, now).items_count == 0


@pytest.mark.parametrize('kind,category,section', [
    ('news_article', 'news', 'watchlist_news_text'),
    ('press_release', 'press_releases', 'press_releases_text'),
])
def test_distinct_same_ticker_content_survives_and_overlap_is_deduplicated(fixture, kind, category, section):
    db, user, now = fixture
    first, _ = watchlist(db, user, {category: 'daily'})
    second, _ = watchlist(db, user, {category: 'daily'})
    for index in range(5):
        item = event(db, now, kind, title=f'Unique source report {index}', url=f'https://issuer.example/report/{index}')
        for row in (first, second):
            _ensure_alert_for_event(db, user_id=user.id, watchlist=row, event=item)
    result = digest(db, user, now)
    assert result.items_count == 4
    assert result.context[section].count('Unique source report') == 4
    assert result.diagnostics['excluded_reasons']['duplicate'] == 5
    assert result.diagnostics['excluded_reasons']['content_category_display_limit'] == 1
    assert db.scalar(select(func.count()).select_from(MonitoringAlert)) == 10
    assert digest(db, user, now).context == result.context


@pytest.mark.parametrize('mode', ['off', 'intraday'])
def test_explicit_content_matrix_cannot_fall_back_to_old_trigger_chips(fixture, mode):
    db, user, now = fixture
    row, _ = watchlist(db, user, {'news': mode})
    item = event(db, now, 'news_article', url='https://issuer.example/report')
    _ensure_alert_for_event(db, user_id=user.id, watchlist=row, event=item)
    assert digest(db, user, now).items_count == 0


def test_actual_finnhub_cache_publishes_distinct_headlines_once_across_watchlists(fixture):
    db, user, now = fixture
    watchlist(db, user, {'news': 'daily'})
    watchlist(db, user, {'news': 'daily'})
    items = [{'source': 'finnhub', 'symbol': 'NVDA', 'site': 'Publisher',
        'title': f'Distinct prepared headline {index}', 'url': f'https://publisher.example/news/{index}',
        'published_at': (now - timedelta(minutes=30 + index)).isoformat(),
        'observed_at': (now - timedelta(minutes=5)).isoformat()} for index in range(3)]
    cache = InsightsSnapshot(kind='finnhub-news:company:NVDA', source='finnhub',
        fetched_at=now - timedelta(minutes=5), payload_json=json.dumps({'source': 'finnhub', 'items': items}))
    db.add(cache)
    db.commit()
    result = digest(db, user, now)
    assert result.items_count == 3
    assert result.context['watchlist_news_text'].count('Distinct prepared headline') == 3
    originals = [(e.id, e.source_filing_id, e.ts, e.event_date, e.payload_json) for e in db.scalars(select(Event).order_by(Event.id))]
    assert len(originals) == 3
    assert db.scalar(select(func.count()).select_from(MonitoringAlert)) == 6
    assert digest(db, user, now).context == result.context
    assert originals == [(e.id, e.source_filing_id, e.ts, e.event_date, e.payload_json) for e in db.scalars(select(Event).order_by(Event.id))]
    assert db.scalar(select(func.count()).select_from(MonitoringAlert)) == 6



@pytest.mark.parametrize('second_url,expected', [
    ('https://issuer.example/Report?id=1&utm_source=mail#read', 1),
    ('https://issuer.example/report?id=1', 2),
    ('https://issuer.example/Report?id=2', 2),
])
def test_content_identity_reconciles_providers_without_merging_distinct_urls(fixture, second_url, expected):
    db, user, now = fixture
    row, _ = watchlist(db, user, {'news': 'daily'})
    for index, url in enumerate(['https://issuer.example/Report?id=1', second_url]):
        item = event(db, now, 'news_article', title=f'Provider title {index}', url=url)
        item.source_provider = 'fmp' if index == 0 else 'finnhub'
        _ensure_alert_for_event(db, user_id=user.id, watchlist=row, event=item)
    db.commit()
    result = digest(db, user, now)
    assert result.items_count == expected
    assert db.scalar(select(func.count()).select_from(Event)) == 2
    assert db.scalar(select(func.count()).select_from(MonitoringAlert)) == 2
    assert digest(db, user, now).context == result.context


def test_one_disabled_watchlist_does_not_suppress_enabled_overlap(fixture):
    db, user, now = fixture
    off, _ = watchlist(db, user, {'congress': 'off'})
    on, _ = watchlist(db, user, {'congress': 'daily'})
    item = event(db, now, 'congress_trade')
    for row in (off, on):
        _ensure_alert_for_event(db, user_id=user.id, watchlist=row, event=item)
    result = digest(db, user, now)
    assert result.items_count == 1
    assert result.context['congress_trades_text'].count('Source Member') == 1


def test_activity_alternate_enabled_trigger_and_legacy_preferences_survive(fixture):
    db, user, now = fixture
    row, subscription = watchlist(db, user, {'congress': 'off', 'cross_source': 'daily'})
    item = event(db, now, 'congress_trade')
    payload = json.loads(item.payload_json)
    payload['source_count'] = 3
    item.payload_json = json.dumps(payload)
    _ensure_alert_for_event(db, user_id=user.id, watchlist=row, event=item)
    assert digest(db, user, now).items_count == 1
    subscription.source_payload_json = json.dumps({'daily_digest_enabled': True})
    subscription.alert_triggers_json = json.dumps(['congress_activity'])
    db.commit()
    assert digest(db, user, now).items_count == 1
    subscription.alert_triggers_json = json.dumps(['news'])
    db.commit()
    assert digest(db, user, now).items_count == 0


def test_daily_window_and_dismissal_are_preserved_for_content(fixture):
    db, user, now = fixture
    row, _ = watchlist(db, user, {'news': 'daily'})
    for index in range(3):
        item = event(db, now, 'news_article', title=f'Window source {index}', url=f'https://issuer.example/{index}')
        _ensure_alert_for_event(db, user_id=user.id, watchlist=row, event=item)
        alert = db.scalar(select(MonitoringAlert).where(MonitoringAlert.event_id == item.id))
        if index == 1:
            alert.dismissed_at = now
        elif index == 2:
            alert.event_created_at = now + timedelta(hours=1)
    db.commit()
    result = digest(db, user, now)
    assert result.items_count == 1
    assert 'Window source 0' in result.context['watchlist_news_text']
    assert 'Window source 1' not in result.context['watchlist_news_text']
    assert 'Window source 2' not in result.context['watchlist_news_text']


@pytest.mark.parametrize('state', ['stale', 'publication_off'])
def test_unavailable_prepared_news_does_not_create_alerts_or_fetch(fixture, monkeypatch, state):
    db, user, now = fixture
    watchlist(db, user, {'news': 'daily'})
    if state == 'publication_off':
        monkeypatch.setenv('FINNHUB_NEWS_PUBLICATION_ENABLED', '0')
    stamp = now - timedelta(days=2) if state == 'stale' else now - timedelta(minutes=1)
    item = {'source': 'finnhub', 'symbol': 'NVDA', 'site': 'Publisher',
        'title': 'Unavailable prepared story', 'url': 'https://publisher.example/held',
        'published_at': (now - timedelta(hours=1)).isoformat(),
        'observed_at': (now - timedelta(minutes=2)).isoformat()}
    db.add(InsightsSnapshot(kind='finnhub-news:company:NVDA', source='finnhub',
        fetched_at=stamp, payload_json=json.dumps({'source': 'finnhub', 'items': [item]})))
    db.commit()
    assert digest(db, user, now).items_count == 0
    assert db.scalar(select(func.count()).select_from(Event)) == 0
    assert db.scalar(select(func.count()).select_from(MonitoringAlert)) == 0
