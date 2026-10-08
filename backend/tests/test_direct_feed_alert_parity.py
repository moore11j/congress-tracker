"""No-send integration checks for SEC publication and downstream alert data."""
from datetime import date, datetime, timedelta, timezone
import hashlib
import json

import pytest
from sqlalchemy import create_engine, select, func
from sqlalchemy.orm import Session

from app.db import Base
from app.models import Event, InsiderTransaction, InsiderTransactionNormalized, MonitoringAlert, EmailDelivery, PriceCache, QuoteCache
from app.services.direct_feed_publication import rehearse_new_form4
from app.services.direct_feed_rehearsal import plan_corrections, include_event_corrections, include_monitoring_corrections, apply_rehearsal
from app.services import email_digests as digests
from app.services.monitoring_alerts import _ensure_alert_for_event
from app.services.monitoring_titles import build_monitoring_event_title
from test_direct_feed_rehearsal import form4_document, linked_events
from test_email_digests import _user, _watchlist


@pytest.fixture
def db(monkeypatch):
    # Any unexpected provider request or mail delivery fails this test.
    attempted = []
    def forbidden(*args, **kwargs):
        attempted.append(True)
        raise AssertionError('Network or delivery attempted during local rehearsal')
    monkeypatch.setattr('requests.sessions.Session.request', forbidden)
    monkeypatch.setattr('httpx.Client.send', forbidden)
    monkeypatch.setattr(digests, '_send_digest', forbidden)
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        yield session
    assert not attempted
    engine.dispose()


def test_new_sec_events_replay_without_duplicate_alerts_and_keep_disclosure_dates(db):
    doc = form4_document(repeat=True)
    user = _user(db, 'sec-alert@example.test')
    watchlist = _watchlist(db, user)
    first = rehearse_new_form4(db, doc, publish_since=date(2026,6,1))
    db.commit()
    assert first['inserted_events'] == first['inserted_transactions'] == 4
    assert rehearse_new_form4(db, doc, publish_since=date(2026,6,1))['inserted_events'] == 0
    events = list(db.scalars(select(Event)))
    assert len({event.source_filing_id for event in events}) == 4
    for event in events:
        payload = json.loads(event.payload_json)
        assert event.ts.date() == date(2026,6,3)
        assert payload['filing_date'] == '2026-06-03'
        assert payload['sec_verification']['sha256'] == doc['content_hash']
        assert _ensure_alert_for_event(db, user_id=user.id, watchlist=watchlist, event=event)
        assert not _ensure_alert_for_event(db, user_id=user.id, watchlist=watchlist, event=event)
        if payload['is_derivative']:
            assert not payload['is_market_trade']
            assert 'Derivative transaction' in build_monitoring_event_title(event, payload)
    db.commit()
    assert db.scalar(select(func.count()).select_from(MonitoringAlert)) == 4
    assert db.scalar(select(func.count()).select_from(EmailDelivery)) == 0


@pytest.mark.parametrize('conflict', ['legacy_raw', 'legacy_event', 'backfill', 'normalized_only'])
def test_publication_holds_unmapped_provider_rows_and_historical_alert_imports(db, conflict):
    doc = form4_document()
    from app.services.direct_feed_collection import parse_document
    row = parse_document('sec_form4', doc['raw'], doc['metadata'])[1]['transactions'][0]
    if conflict == 'legacy_raw':
        db.add(InsiderTransaction(source='fmp', external_id='legacy', symbol=row['ticker_normalized'],
                                 transaction_date=row['transaction_date'], payload_json='{}'))
    elif conflict == 'legacy_event':
        db.add(Event(source='fmp', event_type='insider_trade', symbol=row['ticker_normalized'],
                     ts=datetime(2026,6,3), payload_json=json.dumps({'transaction_date': str(row['transaction_date'])})))
    elif conflict == 'normalized_only':
        db.add(InsiderTransactionNormalized(**{**row, 'accession_number': 'legacy-unmapped',
                                              'normalized_hash': 'legacy', 'price': 999}))
    db.commit()
    since = date(2026,6,4) if conflict == 'backfill' else date(2026,6,1)
    result = rehearse_new_form4(db, doc, publish_since=since)
    assert result['status'] == 'held'
    assert db.scalar(select(func.count()).select_from(InsiderTransactionNormalized)) == (1 if conflict == 'normalized_only' else 0)


@pytest.mark.parametrize('price,expected', [(None, '--'), (0, '$0.00'), (12.5, '$12.50')])
def test_verified_price_and_value_never_fall_back_to_provider(db, price, expected):
    payload = {'sec_verification': {'feed': 'sec_form4'}, 'price': price, 'value': None,
               'is_derivative': True, 'is_market_trade': False,
               'raw': {'price': 999, 'tradePrice': 999, 'value': 999, 'transactionType': 'P-Purchase'}}
    event = Event(event_type='insider_trade', trade_type='purchase', transaction_type='P', payload_json=json.dumps(payload))
    assert digests._activity_trade_price(event, payload) == expected
    assert '999' not in digests._activity_value(event, payload)
    assert digests._activity_action(event, payload) == 'Derivative transaction'


def test_existing_monitoring_copies_are_corrected_without_resending_or_resetting_read_state(db):
    from app.services.email_intraday import _watchlist_candidate, _intraday_key
    doc, rows, raw, events = linked_events(db)
    user = _user(db, 'sec-existing@example.test')
    watchlist = _watchlist(db, user)
    for event in events:
        _ensure_alert_for_event(db, user_id=user.id, watchlist=watchlist, event=event)
    alerts = list(db.scalars(select(MonitoringAlert)))
    read_at = datetime(2026,6,4)
    for alert in alerts:
        alert.read_at = read_at
    db.commit()
    before = [(a.id, a.event_id, a.read_at, a.event_created_at) for a in alerts]
    delivery_keys = [_intraday_key(_watchlist_candidate(db, user, watchlist, event)) for event in events]
    plan = include_monitoring_corrections(db, include_event_corrections(db, plan_corrections(db, [doc])))
    assert plan['counts']['monitoring_corrections'] == 3
    assert apply_rehearsal(db, plan)['updated'] == 9
    db.commit()
    assert apply_rehearsal(db, plan)['updated'] == 0
    assert before == [(a.id, a.event_id, a.read_at, a.event_created_at) for a in alerts]
    assert delivery_keys == [_intraday_key(_watchlist_candidate(db, user, watchlist, event)) for event in events]
    for event, alert in zip(events, alerts):
        assert not _ensure_alert_for_event(db, user_id=user.id, watchlist=watchlist, event=event)
        assert json.loads(alert.payload_json)['event']['price'] == json.loads(event.payload_json)['price']
        assert alert.title == build_monitoring_event_title(event, json.loads(event.payload_json))
    assert db.scalar(select(func.count()).select_from(EmailDelivery)) == 0


def test_complete_monitoring_daily_and_watchlist_builds_use_corrected_data(db, monkeypatch):
    doc, rows, raw, events = linked_events(db)
    user = _user(db, 'sec-digests@example.test')
    watchlist = _watchlist(db, user)
    for event in events:
        _ensure_alert_for_event(db, user_id=user.id, watchlist=watchlist, event=event)
    db.commit()
    plan = include_monitoring_corrections(db, include_event_corrections(db, plan_corrections(db, [doc])))
    apply_rehearsal(db, plan)
    db.commit()
    since, end = datetime(2026,6,1,tzinfo=timezone.utc), datetime(2026,6,5,tzinfo=timezone.utc)
    monkeypatch.setattr(digests, '_upcoming_calendar_events_for_digest', lambda *a, **k: ([], 'Calendar not part of SEC rehearsal'))
    monitoring = digests.build_monitoring_digest(db, user, watchlist, since, window_end=end)
    daily = digests.build_signal_alert_digest(db, user, since, window_end=end)
    assert 'Derivative transaction' in monitoring.context['items_text']
    assert 'Derivative transaction' in daily.context['insider_trades_text']
    assert '$999' not in daily.context['insider_trades_text']
    # Actual activity builder includes records selected by the user's ticker.
    from app.models import Security
    db.scalar(select(Security)).symbol = events[0].symbol
    db.commit()
    activity = digests.build_watchlist_activity_digest(db, user, watchlist, since)
    assert activity.items_count == 3
    assert 'Derivative transaction' in str(activity.items)
    assert db.scalar(select(func.count()).select_from(EmailDelivery)) == 0


@pytest.mark.parametrize('frequency', ['daily', 'weekly'])
def test_top_stocks_real_cached_reader_and_price_alert_inputs_unchanged_by_sec_correction(db, monkeypatch, frequency):
    from app.services import top_stocks, top_ideas_digest
    from app.services.price_alert_reference import daily_price_observation
    from app.entitlements import ENTITLEMENTS
    from test_top_stocks import _refresh, _row
    doc, rows, raw, events = linked_events(db)
    user = _user(db, 'sec-ranking@example.test')
    user.email_verified_at = datetime(2026,6,1)
    user.top_stock_ideas_frequency = frequency
    _refresh(db, monkeypatch, [_row('AAPL', 77), _row('MSFT', 60)], {'AAPL': 77, 'MSFT': 60})
    now = datetime(2026,6,5,19,0,tzinfo=timezone.utc)  # Friday.
    db.add_all([PriceCache(symbol='AAPL', date='2026-06-04', close=100),
                QuoteCache(symbol='AAPL', price=106, asof_ts=now)])
    db.commit()
    monkeypatch.setattr(top_ideas_digest, 'entitlements_for_user', lambda *a: ENTITLEMENTS['premium'])
    before = top_ideas_digest.build_top_ideas_digest(db, user)
    price_before = daily_price_observation(db, 'AAPL', now)
    apply_rehearsal(db, include_event_corrections(db, plan_corrections(db, [doc])))
    db.commit()
    after = top_ideas_digest.build_top_ideas_digest(db, user)
    assert before.context == after.context and before.items == after.items
    assert daily_price_observation(db, 'AAPL', now) == price_before
    assert price_before['change_pct'] == pytest.approx(6)
    previews = top_ideas_digest.run_top_ideas_digest(db, dry_run=True, now=now)
    assert previews[0]['status'] == 'would_send' and previews[0]['items_count'] == 2
    assert db.scalar(select(func.count()).select_from(EmailDelivery)) == 0
