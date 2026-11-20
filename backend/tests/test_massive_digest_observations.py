"""Recorded price evidence survives real digest readers, with no network or mail."""
from datetime import timedelta
import json

import pytest
from sqlalchemy import select

from app.models import MonitoringAlert, NotificationSubscription, EmailDelivery
from app.services import email_digests as digests
from app.services.custom_alert_rules import evaluate_watchlist_custom_alerts
from test_massive_absolute_alerts import NOW, setup_rule, verified_quote
from test_massive_price_core import db, isolated


@pytest.mark.parametrize('observation_kind', ['minute', 'daily_close', 'legacy', 'missing'])
def test_recorded_alert_survives_daily_and_monitoring_digest_repeat(db, monkeypatch, observation_kind):
    def forbidden(*args, **kwargs):
        pytest.fail('Digest attempted network or delivery')
    monkeypatch.setattr('requests.sessions.Session.request', forbidden)
    monkeypatch.setattr('httpx.Client.send', forbidden)
    monkeypatch.setattr(digests, '_send_digest', forbidden)
    monkeypatch.setattr(digests, '_upcoming_calendar_events_for_digest', lambda *a, **k: ([], '', ''))
    user, watchlist, state = setup_rule(db)
    user.entitlement_tier = 'pro'
    subscription = NotificationSubscription(email=user.email, source_type='watchlist', source_id=str(watchlist.id),
        source_name=watchlist.name, active=True, frequency='daily', alert_triggers_json='["custom_alert"]',
        source_payload_json='{"daily_digest_enabled": true}')
    db.add(subscription)
    for offset, price in [(0, 90), (1, 120)]:
        verified_quote(db, price, NOW - timedelta(minutes=16-offset))
        result = evaluate_watchlist_custom_alerts(db, user_id=user.id, watchlist_id=watchlist.id,
                                                 now=NOW + timedelta(minutes=offset))
        db.commit()
    assert result['triggered'] == 1
    alert = db.scalars(select(MonitoringAlert)).one()
    payload = json.loads(alert.payload_json)
    observed = payload['price_observation']['observed_at']
    expected = f'Price delayed 15 minutes; observed {observed}.'
    if observation_kind == 'daily_close':
        payload['price_observation'] = dict(price_provider='massive', price_kind='daily_close', price_date='2026-11-05')
        expected = 'Price is the 2026-11-05 daily close.'
    elif observation_kind == 'legacy':
        payload['price_observation']['price_provider'] = 'fmp'
        expected = ''
    elif observation_kind == 'missing':
        payload.pop('price_observation')
        expected = ''
    alert.payload_json = json.dumps(payload)
    db.commit()
    original = alert.payload_json
    # A later quote and even provider rollback cannot rewrite a recorded alert.
    verified_quote(db, 999, NOW - timedelta(minutes=13))
    monkeypatch.setenv('STOCK_PRICE_PROVIDER', 'fmp')
    since = NOW - timedelta(days=1)
    end = NOW + timedelta(minutes=5)
    def build():
        return (digests.build_monitoring_digest(db, user, watchlist, since, window_end=end),
                digests.build_signal_alert_digest(db, user, since, window_end=end))
    first = build()
    # Custom rules belong to the daily signal digest; the legacy monitoring
    # category selector does not admit custom_alert. Preserve that distinction.
    assert first[0].items_count == 0
    for digest, keys in [(first[1], ('signals_text', 'signals_html'))]:
        assert digest.items_count == 1
        for key in keys:
            if expected:
                assert expected in digest.context[key]
            else:
                assert 'Price delayed' not in digest.context[key] and 'daily close' not in digest.context[key]
        assert '999' not in str(digest.context)
    repeated = build()
    assert [item.context for item in first] == [item.context for item in repeated]
    assert db.get(MonitoringAlert, alert.id).payload_json == original
    assert db.query(MonitoringAlert).count() == 1 and db.query(EmailDelivery).count() == 0
    subscription.active = False
    db.commit()
    assert all(item.items_count == 0 for item in build())
