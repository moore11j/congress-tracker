import json

from app.models import NotificationSubscription
from app.services.watchlist_delivery import categories_for_event, is_delivery_enabled


def _subscription(payload: dict, triggers: list[str] | None = None) -> NotificationSubscription:
    return NotificationSubscription(
        email="ada@example.com",
        source_type="watchlist",
        source_id="1",
        source_name="Ada's watchlist",
        active=True,
        source_payload_json=json.dumps(payload),
        alert_triggers_json=json.dumps(triggers or []),
    )


def test_matrix_routes_news_to_daily_and_intraday_independently():
    subscription = _subscription(
        {"alert_delivery_modes": {"news": "both", "press_releases": "daily"}},
    )

    assert is_delivery_enabled(subscription, "news", "daily")
    assert is_delivery_enabled(subscription, "news", "intraday")
    assert is_delivery_enabled(subscription, "press_releases", "daily")
    assert not is_delivery_enabled(subscription, "press_releases", "intraday")
    assert not is_delivery_enabled(subscription, "congress", "daily")


def test_legacy_subscription_keeps_its_existing_global_toggle_behavior():
    subscription = _subscription(
        {"daily_digest_enabled": True, "intraday_alerts_enabled": False},
        ["congress_activity"],
    )

    assert is_delivery_enabled(subscription, "congress", "daily")
    assert not is_delivery_enabled(subscription, "congress", "intraday")


def test_news_and_press_release_remain_distinct_categories():
    assert categories_for_event("news_article") == {"news"}
    assert categories_for_event("press_release") == {"press_releases"}


import pytest
from app.models import Event
from app.services.institutional_activity import INSTITUTIONAL_EVENT_TYPES
from app.services.email_digests import _watchlist_event_allowed


@pytest.mark.parametrize('event_type', INSTITUTIONAL_EVENT_TYPES)
@pytest.mark.parametrize('scored', [False, True])
def test_institutional_only_delivery_includes_every_institutional_event_type(event_type, scored):
    # Real 13F exits/new positions do not start with "institutional". A score
    # must not force users to opt into another category to receive them.
    payload = {'smart_score': 95} if scored else {}
    event = Event(event_type=event_type, symbol='TEST', payload_json=json.dumps(payload))
    enabled = _subscription({'alert_delivery_modes': {'institutional_activity': 'daily'}}, ['institutional_activity'])
    disabled = _subscription({'alert_delivery_modes': {'institutional_activity': 'off'}}, ['institutional_activity'])
    assert _watchlist_event_allowed(enabled, event)
    assert not _watchlist_event_allowed(disabled, event)
