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
