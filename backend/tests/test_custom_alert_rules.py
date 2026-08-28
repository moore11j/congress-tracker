from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from app.db import Base
from app.models import MonitoringAlert, QuoteCache, Security, UserAccount, Watchlist, WatchlistAlertRule, WatchlistItem
from app.services.custom_alert_rules import _compare, evaluate_watchlist_custom_alerts, format_rule_summary, validate_conditions


def test_crossing_requires_a_real_transition() -> None:
    assert _compare(51, 50, "crosses_above", 49) is True
    assert _compare(53, 50, "crosses_above", 51) is False
    assert _compare(48, 50, "crosses_above", 51) is False
    # Once the state has fallen below its threshold, a later rise is eligible again.
    assert _compare(52, 50, "crosses_above", 48) is True


def test_threshold_and_any_all_condition_semantics_are_structured() -> None:
    conditions = validate_conditions([
        {"metric": "rsi", "metric_params": {"period": 14}, "operator": "lt", "comparison_type": "value", "comparison_value": 35},
        {"metric": "price", "operator": "gt", "comparison_type": "metric", "comparison_metric": "sma", "comparison_metric_params": {"period": 200}},
        {"metric": "congress_unique_buyers", "operator": "gte", "comparison_type": "value", "comparison_value": 2, "time_window": {"value": 7, "unit": "day"}},
    ])
    assert len(conditions) == 3
    assert conditions[1]["comparison_metric"] == "sma"
    assert "RSI (14)" in format_rule_summary({"conditions": conditions, "match_type": "all"})


def test_invalid_or_unsafe_rule_shapes_are_rejected() -> None:
    for payload in (
        [{"metric": "rsi", "operator": "crosses_above", "comparison_type": "value", "comparison_value": "hello"}],
        [{"metric": "sma", "operator": "crosses_above", "comparison_type": "metric", "comparison_metric": "congress_purchase_count"}],
        [{"metric": "price_change_pct", "operator": "increases_by", "comparison_type": "value", "comparison_value": -500, "time_window": {"value": 1, "unit": "day"}}],
    ):
        try:
            validate_conditions(payload)
        except ValueError:
            continue
        raise AssertionError("invalid custom rule was accepted")


def test_event_rules_require_a_time_window() -> None:
    try:
        validate_conditions([{"metric": "congress_unique_buyers", "operator": "gte", "comparison_type": "value", "comparison_value": 2}])
    except ValueError as exc:
        assert "time window" in str(exc).lower()
    else:
        raise AssertionError("event aggregate without a window was accepted")


def test_bullish_and_bearish_states_use_boolean_conditions() -> None:
    conditions = validate_conditions([{"metric": "bullish_state", "operator": "is_true", "comparison_type": "none"}])
    assert conditions[0]["comparison_value"] is None


def test_worker_triggers_once_then_rearms_after_the_condition_resets() -> None:
    engine = create_engine("sqlite://")
    Base.metadata.create_all(engine)
    now = datetime.now(timezone.utc)
    with Session(engine) as db:
        user = UserAccount(email="custom-rules@example.test")
        watchlist = Watchlist(name="Custom rule test watchlist", owner_user_id=1)
        security = Security(symbol="TEST", name="Test Corp", asset_class="equity")
        db.add_all([user, watchlist, security])
        db.flush()
        watchlist.owner_user_id = user.id
        db.add(WatchlistItem(watchlist_id=watchlist.id, security_id=security.id, target_type="ticker"))
        db.add(QuoteCache(symbol="TEST", price=90, market_cap=None, asof_ts=now.replace(tzinfo=None)))
        db.add(WatchlistAlertRule(
            user_id=user.id,
            watchlist_id=watchlist.id,
            name="Price threshold",
            enabled=True,
            scope_type="any_watchlist_ticker",
            match_type="all",
            delivery="both",
            conditions_json=json.dumps(validate_conditions([{"metric": "price", "operator": "gt", "comparison_type": "value", "comparison_value": 100}])),
        ))
        db.commit()

        # The first pass establishes state only; it must never backfill a trigger.
        assert evaluate_watchlist_custom_alerts(db, user_id=user.id, watchlist_id=watchlist.id, now=now)["triggered"] == 0
        db.commit()
        db.get(QuoteCache, "TEST").price = 120
        assert evaluate_watchlist_custom_alerts(db, user_id=user.id, watchlist_id=watchlist.id, now=now + timedelta(minutes=5))["triggered"] == 1
        db.commit()
        alert = db.execute(select(MonitoringAlert).where(MonitoringAlert.alert_type == "custom_alert")).scalar_one()
        payload = json.loads(alert.payload_json)
        assert payload["price_alert"] is True
        assert payload["trigger_price"] == 120

        # Staying above is deduped; falling below re-arms a later crossing.
        assert evaluate_watchlist_custom_alerts(db, user_id=user.id, watchlist_id=watchlist.id, now=now + timedelta(minutes=10))["triggered"] == 0
        db.get(QuoteCache, "TEST").price = 90
        assert evaluate_watchlist_custom_alerts(db, user_id=user.id, watchlist_id=watchlist.id, now=now + timedelta(minutes=15))["triggered"] == 0
        db.get(QuoteCache, "TEST").price = 120
        assert evaluate_watchlist_custom_alerts(db, user_id=user.id, watchlist_id=watchlist.id, now=now + timedelta(minutes=20))["triggered"] == 1
