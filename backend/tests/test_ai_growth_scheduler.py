from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db import Base, ensure_ai_marketing_schema
from app.models import AiMarketingCampaign, AiMarketingOpportunity
from app.services.ai_marketing import (
    SCHEDULED_X_CAMPAIGN_TYPE,
    X_REPLY_CAMPAIGN_TYPE,
    create_campaign,
    post_approved_draft_to_x,
    run_due_x_reply_campaigns,
    run_due_scheduled_x_campaigns,
    scheduled_x_campaign_due,
    x_reply_campaign_due,
)


def _session():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    ensure_ai_marketing_schema(engine)
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    return Session()


def _scheduled_campaign(**overrides) -> AiMarketingCampaign:
    values = {
        "name": "Daily Bullish Confirmation",
        "enabled": True,
        "status": "active",
        "mode": SCHEDULED_X_CAMPAIGN_TYPE,
        "campaign_type": SCHEDULED_X_CAMPAIGN_TYPE,
        "content_type": "x_post",
        "schedule_config_json": '{"cadence":"weekdays"}',
        "weekdays_only": True,
        "run_time": "06:55",
        "timezone": "America/Los_Angeles",
        "recipient_email": "jarod@walnutmarkets.com",
        "source_type": "bullish_confirmation",
        "platforms_json": '["x"]',
        "max_drafts_per_day": 1,
    }
    values.update(overrides)
    return AiMarketingCampaign(**values)


def _x_reply_campaign(**overrides) -> AiMarketingCampaign:
    values = {
        "name": "Daily X Reply Suggestions",
        "enabled": True,
        "status": "active",
        "mode": X_REPLY_CAMPAIGN_TYPE,
        "campaign_type": X_REPLY_CAMPAIGN_TYPE,
        "content_type": "x_reply",
        "schedule_config_json": '{"cadence":"daily","start_date":"2026-07-15"}',
        "weekdays_only": False,
        "run_time": "07:15",
        "timezone": "America/Los_Angeles",
        "recipient_email": "jarod@walnutmarkets.com",
        "source_type": "home_feed",
        "platforms_json": '["x"]',
        "filters_json": '{"ignore_handles":["WalnutMarkets"]}',
        "max_items_per_run": 25,
        "max_drafts_per_day": 5,
    }
    values.update(overrides)
    return AiMarketingCampaign(**values)


def test_scheduled_x_campaign_due_uses_saved_run_time_timezone_and_last_run():
    campaign = _scheduled_campaign()
    before_run_time = datetime(2026, 7, 13, 13, 54, tzinfo=timezone.utc)
    after_run_time = datetime(2026, 7, 13, 13, 56, tzinfo=timezone.utc)

    assert scheduled_x_campaign_due(campaign, now=before_run_time) is False
    assert scheduled_x_campaign_due(campaign, now=after_run_time) is True

    campaign.last_run_at = after_run_time - timedelta(minutes=1)
    assert scheduled_x_campaign_due(campaign, now=after_run_time) is False


def test_scheduled_x_weekdays_campaign_skips_weekends():
    campaign = _scheduled_campaign()
    saturday_after_run_time = datetime(2026, 7, 18, 14, 0, tzinfo=timezone.utc)

    assert scheduled_x_campaign_due(campaign, now=saturday_after_run_time) is False


def test_run_due_scheduled_x_campaigns_runs_due_campaign(monkeypatch):
    calls: list[int] = []

    def fake_run_scheduled_x_campaign(db, campaign):
        calls.append(campaign.id)
        return {
            "status": "ok",
            "candidates_considered": 1,
            "drafts_generated": 1,
            "emails_sent": 1,
            "opportunities": [],
        }

    monkeypatch.setattr("app.services.ai_marketing.run_scheduled_x_campaign", fake_run_scheduled_x_campaign)
    db = _session()
    try:
        campaign = _scheduled_campaign(run_time="06:30")
        db.add(campaign)
        db.commit()

        result = run_due_scheduled_x_campaigns(db, force=True)

        assert result["campaigns_checked"] == 1
        assert result["campaigns_run"] == 1
        assert calls == [campaign.id]
        assert result["items"][0]["emails_sent"] == 1
    finally:
        db.close()


def test_x_reply_campaign_due_respects_start_date_and_daily_limit():
    campaign = _x_reply_campaign()
    before_start = datetime(2026, 7, 14, 15, 0, tzinfo=timezone.utc)
    before_run_time = datetime(2026, 7, 15, 14, 14, tzinfo=timezone.utc)
    after_run_time = datetime(2026, 7, 15, 14, 16, tzinfo=timezone.utc)

    assert x_reply_campaign_due(campaign, now=before_start) is False
    assert x_reply_campaign_due(campaign, now=before_run_time) is False
    assert x_reply_campaign_due(campaign, now=after_run_time) is True

    campaign.last_run_at = after_run_time - timedelta(minutes=1)
    assert x_reply_campaign_due(campaign, now=after_run_time) is False


def test_x_reply_campaign_normalization_allows_five_daily_suggestions():
    db = _session()
    try:
        campaign = create_campaign(
            db,
            {
                "name": "Daily X Reply Suggestions",
                "enabled": True,
                "status": "active",
                "mode": X_REPLY_CAMPAIGN_TYPE,
                "campaign_type": X_REPLY_CAMPAIGN_TYPE,
                "content_type": "x_reply",
                "platforms": ["x"],
                "schedule_config": {"cadence": "daily", "start_date": "2026-07-15"},
                "weekdays_only": False,
                "run_time": "07:15",
                "timezone": "America/Los_Angeles",
                "source_type": "home_feed",
                "max_items_per_run": 25,
                "max_drafts_per_day": 5,
            },
        )

        assert campaign.mode == X_REPLY_CAMPAIGN_TYPE
        assert campaign.content_type == "x_reply"
        assert campaign.max_drafts_per_day == 5
    finally:
        db.close()


def test_run_due_x_reply_campaigns_runs_due_campaign(monkeypatch):
    calls: list[int] = []

    def fake_run_x_reply_campaign(db, campaign):
        calls.append(campaign.id)
        return {
            "status": "ok",
            "candidates_considered": 5,
            "drafts_generated": 5,
            "emails_sent": 5,
            "opportunities": [],
        }

    monkeypatch.setattr("app.services.ai_marketing.run_x_reply_campaign", fake_run_x_reply_campaign)
    db = _session()
    try:
        campaign = _x_reply_campaign()
        db.add(campaign)
        db.commit()

        result = run_due_x_reply_campaigns(db, force=True)

        assert result["campaigns_checked"] == 1
        assert result["campaigns_run"] == 1
        assert calls == [campaign.id]
        assert result["items"][0]["drafts_generated"] == 5
    finally:
        db.close()


def test_x_reply_draft_approval_does_not_auto_post():
    db = _session()
    try:
        opportunity = AiMarketingOpportunity(
            platform="x",
            source_provider="x_api",
            source_id="x-reply:123",
            source_url="https://x.com/example/status/123",
            source_dedupe_key="x-reply-123",
            title="Reply candidate",
            campaign_type=X_REPLY_CAMPAIGN_TYPE,
            content_type="x_reply",
            source_platform="x",
            recommended_action="reply",
            generated_content="The market tell is confirmation, not noise.",
        )
        db.add(opportunity)
        db.commit()

        result = post_approved_draft_to_x(db, opportunity)

        assert result["attempted"] is False
        assert "not an X post" in result["reason"]
    finally:
        db.close()
