from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db import Base, ensure_ai_marketing_schema
from app.models import AiMarketingCampaign
from app.services.ai_marketing import (
    SCHEDULED_X_CAMPAIGN_TYPE,
    run_due_scheduled_x_campaigns,
    scheduled_x_campaign_due,
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
