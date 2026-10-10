"""Retirement must skip article automation without rewriting its saved state."""
import pytest

from app.models import AiMarketingCampaign, AiMarketingCampaignRun, AiMarketingOpportunity, AiMarketingArticleCandidate, AiMarketingEmailLog
from app.services import ai_marketing as marketing
from test_ai_marketing import _session, _user, _request_for_user, _article_campaign_payload
from app.routers.ai_marketing import admin_ai_marketing_create_campaign, admin_ai_marketing_run_campaign


def forbidden(*args, **kwargs):
    raise AssertionError("Retired article automation performed work")


@pytest.mark.parametrize("dry_run", [True, False])
def test_disabled_due_articles_skip_even_forced_without_database(monkeypatch, dry_run):
    monkeypatch.setenv("FMP_PROVIDER_DISABLED", "true")
    monkeypatch.setattr(marketing, "fetch_fmp_articles", forbidden)
    result = marketing.run_due_article_reactive_campaigns(None, force=True, dry_run=dry_run)
    assert result == {"status": "provider_disabled", "campaigns_checked": 0,
                      "campaigns_run": 0, "dry_run": dry_run, "items": []}


def test_disabled_manual_article_runs_preserve_campaign_history_and_drafts(monkeypatch):
    db = _session()
    try:
        admin = _user(db, "retirement@example.test", role="admin")
        payload = admin_ai_marketing_create_campaign(_article_campaign_payload(), _request_for_user(admin), db)
        campaign = db.get(AiMarketingCampaign, payload["id"])
        before = (campaign.status, campaign.enabled, campaign.last_run_at, campaign.next_run_at)
        models = (AiMarketingCampaignRun, AiMarketingOpportunity, AiMarketingArticleCandidate, AiMarketingEmailLog)
        counts = [db.query(model).count() for model in models]
        monkeypatch.setenv("FMP_PROVIDER_DISABLED", "true")
        monkeypatch.setenv("FMP_API_KEY", "retained-test-key")
        for name in ("fetch_fmp_articles", "generate_suggestion", "send_draft_email", "record_campaign_run"):
            monkeypatch.setattr(marketing, name, forbidden)
        for _ in range(2):
            result = admin_ai_marketing_run_campaign(campaign.id, _request_for_user(admin), db)
            assert result["status"] == "provider_disabled"
            assert result["drafts_generated"] == result["emails_sent"] == result["articles_fetched"] == 0
            assert result["errors"] == []
        db.refresh(campaign)
        assert before == (campaign.status, campaign.enabled, campaign.last_run_at, campaign.next_run_at)
        assert counts == [db.query(model).count() for model in models]
    finally:
        db.close()


def test_disabled_provider_status_does_not_report_missing_credentials(monkeypatch):
    monkeypatch.setenv("FMP_PROVIDER_DISABLED", "true")
    monkeypatch.setattr(marketing, "resolved_setting_value", forbidden)
    result = marketing._article_provider_status()
    assert result["status"] == "disabled" and result["configured"] is False
    assert "disabled" in result["admin_message"]


@pytest.mark.parametrize("key_present", [True, False])
def test_public_config_reports_disabled_feed_without_credential_repair_prompt(monkeypatch, key_present):
    monkeypatch.setenv("FMP_PROVIDER_DISABLED", "true")
    if key_present:
        monkeypatch.setenv("FMP_API_KEY", "retained-test-key")
    else:
        monkeypatch.delenv("FMP_API_KEY", raising=False)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.setattr(marketing.requests, "get", forbidden)
    result = marketing.config_status()
    assert result["fmp_articles_status"] == "disabled"
    assert result["fmp_articles_configured"] is False
    assert result["fmp_articles_missing"] == []
    assert "Article feed credentials missing" not in result["warnings"]


def test_retirement_leaves_other_growth_schedulers_running(monkeypatch):
    monkeypatch.setenv("FMP_PROVIDER_DISABLED", "true")
    calls = []
    def other(db, **kwargs):
        calls.append(kwargs)
        return {"campaigns_checked": 1, "campaigns_run": 0, "items": []}
    monkeypatch.setattr(marketing, "run_due_scheduled_x_campaigns", other)
    monkeypatch.setattr(marketing, "run_due_x_reply_campaigns", other)
    result = marketing.run_due_ai_growth_campaigns(None, dry_run=True)
    assert len(calls) == 2 and all(call["dry_run"] for call in calls)
    assert result["campaigns_checked"] == 2
    assert result["article_reactive_x"]["status"] == "provider_disabled"
