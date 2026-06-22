from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest
from fastapi import HTTPException
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from starlette.requests import Request

from app.auth import SESSION_COOKIE_NAME, sign_session_payload
from app.db import Base, ensure_ai_marketing_schema, ensure_email_notification_schema
from app.models import AiMarketingEmailLog, AiMarketingOpportunity, AiMarketingSuggestion, UserAccount
from app.routers.ai_marketing import (
    CampaignPayload,
    EmailDigestPayload,
    ManualUrlPayload,
    admin_ai_marketing_campaigns,
    admin_ai_marketing_create_campaign,
    admin_ai_marketing_email_digest,
    admin_ai_marketing_manual_url,
    admin_ai_marketing_run_campaign,
)
from app.services.ai_marketing import SourceItem, recommended_destination_url
from app.services.email_templates import seed_default_email_templates


def _session():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    ensure_email_notification_schema(engine)
    ensure_ai_marketing_schema(engine)
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    db = Session()
    seed_default_email_templates(db)
    return db


def _request_for_user(user: UserAccount) -> Request:
    token = sign_session_payload({"uid": user.id, "email": user.email})
    return Request({"type": "http", "method": "POST", "path": "/", "headers": [(b"cookie", f"{SESSION_COOKIE_NAME}={token}".encode())]})


def _user(db, email: str, *, role: str = "user") -> UserAccount:
    user = UserAccount(email=email, role=role, entitlement_tier="premium")
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


def _campaign_payload(**overrides):
    payload = {
        "name": "Reddit ticker assist",
        "enabled": True,
        "mode": "ticker_thread_assist",
        "platforms": ["reddit"],
        "keywords": ["insider buying", "congress trades"],
        "tickers": ["NVDA"],
        "subreddits": ["stocks"],
        "minimum_relevance_score": 60,
        "max_items_per_run": 5,
        "default_destination_page": "https://walnutmarkets.com",
        "include_disclosure": True,
        "scheduled_digest_enabled": False,
    }
    payload.update(overrides)
    return CampaignPayload(**payload)


def test_ai_marketing_campaigns_require_admin():
    db = _session()
    try:
        user = _user(db, "reader@example.com")
        with pytest.raises(HTTPException) as exc:
            admin_ai_marketing_campaigns(_request_for_user(user), db)
        assert exc.value.status_code == 403

        admin = _user(db, "admin@example.com", role="admin")
        payload = admin_ai_marketing_campaigns(_request_for_user(admin), db)
        assert payload["items"] == []
        assert "openai_configured" in payload["config"]
    finally:
        db.close()


def test_manual_url_mode_saves_without_social_credentials(monkeypatch):
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("REDDIT_CLIENT_ID", raising=False)
    monkeypatch.delenv("REDDIT_CLIENT_SECRET", raising=False)
    monkeypatch.delenv("REDDIT_USER_AGENT", raising=False)
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        campaign = admin_ai_marketing_create_campaign(_campaign_payload(mode="manual_url_review"), _request_for_user(admin), db)
        result = admin_ai_marketing_manual_url(
            ManualUrlPayload(
                url="https://www.reddit.com/r/stocks/comments/example/thread/",
                text="Is there a good way to cross-check NVDA insider buying?",
                campaign_id=campaign["id"],
                generate=True,
            ),
            _request_for_user(admin),
            db,
        )

        assert result["warning"] == "OpenAI API key missing; manual opportunity was saved without an AI suggestion."
        assert result["opportunity"]["platform"] == "reddit"
        assert result["opportunity"]["matched_tickers"] == ["NVDA"]
        assert db.execute(select(AiMarketingOpportunity)).scalar_one().source_url.startswith("https://www.reddit.com/")
    finally:
        db.close()


def test_manual_url_mode_generates_from_pasted_text_without_reddit_credentials(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.delenv("REDDIT_CLIENT_ID", raising=False)
    monkeypatch.delenv("REDDIT_CLIENT_SECRET", raising=False)
    monkeypatch.delenv("REDDIT_USER_AGENT", raising=False)
    calls = []

    class FakeResponse:
        status_code = 200

        def json(self):
            return {
                "choices": [
                    {
                        "message": {
                            "content": json.dumps(
                                {
                                    "relevance_score": 91,
                                    "spam_risk_score": 12,
                                    "detected_tickers": ["NVDA"],
                                    "intent": "question",
                                    "suggested_destination_url": "https://walnutmarkets.com/ticker/NVDA",
                                    "suggested_reply": "I'm building Walnut, so obvious bias, but this may help with NVDA context.",
                                    "short_reason": "The pasted source asks about NVDA research.",
                                    "compliance_notes": "Disclose affiliation and avoid investment advice.",
                                }
                            )
                        }
                    }
                ]
            }

    def fake_post(url, *args, **kwargs):
        calls.append((url, kwargs))
        return FakeResponse()

    monkeypatch.setattr("app.services.ai_marketing.requests.post", fake_post)
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        campaign = admin_ai_marketing_create_campaign(_campaign_payload(mode="manual_url_review"), _request_for_user(admin), db)
        pasted_text = "Can I compare NVDA insider buying and Congress trades from one research page?"

        result = admin_ai_marketing_manual_url(
            ManualUrlPayload(
                url="https://www.reddit.com/r/stocks/comments/example/thread/",
                text=pasted_text,
                campaign_id=campaign["id"],
                generate=True,
            ),
            _request_for_user(admin),
            db,
        )

        assert result["warning"] is None
        assert result["opportunity"]["suggestion"]["relevance_score"] == 91
        assert len(calls) == 1
        assert calls[0][0] == "https://api.openai.com/v1/chat/completions"
        prompt = json.loads(calls[0][1]["json"]["messages"][1]["content"])
        assert prompt["opportunity"]["excerpt"] == pasted_text
    finally:
        db.close()


def test_manual_url_only_reddit_post_without_credentials_returns_validation(monkeypatch):
    monkeypatch.delenv("REDDIT_CLIENT_ID", raising=False)
    monkeypatch.delenv("REDDIT_CLIENT_SECRET", raising=False)
    monkeypatch.delenv("REDDIT_USER_AGENT", raising=False)
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        with pytest.raises(HTTPException) as exc:
            admin_ai_marketing_manual_url(
                ManualUrlPayload(url="https://www.reddit.com/r/stocks/comments/example/thread/"),
                _request_for_user(admin),
                db,
            )

        assert exc.value.status_code == 422
        assert exc.value.detail == (
            "Reddit API credentials are not configured. "
            "Paste the post/comment text manually or configure Reddit API credentials."
        )
        assert db.query(AiMarketingOpportunity).count() == 0
    finally:
        db.close()


def test_manual_url_subreddit_listing_without_text_returns_validation():
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        with pytest.raises(HTTPException) as exc:
            admin_ai_marketing_manual_url(
                ManualUrlPayload(url="https://www.reddit.com/r/stocks/"),
                _request_for_user(admin),
                db,
            )

        assert exc.value.status_code == 422
        assert exc.value.detail == (
            "Manual URL mode works best with a specific post/comment URL or pasted text. "
            "Subreddit listing URLs require Reddit API discovery."
        )
        assert db.query(AiMarketingOpportunity).count() == 0
    finally:
        db.close()


def test_campaign_run_dedupes_source_items(monkeypatch):
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)

    def fake_search(_adapter, _campaign):
        return [
            SourceItem(
                platform="reddit",
                source_id="t3_same",
                source_url="https://www.reddit.com/r/stocks/comments/same/thread/",
                title="NVDA insider buying question",
                excerpt="How do people check insider buying and Congress trades?",
                community="stocks",
                comment_count=4,
                source_created_at=datetime(2026, 6, 1, tzinfo=timezone.utc),
            )
        ]

    monkeypatch.setattr("app.services.ai_marketing.RedditSourceAdapter.search", fake_search)
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        campaign = admin_ai_marketing_create_campaign(_campaign_payload(), _request_for_user(admin), db)

        first = admin_ai_marketing_run_campaign(campaign["id"], _request_for_user(admin), db)
        second = admin_ai_marketing_run_campaign(campaign["id"], _request_for_user(admin), db)

        assert first["created"] == 1
        assert second["created"] == 0
        assert second["deduped"] == 1
        assert db.query(AiMarketingOpportunity).count() == 1
    finally:
        db.close()


def test_openai_suggestion_returns_structured_payload_with_walnut_utm(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")

    class FakeResponse:
        status_code = 200

        def json(self):
            return {
                "choices": [
                    {
                        "message": {
                            "content": json.dumps(
                                {
                                    "relevance_score": 88,
                                    "spam_risk_score": 18,
                                    "detected_tickers": ["NVDA"],
                                    "intent": "question",
                                    "suggested_destination_url": "https://walnutmarkets.com/ticker/NVDA",
                                    "suggested_reply": "I'm building Walnut, so obvious bias, but this may be useful for cross-checking NVDA insider context: https://walnutmarkets.com/ticker/NVDA",
                                    "short_reason": "The post asks for source-backed ticker research.",
                                    "compliance_notes": "Discloses affiliation and avoids investment advice.",
                                }
                            )
                        }
                    }
                ]
            }

    monkeypatch.setattr("app.services.ai_marketing.requests.post", lambda *args, **kwargs: FakeResponse())
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        campaign = admin_ai_marketing_create_campaign(_campaign_payload(), _request_for_user(admin), db)
        result = admin_ai_marketing_manual_url(
            ManualUrlPayload(
                url="https://www.reddit.com/r/stocks/comments/nvda/thread/",
                text="Can I see recent NVDA insider buying and Congress trades in one place?",
                campaign_id=campaign["id"],
                generate=True,
            ),
            _request_for_user(admin),
            db,
        )

        suggestion = result["opportunity"]["suggestion"]
        assert suggestion["relevance_score"] == 88
        assert suggestion["spam_risk_score"] == 18
        assert suggestion["suggested_destination_url"].startswith("https://walnutmarkets.com/ticker/NVDA?")
        assert "utm_source=reddit" in suggestion["suggested_destination_url"]
        assert "utm_campaign=ai_outreach" in suggestion["suggested_destination_url"]
        assert db.query(AiMarketingSuggestion).count() == 1
    finally:
        db.close()


def test_digest_send_uses_founder_recipient_and_marks_emailed(monkeypatch):
    sent = {}

    def fake_send_email(db, **kwargs):
        sent.update(kwargs)
        return {
            "id": 42,
            "status": "sent",
            "to_email": kwargs["to_email"],
            "subject": kwargs["context"]["subject"],
        }

    monkeypatch.setattr("app.services.ai_marketing.send_email", fake_send_email)
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        campaign = admin_ai_marketing_create_campaign(_campaign_payload(), _request_for_user(admin), db)
        opportunity_result = admin_ai_marketing_manual_url(
            ManualUrlPayload(
                url="https://www.reddit.com/r/stocks/comments/manual/thread/",
                text="NVDA research tools?",
                campaign_id=campaign["id"],
                generate=False,
            ),
            _request_for_user(admin),
            db,
        )
        opportunity = db.get(AiMarketingOpportunity, opportunity_result["opportunity"]["id"])
        db.add(
            AiMarketingSuggestion(
                opportunity_id=opportunity.id,
                campaign_id=campaign["id"],
                model="test-model",
                relevance_score=80,
                spam_risk_score=10,
                detected_tickers_json=json.dumps(["NVDA"]),
                intent="question",
                suggested_destination_url=recommended_destination_url(
                    mode="ticker_thread_assist",
                    platform="reddit",
                    campaign_id=campaign["id"],
                    tickers=["NVDA"],
                ),
                suggested_reply="I'm building Walnut, so obvious bias, but this may be useful.",
                short_reason="Relevant ticker research question.",
                compliance_notes="Review before posting.",
            )
        )
        db.commit()

        result = admin_ai_marketing_email_digest(
            EmailDigestPayload(send=True, opportunity_ids=[opportunity.id]),
            _request_for_user(admin),
            db,
        )

        assert sent["to_email"] == "jarod@walnutmarkets.com"
        assert sent["template_key"] == "ai_marketing.digest"
        assert result["email_log"]["status"] == "sent"
        assert db.execute(select(AiMarketingEmailLog)).scalar_one().delivery_id == 42
        db.refresh(opportunity)
        assert opportunity.status == "emailed"
    finally:
        db.close()
