from __future__ import annotations

import base64
import json
from datetime import date, datetime, timedelta, timezone
from urllib.parse import unquote

import pytest
from fastapi import HTTPException
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from starlette.requests import Request

from app.auth import SESSION_COOKIE_NAME, sign_session_payload
from app.db import Base, ensure_ai_marketing_schema, ensure_email_notification_schema
from app.models import (
    AiMarketingArticleCandidate,
    AiMarketingCampaignRun,
    AiMarketingEmailLog,
    AiMarketingOpportunity,
    AiMarketingSetting,
    AiMarketingSuggestion,
    ConfirmationMonitoringSnapshot,
    Security,
    UserAccount,
    Watchlist,
    WatchlistItem,
)
from app.routers.ai_marketing import (
    CampaignPayload,
    CampaignPatchPayload,
    EmailDigestPayload,
    GrowthDraftPayload,
    GrowthDraftRegeneratePayload,
    ManualUrlPayload,
    admin_ai_growth_email_action,
    admin_ai_growth_drafts,
    admin_ai_growth_clear_draft_history,
    admin_ai_growth_create_draft,
    admin_ai_growth_email_draft,
    admin_ai_growth_mark_copied,
    admin_ai_growth_mark_posted,
    admin_ai_growth_regenerate_draft,
    admin_ai_marketing_campaigns,
    admin_ai_marketing_create_campaign,
    admin_ai_marketing_delete_campaign,
    admin_ai_marketing_email_digest,
    admin_ai_marketing_manual_url,
    admin_ai_marketing_run_campaign,
    admin_ai_marketing_settings,
    admin_ai_marketing_update_campaign,
    router as ai_marketing_router,
)
from app.services.ai_marketing import (
    FMP_API_KEY,
    OPENAI_WEB_SEARCH_ENABLED,
    OPENAI_WEB_SEARCH_NOT_CONFIGURED_MESSAGE,
    ARTICLE_REACTIVE_CAMPAIGN_TYPE,
    SourceItem,
    X_ACCESS_TOKEN,
    X_CLIENT_ID,
    X_CLIENT_SECRET,
    X_CURRENT_ACCESS_TOKEN_SETTING,
    X_CURRENT_REFRESH_TOKEN_SETTING,
    X_POST_CHARACTER_LIMIT,
    X_REFRESH_TOKEN,
    _normalize_social_card_spec,
    _ensure_x_hashtags,
    _generated_thumbnail_asset,
    _social_card_asset,
    create_email_action_token,
    fetch_fmp_articles,
    score_article_candidate,
    recommended_destination_url,
)
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


def _email_action_request() -> Request:
    return Request({"type": "http", "method": "GET", "path": "/api/admin/ai-growth/email-action", "headers": [(b"accept", b"text/html")], "client": ("127.0.0.1", 12345)})


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


def _growth_openai_payload(**overrides):
    payload = {
        "relevance_score": 86,
        "spam_risk_score": 18,
        "detected_tickers": ["NVDA"],
        "intent": "tool_search",
        "campaign_type": "x_chart_drop",
        "content_type": "x_post",
        "platform": "x",
        "audience": "sophisticated retail investors",
        "recommended_action": "draft_post",
        "reply_angle": "ticker_context",
        "content_angle": "ticker context",
        "value_added_insight": "Use disclosed activity with price/volume confirmation and filings context.",
        "walnut_feature_to_mention": "ticker pages with evidence trail and signal context",
        "suggested_destination_url": "https://walnutmarkets.com/ticker/NVDA",
        "suggested_reply": "",
        "suggested_post": "I am building Walnut, so bias disclosed: NVDA has a cleaner read when reported Congress activity is checked against price/volume and filings context.",
        "suggested_ad_variants": [],
        "visual_brief": {
            "title": "NVDA disclosure stack",
            "chart_type": "ranked_bars",
            "metric_label": "Confirmation strength",
            "rows": [
                {"label": "Price/volume", "value": "82", "note": "Confirmation layer"},
                {"label": "Filings", "value": "74", "note": "Evidence layer"},
                {"label": "Disclosures", "value": "68", "note": "Reported activity"},
            ],
            "source_note": "Source: Walnut signal context and linked source.",
            "missing_data_note": "",
        },
        "social_card": {
            "card_type": "ticker_signal",
            "template": "ticker_signal",
            "ticker": "NVDA",
            "tickers": ["NVDA"],
            "sentiment": "bullish",
            "headline": "NVDA signal stack is active",
            "subheadline": "Price, filings, and disclosed activity are cleaner together than alone.",
            "bullets": [
                "Price and volume confirmation leads the stack.",
                "Filings context adds the evidence layer.",
                "Disclosed activity needs human review before posting.",
            ],
            "key_stats": [
                {"label": "Confirmation", "value": "82/100"},
                {"label": "Signal", "value": "Bullish"},
            ],
            "chips": ["Signals", "Filings", "Disclosures"],
            "cta": "Track the stack on Walnut",
            "url": "https://walnutmarkets.com/ticker/NVDA",
            "visual_emphasis": "confirmation stack",
            "source_label": "Walnut context",
            "tone": "market-native",
            "include_chart": True,
            "include_cta": True,
            "include_source_tag": True,
            "include_walnut_url": True,
        },
        "influencer_outreach_draft": "",
        "report_pack_outline": "",
        "alternate_hooks": ["The market has tells. Walnut finds them."],
        "title_options": [],
        "disclosure_text": "I am building Walnut, so bias disclosed.",
        "assets": [],
        "alternate_reply_more_direct": "",
        "short_reason": "Ticker-specific data story.",
        "compliance_notes": "Human review required. No investment advice.",
    }
    payload.update(overrides)
    return payload


def _reddit_dd_payload(**overrides):
    markdown = """# $NVDA DD: reported disclosure stack plus technical and fundamental context

## TL;DR
- NVDA surfaced because Walnut reported/disclosed signal context showed cross-source activity worth reviewing.
- The setup has constructive fundamentals, but valuation and execution risk matter.
- 13F institutional context reflects quarter-end reported holdings and filing date context, not live buying.
- This is research, not investment advice.

## Why this name came up
NVDA came up from a saved screen combining reported Congress activity, insider filings, institutional reported holdings/activity, and confirmation signals. It was selected today because the disclosure stack had fresh filing-date context and the technical picture was still constructive.

## Company snapshot
NVIDIA designs accelerated computing hardware and software for data centers, gaming, visualization, and automotive markets. The business model is tied to GPU/platform sales and ecosystem software, with market cap and exact valuation requiring live verification.

## Walnut disclosure stack
Congress activity is described as reported/disclosed activity with filing dates. Insider activity is based on filed Form 4 context. Institutional activity is reported holdings/activity from 13F filings, reflecting quarter-end holdings and filing date context rather than live trades. Government contracts and options flow should be verified where available.

## Technical picture
The technical context should review price trend, moving averages, support/resistance, RSI or momentum, relative volume, and breakout/breakdown context. Do not overstate technicals; this is a plain-English read of price behavior.

## Fundamental picture
The fundamental context should review revenue growth, earnings trend, margins, cash flow, balance sheet, debt/liquidity, valuation, earnings context, and forward expectations where verified.

## Recent news / filings / press releases
Recent company news, SEC filings, earnings releases, product announcements, and sector catalysts should be cited internally. Any item that could not be verified should be called out explicitly.

## Catalysts
Near-term catalysts include upcoming earnings, guidance updates, product/data-center demand signals, regulatory or export-control developments, and disclosure-related follow-through.

## Bull case
The strongest constructive argument is that reported/disclosed activity and fundamentals may align with durable AI infrastructure demand and technical strength.

## Bear case / risks
Risks include valuation risk, execution risk, earnings risk, technical breakdown risk, liquidity/debt surprises, macro/sector risk, and disclosure interpretation risk.

## What would confirm the setup
Confirmation would include continued revenue and margin execution, clean earnings commentary, resilient moving-average support, volume confirmation, and fresh reported/disclosed activity across the stack.

## What would weaken the setup
Weakening signals would include earnings misses, margin pressure, technical breakdown below key levels, stale disclosure data, or 13F/institutional activity being misread as live trading.

## Bottom line
NVDA is worth watching because multiple reported/disclosed data streams can be cross-checked against technical and fundamental context. This is not investment advice or a buy/sell recommendation.

## Suggested Reddit disclosure
I'm building Walnut Markets, a market intelligence terminal that tracks public disclosures, ticker context, and signal confirmation. Sharing this as research, not investment advice."""
    payload = _growth_openai_payload(
        campaign_type="reddit_research_thread",
        content_type="reddit_thread",
        platform="reddit",
        recommended_action="draft_post",
        social_card={
            "card_type": "research_cover",
            "template": "research_cover",
            "ticker": "NVDA",
            "tickers": ["NVDA"],
            "sentiment": "notable",
            "headline": "$NVDA DD: disclosure stack plus confirmation",
            "subheadline": "A research cover for technicals, fundamentals, catalysts, alternative data, and risks.",
            "bullets": [
                "Technical picture needs confirmation.",
                "Fundamentals and catalysts frame the setup.",
                "Disclosure intelligence adds the why-now layer.",
            ],
            "key_stats": [
                {"label": "Depth", "value": "DD"},
                {"label": "Risk", "value": "Balanced"},
            ],
            "chips": ["Research", "DD", "Evidence"],
            "cta": "Track the stack on Walnut",
            "url": "https://walnutmarkets.com/ticker/NVDA",
            "visual_emphasis": "research pillars",
            "source_label": "Walnut DD",
            "tone": "market-native",
            "include_chart": True,
            "include_cta": True,
            "include_source_tag": True,
            "include_walnut_url": True,
        },
        title="$NVDA DD: reported disclosure stack plus technical and fundamental context",
        tldr_bullets=[
            "NVDA surfaced from reported/disclosed cross-source activity.",
            "The thesis needs technical, fundamental, catalyst, and risk confirmation.",
            "13F context is reported quarter-end holdings, not live buying.",
        ],
        why_selected="Selected from a saved screen with reported Congress, insider, institutional, and signal context.",
        company_snapshot="NVIDIA designs accelerated computing hardware and software for data centers and other markets.",
        walnut_disclosure_stack="Congress and insider items are reported/disclosed filings; institutional activity is 13F reported holdings/activity with filing date context.",
        technical_picture="Review trend, moving averages, support/resistance, momentum, RSI, and volume confirmation.",
        fundamental_picture="Review revenue growth, earnings trend, margins, cash flow, balance sheet, debt/liquidity, and valuation.",
        recent_news_and_filings="Recent news, filings, press releases, and earnings context should be cited internally; unverified items are noted.",
        catalysts="Upcoming earnings, guidance, product launches, sector demand, and disclosure follow-through.",
        bull_case="Reported/disclosed activity may align with strong AI infrastructure demand and constructive technicals.",
        bear_case_and_risks="Valuation, execution, earnings, technical breakdown, macro/sector, and disclosure interpretation risks.",
        what_would_confirm="Revenue/margin execution, volume confirmation, resilient moving averages, and fresh reported/disclosed activity.",
        what_would_weaken="Earnings miss, margin pressure, technical breakdown, stale data, or 13F data misread as live trades.",
        bottom_line="Worth watching, not investment advice, because disclosure stack signals can be cross-checked against fundamentals and technicals.",
        reddit_disclosure="I'm building Walnut Markets, a market intelligence terminal that tracks public disclosures, ticker context, and signal confirmation. Sharing this as research, not investment advice.",
        full_reddit_post_markdown=markdown,
        source_notes=["Internal Walnut disclosure stack and public market context."],
        missing_data_notes=["Market cap, live technical levels, and latest filings should be verified before posting."],
        quality_scores={
            "research_depth_score": 88,
            "evidence_score": 82,
            "catalyst_score": 80,
            "balance_score": 86,
            "reddit_native_score": 78,
            "promotional_risk_score": 18,
            "compliance_risk_score": 14,
        },
        suggested_image_asset={
            "title": "NVDA disclosure stack card",
            "asset_type": "image",
            "url": "https://walnutmarkets.com/admin/assets/nvda-dd.png",
            "thumbnail_url": "https://walnutmarkets.com/admin/assets/nvda-dd-thumb.png",
            "suggested_caption": "Reported disclosures plus technical/fundamental context.",
            "source_data_notes": "Walnut disclosure stack snapshot.",
        },
        suggested_flair="DD",
        suggested_subreddits=["stocks", "SecurityAnalysis"],
        suggested_post=markdown,
        title_options=["$NVDA DD: reported disclosure stack plus technical and fundamental context"],
        disclosure_text="I'm building Walnut Markets. Sharing this as research, not investment advice.",
        short_reason="Comprehensive Reddit DD draft with disclosure-stack, technical, fundamental, catalyst, and risk context.",
    )
    payload.update(overrides)
    return payload


def _mock_openai(monkeypatch, payload):
    class FakeResponse:
        status_code = 200

        def json(self):
            return {
                "choices": [{"message": {"content": json.dumps(payload)}}],
                "usage": {
                    "prompt_tokens": 1000,
                    "completion_tokens": 250,
                    "prompt_tokens_details": {"cached_tokens": 100},
                },
            }

    monkeypatch.setattr("app.services.ai_marketing.requests.post", lambda *args, **kwargs: FakeResponse())


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


def test_ai_marketing_campaign_lifecycle_controls():
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        request = _request_for_user(admin)
        campaign = admin_ai_marketing_create_campaign(_campaign_payload(), request, db)

        paused = admin_ai_marketing_update_campaign(
            campaign["id"],
            CampaignPatchPayload(enabled=False, status="paused"),
            request,
            db,
        )
        assert paused["enabled"] is False
        assert paused["status"] == "paused"
        paused_run = admin_ai_marketing_run_campaign(campaign["id"], request, db)
        assert paused_run["status"] == "paused"
        assert paused_run["created"] == 0

        stopped = admin_ai_marketing_update_campaign(
            campaign["id"],
            CampaignPatchPayload(enabled=False, status="stopped"),
            request,
            db,
        )
        assert stopped["enabled"] is False
        assert stopped["status"] == "stopped"
        stopped_run = admin_ai_marketing_run_campaign(campaign["id"], request, db)
        assert stopped_run["status"] == "stopped"
        assert stopped_run["created"] == 0

        active = admin_ai_marketing_update_campaign(
            campaign["id"],
            CampaignPatchPayload(enabled=True, status="active"),
            request,
            db,
        )
        assert active["enabled"] is True
        assert active["status"] == "active"

        deleted = admin_ai_marketing_delete_campaign(campaign["id"], request, db)
        assert deleted == {"ok": True, "id": campaign["id"]}
        assert admin_ai_marketing_campaigns(request, db)["items"] == []
    finally:
        db.close()


def test_scheduled_x_campaign_lifecycle_run_email_and_delete(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "openai-test")
    _mock_openai(
        monkeypatch,
        _growth_openai_payload(
            campaign_type="scheduled_x_campaign",
            content_type="x_post",
            platform="x",
            suggested_post="Daily watchlist opportunity: NVDA has a cleaner signal stack today. Cross-check price, filings, and disclosure context in Walnut. $NVDA #Markets",
        ),
    )
    sent = []

    def fake_send_email(db, **kwargs):
        sent.append(kwargs)
        return {"id": 123, "status": "sent"}

    monkeypatch.setattr("app.services.ai_marketing.send_email", fake_send_email)
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        request = _request_for_user(admin)
        campaign = admin_ai_marketing_create_campaign(
            CampaignPayload(
                name="Daily Watchlist Opportunities",
                enabled=True,
                status="active",
                mode="scheduled_x_campaign",
                campaign_type="scheduled_x_campaign",
                content_type="x_post",
                schedule_config={"cadence": "weekdays"},
                weekdays_only=True,
                run_time="06:30",
                timezone="America/Los_Angeles",
                recipient_email="jarod@walnutmarkets.com",
                source_type="watchlist",
                source_reference_id="AI Leaders",
                filters={"min_signal_score": 70},
                output_preferences={"tone": "market-native", "cta_mode": "soft", "hashtag_mode": "ticker/theme only"},
                platforms=["x"],
                max_items_per_run=1,
                max_drafts_per_day=1,
            ),
            request,
            db,
        )
        assert campaign["campaign_type"] == "scheduled_x_campaign"
        assert campaign["source_type"] == "watchlist"
        assert campaign["run_time"] == "06:30"

        edited = admin_ai_marketing_update_campaign(
            campaign["id"],
            CampaignPatchPayload(run_time="07:15", timezone="America/New_York", max_drafts_per_day=2, status="paused", enabled=False),
            request,
            db,
        )
        assert edited["run_time"] == "07:15"
        assert edited["timezone"] == "America/New_York"
        assert edited["max_drafts_per_day"] == 2
        assert edited["status"] == "paused"

        active = admin_ai_marketing_update_campaign(campaign["id"], CampaignPatchPayload(status="active", enabled=True), request, db)
        assert active["enabled"] is True
        result = admin_ai_marketing_run_campaign(campaign["id"], request, db)
        assert result["drafts_generated"] == 2
        assert result["emails_sent"] == 2
        assert sent and sent[0]["to_email"] == "jarod@walnutmarkets.com"
        assert db.query(AiMarketingOpportunity).filter(AiMarketingOpportunity.campaign_id == campaign["id"]).count() == 2
        run = db.query(AiMarketingCampaignRun).filter(AiMarketingCampaignRun.campaign_id == campaign["id"]).one()
        assert run.status == "ok"
        assert run.drafts_generated == 2

        payload = admin_ai_marketing_campaigns(request, db)
        row = next(item for item in payload["items"] if item["id"] == campaign["id"])
        assert row["recent_runs"][0]["drafts_generated"] == 2
        assert row["last_status"] == "ok"

        deleted = admin_ai_marketing_delete_campaign(campaign["id"], request, db)
        assert deleted == {"ok": True, "id": campaign["id"]}
        assert all(item["id"] != campaign["id"] for item in admin_ai_marketing_campaigns(request, db)["items"])
    finally:
        db.close()


def test_ai_growth_clear_draft_history_hides_generated_assets():
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        request = _request_for_user(admin)
        first = admin_ai_growth_create_draft(
            GrowthDraftPayload(
                campaign_type="x_chart_drop",
                content_type="x_post",
                source_platform="x",
                title="NVDA X draft",
                text="NVDA context",
                generate=False,
            ),
            request,
            db,
        )
        second = admin_ai_growth_create_draft(
            GrowthDraftPayload(
                campaign_type="reddit_research_thread",
                content_type="reddit_thread",
                source_platform="reddit",
                title="MSFT DD draft",
                text="MSFT context",
                generate=False,
            ),
            request,
            db,
        )
        assert {first["opportunity"]["id"], second["opportunity"]["id"]} == {
            item["id"] for item in admin_ai_growth_drafts(request, db, status="all", limit=50)["items"]
        }

        cleared = admin_ai_growth_clear_draft_history(request, db)

        assert cleared == {"ok": True, "cleared": 2}
        assert admin_ai_growth_drafts(request, db, status="all", limit=50)["items"] == []
        assert db.query(AiMarketingOpportunity).filter(AiMarketingOpportunity.status == "dismissed").count() == 2
    finally:
        db.close()


def test_scheduled_x_campaign_uses_real_confirmation_triggers(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "openai-test")
    _mock_openai(
        monkeypatch,
        _growth_openai_payload(
            campaign_type="scheduled_x_campaign",
            content_type="x_post",
            platform="x",
            detected_tickers=["SPY"],
            suggested_destination_url="https://app.walnutmarkets.com/ticker/SPY",
            suggested_post="$SPY hit our Bullish Confirmation monitor this week. Bullish MACD. Neutral RSI at 54. Institutional accumulation. 73/100 confirmation score. One signal is noise. A stack is intelligence. https://app.walnutmarkets.com/ticker/SPY $SPY",
        ),
    )
    monkeypatch.setattr("app.services.ai_marketing.send_email", lambda *args, **kwargs: {"id": 123, "status": "sent"})
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        request = _request_for_user(admin)
        db.add(
            ConfirmationMonitoringSnapshot(
                user_id=admin.id,
                watchlist_id=1,
                ticker="SPY",
                score=73,
                band="strong",
                direction="bullish",
                source_count=3,
                status="3-source bullish confirmation",
                source_states_json=json.dumps(
                    {
                        "price_volume": {"present": True, "direction": "bullish", "label": "Bullish tape confirmation", "summary": "Bullish MACD. Neutral RSI at 54."},
                        "institutional_activity": {"present": True, "direction": "bullish", "label": "Institutional Activity", "summary": "Net reported accumulation."},
                        "macro_positioning": {"present": True, "direction": "neutral", "label": "Macro Positioning", "summary": "Macro positioning currently supports banks."},
                    }
                ),
                observed_at=datetime.now(timezone.utc),
            )
        )
        db.commit()

        campaign = admin_ai_marketing_create_campaign(
            CampaignPayload(
                name="Daily Bullish Confirmation",
                enabled=True,
                status="active",
                mode="scheduled_x_campaign",
                campaign_type="scheduled_x_campaign",
                content_type="x_post",
                schedule_config={"cadence": "weekdays"},
                weekdays_only=True,
                run_time="09:30",
                timezone="America/Los_Angeles",
                recipient_email="jarod@walnutmarkets.com",
                source_type="bullish_confirmation",
                source_reference_id="Bullish confirmation monitor",
                output_preferences={"tone": "sharp", "cta_mode": "soft", "hashtag_mode": "ticker/theme only"},
                platforms=["x"],
                max_items_per_run=1,
                max_drafts_per_day=1,
            ),
            request,
            db,
        )

        result = admin_ai_marketing_run_campaign(campaign["id"], request, db)
        assert result["drafts_generated"] == 1
        opportunity = db.query(AiMarketingOpportunity).filter(AiMarketingOpportunity.campaign_id == campaign["id"]).one()
        metadata = json.loads(opportunity.raw_metadata_json)
        assert "$SPY" in opportunity.excerpt
        assert "https://app.walnutmarkets.com/ticker/SPY" in opportunity.excerpt
        assert "73/100 confirmation score" in opportunity.excerpt
        assert "Price / Volume" in opportunity.excerpt
        assert "Institutional Activity" in opportunity.excerpt
        assert "Net reported accumulation" in opportunity.excerpt
        assert "Macro Positioning" in opportunity.excerpt
        assert "SPY" in json.loads(opportunity.matched_tickers_json)
        assert metadata["article_tickers"] == ["SPY"]
        assert metadata["walnut_context"][0]["ticker"] == "SPY"
        assert metadata["walnut_context"][0]["ticker_url"] == "https://app.walnutmarkets.com/ticker/SPY"
        assert metadata["walnut_context"][0]["relevant_url"] == "https://app.walnutmarkets.com/ticker/SPY"
        assert metadata["walnut_context"][0]["source_count"] == 3
        assert [item["key"] for item in metadata["walnut_context"][0]["source_stack"][:3]] == [
            "price_volume",
            "institutional_activity",
            "macro_positioning",
        ]
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
        assert result["opportunity"]["metadata"]["ai_suggestion_error"] == (
            "OpenAI API key missing. Configure OPENAI_API_KEY, then regenerate."
        )
        assert db.execute(select(AiMarketingOpportunity)).scalar_one().source_url.startswith("https://www.reddit.com/")
    finally:
        db.close()


def test_manual_url_mode_generates_from_pasted_text_without_reddit_credentials(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.delenv(OPENAI_WEB_SEARCH_ENABLED, raising=False)
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
                                    "recommended_action": "reply",
                                    "reply_angle": "ticker_context",
                                    "value_added_insight": "NVDA research should cross-check insider and Congress activity with company context.",
                                    "walnut_feature_to_mention": "ticker pages with insider and Congress activity",
                                    "suggested_destination_url": "https://walnutmarkets.com/ticker/NVDA",
                                    "suggested_reply": "I'm building Walnut, so obvious bias, but this may help with NVDA context.",
                                    "alternate_reply_more_direct": "Bias disclosed - I'm building Walnut, and the NVDA ticker page pulls this context together.",
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


def test_manual_url_invalid_openai_key_returns_helpful_warning(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "sk-invalid")

    class FakeResponse:
        status_code = 401

        def json(self):
            return {"error": {"message": "Incorrect API key provided.", "type": "invalid_request_error", "code": "invalid_api_key"}}

    monkeypatch.setattr("app.services.ai_marketing.requests.post", lambda *args, **kwargs: FakeResponse())
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        campaign = admin_ai_marketing_create_campaign(_campaign_payload(mode="manual_url_review"), _request_for_user(admin), db)

        result = admin_ai_marketing_manual_url(
            ManualUrlPayload(
                url="https://www.reddit.com/r/stocks/comments/example/thread/",
                text="Can I compare NVDA insider buying and Congress trades?",
                campaign_id=campaign["id"],
                generate=True,
            ),
            _request_for_user(admin),
            db,
        )

        assert result["warning"] == (
            "OpenAI API key was rejected. Check the OPENAI_API_KEY server environment variable, then regenerate."
        )
        assert result["opportunity"]["suggestion"] is None
        assert result["opportunity"]["metadata"]["ai_suggestion_error"] == result["warning"]
        assert result["opportunity"]["metadata"]["ai_suggestion_error_code"] == "invalid_key"
    finally:
        db.close()


def test_manual_url_openai_insufficient_quota_returns_billing_message(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "sk-no-credits")

    class FakeResponse:
        status_code = 429

        def json(self):
            return {
                "error": {
                    "message": "You exceeded your current quota, please check your plan and billing details.",
                    "type": "insufficient_quota",
                    "code": "insufficient_quota",
                }
            }

    monkeypatch.setattr("app.services.ai_marketing.requests.post", lambda *args, **kwargs: FakeResponse())
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        campaign = admin_ai_marketing_create_campaign(_campaign_payload(mode="manual_url_review"), _request_for_user(admin), db)

        result = admin_ai_marketing_manual_url(
            ManualUrlPayload(
                url="https://www.reddit.com/r/stocks/comments/example/thread/",
                text="Can I compare NVDA insider buying and Congress trades?",
                campaign_id=campaign["id"],
                generate=True,
            ),
            _request_for_user(admin),
            db,
        )

        assert result["warning"] == (
            "OpenAI API billing/credits are unavailable. Add credits in the OpenAI Platform billing page, then regenerate."
        )
        assert result["opportunity"]["suggestion"] is None
        assert result["opportunity"]["metadata"]["ai_suggestion_error"] == result["warning"]
        assert result["opportunity"]["metadata"]["ai_suggestion_error_code"] == "insufficient_quota"
        assert result["opportunity"]["metadata"]["ai_suggestion_error_status_code"] == 429
    finally:
        db.close()


def test_manual_url_generic_openai_failure_returns_safe_message(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")

    class FakeResponse:
        status_code = 500

        def json(self):
            return {"error": {"message": "Internal server error.", "type": "server_error"}}

    monkeypatch.setattr("app.services.ai_marketing.requests.post", lambda *args, **kwargs: FakeResponse())
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        campaign = admin_ai_marketing_create_campaign(_campaign_payload(mode="manual_url_review"), _request_for_user(admin), db)

        result = admin_ai_marketing_manual_url(
            ManualUrlPayload(
                url="https://www.reddit.com/r/stocks/comments/example/thread/",
                text="Can I compare NVDA insider buying and Congress trades?",
                campaign_id=campaign["id"],
                generate=True,
            ),
            _request_for_user(admin),
            db,
        )

        assert result["warning"] == "OpenAI suggestion request failed. Check OpenAI status and the configured model, then regenerate."
        assert result["opportunity"]["suggestion"] is None
        assert result["opportunity"]["metadata"]["ai_suggestion_error"] == result["warning"]
        assert result["opportunity"]["metadata"]["ai_suggestion_error_code"] == "openai_error"
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


def test_web_search_provider_missing_returns_clear_admin_warning(monkeypatch):
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv(OPENAI_WEB_SEARCH_ENABLED, raising=False)
    monkeypatch.delenv("BING_SEARCH_API_KEY", raising=False)
    monkeypatch.delenv("REDDIT_CLIENT_ID", raising=False)
    monkeypatch.delenv("REDDIT_CLIENT_SECRET", raising=False)
    monkeypatch.delenv("REDDIT_USER_AGENT", raising=False)
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        campaign = admin_ai_marketing_create_campaign(
            _campaign_payload(platforms=["web_search_reddit"], query_templates=["site:reddit.com/r/{subreddit} {term}"]),
            _request_for_user(admin),
            db,
        )

        result = admin_ai_marketing_run_campaign(campaign["id"], _request_for_user(admin), db)

        assert result["created"] == 0
        assert OPENAI_WEB_SEARCH_NOT_CONFIGURED_MESSAGE in result["warnings"]
        assert all("Reddit discovery disabled" not in warning for warning in result["warnings"])
        assert db.query(AiMarketingOpportunity).count() == 0
    finally:
        db.close()


def test_web_search_results_create_deduped_reddit_opportunities_without_fetching_reddit(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setenv(OPENAI_WEB_SEARCH_ENABLED, "true")
    called_urls = []

    class FakeSearchResponse:
        status_code = 200

        def json(self):
            return {
                "output_text": json.dumps(
                    {
                        "results": [
                            {
                                "title": "NVDA insider buying discussion",
                                "url": "https://www.reddit.com/r/stocks/comments/same/thread/?utm_source=search",
                                "snippet": "How are people checking NVDA insider buying and Congress trades?",
                            },
                            {
                                "title": "Duplicate NVDA thread",
                                "url": "https://old.reddit.com/r/stocks/comments/same/thread/#comments",
                                "snippet": "Same discussion through an old Reddit URL.",
                            },
                        ]
                    }
                )
            }

    class FakeSuggestionResponse:
        status_code = 200

        def json(self):
            return {
                "choices": [
                    {"message": {"content": json.dumps(_growth_openai_payload(content_type="reddit_reply", platform="reddit"))}}
                ]
            }

    def fake_post(url, **kwargs):
        called_urls.append(url)
        if url == "https://api.openai.com/v1/responses":
            return FakeSearchResponse()
        return FakeSuggestionResponse()

    monkeypatch.setattr("app.services.ai_marketing.requests.post", fake_post)
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        campaign = admin_ai_marketing_create_campaign(
            _campaign_payload(
                platforms=["web_search_reddit"],
                keywords=["insider buying"],
                tickers=["NVDA"],
                subreddits=["stocks"],
                query_templates=["site:reddit.com/r/{subreddit} {term}"],
                max_items_per_run=5,
            ),
            _request_for_user(admin),
            db,
        )

        first = admin_ai_marketing_run_campaign(campaign["id"], _request_for_user(admin), db)
        second = admin_ai_marketing_run_campaign(campaign["id"], _request_for_user(admin), db)

        assert first["created"] == 1
        assert second["created"] == 0
        assert second["deduped"] == 1
        assert db.query(AiMarketingOpportunity).count() == 1
        opportunity = db.execute(select(AiMarketingOpportunity)).scalar_one()
        assert opportunity.source_provider == "web_search_reddit"
        assert opportunity.source_url == "https://www.reddit.com/r/stocks/comments/same/thread"
        metadata = json.loads(opportunity.raw_metadata_json)
        assert metadata["web_search_provider"] == "openai_web_search"
        assert metadata["query"].startswith("site:reddit.com/r/stocks")
        assert metadata["snippet_only"] is True
        assert all("reddit.com" not in url for url in called_urls)
    finally:
        db.close()


def test_reddit_research_threads_attach_web_and_walnut_context(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setenv(OPENAI_WEB_SEARCH_ENABLED, "true")
    prompts = []

    class FakeSearchResponse:
        status_code = 200

        def __init__(self, payload):
            self._payload = payload

        def json(self):
            return {"output_text": json.dumps({"results": self._payload})}

    class FakeSuggestionResponse:
        status_code = 200

        def json(self):
            return {
                "choices": [
                    {"message": {"content": json.dumps(_reddit_dd_payload(content_type="reddit_thread", platform="reddit"))}}
                ]
            }

    def fake_post(url, **kwargs):
        if url == "https://api.openai.com/v1/responses":
            prompt = kwargs["json"]["input"]
            prompts.append(prompt)
            if "site:reddit.com/r" in prompt:
                return FakeSearchResponse(
                    [
                        {
                            "title": "NVDA DD setup discussion",
                            "url": "https://www.reddit.com/r/wallstreetbets/comments/nvda/dd_setup/",
                            "snippet": "NVDA bulls are debating earnings catalysts, risks, and technical setup.",
                        }
                    ]
                )
            return FakeSearchResponse(
                [
                    {
                        "title": "NVIDIA latest earnings and product catalysts",
                        "url": "https://investor.nvidia.com/news/",
                        "snippet": "Recent public updates highlight data-center demand, product launches, and margin considerations.",
                    }
                ]
            )
        return FakeSuggestionResponse()

    monkeypatch.setattr("app.services.ai_marketing.requests.post", fake_post)
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        db.add(
            ConfirmationMonitoringSnapshot(
                user_id=admin.id,
                watchlist_id=1,
                ticker="NVDA",
                score=88,
                band="strong",
                direction="bullish",
                source_count=3,
                status="3-source bullish confirmation",
                source_states_json=json.dumps({"congress": {"present": True}, "insider": {"present": True}}),
                observed_at=datetime.now(timezone.utc),
            )
        )
        db.commit()
        campaign = admin_ai_marketing_create_campaign(
            _campaign_payload(
                name="Reddit DD threads",
                mode="reddit_research_thread",
                campaign_type="reddit_research_thread",
                content_type="reddit_thread",
                platforms=["web_search_reddit"],
                keywords=["DD"],
                tickers=["NVDA"],
                subreddits=["wallstreetbets"],
                query_templates=["site:reddit.com/r/{subreddit} {term}"],
                max_items_per_run=2,
            ),
            _request_for_user(admin),
            db,
        )

        result = admin_ai_marketing_run_campaign(campaign["id"], _request_for_user(admin), db)

        assert result["created"] == 1
        assert any("latest filings" in prompt for prompt in prompts)
        opportunity = db.execute(select(AiMarketingOpportunity)).scalar_one()
        metadata = json.loads(opportunity.raw_metadata_json)
        assert metadata["article_tickers"] == ["NVDA"]
        assert metadata["web_market_context"][0]["url"] == "https://investor.nvidia.com/news/"
        assert metadata["walnut_context"]["ticker_pages"] == [{"ticker": "NVDA", "url": "https://walnutmarkets.com/ticker/NVDA"}]
        assert metadata["walnut_context"]["confirmation"][0]["ticker"] == "NVDA"
    finally:
        db.close()


def test_openai_web_search_provider_uses_responses_api(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setenv("AI_MARKETING_MODEL", "gpt-web-test")
    monkeypatch.setenv(OPENAI_WEB_SEARCH_ENABLED, "true")
    captured = {}

    class FakeSearchResponse:
        status_code = 200

        def json(self):
            return {
                "output": [
                    {
                        "type": "message",
                        "content": [
                            {
                                "type": "output_text",
                                "text": json.dumps(
                                    {
                                        "results": [
                                            {
                                                "title": "NVDA market discussion",
                                                "url": "https://www.reddit.com/r/investing/comments/nvda/research_tools/",
                                                "snippet": "Investors compare NVDA filings, insider activity, and Congress trades.",
                                            }
                                        ]
                                    }
                                ),
                            }
                        ],
                    }
                ]
            }

    class FakeSuggestionResponse:
        status_code = 200

        def json(self):
            return {
                "choices": [
                    {"message": {"content": json.dumps(_growth_openai_payload(content_type="reddit_reply", platform="reddit"))}}
                ]
            }

    def fake_post(url, **kwargs):
        if url == "https://api.openai.com/v1/responses":
            captured["search_payload"] = kwargs["json"]
            captured["search_headers"] = kwargs["headers"]
            return FakeSearchResponse()
        return FakeSuggestionResponse()

    monkeypatch.setattr("app.services.ai_marketing.requests.post", fake_post)
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        campaign = admin_ai_marketing_create_campaign(
            _campaign_payload(
                platforms=["web_search_reddit"],
                keywords=["research tools"],
                subreddits=["investing"],
                query_templates=["site:reddit.com/r/{subreddit} {term}"],
                max_items_per_run=1,
            ),
            _request_for_user(admin),
            db,
        )

        result = admin_ai_marketing_run_campaign(campaign["id"], _request_for_user(admin), db)

        assert result["created"] == 1
        assert captured["search_payload"]["model"] == "gpt-web-test"
        assert captured["search_payload"]["tools"] == [{"type": "web_search"}]
        assert captured["search_headers"]["Authorization"] == "Bearer test-key"
    finally:
        db.close()


def test_web_search_openai_scoring_receives_title_snippet_and_url(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setenv(OPENAI_WEB_SEARCH_ENABLED, "true")
    captured = {}

    class FakeSearchResponse:
        status_code = 200

        def json(self):
            return {
                "output_text": json.dumps(
                    {
                        "results": [
                            {
                                "title": "Is there one page for NVDA research?",
                                "url": "https://www.reddit.com/r/investing/comments/nvda/research_tools/",
                                "snippet": "Looking for a way to compare NVDA filings, insider activity, and Congress trades.",
                            }
                        ]
                    }
                )
            }

    class FakeOpenAIResponse:
        status_code = 200

        def json(self):
            return {
                "choices": [
                    {
                        "message": {
                            "content": json.dumps(
                                {
                                    "relevance_score": 90,
                                    "spam_risk_score": 12,
                                    "detected_tickers": ["NVDA"],
                                    "intent": "tool_search",
                                    "recommended_action": "reply",
                                    "reply_angle": "ticker_context",
                                    "value_added_insight": "The search result asks for a ticker research workflow.",
                                    "walnut_feature_to_mention": "NVDA ticker page with filings, insider activity, and Congress trades",
                                    "suggested_destination_url": "https://walnutmarkets.com/ticker/NVDA",
                                    "suggested_reply": "I'm building Walnut, so obvious bias, but this is exactly the kind of NVDA cross-check it helps with.",
                                    "alternate_reply_more_direct": "",
                                    "short_reason": "Relevant ticker research tooling request.",
                                    "compliance_notes": "Uses search snippet only; review before posting.",
                                }
                            )
                        }
                    }
                ]
            }

    def fake_post(url, **kwargs):
        if url == "https://api.openai.com/v1/responses":
            captured["search_payload"] = kwargs["json"]
            return FakeSearchResponse()
        captured["url"] = url
        captured["payload"] = kwargs["json"]
        return FakeOpenAIResponse()

    monkeypatch.setattr("app.services.ai_marketing.requests.post", fake_post)
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        campaign = admin_ai_marketing_create_campaign(
            _campaign_payload(
                platforms=["web_search_reddit"],
                keywords=["research tools"],
                tickers=["NVDA"],
                subreddits=["investing"],
                query_templates=["site:reddit.com/r/{subreddit} {term}"],
                max_items_per_run=1,
            ),
            _request_for_user(admin),
            db,
        )

        result = admin_ai_marketing_run_campaign(campaign["id"], _request_for_user(admin), db)

        prompt = json.loads(captured["payload"]["messages"][1]["content"])
        opportunity_payload = prompt["opportunity"]
        assert result["suggested"] == 1
        assert captured["search_payload"]["tools"] == [{"type": "web_search"}]
        assert opportunity_payload["source_provider"] == "web_search_reddit"
        assert opportunity_payload["title"] == "Is there one page for NVDA research?"
        assert opportunity_payload["excerpt"] == "Looking for a way to compare NVDA filings, insider activity, and Congress trades."
        assert opportunity_payload["source_url"] == "https://www.reddit.com/r/investing/comments/nvda/research_tools"
        assert opportunity_payload["metadata"]["snippet_only"] is True
        assert opportunity_payload["metadata"]["discovery_query"].startswith("site:reddit.com/r/investing")
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
                                    "recommended_action": "reply",
                                    "reply_angle": "ticker_context",
                                    "value_added_insight": "The thread is asking how to validate NVDA insider and Congress activity in context.",
                                    "walnut_feature_to_mention": "ticker pages with filings, insiders, Congress trades, and signal context",
                                    "suggested_destination_url": "https://walnutmarkets.com/ticker/NVDA",
                                    "suggested_reply": "I'm building Walnut, so obvious bias, but this may be useful for cross-checking NVDA insider context: https://walnutmarkets.com/ticker/NVDA",
                                    "alternate_reply_more_direct": "Bias disclosed - I'm building Walnut. The NVDA page pulls insider, Congress, filings, and signal context together.",
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
        assert suggestion["recommended_action"] == "reply"
        assert suggestion["reply_angle"] == "ticker_context"
        assert "insider" in suggestion["walnut_feature_to_mention"].lower()
        assert suggestion["suggested_destination_url"].startswith("https://walnutmarkets.com/ticker/NVDA?")
        assert "utm_source=reddit" in suggestion["suggested_destination_url"]
        assert "utm_campaign=ai_outreach" in suggestion["suggested_destination_url"]
        assert db.query(AiMarketingSuggestion).count() == 1
    finally:
        db.close()


def test_openai_suggestion_supports_specific_margin_analysis_reply(monkeypatch):
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
                                    "relevance_score": 86,
                                    "spam_risk_score": 14,
                                    "detected_tickers": ["NKE"],
                                    "intent": "trade_idea",
                                    "recommended_action": "reply",
                                    "reply_angle": "margin_analysis",
                                    "value_added_insight": "Revenue growth can look fine while gross margin and operating leverage weaken the setup.",
                                    "walnut_feature_to_mention": "financials, filings, and ticker context on one ticker page",
                                    "suggested_destination_url": "https://walnutmarkets.com/ticker/NKE",
                                        "suggested_reply": (
                                            "I'm building Walnut, so obvious bias, but that margin point is exactly what gets missed "
                                            "when people only look at revenue growth. Sales can be up while gross margin, freight, product mix, "
                                            "discounting, or operating leverage eats the upside. Walnut's ticker pages put filings, "
                                        "financials, insider/Congress activity, and signal context next to the chart, which is useful "
                                        "for checking whether the issue is brand demand or margin mechanics."
                                    ),
                                    "alternate_reply_more_direct": (
                                        "Bias disclosed - I'm building Walnut. For NKE, I would start by comparing revenue growth "
                                        "against gross margin, operating margin, and segment commentary before blaming the brand alone."
                                    ),
                                    "short_reason": "The source is a ticker-specific margin discussion where Walnut can add context.",
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
        campaign = admin_ai_marketing_create_campaign(_campaign_payload(mode="manual_url_review"), _request_for_user(admin), db)
        result = admin_ai_marketing_manual_url(
            ManualUrlPayload(
                url="https://www.reddit.com/r/stocks/comments/nke/thread/",
                text="NKE revenue is still growing, but margins look ugly. Is this a brand problem or a cost structure problem?",
                campaign_id=campaign["id"],
                generate=True,
            ),
            _request_for_user(admin),
            db,
        )

        suggestion = result["opportunity"]["suggestion"]
        assert suggestion["recommended_action"] == "reply"
        assert suggestion["reply_angle"] == "margin_analysis"
        assert "gross margin" in suggestion["suggested_reply"]
        assert "filings" in suggestion["suggested_reply"]
        assert "I'm building Walnut" in suggestion["suggested_reply"]
        assert "you should buy" not in suggestion["suggested_reply"].lower()
        assert "you should sell" not in suggestion["suggested_reply"].lower()
    finally:
        db.close()


def test_openai_suggestion_can_recommend_skip_for_unrelated_reddit_thread(monkeypatch):
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
                                    "relevance_score": 12,
                                    "spam_risk_score": 76,
                                    "detected_tickers": [],
                                    "intent": "complaint",
                                    "recommended_action": "skip",
                                    "reply_angle": "other",
                                    "value_added_insight": "",
                                    "walnut_feature_to_mention": "homepage",
                                    "suggested_destination_url": "https://walnutmarkets.com",
                                    "suggested_reply": "",
                                    "alternate_reply_more_direct": "",
                                    "short_reason": "The thread is a consumer service complaint, not a market research discussion.",
                                    "compliance_notes": "No Walnut angle; do not force a reply.",
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
        campaign = admin_ai_marketing_create_campaign(_campaign_payload(mode="manual_url_review"), _request_for_user(admin), db)
        result = admin_ai_marketing_manual_url(
            ManualUrlPayload(
                url="https://www.reddit.com/r/mildlyinfuriating/comments/support/thread/",
                text="The delivery driver left my package in the rain and customer support never replied.",
                campaign_id=campaign["id"],
                generate=True,
            ),
            _request_for_user(admin),
            db,
        )

        suggestion = result["opportunity"]["suggestion"]
        assert suggestion["recommended_action"] == "skip"
        assert suggestion["suggested_reply"] == "Skip - not relevant enough."
        assert suggestion["suggested_destination_url"] == ""
        assert suggestion["walnut_feature_to_mention"] == ""
    finally:
        db.close()


def test_openai_suggestion_routes_detected_ticker_and_adds_missing_disclosure(monkeypatch):
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
                                    "relevance_score": 82,
                                    "spam_risk_score": 19,
                                    "detected_tickers": ["TSLA"],
                                    "intent": "question",
                                    "recommended_action": "reply",
                                    "reply_angle": "ticker_context",
                                    "value_added_insight": "The question needs source-backed TSLA context rather than a one-factor answer.",
                                    "walnut_feature_to_mention": "ticker page with filings, insider activity, Congress activity, and signal context",
                                    "suggested_destination_url": "https://walnutmarkets.com",
                                    "suggested_reply": "Walnut is useful here because it puts TSLA filings, insider activity, Congress activity, and price context together.",
                                    "alternate_reply_more_direct": "",
                                    "short_reason": "Ticker-specific research question.",
                                    "compliance_notes": "Needs affiliation disclosure.",
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
        campaign = admin_ai_marketing_create_campaign(_campaign_payload(mode="manual_url_review"), _request_for_user(admin), db)
        result = admin_ai_marketing_manual_url(
            ManualUrlPayload(
                url="https://www.reddit.com/r/stocks/comments/tsla/thread/",
                text="Does TSLA have any recent insider or Congress trading context?",
                campaign_id=campaign["id"],
                generate=True,
            ),
            _request_for_user(admin),
            db,
        )

        suggestion = result["opportunity"]["suggestion"]
        assert suggestion["suggested_destination_url"].startswith("https://walnutmarkets.com/ticker/TSLA?")
        assert suggestion["suggested_reply"].startswith("Bias disclosed: I'm building Walnut.")
    finally:
        db.close()


def test_openai_suggestion_downgrades_direct_trading_advice(monkeypatch):
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
                                    "relevance_score": 75,
                                    "spam_risk_score": 20,
                                    "detected_tickers": ["NVDA"],
                                    "intent": "trade_idea",
                                    "recommended_action": "reply",
                                    "reply_angle": "ticker_context",
                                    "value_added_insight": "The source is asking about NVDA context.",
                                    "walnut_feature_to_mention": "ticker context",
                                    "suggested_destination_url": "https://walnutmarkets.com/ticker/NVDA",
                                    "suggested_reply": "I'm building Walnut, so obvious bias, but you should buy NVDA after checking the signals.",
                                    "alternate_reply_more_direct": "",
                                    "short_reason": "Ticker discussion.",
                                    "compliance_notes": "Needs compliance review.",
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
        campaign = admin_ai_marketing_create_campaign(_campaign_payload(mode="manual_url_review"), _request_for_user(admin), db)
        result = admin_ai_marketing_manual_url(
            ManualUrlPayload(
                url="https://www.reddit.com/r/stocks/comments/nvda/thread/",
                text="What do people think about NVDA signals?",
                campaign_id=campaign["id"],
                generate=True,
            ),
            _request_for_user(admin),
            db,
        )

        suggestion = result["opportunity"]["suggestion"]
        assert suggestion["recommended_action"] == "monitor"
        assert "buy" not in suggestion["suggested_reply"].lower()
        assert "sell" not in suggestion["suggested_reply"].lower()
        assert suggestion["suggested_destination_url"] == ""
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


def test_x_chart_drop_creates_compliant_growth_draft(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    _mock_openai(monkeypatch, _growth_openai_payload())
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        result = admin_ai_growth_create_draft(
            GrowthDraftPayload(
                campaign_type="x_chart_drop",
                content_type="x_post",
                source_platform="X",
                ticker_theme="NVDA",
                inputs={"source_types": ["signals", "Congress", "price/volume"], "timeframe": "30 days"},
            ),
            _request_for_user(admin),
            db,
        )

        suggestion = result["opportunity"]["suggestion"]
        assert result["opportunity"]["campaign_type"] == "x_chart_drop"
        assert result["opportunity"]["content_type"] == "x_post"
        assert suggestion["recommended_action"] == "draft_post"
        draft_text = result["opportunity"]["generated_content"].lower()
        assert "reported congress" in draft_text
        assert "bias disclosed" not in draft_text
        assert "building walnut" not in draft_text
        assert "$nvda" in draft_text
        assert "#nvda" not in draft_text
        assert "#markets" not in draft_text
        assert "buy" not in draft_text
        assert "sell" not in draft_text
        assert "about to explode" not in draft_text
        assert result["opportunity"]["assets"] == []
    finally:
        db.close()


def test_social_card_renderer_keeps_story_and_evidence_zones_separate():
    spec = _normalize_social_card_spec(
        {
            "card_type": "ticker_signal",
            "template": "ticker_signal",
            "ticker": "SPCX",
            "tickers": ["SPCX", "NBIS"],
            "sentiment": "bearish",
            "headline": "Bearish confirmation is cleaner in $SPCX than $NBIS",
            "subheadline": "One name has a tight bearish stack. The other is mixed: weak tape, but reported accumulation and fundamental context.",
            "bullets": [
                "$SPCX: 76/100 confirmation score",
                "$NBIS: 59/100, mixed stack",
                "Price / Volume is bearish on both",
            ],
            "key_stats": [
                {"label": "SPCX score", "value": "76/100"},
                {"label": "NBIS score", "value": "59/100"},
                {"label": "NBIS 13F", "value": "Reported accumulation"},
            ],
            "chips": ["Price / Volume", "Macro Positioning", "Institutional Activity", "Insiders"],
            "cta": "View the signal stack",
            "url": "https://walnutmarkets.com/ticker/SPCX",
            "visual_emphasis": "Split-screen comparison of a clean bearish stack",
            "source_label": "Walnut confirmation monitoring",
            "tone": "market-native",
            "include_chart": True,
            "include_cta": True,
            "include_source_tag": True,
            "include_walnut_url": True,
        },
        fallback_card_type="ticker_signal",
        fallback_tickers=["SPCX", "NBIS"],
        fallback_url="https://walnutmarkets.com/ticker/SPCX",
    )

    svg = unquote(_social_card_asset(spec)["url"].split(",", 1)[1])

    assert "Walnut Markets" in svg
    assert ">W</text>" in svg
    assert "Evidence panel" in svg
    assert 'x="1036"' in svg
    assert 'x="1086"' in svg
    assert 'x="930"' not in svg
    assert "......" not in svg
    assert "Reported accumulation" in svg


def test_generated_thumbnail_asset_uses_image_model_when_enabled(monkeypatch):
    monkeypatch.setenv("AI_MARKETING_IMAGE_GENERATION_ENABLED", "true")
    monkeypatch.setenv("AI_MARKETING_IMAGE_MODEL", "gpt-image-test")
    captured = {}

    class FakeResponse:
        status_code = 200
        text = ""

        def json(self):
            return {
                "data": [
                    {
                        "b64_json": base64.b64encode(b"fake-jpeg").decode("ascii"),
                        "revised_prompt": "revised premium thumbnail prompt",
                    }
                ]
            }

    def fake_post(url, **kwargs):
        captured["url"] = url
        captured["json"] = kwargs["json"]
        return FakeResponse()

    monkeypatch.setattr("app.services.ai_marketing.requests.post", fake_post)

    asset = _generated_thumbnail_asset(
        api_key="test-key",
        card_spec={
            "card_type": "ticker_signal",
            "ticker": "JPM",
            "headline": "JPM leads the institutional accumulation stack",
            "visual_emphasis": "large glowing bank filings stack",
            "source_label": "Reported 13F filings",
        },
        suggested_post="$JPM reported 13F activity shows broad holder increases, per filings.",
        visual_brief={"rows": [{"label": "Confirmation", "value": "9.0/10"}]},
    )

    assert captured["url"] == "https://api.openai.com/v1/images/generations"
    assert captured["json"]["model"] == "gpt-image-test"
    assert captured["json"]["output_format"] == "jpeg"
    assert asset is not None
    assert asset["template"] == "generated_thumbnail"
    assert asset["url"].startswith("data:image/jpeg;base64,")
    assert asset["image_model"] == "gpt-image-test"
    assert "Avoid: dashboard cards" in asset["image_prompt"]


def test_x_copy_normalizer_strips_hashtags_and_adds_cashtags():
    text = _ensure_x_hashtags("TSM margin context is cleaner. #TSM #Markets", ["TSM"])

    assert text == "TSM margin context is cleaner. $TSM"
    assert "#TSM" not in text
    assert "#Markets" not in text


def test_x_chart_drop_caps_generated_post_to_x_character_limit(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    long_post = (
        "I am building Walnut, so bias disclosed. "
        + "TSM margin context gets cleaner when reported disclosures, filings, and price confirmation are checked together. " * 8
    )
    _mock_openai(
        monkeypatch,
        _growth_openai_payload(
            detected_tickers=["TSM"],
            suggested_post=long_post,
            alternate_hooks=[long_post],
            assets=[
                {
                    "title": "TSM ticker page screenshot",
                    "asset_type": "screenshot",
                    "url": "https://walnutmarkets.com/ticker/TSM",
                    "thumbnail_url": "https://walnutmarkets.com/ticker/TSM",
                    "suggested_caption": "TSM continues to be one of the strongest names in our datasets.",
                    "source_data_notes": "Ticker page URL, not an image asset.",
                }
            ],
        ),
    )
    db = _session()
    try:
        admin = _user(db, "admin-x-long@example.com", role="admin")
        result = admin_ai_growth_create_draft(
            GrowthDraftPayload(
                campaign_type="x_chart_drop",
                content_type="x_post",
                source_platform="X",
                ticker_theme="TSM",
                inputs={"source_types": ["signals", "financials/filings"], "timeframe": "30 days"},
            ),
            _request_for_user(admin),
            db,
        )

        suggestion = result["opportunity"]["suggestion"]
        draft_text = result["opportunity"]["generated_content"]
        assert len(result["opportunity"]["generated_content"]) <= X_POST_CHARACTER_LIMIT
        assert len(suggestion["suggested_post"]) <= X_POST_CHARACTER_LIMIT
        assert len(suggestion["alternate_hooks"][0]) <= X_POST_CHARACTER_LIMIT
        assert "bias disclosed" not in draft_text.lower()
        assert "building walnut" not in draft_text.lower()
        assert "$TSM" in draft_text
        assert "#TSM" not in draft_text
        assert "#Markets" not in draft_text
        assert result["opportunity"]["assets"] == []
    finally:
        db.close()


def test_ai_growth_all_drafts_excludes_dismissed(monkeypatch):
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    db = _session()
    try:
        admin = _user(db, "admin-delete@example.com", role="admin")
        keep = admin_ai_growth_create_draft(
            GrowthDraftPayload(
                campaign_type="manual_research_input",
                content_type="reddit_reply",
                source_platform="Reddit",
                text="Keep this draft",
                generate=False,
            ),
            _request_for_user(admin),
            db,
        )
        delete = admin_ai_growth_create_draft(
            GrowthDraftPayload(
                campaign_type="manual_research_input",
                content_type="reddit_reply",
                source_platform="Reddit",
                text="Delete this draft",
                generate=False,
            ),
            _request_for_user(admin),
            db,
        )
        dismissed = db.get(AiMarketingOpportunity, delete["opportunity"]["id"])
        assert dismissed is not None
        dismissed.status = "dismissed"
        db.commit()

        all_payload = admin_ai_growth_drafts(_request_for_user(admin), db, status="all", limit=100)
        all_ids = {item["id"] for item in all_payload["items"]}
        assert keep["opportunity"]["id"] in all_ids
        assert delete["opportunity"]["id"] not in all_ids

        dismissed_payload = admin_ai_growth_drafts(_request_for_user(admin), db, status="dismissed", limit=100)
        assert {item["id"] for item in dismissed_payload["items"]} == {delete["opportunity"]["id"]}
    finally:
        db.close()


def test_ai_growth_regenerate_uses_change_request(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    captured = {}

    class FakeResponse:
        status_code = 200

        def json(self):
            return {"choices": [{"message": {"content": json.dumps(_growth_openai_payload(
                detected_tickers=["TSM"],
                suggested_post="I am building Walnut, so bias disclosed. TSM margin context is cleaner when filings and price action confirm the same tell.",
                short_reason="Shorter TSM margin-focused X draft.",
            ))}}]}

    def fake_post(url, *args, **kwargs):
        captured["url"] = url
        captured["payload"] = kwargs["json"]
        return FakeResponse()

    monkeypatch.setattr("app.services.ai_marketing.requests.post", fake_post)
    db = _session()
    try:
        admin = _user(db, "admin-regenerate@example.com", role="admin")
        result = admin_ai_growth_create_draft(
            GrowthDraftPayload(
                campaign_type="x_chart_drop",
                content_type="x_post",
                source_platform="X",
                ticker_theme="TSM",
                text="Original TSM draft context",
                generate=False,
            ),
            _request_for_user(admin),
            db,
        )

        updated = admin_ai_growth_regenerate_draft(
            result["opportunity"]["id"],
            GrowthDraftRegeneratePayload(change_request="Make it shorter and focus on the TSM margin angle."),
            _request_for_user(admin),
            db,
        )

        prompt = json.loads(captured["payload"]["messages"][1]["content"])
        schema = captured["payload"]["response_format"]["json_schema"]["schema"]
        assert captured["url"] == "https://api.openai.com/v1/chat/completions"
        assert prompt["content_constraints"]["x_post"]["max_characters"] == X_POST_CHARACTER_LIMIT
        assert prompt["content_constraints"]["x_post"]["hard_requirement"] is True
        assert prompt["opportunity"]["metadata"]["change_request"] == "Make it shorter and focus on the TSM margin angle."
        assert "visual_brief" in schema["properties"]
        assert "visual_brief" in schema["required"]
        assert "social_card" in schema["properties"]
        assert "social_card" in schema["required"]
        assert schema["properties"]["social_card"]["properties"]["card_type"]["enum"] == [
            "article_reactive",
            "congress_insider_activity",
            "research_cover",
            "ticker_signal",
        ]
        assert schema["properties"]["visual_brief"]["properties"]["chart_type"]["enum"] == [
            "ranked_bars",
            "bucket_breakdown",
            "signal_stack",
            "comparison_card",
        ]
        assert schema["properties"]["suggested_post"]["maxLength"] == X_POST_CHARACTER_LIMIT
        assert schema["properties"]["alternate_hooks"]["items"]["maxLength"] == X_POST_CHARACTER_LIMIT
        assert updated["status"] == "needs_review"
        assert "TSM margin context" in updated["generated_content"]
        assert "bias disclosed" not in updated["generated_content"].lower()
        assert "building walnut" not in updated["generated_content"].lower()
        assert "$TSM" in updated["generated_content"]
        assert "#TSM" not in updated["generated_content"]
        assert "#Markets" not in updated["generated_content"]
        assert updated["assets"] == []
    finally:
        db.close()


def test_influencer_growth_workflow_is_removed(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        with pytest.raises(HTTPException) as exc:
            admin_ai_growth_create_draft(
                GrowthDraftPayload(
                    campaign_type="influencer_report_pack",
                    content_type="influencer_dm",
                    source_platform="X",
                    ticker_theme="NVDA",
                ),
                _request_for_user(admin),
                db,
            )
        assert exc.value.status_code == 422
        assert "Influencer growth workflows have been removed" in exc.value.detail
    finally:
        db.close()


def test_reddit_research_thread_discloses_walnut_affiliation(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    _mock_openai(
        monkeypatch,
            _reddit_dd_payload(),
    )
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        result = admin_ai_growth_create_draft(
            GrowthDraftPayload(
                campaign_type="reddit_research_thread",
                content_type="reddit_thread",
                source_platform="Reddit",
                ticker_theme="Congress trade research workflow",
                inputs={"subreddit": "stocks", "post_type": "data walkthrough"},
            ),
            _request_for_user(admin),
            db,
        )

        generated = result["opportunity"]["generated_content"]
        assert "## Walnut disclosure stack" in generated
        assert "## Technical picture" in generated
        assert "## Fundamental picture" in generated
        assert "## Catalysts" in generated
        assert "## Bear case / risks" in generated
        assert "## What would confirm the setup" in generated
        assert "## What would weaken the setup" in generated
        assert "not investment advice" in generated.lower()
        assert "reported/disclosed" in generated.lower()
        assert "quarter-end holdings and filing date context" in generated.lower()
        assert result["opportunity"]["quality_scores"]["research_depth_score"] >= 75
        assert result["opportunity"]["status"] == "new"
        assert result["opportunity"]["full_markdown"] == generated
        assert result["opportunity"]["assets"][0]["template"] != "research_cover"
        assert any(asset["title"] == "NVDA disclosure stack card" for asset in result["opportunity"]["assets"])
    finally:
        db.close()


def test_low_quality_reddit_research_thread_fails_quality_gate(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    _mock_openai(
        monkeypatch,
        _reddit_dd_payload(
            full_reddit_post_markdown="NVDA is interesting. Walnut can help. Not investment advice.",
            quality_scores={
                "research_depth_score": 40,
                "evidence_score": 35,
                "catalyst_score": 20,
                "balance_score": 45,
                "reddit_native_score": 50,
                "promotional_risk_score": 70,
                "compliance_risk_score": 55,
            },
            missing_data_notes=[],
            source_notes=[],
        ),
    )
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        result = admin_ai_growth_create_draft(
            GrowthDraftPayload(
                campaign_type="reddit_research_thread",
                content_type="reddit_thread",
                source_platform="Reddit",
                ticker_theme="NVDA",
            ),
            _request_for_user(admin),
            db,
        )

        assert result["opportunity"]["status"] == "regeneration_needed"
        assert result["opportunity"]["suggestion"]["recommended_action"] == "monitor"
        assert any("too short" in note.lower() for note in result["opportunity"]["missing_data_notes"])
    finally:
        db.close()


def test_reddit_research_thread_can_recommend_skip_when_too_promotional(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    _mock_openai(
        monkeypatch,
        _growth_openai_payload(
            campaign_type="reddit_research_thread",
            content_type="reddit_thread",
            platform="reddit",
            relevance_score=25,
            spam_risk_score=92,
            recommended_action="skip",
            suggested_post="",
            suggested_reply="Skip - not relevant enough.",
            suggested_destination_url="",
            short_reason="The requested angle is too promotional for the subreddit.",
            compliance_notes="Probably do not post.",
        ),
    )
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        result = admin_ai_growth_create_draft(
            GrowthDraftPayload(
                campaign_type="reddit_research_thread",
                content_type="reddit_thread",
                source_platform="Reddit",
                text="Write a post that mostly pushes users to Walnut.",
            ),
            _request_for_user(admin),
            db,
        )

        suggestion = result["opportunity"]["suggestion"]
        assert suggestion["recommended_action"] == "skip"
        assert result["opportunity"]["spam_risk_score"] == 92
        assert result["opportunity"]["suggested_destination_url"] == ""
    finally:
        db.close()


def test_reddit_paid_ad_ideas_return_variants(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    variants = [
        "Headline: Find the market tells. Body: Cross-check filings, signals, and disclosures before your next research session. CTA: Start free.",
        "Headline: Research beyond the chart. Body: Walnut combines filings, insider activity, and signal context. CTA: Try Walnut.",
    ]
    _mock_openai(
        monkeypatch,
        _growth_openai_payload(
            campaign_type="reddit_paid_ad",
            content_type="paid_ad",
            platform="reddit",
            recommended_action="draft_ad",
            suggested_post="",
            suggested_ad_variants=variants,
            short_reason="Paid native ad variants for a research audience.",
        ),
    )
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        result = admin_ai_growth_create_draft(
            GrowthDraftPayload(
                campaign_type="reddit_paid_ad",
                content_type="paid_ad",
                source_platform="Reddit",
                audience="r/stocks",
                inputs={"offer": "Free plan", "pain_point": "fragmented market research"},
            ),
            _request_for_user(admin),
            db,
        )

        suggestion = result["opportunity"]["suggestion"]
        assert suggestion["recommended_action"] == "draft_ad"
        assert suggestion["suggested_ad_variants"] == variants
        assert "Start free" in result["opportunity"]["generated_content"]
    finally:
        db.close()


def test_growth_draft_email_includes_posting_assist_checklist_and_assets(monkeypatch):
    sent = {}

    def fake_send_email(db, **kwargs):
        sent.update(kwargs)
        return {"id": 77, "status": "sent", "to_email": kwargs["to_email"], "subject": kwargs["context"]["subject"]}

    monkeypatch.setattr("app.services.ai_marketing.send_email", fake_send_email)
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        result = admin_ai_growth_create_draft(
            GrowthDraftPayload(
                campaign_type="x_chart_drop",
                content_type="x_post",
                source_platform="X",
                ticker_theme="NVDA",
                destination_url="https://walnutmarkets.com/ticker/NVDA",
                text="NVDA source context",
                assets=[
                    {
                        "title": "NVDA chart",
                        "asset_type": "chart",
                        "url": "data:image/svg+xml;charset=utf-8,%3Csvg%20xmlns%3D%22http://www.w3.org/2000/svg%22%3E%3C/svg%3E",
                        "thumbnail_url": "data:image/svg+xml;charset=utf-8,%3Csvg%20xmlns%3D%22http://www.w3.org/2000/svg%22%3E%3C/svg%3E",
                        "suggested_caption": "Reported disclosures plus price context.",
                    }
                ],
                generate=False,
            ),
            _request_for_user(admin),
            db,
        )
        opportunity = db.get(AiMarketingOpportunity, result["opportunity"]["id"])
        db.add(
            AiMarketingSuggestion(
                opportunity_id=opportunity.id,
                campaign_id=None,
                model="test-model",
                relevance_score=91,
                spam_risk_score=12,
                detected_tickers_json=json.dumps(["NVDA"]),
                intent="tool_search",
                campaign_type="x_chart_drop",
                content_type="x_post",
                platform="x",
                audience="traders",
                recommended_action="draft_post",
                reply_angle="ticker_context",
                content_angle="x chart drop",
                value_added_insight="Reported disclosures checked against price/volume context.",
                walnut_feature_to_mention="ticker page",
                suggested_destination_url="https://walnutmarkets.com/ticker/NVDA",
                suggested_reply="",
                suggested_post="I am building Walnut, so bias disclosed. NVDA context draft.",
                alternate_reply_more_direct="",
                short_reason="High-fit X draft.",
                compliance_notes="Review disclosure. No investment advice.",
                disclosure_text="I am building Walnut, so bias disclosed.",
                assets_json=json.dumps([]),
            )
        )
        db.commit()

        email = admin_ai_growth_email_draft(opportunity.id, _request_for_user(admin), db)

        context = sent["context"]
        assert sent["to_email"] == "jarod@walnutmarkets.com"
        assert sent["template_key"] == "ai_marketing.digest"
        assert "Walnut AI Growth" in context["subject"]
        assert "Platform: X" in context["items_text"]
        assert "Content type: X post" in context["items_text"]
        assert "Source URL:" in context["items_text"]
        assert "Suggested destination URL: https://walnutmarkets.com/ticker/NVDA" in context["items_text"]
        assert "Copy draft" in context["items_text"]
        assert "Attach image if relevant" in context["items_text"]
        assert "Review disclosure" in context["items_text"]
        assert "https://walnutmarkets.com/admin/ai-marketing?draft=" in context["items_text"]
        assert "NVDA chart" in context["items_text"]
        assert sent["attachments"][0]["name"].endswith(".png")
        assert sent["attachments"][0]["content_type"] == "image/png"
        assert sent["attachments"][0]["content"].startswith(b"\x89PNG")
        assert email["email_log"]["status"] == "sent"
        db.refresh(opportunity)
        assert opportunity.status == "emailed"
    finally:
        db.close()


def test_growth_draft_manual_lifecycle_buttons_update_status(monkeypatch):
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        result = admin_ai_growth_create_draft(
            GrowthDraftPayload(
                campaign_type="manual_research_input",
                content_type="reddit_reply",
                source_platform="Reddit",
                text="Manual source text",
                generate=False,
            ),
            _request_for_user(admin),
            db,
        )
        draft_id = result["opportunity"]["id"]

        copied = admin_ai_growth_mark_copied(draft_id, _request_for_user(admin), db)
        assert copied["status"] == "copied"
        assert copied["copied_at"] is not None

        posted = admin_ai_growth_mark_posted(draft_id, _request_for_user(admin), db)
        assert posted["status"] == "posted_manually"
        assert posted["posted_manually_at"] is not None
    finally:
        db.close()


def test_email_approve_posts_x_draft_when_access_token_is_configured(monkeypatch):
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.setenv(X_ACCESS_TOKEN, "x-post-token")
    monkeypatch.setenv("X_CONNECTED_HANDLE", "walnutmarkets")
    captured = {}

    class FakeResponse:
        status_code = 201

        def json(self):
            return {"data": {"id": "12345", "text": captured["json"]["text"]}}

    def fake_post(url, *args, **kwargs):
        captured["url"] = url
        captured["headers"] = kwargs["headers"]
        captured["json"] = kwargs["json"]
        return FakeResponse()

    monkeypatch.setattr("app.services.ai_marketing.requests.post", fake_post)
    db = _session()
    try:
        admin = _user(db, "admin-email-post@example.com", role="admin")
        result = admin_ai_growth_create_draft(
            GrowthDraftPayload(
                campaign_type="x_chart_drop",
                content_type="x_post",
                source_platform="X",
                ticker_theme="TSM",
                text="$TSM margin context is cleaner when filings and price action confirm the same tell. #TSM #Markets",
                generate=False,
            ),
            _request_for_user(admin),
            db,
        )
        draft_id = result["opportunity"]["id"]
        opportunity = db.get(AiMarketingOpportunity, draft_id)
        opportunity.generated_content = "$TSM margin context is cleaner when filings and price action confirm the same tell. #TSM #Markets"
        db.commit()
        token = create_email_action_token(db, draft_id, "approve", actor_email="jarod@walnutmarkets.com")

        response = admin_ai_growth_email_action(token, _email_action_request(), db)
        body = response.body.decode("utf-8")
        db.expire_all()
        opportunity = db.get(AiMarketingOpportunity, draft_id)

        assert response.status_code == 200
        assert "Posted to X" in body
        assert "Open X Post" in body
        assert captured["url"] == "https://api.x.com/2/tweets"
        assert captured["headers"]["Authorization"] == "Bearer x-post-token"
        assert captured["json"]["text"].startswith("$TSM margin context")
        assert opportunity.status == "posted"
        assert json.loads(opportunity.raw_metadata_json)["x_post_id"] == "12345"
    finally:
        db.close()


def test_email_approve_posts_multi_symbol_x_draft_with_one_api_cashtag(monkeypatch):
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.setenv(X_ACCESS_TOKEN, "x-post-token")
    captured = {}

    class FakeResponse:
        status_code = 201

        def json(self):
            return {"data": {"id": "multi-symbol-post", "text": captured["json"]["text"]}}

    def fake_post(url, *args, **kwargs):
        captured["url"] = url
        captured["json"] = kwargs["json"]
        return FakeResponse()

    monkeypatch.setattr("app.services.ai_marketing.requests.post", fake_post)
    db = _session()
    try:
        admin = _user(db, "admin-email-multi-symbol@example.com", role="admin")
        post_text = "$TSM $NVDA $AMD confirmation stack is bearish across price, volume, and macro pressure."
        result = admin_ai_growth_create_draft(
            GrowthDraftPayload(
                campaign_type="x_chart_drop",
                content_type="x_post",
                source_platform="X",
                ticker_theme="TSM",
                text=post_text,
                generate=False,
            ),
            _request_for_user(admin),
            db,
        )
        draft_id = result["opportunity"]["id"]
        opportunity = db.get(AiMarketingOpportunity, draft_id)
        opportunity.generated_content = post_text
        db.commit()
        token = create_email_action_token(db, draft_id, "approve", actor_email="jarod@walnutmarkets.com")

        response = admin_ai_growth_email_action(token, _email_action_request(), db)
        body = response.body.decode("utf-8")
        db.expire_all()
        opportunity = db.get(AiMarketingOpportunity, draft_id)

        assert response.status_code == 200
        assert "Posted to X" in body
        assert captured["url"] == "https://api.x.com/2/tweets"
        assert captured["json"]["text"] == "$TSM NVDA AMD confirmation stack is bearish across price, volume, and macro pressure."
        assert opportunity.status == "posted"
    finally:
        db.close()


def test_email_approve_refreshes_expired_x_access_token_and_posts(monkeypatch):
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.setenv(X_ACCESS_TOKEN, "expired-token")
    monkeypatch.setenv(X_REFRESH_TOKEN, "old-refresh-token")
    monkeypatch.setenv(X_CLIENT_ID, "x-client-id")
    monkeypatch.setenv(X_CLIENT_SECRET, "x-client-secret")
    requests_seen = []

    class FakeResponse:
        def __init__(self, status_code, payload):
            self.status_code = status_code
            self._payload = payload

        def json(self):
            return self._payload

    def fake_post(url, *args, **kwargs):
        requests_seen.append({"url": url, "kwargs": kwargs})
        if url.endswith("/2/oauth2/token"):
            assert kwargs["auth"] == ("x-client-id", "x-client-secret")
            assert kwargs["data"]["grant_type"] == "refresh_token"
            assert kwargs["data"]["refresh_token"] == "old-refresh-token"
            return FakeResponse(200, {"access_token": "fresh-access-token", "refresh_token": "fresh-refresh-token"})
        if len([request for request in requests_seen if request["url"].endswith("/2/tweets")]) == 1:
            assert kwargs["headers"]["Authorization"] == "Bearer expired-token"
            return FakeResponse(401, {"title": "Unauthorized"})
        assert kwargs["headers"]["Authorization"] == "Bearer fresh-access-token"
        return FakeResponse(201, {"data": {"id": "67890", "text": kwargs["json"]["text"]}})

    monkeypatch.setattr("app.services.ai_marketing.requests.post", fake_post)
    db = _session()
    try:
        admin = _user(db, "admin-email-refresh-post@example.com", role="admin")
        result = admin_ai_growth_create_draft(
            GrowthDraftPayload(
                campaign_type="x_chart_drop",
                content_type="x_post",
                source_platform="X",
                ticker_theme="TSM",
                text="$TSM margin context is cleaner when filings and price action confirm the same tell. #TSM #Markets",
                generate=False,
            ),
            _request_for_user(admin),
            db,
        )
        draft_id = result["opportunity"]["id"]
        opportunity = db.get(AiMarketingOpportunity, draft_id)
        opportunity.generated_content = "$TSM margin context is cleaner when filings and price action confirm the same tell. #TSM #Markets"
        db.commit()
        token = create_email_action_token(db, draft_id, "approve", actor_email="jarod@walnutmarkets.com")

        response = admin_ai_growth_email_action(token, _email_action_request(), db)
        body = response.body.decode("utf-8")
        db.expire_all()
        opportunity = db.get(AiMarketingOpportunity, draft_id)

        assert response.status_code == 200
        assert "Posted to X" in body
        assert opportunity.status == "posted"
        assert json.loads(opportunity.raw_metadata_json)["x_post_id"] == "67890"
        assert db.get(AiMarketingSetting, X_CURRENT_ACCESS_TOKEN_SETTING).value == "fresh-access-token"
        assert db.get(AiMarketingSetting, X_CURRENT_REFRESH_TOKEN_SETTING).value == "fresh-refresh-token"
        assert [request["url"] for request in requests_seen] == [
            "https://api.x.com/2/tweets",
            "https://api.x.com/2/oauth2/token",
            "https://api.x.com/2/tweets",
        ]
    finally:
        db.close()


def test_email_approve_returns_html_when_x_access_token_is_missing(monkeypatch):
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv(X_ACCESS_TOKEN, raising=False)
    db = _session()
    try:
        admin = _user(db, "admin-email-approve@example.com", role="admin")
        result = admin_ai_growth_create_draft(
            GrowthDraftPayload(
                campaign_type="x_chart_drop",
                content_type="x_post",
                source_platform="X",
                ticker_theme="TSM",
                text="$TSM margin context is cleaner when filings and price action confirm the same tell. #TSM #Markets",
                generate=False,
            ),
            _request_for_user(admin),
            db,
        )
        draft_id = result["opportunity"]["id"]
        token = create_email_action_token(db, draft_id, "approve", actor_email="jarod@walnutmarkets.com")

        response = admin_ai_growth_email_action(token, _email_action_request(), db)
        body = response.body.decode("utf-8")
        db.expire_all()
        opportunity = db.get(AiMarketingOpportunity, draft_id)

        assert response.status_code == 200
        assert "Draft approved" in body
        assert "not posted to X" in body
        assert "X_ACCESS_TOKEN is not configured" in body
        assert opportunity.status == "approved"
    finally:
        db.close()


def test_no_auto_post_endpoint_exists():
    paths = [getattr(route, "path", "") for route in ai_marketing_router.routes]
    assert not any("auto-post" in path or "autopost" in path or "auto_post" in path for path in paths)


def _article_campaign_payload(**overrides):
    payload = {
        "name": "Daily Article-Reactive X",
        "enabled": True,
        "mode": ARTICLE_REACTIVE_CAMPAIGN_TYPE,
        "campaign_type": ARTICLE_REACTIVE_CAMPAIGN_TYPE,
        "content_type": "x_post",
        "status": "active",
        "schedule_config": {"cadence": "daily"},
        "weekdays_only": True,
        "run_time": "07:35",
        "timezone": "America/Los_Angeles",
        "recipient_email": "jarod@walnutmarkets.com",
        "source_type": "fmp_articles",
        "output_preferences": {"include_image_card": True, "include_walnut_link": True},
        "platforms": ["x"],
        "minimum_relevance_score": 58,
        "max_items_per_run": 20,
        "max_drafts_per_day": 1,
        "recency": "day",
        "default_destination_page": "https://walnutmarkets.com",
        "include_disclosure": True,
        "scheduled_digest_enabled": False,
    }
    payload.update(overrides)
    return CampaignPayload(**payload)


def _seed_watchlist_context(db, symbol: str = "NVDA") -> None:
    security = Security(symbol=symbol, name="NVIDIA Corporation", asset_class="equity")
    db.add(security)
    db.commit()
    db.refresh(security)
    watchlist = Watchlist(name="AI Leaders", owner_user_id=None)
    db.add(watchlist)
    db.commit()
    db.refresh(watchlist)
    db.add(WatchlistItem(watchlist_id=watchlist.id, security_id=security.id))
    db.commit()


def _fmp_article(**overrides):
    article = {
        "id": "fmp-nvda-ai",
        "title": "Nvidia AI data center demand keeps semiconductor investors focused",
        "url": "https://example.com/nvda-ai",
        "site": "Example Markets",
        "publishedDate": datetime.now(timezone.utc).isoformat(),
        "tickers": ["NVDA"],
        "text": "AI accelerator demand is a read-through for semiconductors and the data center stack.",
    }
    article.update(overrides)
    return article


def test_article_reactive_missing_fmp_key_returns_safe_config_error(monkeypatch):
    monkeypatch.delenv(FMP_API_KEY, raising=False)
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        campaign = admin_ai_marketing_create_campaign(_article_campaign_payload(), _request_for_user(admin), db)
        result = admin_ai_marketing_run_campaign(campaign["id"], _request_for_user(admin), db)
        assert result["status"] == "configuration_failed"
        assert "FMP Articles API key missing" in result["errors"][0]
        assert "apikey" not in json.dumps(result).lower()
    finally:
        db.close()


def test_fmp_articles_fetcher_uses_env_key_without_storing_or_returning_it(monkeypatch):
    monkeypatch.setenv(FMP_API_KEY, "fmp-secret-test")
    captured = {}

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return [_fmp_article()]

    def fake_get(url, **kwargs):
        captured["url"] = url
        captured["params"] = kwargs.get("params")
        return FakeResponse()

    monkeypatch.setattr("app.services.ai_marketing.requests.get", fake_get)
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        articles = fetch_fmp_articles(db)
        settings = admin_ai_marketing_settings(_request_for_user(admin), db)
        assert articles[0]["title"].startswith("Nvidia")
        assert captured["url"].endswith("/stable/fmp-articles")
        assert captured["params"]["apikey"] == "fmp-secret-test"
        assert db.query(AiMarketingSetting).filter(AiMarketingSetting.key == FMP_API_KEY).count() == 0
        assert "fmp-secret-test" not in json.dumps(settings)
    finally:
        db.close()


def test_article_candidates_are_deduped_and_scoring_prefers_walnut_context(monkeypatch):
    monkeypatch.setenv(FMP_API_KEY, "fmp-test")
    db = _session()
    try:
        _seed_watchlist_context(db, "NVDA")
        now = datetime.now(timezone.utc)
        relevant = AiMarketingArticleCandidate(
            provider="fmp",
            provider_article_id="relevant",
            title="Nvidia AI demand keeps semiconductor investors focused",
            url="https://example.com/relevant",
            published_at=now,
            tickers_json=json.dumps(["NVDA"]),
            summary="AI data center demand and HBM supply are the market hook.",
            raw_metadata_json="{}",
            first_seen_at=now,
            last_seen_at=now,
            dedupe_hash="relevant",
        )
        generic = AiMarketingArticleCandidate(
            provider="fmp",
            provider_article_id="generic",
            title="Markets mixed as investors wait",
            url="https://example.com/generic",
            published_at=now,
            tickers_json="[]",
            summary="A broad market recap with no specific Walnut angle.",
            raw_metadata_json="{}",
            first_seen_at=now,
            last_seen_at=now,
            dedupe_hash="generic",
        )
        db.add_all([relevant, generic])
        db.commit()
        relevant_score = score_article_candidate(db, relevant)
        generic_score = score_article_candidate(db, generic)
        assert relevant_score["final_score"] > generic_score["final_score"]
        assert relevant_score["clear_walnut_angle"] is True
        assert generic_score["rejected"] is True
        assert "No clear Walnut angle." in generic_score["rejected_reasons"]
    finally:
        db.close()


def test_article_reactive_campaign_generates_draft_emails_and_enforces_daily_cap(monkeypatch):
    monkeypatch.setenv(FMP_API_KEY, "fmp-test")
    monkeypatch.setenv("OPENAI_API_KEY", "openai-test")
    _mock_openai(
        monkeypatch,
        _growth_openai_payload(
            campaign_type=ARTICLE_REACTIVE_CAMPAIGN_TYPE,
            content_type="x_post",
            platform="x",
            suggested_post="NVDA AI demand is the hook. The useful question is what confirms it across price, filings, disclosures, and Walnut signal context. $NVDA #AI",
            alternate_hooks=["NVDA AI demand is a clean read-through."],
            alternate_reply_more_direct="NVDA AI demand matters most when it confirms across disclosures and price.",
            compliance_notes="Human review required. No investment advice.",
        ),
    )
    sent = []

    def fake_send_email(db, **kwargs):
        sent.append(kwargs)
        return {"id": 99, "status": "sent"}

    class FakeFmpResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return [_fmp_article(), _fmp_article()]

    monkeypatch.setattr("app.services.ai_marketing.requests.get", lambda *args, **kwargs: FakeFmpResponse())
    monkeypatch.setattr("app.services.ai_marketing.send_email", fake_send_email)
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        _seed_watchlist_context(db, "NVDA")
        campaign = admin_ai_marketing_create_campaign(_article_campaign_payload(max_drafts_per_day=1), _request_for_user(admin), db)
        first = admin_ai_marketing_run_campaign(campaign["id"], _request_for_user(admin), db)
        second = admin_ai_marketing_run_campaign(campaign["id"], _request_for_user(admin), db)
        assert first["drafts_generated"] == 1
        assert first["emails_sent"] == 1
        assert sent[0]["to_email"] == "jarod@walnutmarkets.com"
        assert "Open Article" in sent[0]["context"]["items_html"]
        assert "https://example.com/nvda-ai" in sent[0]["context"]["items_html"]
        assert "Article URL: https://example.com/nvda-ai" in sent[0]["context"]["items_text"]
        assert second["drafts_generated"] == 0
        assert db.query(AiMarketingArticleCandidate).count() == 1
        opportunity = db.execute(select(AiMarketingOpportunity)).scalar_one()
        assert opportunity.campaign_type == ARTICLE_REACTIVE_CAMPAIGN_TYPE
        assert opportunity.content_type == "x_post"
        payload = admin_ai_growth_drafts(_request_for_user(admin), db, status="all", limit=10)
        draft = payload["items"][0]
        assert draft["generated_content"]
        assert "$NVDA" in draft["generated_content"]
        assert draft["assets"][0]["source_data_notes"].startswith("Source: FMP")
        assert draft["compliance_notes"]
    finally:
        db.close()
