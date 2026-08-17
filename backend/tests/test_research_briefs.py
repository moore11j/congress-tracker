from __future__ import annotations

import json
from copy import deepcopy
from datetime import datetime, timedelta, timezone

import pytest
from fastapi import HTTPException, Response
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from starlette.requests import Request

from app.auth import SESSION_COOKIE_NAME, sign_session_payload
from app.db import Base
from app.models import Event, FundamentalsCache, QuoteCache, Security, TickerFinancialsCache, TickerMeta, UserAccount
from app.routers.research_briefs import ResearchBriefGeneratePayload, admin_research_brief_generate
from app.services import research_briefs as service


def _session():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    return Session()


def test_research_brief_schema_migrates_legacy_drafts_before_keyword_index(tmp_path, monkeypatch):
    monkeypatch.setenv(service.STORE_ENV, str(tmp_path / "drafts.json"))
    db = _session()
    db.execute(
        text(
            """
            CREATE TABLE research_brief_drafts (
                id TEXT PRIMARY KEY,
                status TEXT,
                created_by INTEGER,
                primary_ticker TEXT,
                slug TEXT,
                campaign_id TEXT,
                campaign_item_id TEXT,
                scheduled_at TEXT,
                approved_at TEXT,
                data_as_of TEXT,
                earnings_period_used TEXT,
                generator_version TEXT,
                last_publish_error TEXT,
                updated_at TEXT,
                published_at TEXT,
                payload_json TEXT NOT NULL
            )
            """
        )
    )
    draft = {
        "id": "legacy-published-brief",
        "status": "published",
        "primary_ticker": "AAPL",
        "updated_at": "2026-08-15T00:00:00+00:00",
        "published_at": "2026-08-15T00:00:00+00:00",
        "article": {"slug": "aapl-legacy-brief", "title": "AAPL legacy research", "primary_ticker": "AAPL"},
    }
    db.execute(
        text(
            """
            INSERT INTO research_brief_drafts (
                id, status, primary_ticker, slug, updated_at, published_at, payload_json
            ) VALUES (
                :id, :status, :primary_ticker, :slug, :updated_at, :published_at, :payload_json
            )
            """
        ),
        {
            "id": draft["id"],
            "status": draft["status"],
            "primary_ticker": draft["primary_ticker"],
            "slug": draft["article"]["slug"],
            "updated_at": draft["updated_at"],
            "published_at": draft["published_at"],
            "payload_json": json.dumps(draft),
        },
    )
    db.commit()

    cards = service.published_cards(db=db)

    assert cards["items"]
    assert cards["items"][0]["route"] == "/research/aapl-legacy-brief"
    columns = {row["name"] for row in db.execute(text("PRAGMA table_info(research_brief_drafts)")).mappings()}
    assert "target_keyword" in columns


def test_research_brief_schema_uses_postgres_safe_boolean_default():
    class CapturingSession:
        def __init__(self):
            self.statements = []

        def execute(self, statement):
            self.statements.append(str(statement))

        def commit(self):
            pass

    db = CapturingSession()

    service.ensure_research_brief_store_schema(db)

    assert any("active BOOLEAN NOT NULL DEFAULT TRUE" in statement for statement in db.statements)


def test_keyword_opportunity_discovery_stores_admin_review_candidates(monkeypatch):
    db = _session()
    admin = _user(db, "keywords@example.com", role="admin")
    monkeypatch.setattr(service, "resolved_setting_value", lambda _db, key: "test-key" if key == service.OPENAI_API_KEY else "gpt-test")

    class Response:
        status_code = 200
        text = "ok"

        def json(self):
            return {
                "output_text": json.dumps(
                    {
                        "market_note": "Recent AI-infrastructure discussion is active.",
                        "candidates": [
                            {
                                "target_keyword": "NBIS government contracts",
                                "secondary_keywords": ["Nebius federal contracts"],
                                "search_intent": "Does NBIS have meaningful government contract exposure?",
                                "content_type": "ticker",
                                "ticker": "NBIS",
                                "topic": "NBIS government contract exposure",
                                "recommended_theme": "government_contracts",
                                "trend_signal": "recent",
                                "competition_assessment": "lower",
                                "opportunity_score": 84,
                                "rationale": "A specific, answerable question has recent attention.",
                                "walnut_angle": "Compare awarded contract data with price and fundamentals.",
                                "source_urls": ["https://www.reddit.com/r/stocks/comments/example", "https://trends.google.com/trends/explore?q=NBIS"],
                                "metric_note": "Directional SERP assessment; no licensed volume feed connected.",
                            }
                        ],
                    }
                )
            }

    request_body = {}

    def fake_post(*_args, **kwargs):
        request_body.update(kwargs["json"])
        return Response()

    monkeypatch.setattr(service.requests, "post", fake_post)
    result = service.discover_research_keyword_opportunities(
        db,
        admin,
        {"seed_topics": ["AI infrastructure"], "tickers": ["NBIS"], "theme": "government_contracts", "max_candidates": 7},
    )

    assert result["items"][0]["target_keyword"] == "NBIS government contracts"
    assert result["items"][0]["status"] == "new"
    assert result["metric_provider_configured"] is False
    assert request_body["tools"] == [{"type": "web_search", "search_context_size": "medium"}]
    assert "CAMPAIGN_THEME: Government Contracts" in request_body["input"]
    assert "up to 7 candidates" in request_body["input"]
    assert request_body["text"]["format"]["schema"]["properties"]["candidates"]["maxItems"] == 7
    stored = service.list_research_keyword_opportunities(db)
    assert stored["items"][0]["ticker"] == "NBIS"


def test_keyword_opportunity_status_can_be_updated(monkeypatch):
    db = _session()
    service.ensure_research_brief_store_schema(db)
    db.execute(
        text(
            """
            INSERT INTO research_keyword_opportunities (
                id, status, target_keyword, opportunity_score, discovered_at, updated_at, payload_json
            ) VALUES ('rko_test', 'new', 'stock keyword', 50, '2026-08-16T00:00:00+00:00', '2026-08-16T00:00:00+00:00', :payload_json)
            """
        ),
        {"payload_json": json.dumps({"target_keyword": "stock keyword", "opportunity_score": 50})},
    )
    db.commit()

    updated = service.update_research_keyword_opportunity_status(db, "rko_test", "used")

    assert updated["status"] == "used"


def test_keyword_opportunity_regeneration_replaces_saved_candidate_and_preserves_id(monkeypatch):
    db = _session()
    admin = _user(db, "keywords@example.com", role="admin")
    service.ensure_research_brief_store_schema(db)
    existing = {
        "id": "rko_regenerate",
        "status": "new",
        "target_keyword": "NBIS stock buy now",
        "secondary_keywords": ["Nebius earnings"],
        "search_intent": "Is NBIS stock a good buy right now?",
        "content_type": "ticker",
        "ticker": "NBIS",
        "topic": "Nebius investment thesis",
        "recommended_theme": "good_buy_now",
        "trend_signal": "recent",
        "competition_assessment": "moderate",
        "opportunity_score": 70,
        "rationale": "Original angle.",
        "walnut_angle": "Original Walnut angle.",
        "source_urls": ["https://example.com/original"],
        "metric_note": "Directional only.",
        "discovered_at": "2026-08-16T00:00:00+00:00",
        "updated_at": "2026-08-16T00:00:00+00:00",
    }
    db.execute(
        text(
            """
            INSERT INTO research_keyword_opportunities (
                id, status, created_by, created_by_email, target_keyword, opportunity_score, ticker, topic,
                discovered_at, updated_at, payload_json
            ) VALUES (
                :id, :status, :created_by, :created_by_email, :target_keyword, :opportunity_score, :ticker, :topic,
                :discovered_at, :updated_at, :payload_json
            )
            """
        ),
        {**existing, "created_by": admin.id, "created_by_email": admin.email, "payload_json": json.dumps(existing)},
    )
    db.commit()
    monkeypatch.setattr(service, "resolved_setting_value", lambda _db, key: "test-key" if key == service.OPENAI_API_KEY else "gpt-test")
    request_body = {}

    class Response:
        status_code = 200
        text = "ok"

        def json(self):
            return {"output_text": json.dumps({"market_note": "", "candidates": [{
                "target_keyword": "Is Nebius stock overvalued after Q2 earnings?",
                "secondary_keywords": ["NBIS valuation", "Nebius Q2 earnings"],
                "search_intent": "Is NBIS stock overvalued after Q2 earnings?",
                "content_type": "ticker",
                "ticker": "NBIS",
                "topic": "Nebius valuation after earnings",
                "recommended_theme": "good_buy_now",
                "trend_signal": "rising",
                "competition_assessment": "lower",
                "opportunity_score": 91,
                "rationale": "A revised post-earnings valuation question.",
                "walnut_angle": "Compare earnings execution with valuation and Walnut signals.",
                "source_urls": ["https://example.com/earnings"],
                "metric_note": "Directional only.",
            }]})}

    def fake_post(*_args, **kwargs):
        request_body.update(kwargs["json"])
        return Response()

    monkeypatch.setattr(service.requests, "post", fake_post)
    revised = service.regenerate_research_keyword_opportunity(
        db,
        admin,
        "rko_regenerate",
        "Focus on valuation risk after earnings and avoid the generic buy-now framing.",
    )

    assert revised["id"] == "rko_regenerate"
    assert revised["status"] == "new"
    assert revised["opportunity_score"] == 91
    assert revised["revision_count"] == 1
    assert "Focus on valuation risk" in revised["last_revision_instructions"]
    assert request_body["tools"] == [{"type": "web_search", "search_context_size": "medium"}]
    assert "EDITOR_INSTRUCTIONS: Focus on valuation risk" in request_body["input"]
    stored = service.list_research_keyword_opportunities(db)["items"][0]
    assert stored["id"] == "rko_regenerate"
    assert stored["target_keyword"] == "Is Nebius stock overvalued after Q2 earnings?"
    assert stored["revision_count"] == 1


def test_openai_credit_balance_exhaustion_is_not_reported_as_rate_limit():
    class Response:
        status_code = 429
        headers = {}

        def json(self):
            return {"error": {"type": "insufficient_quota", "code": "credit_balance_exhausted"}}

    with pytest.raises(HTTPException) as raised:
        service._raise_openai_response_error(Response(), operation="keyword discovery")

    assert raised.value.status_code == 503
    assert "credit balance is exhausted" in str(raised.value.detail).lower()


def test_openai_true_rate_limit_retains_retry_guidance():
    class Response:
        status_code = 429
        headers = {"retry-after": "15"}

        def json(self):
            return {"error": {"type": "rate_limit_error", "code": "rate_limit_exceeded"}}

    with pytest.raises(HTTPException) as raised:
        service._raise_openai_response_error(Response(), operation="keyword discovery")

    assert raised.value.status_code == 429
    assert "15 seconds" in str(raised.value.detail)


def test_sanitizer_removes_sentence_that_conflates_confirmation_score_with_data():
    article = {
        "title": "NBIS research",
        "summary": "The confirmation score is based on fundamentals and price/volume.",
        "suggested_card": {"title": "NBIS", "description": "The confirmation score is based on reported institutional activity."},
        "sections": [
            {
                "heading": "The confirmation score is based on fundamentals",
                "body_markdown": "The confirmation score is based on fundamentals and price/volume. The operating evidence remains mixed.",
            }
        ],
    }

    cleaned = service.sanitize_research_brief_article(
        article,
        {"include_confirmation_score": False, "include_cross_source_confirmations": True},
        {},
    )
    heading_cleaned = service._remove_confirmation_data_conflation_from_section(
        {"heading": "The confirmation score is based on fundamentals", "body_markdown": "The operating evidence remains mixed."}
    )

    body = " ".join(section["body_markdown"] for section in cleaned["sections"])
    assert "confirmation score is based on fundamentals" not in body.lower()
    assert "confirmation score is based on fundamentals" not in heading_cleaned["heading"].lower()
    assert "operating evidence remains mixed" in heading_cleaned["body_markdown"].lower()
    assert "confirmation score is based on" not in cleaned["summary"].lower()
    assert "confirmation score is based on" not in cleaned["suggested_card"]["description"].lower()


def test_sanitizer_removes_cross_source_inputs_claim_for_confirmation_score():
    text = (
        "Cross-source data categories are inputs to the proprietary confirmation score. "
        "The operating evidence remains mixed."
    )

    cleaned = service._remove_confirmation_data_conflation_from_text(text)

    assert "confirmation score" not in cleaned.lower()
    assert cleaned == "The operating evidence remains mixed."


def test_confirmation_preferences_restore_only_canonical_score_after_novel_conflation():
    article = {
        "sections": [
            {
                "heading": "Executive thesis",
                "body_markdown": (
                    "Cross-source data categories are inputs to the proprietary confirmation score. "
                    "The operating evidence remains mixed."
                ),
            }
        ],
    }
    context = {"primary": {"confirmation": {"score": 72, "direction": "mixed"}}}

    cleaned = service._apply_confirmation_preferences(
        article,
        {"include_confirmation_score": True, "include_cross_source_confirmations": False},
        context,
    )

    public_text = service._article_public_text(cleaned).lower()
    assert not service._conflates_confirmation_score_with_data(public_text)
    assert "our proprietary confirmation score is 72/100" in public_text


def _user(db, email: str, *, role: str = "user") -> UserAccount:
    user = UserAccount(email=email, role=role, entitlement_tier="admin" if role == "admin" else "premium")
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


def _request_for_user(user: UserAccount) -> Request:
    token = sign_session_payload({"uid": user.id, "email": user.email})
    return Request({"type": "http", "method": "POST", "path": "/", "headers": [(b"cookie", f"{SESSION_COOKIE_NAME}={token}".encode())], "client": ("127.0.0.1", 12345)})


def _seed_ticker(db, symbol: str = "MU"):
    db.add(Security(symbol=symbol, name=f"{symbol} Corp", asset_class="stock", sector="Technology"))
    db.add(TickerMeta(symbol=symbol, company_name=f"{symbol} Corp", exchange="NASDAQ", sector="Technology", industry="Semiconductors", country="US"))
    db.add(
        FundamentalsCache(
            symbol=symbol,
            provider="test",
            fetched_at=datetime(2026, 7, 20, tzinfo=timezone.utc),
            status="ok",
            company_name=f"{symbol} Corp",
            sector="Technology",
            industry="Semiconductors",
            volume=50_000_000,
            avg_volume=45_000_000,
            revenue_growth=42.5,
            eps_growth=18.0,
            gross_margin=61.2,
            forward_pe=24.0,
            debt_to_equity=0.3,
        )
    )
    db.add(QuoteCache(symbol=symbol, price=125.0, asof_ts=datetime(2026, 7, 20, 16, 0), market_cap=100_000_000_000))
    db.commit()


def _seed_financials_cache(db, symbol: str = "MU"):
    db.add(
        TickerFinancialsCache(
            symbol=symbol,
            status="ok",
            fetched_at=datetime(2026, 7, 20, tzinfo=timezone.utc),
            payload_json=json.dumps(
                {
                    "status": "ok",
                    "summary": {"latestQuarter": "Q2 2026"},
                    "forecasts": {
                        "nextQuarter": {
                            "period": "Q2 2026",
                            "revenueEstimate": 101_000_000_000,
                            "epsEstimate": 1.5,
                        }
                    },
                    "subsections": {
                        "analyst_estimates": {
                            "status": "ok",
                            "data": {
                                "nextQuarter": {
                                    "period": "Q2 2026",
                                    "revenueEstimate": 101_000_000_000,
                                    "epsEstimate": 1.5,
                                }
                            },
                        }
                    },
                }
            ),
        )
    )
    db.commit()


def _seed_event(db, symbol: str, event_type: str):
    db.add(
        Event(
            event_type=event_type,
            ts=datetime(2026, 7, 20, tzinfo=timezone.utc),
            event_date=datetime(2026, 7, 20, tzinfo=timezone.utc),
            symbol=symbol,
            source="test",
            impact_score=50,
            payload_json=json.dumps({"title": f"{symbol} {event_type}", "summary": "Reported activity"}),
        )
    )
    db.commit()


def _confirmation_bundle(score: int = 79):
    return {
        "score": score,
        "direction": "bullish",
        "status": "Strong",
        "sources": {
            "fundamentals": {"present": True, "direction": "bullish"},
            "institutional_activity": {"present": True, "direction": "bullish"},
            "price_volume": {"present": True, "direction": "mixed"},
            "options_flow": {"present": True, "direction": "bearish"},
        },
    }


def _payload(**overrides) -> ResearchBriefGeneratePayload:
    data = {
        "ticker": "MU",
        "research_question": "Is MU momentum supported by the current fundamentals?",
        "desired_angle": "Full company DD",
        "time_horizon": "Near term",
        "intended_audience": "Walnut Research Brief",
        "judgment_preference": "Let the data decide",
        "external_research_mode": "Off",
        "include_source_links": True,
    }
    data.update(overrides)
    return ResearchBriefGeneratePayload(**data)


def _fake_openai_response(*_args, **kwargs):
    class Response:
        status_code = 200

        def json(self):
            article = {
                "title": "MU fundamentals still matter",
                "slug": "mu-generated-test",
                "subtitle": "A grounded Walnut research brief.",
                "summary": "MU has supportive data, but this is research only. Not investment advice.",
                "preview_body": "MU has supportive data, but this is research only. Not investment advice.",
                "judgment": "mixed",
                "confidence": "medium",
                "primary_ticker": "MU",
                "comparison_tickers": [],
                "category": "Semiconductors",
                "reading_minutes": 8,
                "sections": [
                    {
                        "key": "thesis",
                        "heading": "Executive thesis",
                        "body_markdown": (
                            "MU's setup is still tied to observable data. Revenue growth of 42.5 and gross margin of 61.2 "
                            "support the constructive side, while cycle risk remains real.\n\n"
                            "Our confirmation score is 79/100, and it is separate from the revenue, margin, and tape data. "
                            "Research only. Not investment advice. Sources: https://www.sec.gov/edgar/search/#/q=MU and https://www.nasdaq.com/market-activity/stocks/mu. "
                            + " ".join(["Evidence remains specific."] * 120)
                        ),
                    }
                ],
                "key_points": ["Use underlying data before drawing a conclusion."],
                "catalysts": ["Next earnings update"],
                "risks": ["Memory cycle deterioration"],
                "watch_items": ["Revenue growth", "Gross margin"],
                "data_freshness": ["2026-07-20"],
                "missing_data_notes": [],
                "source_links": [
                    {"label": "SEC EDGAR company search", "url": "https://www.sec.gov/edgar/search/#/q=MU", "source_type": "filing_search"},
                    {"label": "MU Nasdaq market activity", "url": "https://www.nasdaq.com/market-activity/stocks/mu", "source_type": "reputable_market_source"},
                ],
                "suggested_card": {
                    "title": "MU fundamentals still matter",
                    "description": "A Walnut DD draft for MU.",
                    "judgment": "mixed",
                    "tickers": ["MU"],
                },
                "seo": {"title": "MU DD", "description": "Walnut MU research. Not investment advice."},
            }
            return {"output_text": json.dumps(article), "usage": {"input_tokens": 100, "output_tokens": 200}}

    assert kwargs["json"]["store"] is False
    assert kwargs["json"]["text"]["format"]["type"] == "json_schema"
    assert kwargs["json"]["input"]
    return Response()


def test_research_brief_generation_requires_admin(tmp_path, monkeypatch):
    monkeypatch.setenv(service.STORE_ENV, str(tmp_path / "drafts.json"))
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
    monkeypatch.setattr(service.requests, "post", _fake_openai_response)
    db = _session()
    _seed_ticker(db)
    user = _user(db, "user@example.com")

    with pytest.raises(HTTPException) as exc:
        admin_research_brief_generate(_payload(), _request_for_user(user), Response(), db=db)

    assert exc.value.status_code == 403


def test_research_brief_generation_uses_responses_and_saves_draft(tmp_path, monkeypatch):
    store_path = tmp_path / "drafts.json"
    monkeypatch.setenv(service.STORE_ENV, str(store_path))
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
    monkeypatch.setattr(service.requests, "post", _fake_openai_response)
    monkeypatch.setattr(service, "_start_research_brief_job_worker", lambda job_id: None)
    db = _session()
    _seed_ticker(db)
    admin = _user(db, "admin@example.com", role="admin")
    response = Response()

    job = admin_research_brief_generate(_payload(client_request_id="req-1"), _request_for_user(admin), response=response, db=db)

    assert response.status_code == 202
    assert job["status"] == "queued"
    assert job["job_id"]
    assert service.list_generation_jobs(db=db)["items"][0]["job_id"] == job["job_id"]

    service.run_research_brief_generation_job(job["job_id"], db)
    store_path.unlink(missing_ok=True)
    completed = service.get_research_brief_generation_job(job["job_id"], db)
    draft = service.get_research_brief_generation_job_draft(job["job_id"], db)

    assert completed["status"] == "completed"
    assert completed["draft_id"] == draft["id"]
    assert draft["status"] == "draft"
    assert draft["article"]["slug"] == "mu-generated-test"
    assert draft["article"]["preview_body"]
    assert draft["validation"]["status"] == "passed"
    assert draft["validation"]["source_link_count"] >= 2
    assert draft["model"] == "gpt-5.6-sol"
    saved = service.list_drafts(db=db)["items"]
    assert saved[0]["id"] == draft["id"]


def test_research_brief_generation_dedupes_client_request_id(tmp_path, monkeypatch):
    monkeypatch.setenv(service.STORE_ENV, str(tmp_path / "drafts.json"))
    monkeypatch.setattr(service, "_start_research_brief_job_worker", lambda job_id: None)
    db = _session()
    _seed_ticker(db)
    admin = _user(db, "admin@example.com", role="admin")
    request = _request_for_user(admin)

    first = admin_research_brief_generate(_payload(client_request_id="same-request"), request, Response(), db=db)
    second = admin_research_brief_generate(_payload(client_request_id="same-request"), request, Response(), db=db)

    assert second["job_id"] == first["job_id"]
    assert len(service.list_generation_jobs(db=db)["items"]) == 1


def test_research_campaign_create_distributes_ticker_items(tmp_path, monkeypatch):
    monkeypatch.setenv(service.STORE_ENV, str(tmp_path / "drafts.json"))
    db = _session()
    admin = _user(db, "admin@example.com", role="admin")

    campaign = service.create_research_campaign(
        db,
        admin,
        {
            "name": "Post-Earnings: Is It a Good Buy?",
            "theme": "good_buy_now",
            "content_type": "ticker",
            "tickers": ["NBIS", "CRWV", "COHR"],
            "cadence": "one_time",
            "publish_start_at": "2026-08-14T09:00:00+00:00",
            "article_count": 3,
            "window_days": 5,
            "target_keywords": {
                "NBIS": "Is NBIS stock overvalued after Q2 2026 earnings?",
                "CRWV": "Does CoreWeave backlog outweigh debt and capex risk?",
                "COHR": "Is Coherent stock an AI optics play after fiscal 2026 earnings?",
            },
            "target_search_intents": {
                "NBIS": "Is NBIS stock overvalued after Q2 2026 earnings?",
                "CRWV": "Does CoreWeave backlog outweigh debt and capex risk?",
                "COHR": "Is Coherent stock an AI optics play after fiscal 2026 earnings?",
            },
        },
    )

    assert campaign["name"] == "Post-Earnings: Is It a Good Buy?"
    assert campaign["theme"] == "good_buy_now"
    assert len(campaign["items"]) == 3
    assert [item["ticker"] for item in campaign["items"]] == ["NBIS", "CRWV", "COHR"]
    assert len({item["publish_at"][:10] for item in campaign["items"]}) == 3
    listed = service.list_research_campaigns(db)["items"][0]
    assert listed["pending_count"] == 3
    assert len(listed["items"]) == 3
    assert campaign["config"]["target_search_intents"]["CRWV"] == "Does CoreWeave backlog outweigh debt and capex risk?"
    item_config = service._campaign_item_generation_config({"ticker": "CRWV", "target_keyword": "Does CoreWeave backlog outweigh debt and capex risk?"}, campaign["config"])
    assert item_config["search_intent"] == "Does CoreWeave backlog outweigh debt and capex risk?"


def test_research_campaign_normalizes_long_search_intent_without_rejecting_payload():
    normalized = service._normalize_campaign_payload(
        {
            "name": "Campaign",
            "theme": "good_buy_now",
            "content_type": "ticker",
            "tickers": ["NBIS"],
            "search_intent": "x" * 240,
            "target_search_intents": {"NBIS": "y" * 240},
        }
    )

    assert len(normalized["search_intent"]) == 120
    assert len(normalized["target_search_intents"]["NBIS"]) == 120


def test_pending_campaign_item_can_be_rescheduled_or_run_individually(tmp_path, monkeypatch):
    monkeypatch.setenv(service.STORE_ENV, str(tmp_path / "drafts.json"))
    db = _session()
    admin = _user(db, "admin@example.com", role="admin")
    campaign = service.create_research_campaign(
        db,
        admin,
        {
            "name": "Campaign",
            "theme": "good_buy_now",
            "content_type": "ticker",
            "tickers": ["NBIS", "CRWV"],
            "publish_start_at": "2026-08-17T09:00:00+00:00",
            "article_count": 2,
            "window_days": 2,
        },
    )
    nbis = campaign["items"][0]
    updated = service.reschedule_research_campaign_item(db, campaign["id"], nbis["id"], "2026-08-18T09:00:00-07:00")

    assert updated["id"] == nbis["id"]
    assert updated["publish_at"].startswith("2026-08-18T16:00:00")
    calls = []

    def fake_run_due(_db, **kwargs):
        calls.append(kwargs)
        return {"generated": 1, "failed": 0, "skipped": 0, "checked": 1}

    monkeypatch.setattr(service, "run_due_research_campaign_generation", fake_run_due)
    result = service.run_research_campaign_item_now(db, campaign["id"], nbis["id"])

    assert result["generated"] == 1
    assert calls == [{"limit": 1, "campaign_id": campaign["id"], "item_id": nbis["id"]}]


def test_rejected_campaign_draft_creates_corrected_replacement_and_review_email(tmp_path, monkeypatch):
    monkeypatch.setenv(service.STORE_ENV, str(tmp_path / "drafts.json"))
    db = _session()
    admin = _user(db, "admin@example.com", role="admin")
    campaign = service.create_research_campaign(
        db,
        admin,
        {
            "name": "Campaign",
            "theme": "good_buy_now",
            "content_type": "ticker",
            "tickers": ["MU"],
            "publish_start_at": "2026-08-18T09:00:00+00:00",
        },
    )
    item = campaign["items"][0]
    rejected = _minimal_scheduled_draft(admin, campaign_id=campaign["id"])
    rejected["campaign_item_id"] = item["id"]
    rejected["campaign_name"] = campaign["name"]
    service._upsert_db_draft(db, rejected)

    replacement = _minimal_scheduled_draft(admin, campaign_id=campaign["id"])
    replacement["id"] = "rb_corrected"
    replacement["generated_at"] = datetime.now(timezone.utc).isoformat()
    sent = []
    monkeypatch.setattr(service, "generate_research_brief", lambda *_args, **_kwargs: deepcopy(replacement))
    monkeypatch.setattr(service, "send_research_campaign_review_email", lambda *_args: sent.append(True))

    result = service.reject_scheduled_research_brief(
        db,
        admin,
        rejected["id"],
        "Make the valuation explanation more concrete.",
    )

    assert service.get_draft(rejected["id"], db=db)["status"] == "rejected"
    assert result["status"] == "scheduled_review"
    assert result["revision_of"] == rejected["id"]
    assert result["revision_number"] == 1
    assert result["revision_request"] == "Make the valuation explanation more concrete."
    assert sent == [True]


def test_campaign_quality_gate_failure_is_retried_with_correction_note(tmp_path, monkeypatch):
    monkeypatch.setenv(service.STORE_ENV, str(tmp_path / "drafts.json"))
    db = _session()
    admin = _user(db, "admin@example.com", role="admin")
    campaign = service.create_research_campaign(
        db,
        admin,
        {"name": "Campaign", "theme": "good_buy_now", "content_type": "ticker", "tickers": ["MU"]},
    )
    item = campaign["items"][0]
    row = db.execute(
        text(
            """
            SELECT i.*, c.name AS campaign_name, c.theme AS campaign_theme, c.content_type AS campaign_content_type,
                   c.config_json AS campaign_config_json, c.created_by AS campaign_created_by, c.created_by_email AS campaign_created_by_email
            FROM research_campaign_items i JOIN research_campaigns c ON c.id = i.campaign_id WHERE i.id = :id
            """
        ),
        {"id": item["id"]},
    ).mappings().first()
    generated = _minimal_scheduled_draft(admin, campaign_id=campaign["id"])
    generated["id"] = "rb_quality_retry"
    generated["generated_at"] = datetime.now(timezone.utc).isoformat()
    configs = []

    def fake_generate(_db, _admin, config):
        configs.append(config)
        if len(configs) == 1:
            raise HTTPException(status_code=422, detail="Draft generation failed validation. Source links are required.")
        return deepcopy(generated)

    monkeypatch.setattr(service, "generate_research_brief", fake_generate)
    monkeypatch.setattr(service, "send_research_campaign_review_email", lambda *_args: None)

    result = service._generate_research_campaign_item(db, row)

    assert result["quality_gate_correction_note"].startswith("Walnut quality-gate correction note:")
    assert len(configs) == 2
    assert "Source links are required" in configs[1]["additional_context"]
    assert configs[1]["retry_output_tokens"] == 10000


def test_campaign_invalid_structured_output_is_retried_with_format_correction(tmp_path, monkeypatch):
    monkeypatch.setenv(service.STORE_ENV, str(tmp_path / "drafts.json"))
    db = _session()
    admin = _user(db, "admin@example.com", role="admin")
    config = _payload().model_dump()
    generated = _minimal_scheduled_draft(admin)
    configs = []

    def fake_generate(_db, _admin, attempt_config):
        configs.append(attempt_config)
        if len(configs) == 1:
            raise HTTPException(status_code=502, detail="OpenAI returned invalid structured research JSON.")
        return deepcopy(generated)

    monkeypatch.setattr(service, "generate_research_brief", fake_generate)

    result, notes = service._generate_campaign_brief_with_corrections(db, admin, config)

    assert result["id"] == generated["id"]
    assert len(configs) == 2
    assert len(notes) == 1
    assert "RFC 8259 JSON" in configs[1]["additional_context"]
    assert configs[1]["retry_output_tokens"] == 10000


def test_campaign_uses_walnut_data_fallback_after_exhausted_quality_retries(tmp_path, monkeypatch):
    monkeypatch.setenv(service.STORE_ENV, str(tmp_path / "drafts.json"))
    db = _session()
    admin = _user(db, "admin@example.com", role="admin")
    config = _payload().model_dump()
    generated = _minimal_scheduled_draft(admin)
    configs = []

    def fake_generate(_db, _admin, attempt_config):
        configs.append(deepcopy(attempt_config))
        if attempt_config.get("use_deterministic_draft"):
            return deepcopy(generated)
        raise HTTPException(status_code=422, detail="Draft generation failed validation. Cross-source data categories must not be described as the proprietary confirmation score.")

    monkeypatch.setattr(service, "generate_research_brief", fake_generate)

    result, notes = service._generate_campaign_brief_with_corrections(db, admin, config)

    assert result["id"] == generated["id"]
    assert len(configs) == 4
    assert configs[-1]["use_deterministic_draft"] is True
    assert configs[-1]["generate_thumbnail"] is False
    assert "Walnut data fallback" in notes[-1]


def test_campaign_generation_preserves_selected_opportunity_question():
    config = service._campaign_item_generation_config(
        {"ticker": "NBIS", "target_keyword": "Is Nebius stock overvalued after Q2 2026 earnings?"},
        {"theme": "good_buy_now"},
    )

    assert config["research_question"] == "Is Nebius stock overvalued after Q2 2026 earnings?"


def test_campaign_review_email_uses_editorial_recipient_by_default(monkeypatch):
    db = _session()
    admin = _user(db, "different-admin@example.com", role="admin")
    captured = {}

    def fake_send_email(_db, **kwargs):
        captured.update(kwargs)
        return {"status": "sent"}

    monkeypatch.delenv("RESEARCH_BRIEF_REVIEW_EMAIL", raising=False)
    monkeypatch.setattr(service, "send_email", fake_send_email)

    service.send_research_campaign_review_email(
        db,
        admin,
        {"id": "rb_email", "article": {"title": "NBIS review"}, "primary_ticker": "NBIS"},
        {},
        {},
    )

    assert captured["to_email"] == "jarod@walnutmarkets.com"


def test_walnut_data_fallback_marks_low_information_density_for_review_not_failure():
    article = {
        "_generation_mode": "walnut_data_fallback",
        "sections": [
            {
                "body_markdown": (
                    "This is a cautious research-only review of the current setup, relevant evidence, and the balance of potential upside and downside.\n\n"
                    "The thesis remains mixed until the next update clarifies the operating picture, risk balance, and whether the setup is changing materially.\n\n"
                    "Investors should review the cited sources, check the available information, and edit this draft before any publication decision is made."
                )
            }
        ],
    }
    context = {"primary": {"identity": {"symbol": "MU", "company_name": "Micron"}}}

    warnings = service._style_validation_warnings(article, context)

    density_warning = next(warning for warning in warnings if warning["code"] == "low_information_density")
    assert density_warning["blocking"] is False


def test_generate_research_brief_saves_reviewable_walnut_data_fallback(tmp_path, monkeypatch):
    monkeypatch.setenv(service.STORE_ENV, str(tmp_path / "drafts.json"))
    db = _session()
    _seed_ticker(db)
    admin = _user(db, "admin@example.com", role="admin")
    config = _payload().model_dump()
    config["use_deterministic_draft"] = True

    draft = service.generate_research_brief(db, admin, config)

    assert draft["validation"]["status"] == "passed"
    assert draft["config"]["use_deterministic_draft"] is True
    fallback_text = service._article_full_text(draft["article"]).lower()
    assert "free-cash-flow" not in fallback_text
    assert "watch-and-verify" not in fallback_text
    headings = {section["heading"] for section in draft["article"]["sections"]}
    assert {"Q2 2026 earnings and guidance", "Current price, valuation, and technical context", "Upcoming catalysts", "Risks that could break the thesis", "Final Walnut judgment"} <= headings

    updated = service.update_draft(admin, draft["id"], {"title": "MU Stock: Research Review"}, db=db)

    assert updated["validation"]["status"] == "passed"


def test_walnut_data_fallback_matches_bullish_confirmation_score_direction():
    article = service._walnut_data_fallback_article(
        {"ticker": "NBIS", "research_question": "Is Nebius stock overvalued after Q2 2026 earnings?"},
        {
            "primary": {
                "identity": {"symbol": "NBIS", "company_name": "Nebius"},
                "market_state": {"price": 277.69, "volume": 29_024_669},
                "financials": {"income": {"quarterly": []}, "valuation": {}, "health": {}},
                "confirmation": {"score": 87, "direction": "bullish"},
            },
            "external_research": {"official_facts": {}, "reviewed_sources": []},
        },
    )

    assert article["walnut_call"] == "Bullish but expensive"
    assert article["judgment"] == "bullish"
    assert "Neutral but expensive" not in service._article_full_text(article)


def test_final_confirmation_guard_runs_after_enrichment(tmp_path, monkeypatch):
    monkeypatch.setenv(service.STORE_ENV, str(tmp_path / "drafts.json"))
    monkeypatch.setenv(service.MOCK_ENV, "1")
    db = _session()
    _seed_ticker(db)
    admin = _user(db, "admin@example.com", role="admin")

    def add_conflated_copy(article, _context):
        article = deepcopy(article)
        article["sections"][0]["body_markdown"] += " The confirmation score is based on fundamentals and price/volume."
        return article

    monkeypatch.setattr(service, "enrich_internal_links", add_conflated_copy)
    draft = service.generate_research_brief(db, admin, _payload().model_dump())

    body = service._article_body_text(draft["article"]).lower()
    assert "confirmation score is based on fundamentals" not in body
    assert draft["validation"]["status"] == "passed"


def test_research_campaign_marks_selected_keyword_opportunity_used_only_after_creation(tmp_path, monkeypatch):
    monkeypatch.setenv(service.STORE_ENV, str(tmp_path / "drafts.json"))
    db = _session()
    admin = _user(db, "admin@example.com", role="admin")
    service.ensure_research_brief_store_schema(db)
    db.execute(
        text(
            """
            INSERT INTO research_keyword_opportunities (
                id, status, created_by, target_keyword, opportunity_score, discovered_at, updated_at, payload_json
            ) VALUES ('rko_nbis', 'new', :created_by, 'Is NBIS stock overvalued?', 90, :now, :now, :payload_json)
            """
        ),
        {"created_by": admin.id, "now": "2026-08-16T00:00:00+00:00", "payload_json": json.dumps({"target_keyword": "Is NBIS stock overvalued?"})},
    )
    db.commit()

    campaign = service.create_research_campaign(
        db,
        admin,
        {
            "name": "NBIS earnings question",
            "theme": "good_buy_now",
            "content_type": "ticker",
            "tickers": ["NBIS"],
            "source_opportunity_ids": ["rko_nbis"],
        },
    )

    assert campaign["config"]["source_opportunity_ids"] == ["rko_nbis"]
    assert service.list_research_keyword_opportunities(db, status="used")["items"][0]["id"] == "rko_nbis"


def test_scheduled_research_publish_requires_approval(tmp_path, monkeypatch):
    monkeypatch.setenv(service.STORE_ENV, str(tmp_path / "drafts.json"))
    db = _session()
    admin = _user(db, "admin@example.com", role="admin")
    campaign = service.create_research_campaign(
        db,
        admin,
        {
            "name": "Campaign",
            "theme": "good_buy_now",
            "content_type": "ticker",
            "tickers": ["MU"],
            "publish_start_at": "2026-08-14T09:00:00+00:00",
        },
    )
    draft = _minimal_scheduled_draft(admin, campaign_id=campaign["id"], status="scheduled_review")
    service._upsert_db_draft(db, draft)

    calls = []

    def fake_publish(*args, **kwargs):
        calls.append((args, kwargs))
        published = service.get_draft(draft["id"], db=db)
        published["status"] = "published"
        published["published_at"] = datetime.now(timezone.utc).isoformat()
        service._upsert_db_draft(db, published)
        return published

    monkeypatch.setattr(service, "publish_draft", fake_publish)

    unapproved = service.run_due_scheduled_research_publications(db)
    assert unapproved["checked"] == 0
    assert calls == []

    approved = service.approve_scheduled_research_brief(db, admin, draft["id"])
    assert approved["status"] == "approved_scheduled"

    result = service.run_due_scheduled_research_publications(db)
    assert result["published"] == 1
    assert len(calls) == 1
    assert service.get_draft(draft["id"], db=db)["status"] == "published"


def test_research_brief_job_failure_returns_safe_error(tmp_path, monkeypatch):
    monkeypatch.setenv(service.STORE_ENV, str(tmp_path / "drafts.json"))
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
    monkeypatch.setattr(service, "_start_research_brief_job_worker", lambda job_id: None)

    def bad_post(*_args, **_kwargs):
        raise RuntimeError("provider cache raw token exploded")

    monkeypatch.setattr(service.requests, "post", bad_post)
    db = _session()
    _seed_ticker(db)
    admin = _user(db, "admin@example.com", role="admin")

    job = admin_research_brief_generate(_payload(client_request_id="fail-request"), _request_for_user(admin), Response(), db=db)
    service.run_research_brief_generation_job(job["job_id"], db)
    failed = service.get_research_brief_generation_job(job["job_id"], db)

    assert failed["status"] == "failed"
    assert failed["error_message_safe"] == service.RESEARCH_BRIEF_JOB_SAFE_ERROR
    assert "provider" not in failed["error_message_safe"].lower()


def test_stale_running_research_brief_job_fails_on_poll(tmp_path, monkeypatch):
    monkeypatch.setenv(service.STORE_ENV, str(tmp_path / "drafts.json"))
    monkeypatch.setenv(service.RESEARCH_BRIEF_JOB_STALE_SECONDS, "1")
    monkeypatch.setattr(service, "_start_research_brief_job_worker", lambda job_id: None)
    db = _session()
    _seed_ticker(db)
    admin = _user(db, "admin@example.com", role="admin")
    job = admin_research_brief_generate(_payload(client_request_id="stale-request"), _request_for_user(admin), Response(), db=db)
    stale_at = (datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat()
    db.execute(
        text(
            """
            UPDATE research_brief_generation_jobs
            SET status = 'running', started_at = :stale_at, updated_at = :stale_at, progress_step = 'calling_openai'
            WHERE id = :job_id
            """
        ),
        {"stale_at": stale_at, "job_id": job["job_id"]},
    )
    db.commit()

    polled = service.get_research_brief_generation_job(job["job_id"], db)

    assert polled["status"] == "failed"
    assert polled["error_message_safe"] == service.RESEARCH_BRIEF_JOB_STALE_ERROR


def test_thumbnail_failure_does_not_fail_text_draft(tmp_path, monkeypatch):
    monkeypatch.setenv(service.STORE_ENV, str(tmp_path / "drafts.json"))
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
    monkeypatch.setattr(service.requests, "post", _fake_openai_response)
    monkeypatch.setattr(service, "generate_thumbnail_asset", lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("image failed")))
    db = _session()
    _seed_ticker(db)
    admin = _user(db, "admin@example.com", role="admin")

    draft = service.generate_research_brief(db, admin, _payload(generate_thumbnail=True).model_dump())

    assert draft["status"] == "draft"
    assert draft["article"]["thumbnail_asset"]["url"] == ""
    assert "text draft was saved" in draft["article"]["thumbnail_asset"]["source_notes"]


def test_model_selector_passes_selected_model_and_rejects_invalid(tmp_path, monkeypatch):
    monkeypatch.setenv(service.STORE_ENV, str(tmp_path / "drafts.json"))
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
    monkeypatch.setenv(service.RESEARCH_BRIEF_MODEL_DEFAULT, "gpt-fast")
    monkeypatch.setenv(service.RESEARCH_BRIEF_MODEL_OPTIONS, "gpt-fast,gpt-deep")
    captured = {}

    def fake_post(*_args, **kwargs):
        captured["model"] = kwargs["json"]["model"]
        return _fake_openai_response(*_args, **kwargs)

    monkeypatch.setattr(service.requests, "post", fake_post)
    db = _session()
    _seed_ticker(db)
    admin = _user(db, "admin@example.com", role="admin")

    draft = service.generate_research_brief(db, admin, _payload(selected_model="gpt-deep").model_dump())

    assert captured["model"] == "gpt-deep"
    assert draft["model"] == "gpt-deep"
    with pytest.raises(HTTPException) as exc:
        service.validate_config(_payload(selected_model="not-configured").model_dump())
    assert exc.value.status_code == 422


def test_existing_draft_save_clears_unconfigured_selected_model(tmp_path, monkeypatch):
    monkeypatch.setenv(service.STORE_ENV, str(tmp_path / "drafts.json"))
    monkeypatch.setenv(service.MOCK_ENV, "1")
    db = _session()
    _seed_ticker(db)
    admin = _user(db, "admin@example.com", role="admin")
    draft = service.generate_research_brief(db, admin, _payload().model_dump())
    article = deepcopy(draft["article"])
    config_patch = {**draft["config"], "selected_model": "manual-researched-draft"}

    saved = service.update_draft(admin, draft["id"], article, status="draft", db=db, config_patch=config_patch)

    assert saved["status"] == "draft"
    assert saved["config"]["selected_model"] == ""
    assert saved["article"]["title"] == article["title"]


def test_model_options_default_to_luna_terra_sol(monkeypatch):
    monkeypatch.delenv(service.RESEARCH_BRIEF_MODEL_DEFAULT, raising=False)
    monkeypatch.delenv(service.RESEARCH_BRIEF_MODEL_OPTIONS, raising=False)

    assert service.research_brief_model_options(None) == ["gpt-5.6-luna", "gpt-5.6-terra", "gpt-5.6-sol"]
    assert service.research_brief_model(None) == "gpt-5.6-terra"
    assert service.research_brief_model_labels(None)["gpt-5.6-luna"] == "GPT-5.6 Luna"


def test_article_schema_is_strict_structured_output_compatible():
    def walk(node, path="root"):
        if not isinstance(node, dict):
            return
        if node.get("type") == "object":
            assert node.get("additionalProperties") is False, path
            properties = node.get("properties") or {}
            assert sorted(node.get("required") or []) == sorted(properties.keys()), path
            for key, child in properties.items():
                walk(child, f"{path}.{key}")
        if node.get("type") == "array":
            walk(node.get("items"), f"{path}[]")

    walk(service.article_schema())


def test_confirmation_preferences_pass_booleans_and_add_requested_sections(tmp_path, monkeypatch):
    monkeypatch.setenv(service.STORE_ENV, str(tmp_path / "drafts.json"))
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
    captured = {}

    def fake_post(*args, **kwargs):
        captured["input"] = kwargs["json"]["input"]
        return _fake_openai_response(*args, **kwargs)

    monkeypatch.setattr(service.requests, "post", fake_post)
    monkeypatch.setattr(service, "get_confirmation_score_bundles_for_tickers", lambda *_args, **_kwargs: {"MU": _confirmation_bundle(79)})
    db = _session()
    _seed_ticker(db)
    admin = _user(db, "admin@example.com", role="admin")

    draft = service.generate_research_brief(
        db,
        admin,
        _payload(include_confirmation_score=True, include_cross_source_confirmations=True).model_dump(),
    )
    body = "\n\n".join(f"{section['heading']}\n{section['body_markdown']}" for section in draft["article"]["sections"])

    assert '"include_confirmation_score": true' in captured["input"]
    assert '"include_cross_source_confirmations": true' in captured["input"]
    assert "Our confirmation score is 79/100" in body
    assert draft["article"]["confirmation_score_included"] is True
    assert "Walnut's proprietary confirmation score" not in body
    assert "Cross-source confirmations" in body
    assert "supported by fundamentals, reported institutional activity" in body
    assert "mixed in price/volume" in body
    assert "data categories" in body
    assert "stack" not in body.lower()


def test_confirmation_preferences_omit_unchecked_score_and_cross_source(tmp_path, monkeypatch):
    monkeypatch.setenv(service.STORE_ENV, str(tmp_path / "drafts.json"))
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
    monkeypatch.setattr(service.requests, "post", _fake_openai_response)
    monkeypatch.setattr(service, "get_confirmation_score_bundles_for_tickers", lambda *_args, **_kwargs: {"MU": _confirmation_bundle(79)})
    db = _session()
    _seed_ticker(db)
    admin = _user(db, "admin@example.com", role="admin")

    draft = service.generate_research_brief(
        db,
        admin,
        _payload(include_confirmation_score=False, include_cross_source_confirmations=False).model_dump(),
    )
    body = "\n\n".join(f"{section['heading']}\n{section['body_markdown']}" for section in draft["article"]["sections"])

    assert "confirmation score" not in body.lower()
    assert "we do not publish our proprietary confirmation score" not in body.lower()
    assert draft["article"]["confirmation_score_included"] is False
    assert "cross-source confirmation" not in body.lower()
    assert draft["config"]["include_confirmation_score"] is False
    assert draft["config"]["include_cross_source_confirmations"] is False


def test_confirmation_score_generic_mention_still_appends_actual_score():
    article = {
        "title": "AAPL generic score",
        "slug": "aapl-generic-score",
        "subtitle": "Research only. Not investment advice.",
        "summary": "The confirmation score is separate from underlying data.",
        "preview_body": "Research only. Not investment advice.",
        "judgment": "mixed",
        "sections": [
            {
                "key": "body",
                "heading": "Executive thesis",
                "body_markdown": "The confirmation score is separate from the underlying data. Research only. Not investment advice. https://www.sec.gov/edgar/search/#/q=AAPL https://investor.apple.com/earnings-results/default.aspx " + "word " * 220,
            }
        ],
        "source_links": [
            {"label": "SEC", "url": "https://www.sec.gov/edgar/search/#/q=AAPL", "source_type": "filing_search"},
            {"label": "Apple IR", "url": "https://investor.apple.com/earnings-results/default.aspx", "source_type": "official_company_ir"},
        ],
    }


def _minimal_scheduled_draft(admin: UserAccount, *, campaign_id: str | None = None, status: str = "scheduled_review", scheduled_at: datetime | None = None):
    scheduled = (scheduled_at or datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat()
    return {
        "id": f"rb_test_{status}",
        "status": status,
        "created_by": admin.id,
        "created_by_email": admin.email,
        "created_at": scheduled,
        "updated_at": scheduled,
        "published_at": None,
        "scheduled_at": scheduled,
        "approved_at": None,
        "campaign_id": campaign_id,
        "campaign_item_id": "rci_test" if campaign_id else None,
        "campaign_name": "Test Campaign" if campaign_id else None,
        "data_as_of": scheduled,
        "model": "test-model",
        "prompt_version": service.RESEARCH_BRIEF_PROMPT_VERSION,
        "research_context_timestamp": scheduled,
        "primary_ticker": "MU",
        "comparison_ticker": None,
        "comparison_tickers": [],
        "config": _payload().model_dump(),
        "article": {
            "title": "MU scheduled brief",
            "slug": f"mu-scheduled-{status}",
            "subtitle": "Test",
            "summary": "Scheduled test brief. Not investment advice.",
            "preview_body": "Scheduled test brief. Not investment advice.",
            "judgment": "mixed",
            "walnut_call": "Mixed",
            "confidence": "medium",
            "confirmation_score_included": False,
            "primary_ticker": "MU",
            "comparison_tickers": [],
            "category": "Research",
            "reading_minutes": 4,
            "sections": [{"key": "body", "heading": "Body", "body_markdown": " ".join(["MU has source-backed scheduled evidence."] * 130) + " Research only. Not investment advice."}],
            "key_points": [],
            "catalysts": [],
            "risks": [],
            "watch_items": [],
            "data_freshness": [scheduled],
            "missing_data_notes": [],
            "source_links": [
                {"label": "SEC", "url": "https://www.sec.gov/edgar/search/#/q=MU", "source_type": "filing_search"},
                {"label": "Nasdaq", "url": "https://www.nasdaq.com/market-activity/stocks/mu", "source_type": "market_source"},
            ],
            "suggested_card": {"title": "MU scheduled brief", "description": "Scheduled test brief.", "judgment": "mixed", "tickers": ["MU"]},
            "seo": {"title": "MU scheduled brief", "description": "Scheduled test brief."},
        },
        "validation": {"status": "passed", "warnings": [], "numeric_claims": [], "source_link_count": 2, "estimated_reading_minutes": 4},
        "diagnostics": {"elapsed_ms": 0, "storage": "database", "usage": {}},
        "research_context": {"generated_at": scheduled, "primary": {"identity": {"symbol": "MU", "company_name": "MU Corp"}}},
    }

    cleaned = service.sanitize_research_brief_article(
        article,
        {"include_confirmation_score": True},
        {"primary": {"identity": {"symbol": "AAPL"}, "confirmation": _confirmation_bundle(74)}, "include_confirmation_score": True},
    )
    body = "\n\n".join(section["body_markdown"] for section in cleaned["sections"])

    assert "Our proprietary confirmation score is 74/100" in body
    assert cleaned["confirmation_score_included"] is True


def test_confirmation_score_checked_requires_score_value_in_body():
    article = {
        "title": "AAPL missing score body",
        "slug": "aapl-missing-score-body",
        "summary": "Research only. Not investment advice.",
        "preview_body": "Research only. Not investment advice.",
        "sections": [{"heading": "Executive thesis", "body_markdown": "The confirmation score is separate from underlying data. Research only. Not investment advice. https://www.sec.gov/edgar/search/#/q=AAPL https://investor.apple.com/earnings-results/default.aspx " + "word " * 220}],
        "source_links": [
            {"label": "SEC", "url": "https://www.sec.gov/edgar/search/#/q=AAPL", "source_type": "filing_search"},
            {"label": "Apple IR", "url": "https://investor.apple.com/earnings-results/default.aspx", "source_type": "official_company_ir"},
        ],
    }

    validation = service.validate_article(article, {"primary": {"identity": {"symbol": "AAPL"}, "confirmation": _confirmation_bundle(74)}, "include_confirmation_score": True})

    assert validation["status"] == "failed"
    assert any(warning["code"] == "confirmation_score_missing_from_body" for warning in validation["warnings"])


def test_aapl_earnings_setup_discovers_official_q2_2026_sources(monkeypatch):
    monkeypatch.setattr(service, "_sec_company_record", lambda symbol: {"ticker": symbol, "cik_str": 320193, "title": "Apple Inc."})
    monkeypatch.setattr(service, "_sec_company_facts", lambda _cik: {})

    external = service.discover_external_research(
        "AAPL",
        {"symbol": "AAPL", "company_name": "Apple Inc."},
        mode="Standard",
        desired_angle="Earnings setup",
        research_question="Build an AAPL earnings setup.",
    )

    urls = {source["url"] for source in external["reviewed_sources"]}
    facts = external["official_facts"]

    assert "https://www.apple.com/ca/newsroom/2026/04/apple-reports-second-quarter-results/" in urls
    assert "https://investor.apple.com/earnings-results/default.aspx" in urls
    assert "https://investor.apple.com/sec-filings/default.aspx" in urls
    assert facts["latest_official_quarter"] == "Q2 FY2026"
    assert facts["revenue"]["value"] == 111.2
    assert facts["diluted_eps"]["value"] == 2.01
    assert external["source_discovery"]["official_earnings_release"]["status"] == "found"
    assert external["source_discovery"]["sec_filing"]["status"] == "found"


def test_aapl_context_uses_primary_ticker_confirmation_and_sources(monkeypatch):
    monkeypatch.setattr(service, "_sec_company_record", lambda symbol: {"ticker": symbol, "cik_str": 320193, "title": "Apple Inc."})
    monkeypatch.setattr(service, "_sec_company_facts", lambda _cik: {})
    monkeypatch.setattr(service, "get_confirmation_score_bundles_for_tickers", lambda *_args, **_kwargs: {"AAPL": {**_confirmation_bundle(74), "symbol": "AAPL"}, "MSFT": {**_confirmation_bundle(88), "symbol": "MSFT"}})
    db = _session()
    _seed_ticker(db, "AAPL")
    _seed_ticker(db, "MSFT")

    context = service.assemble_research_context(
        db,
        _payload(
            ticker="AAPL",
            desired_angle="Earnings setup",
            research_question="Build an AAPL earnings setup for the latest official quarter.",
            comparison_tickers=["MSFT"],
            external_research_mode="Standard",
            include_confirmation_score=True,
        ).model_dump(),
    )

    assert context["primary"]["identity"]["symbol"] == "AAPL"
    assert context["primary"]["confirmation"]["symbol"] == "AAPL"
    assert context["primary_ticker_context"]["symbol"] == "AAPL"
    assert context["primary_ticker_context"]["confirmation_score"] == 74
    assert context["comparisons"][0]["identity"]["symbol"] == "MSFT"
    assert context["comparisons"][0]["confirmation"]["symbol"] == "MSFT"
    assert context["external_research"]["official_facts"]["revenue"]["value"] == 111.2
    assert context["external_research"]["official_facts"]["diluted_eps"]["value"] == 2.01


def test_earnings_setup_stops_before_openai_when_research_not_ready(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
    monkeypatch.setattr(service, "_sec_company_record", lambda _symbol: None)
    called = False

    def fake_post(*_args, **_kwargs):
        nonlocal called
        called = True
        raise AssertionError("OpenAI should not be called when research is not ready")

    monkeypatch.setattr(service.requests, "post", fake_post)
    monkeypatch.setattr(service, "get_confirmation_score_bundles_for_tickers", lambda *_args, **_kwargs: {"TEST": _confirmation_bundle(70)})
    db = _session()
    _seed_ticker(db, "TEST")
    admin = _user(db, "admin@example.com", role="admin")

    with pytest.raises(HTTPException) as exc:
        service.generate_research_brief(
            db,
            admin,
            _payload(
                ticker="TEST",
                desired_angle="Earnings setup",
                research_question="Is TEST a buy before earnings?",
                external_research_mode="Standard",
            ).model_dump(),
        )

    assert exc.value.status_code == 422
    assert "Brief not generated" in exc.value.detail
    assert "Official earnings release" in exc.value.detail
    assert called is False


def test_nbis_earnings_setup_generates_after_readiness_and_keeps_nebius_primary(monkeypatch, tmp_path):
    monkeypatch.setenv(service.STORE_ENV, str(tmp_path / "drafts.json"))
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
    monkeypatch.setattr(service, "_sec_company_record", lambda symbol: {"ticker": symbol, "cik_str": 1513845, "title": "Nebius Group N.V."} if symbol == "NBIS" else {"ticker": symbol, "cik_str": 1869392, "title": "CoreWeave, Inc."})
    monkeypatch.setattr(service, "_sec_company_facts", lambda _cik: {})
    monkeypatch.setattr(service, "get_confirmation_score_bundles_for_tickers", lambda *_args, **_kwargs: {"NBIS": {**_confirmation_bundle(73), "symbol": "NBIS"}, "CRWV": {**_confirmation_bundle(62), "symbol": "CRWV"}})

    def fake_post(*_args, **kwargs):
        prompt = kwargs["json"]["input"]
        assert "PRIMARY_TICKER: NBIS" in prompt
        assert "PRIMARY_COMPANY: Nebius Group N.V." in prompt
        assert "COMPARISON_TICKERS: CRWV" in prompt

        class Response:
            status_code = 200

            def json(self):
                body = (
                    "Consensus is roughly $535M of revenue and a $0.67/share loss for Q2 2026. "
                    "Last quarter Nebius reported about $399M of revenue and a $0.23/share loss, ahead of the Street's revenue and EPS marks. "
                    "That makes the setup simple: NBIS needs to show revenue, ARR, and capacity deployment are moving together.\n\n"
                    "What changed since last earnings is concrete. Nebius added Pennsylvania power capacity, kept FY 2026 revenue guidance at $3.0B-$3.4B, "
                    "and continues to spend heavily on AI cloud capacity. CRWV is useful as a comparison because both companies are trying to turn demand into contracted infrastructure revenue.\n\n"
                    "The numbers that matter are revenue consensus near $535M, EPS consensus near -$0.67, Q1 revenue near $399M, Q1 EPS near -$0.23, and FY 2026 guidance of $3.0B-$3.4B. "
                    "Our confirmation score is 73/100, which supports the constructive side but does not remove the capex risk. "
                    "If revenue growth keeps pace with the buildout, the market can stay patient with capex. If spending rises while revenue does not follow, the thesis gets uncomfortable.\n\n"
                    "NBIS is not a low-risk earnings setup. The bull case is that demand fills new capacity quickly, ARR keeps scaling, and guidance holds. "
                    "The bear case is that the company spends ahead of demand and investors start underwriting dilution, debt, or margin pressure instead of growth.\n\n"
                    "Our call: Mixed with capex risk. We like the revenue ramp and the size of the opportunity, but the print has to prove execution. "
                    "We are watching ARR, deployed capacity, capex, gross margin, and whether management keeps the FY 2026 target intact. Research only. Not investment advice. "
                    "Sources: https://nebius.com/financials https://nebius.com/newsroom/nebius-reports-first-quarter-2026-financial-results https://www.sec.gov/edgar/browse/?CIK=0001513845"
                )
                article = {
                    "title": "NBIS Earnings Preview: Can Growth Keep Up With the Buildout?",
                    "slug": "nbis-earnings-preview-growth-buildout",
                    "subtitle": "Nebius needs revenue and ARR to keep pace with capacity spending.",
                    "summary": "NBIS goes into the print with Q2 revenue consensus near $535M and FY 2026 guidance at $3.0B-$3.4B. We're watching ARR, deployed capacity, capex, margins, and whether management keeps the target intact. Research only. Not investment advice.",
                    "preview_body": "NBIS goes into the print with Q2 revenue consensus near $535M after beating last quarter. We're watching ARR, capacity deployment, margins, capex, and whether management keeps its FY 2026 target intact.",
                    "judgment": "mixed",
                    "walnut_call": "Mixed with capex risk",
                    "confidence": "medium",
                    "confirmation_score_included": True,
                    "primary_ticker": "NBIS",
                    "comparison_tickers": ["CRWV"],
                    "category": "AI infrastructure",
                    "reading_minutes": 5,
                    "preview_section_count": 1,
                    "hero_image": "",
                    "current_data_as_of": "2026-08-01",
                    "premium_required": False,
                    "required_plan": None,
                    "paywall_copy": {"heading": "Keep reading", "description": "Full Walnut brief.", "cta_label": "Read the brief"},
                    "analytics": {},
                    "reddit_post": "",
                    "thumbnail_asset": {},
                    "sections": [
                        {"key": "setup", "heading": "Opening setup", "body_markdown": body},
                    ],
                    "key_points": ["Q2 revenue consensus is near $535M.", "Capex execution is the main risk."],
                    "catalysts": ["Q2 2026 earnings", "FY 2026 guidance update"],
                    "risks": ["Capex outruns revenue", "Margins lag capacity growth"],
                    "watch_items": ["ARR", "Revenue", "CapEx", "Gross margin", "Capacity deployment"],
                    "data_freshness": ["Consensus and company source profile reviewed for the NBIS earnings setup."],
                    "missing_data_notes": [],
                    "source_links": [
                        {"label": "Nebius financial results archive", "url": "https://nebius.com/financials", "source_type": "official_company_earnings"},
                        {"label": "Nebius Q1 2026 financial results", "url": "https://nebius.com/newsroom/nebius-reports-first-quarter-2026-financial-results", "source_type": "official_company_earnings"},
                        {"label": "Nebius SEC filings", "url": "https://www.sec.gov/edgar/browse/?CIK=0001513845&owner=exclude", "source_type": "official_filing"},
                        {"label": "Zacks NBIS detailed estimates", "url": "https://stage.zacks.com/stock/quote/NBIS/detailed-earning-estimates", "source_type": "reputable_estimate_source"},
                    ],
                    "suggested_card": {
                        "title": "NBIS Earnings Preview: Can Growth Keep Up?",
                        "description": "NBIS reports with Q2 revenue consensus near $535M after beating last quarter. We're watching ARR, capacity deployment, margins, capex, and whether management keeps FY 2026 targets intact.",
                        "judgment": "Mixed with capex risk",
                        "tickers": ["NBIS", "CRWV"],
                    },
                    "seo": {"title": "NBIS Earnings Preview", "description": "Walnut NBIS earnings preview. Research only. Not investment advice."},
                }
                return {"output_text": json.dumps(article), "usage": {"input_tokens": 1200, "output_tokens": 600}}

        return Response()

    monkeypatch.setattr(service.requests, "post", fake_post)
    db = _session()
    _seed_ticker(db, "NBIS")
    _seed_ticker(db, "CRWV")
    db.get(TickerMeta, "NBIS").company_name = "Nebius Group N.V."
    db.get(TickerMeta, "CRWV").company_name = "CoreWeave, Inc."
    db.commit()
    admin = _user(db, "admin@example.com", role="admin")

    draft = service.generate_research_brief(
        db,
        admin,
        _payload(
            ticker="NBIS",
            desired_angle="Earnings setup",
            research_question="Is NBIS a buy before earnings?",
            comparison_tickers=["CRWV"],
            external_research_mode="Standard",
            include_confirmation_score=True,
        ).model_dump(),
    )

    body = "\n".join(section["body_markdown"] for section in draft["article"]["sections"])
    assert draft["validation"]["status"] == "passed"
    assert draft["research_context"]["research_readiness"]["status"] == "ready"
    assert "Nvidia" not in body
    assert "PRIMARY_TICKER: NBIS" not in body
    assert draft["article"]["suggested_card"]["description"].startswith("NBIS reports with Q2 revenue consensus near $535M")


def test_nbis_nvidia_contamination_is_hard_validation_failure():
    article = {
        "title": "NBIS Earnings Preview: Can Growth Keep Up With the Buildout?",
        "slug": "nbis-nvidia-contamination",
        "summary": "Research only. Not investment advice.",
        "preview_body": "Research only. Not investment advice.",
        "walnut_call": "Mixed with capex risk",
        "primary_ticker": "NBIS",
        "comparison_tickers": ["CRWV"],
        "sections": [
            {
                "heading": "Executive thesis",
                "body_markdown": "NBIS has revenue growth of 42%. Nvidia reported stronger data center revenue, and Nvidia margins are the real subject here. Research only. Not investment advice. https://nebius.com/financials https://www.sec.gov/edgar/browse/?CIK=0001513845 " + "NBIS data remains specific. " * 70,
            }
        ],
        "source_links": [
            {"label": "Nebius financial results archive", "url": "https://nebius.com/financials", "source_type": "official_company_earnings"},
            {"label": "Nebius SEC filings", "url": "https://www.sec.gov/edgar/browse/?CIK=0001513845", "source_type": "official_filing"},
        ],
    }
    context = _earnings_context("NBIS")
    context["primary"]["identity"]["company_name"] = "Nebius Group N.V."
    context["comparisons"] = [{"identity": {"symbol": "CRWV", "company_name": "CoreWeave, Inc."}}]
    context["source_discovery"] = {
        "required_for_earnings_setup": True,
        "official_earnings_release": {"status": "found", "required": True, "url": "https://nebius.com/financials"},
        "sec_filing": {"status": "found", "required": True, "url": "https://www.sec.gov/edgar/browse/?CIK=0001513845"},
    }
    context["external_research"] = {
        "official_facts": {
            "upcoming_earnings_date": {"value": "2026-08-06"},
            "current_revenue_consensus": {"value": 535.03},
            "current_eps_consensus": {"value": -0.67},
            "previous_quarter_revenue": {"value": 399},
            "previous_quarter_eps": {"value": -0.23},
            "previous_quarter_result": {"value": "beat"},
        }
    }
    context["data_availability"].update(
        {
            "upcoming earnings date": True,
            "revenue consensus": True,
            "eps consensus": True,
            "previous quarter revenue": True,
            "previous quarter eps": True,
            "previous quarter result": True,
        }
    )
    context["research_readiness"] = service.research_readiness(context)

    validation = service.validate_article(article, context)

    assert validation["status"] == "failed"
    assert any(warning["code"] == "company_identity_contamination" for warning in validation["warnings"])


def test_primary_ticker_context_mismatch_fails_before_generation(monkeypatch):
    monkeypatch.setattr(service, "get_confirmation_score_bundles_for_tickers", lambda *_args, **_kwargs: {"AAPL": {**_confirmation_bundle(88), "symbol": "MSFT"}})
    db = _session()
    _seed_ticker(db, "AAPL")

    with pytest.raises(HTTPException) as exc:
        service.assemble_research_context(
            db,
            _payload(
                ticker="AAPL",
                desired_angle="Earnings setup",
                research_question="Build an AAPL earnings setup.",
                external_research_mode="Off",
            ).model_dump(),
        )

    assert exc.value.status_code == 422
    assert "Primary ticker context mismatch: expected AAPL, received MSFT" in exc.value.detail


def test_major_earnings_setup_validation_fails_missing_official_source():
    article = {
        "title": "AAPL source failure",
        "slug": "aapl-source-failure",
        "summary": "Research only. Not investment advice.",
        "preview_body": "Research only. Not investment advice.",
        "walnut_call": "Mixed",
        "sections": [{"heading": "Executive thesis", "body_markdown": "AAPL setup. Research only. Not investment advice. https://www.nasdaq.com/market-activity/stocks/aapl https://example.com/source " + "word " * 220}],
        "source_links": [
            {"label": "Nasdaq", "url": "https://www.nasdaq.com/market-activity/stocks/aapl", "source_type": "reputable_market_source"},
            {"label": "Example", "url": "https://example.com/source", "source_type": "reputable_market_source"},
        ],
    }
    context = {
        "desired_angle": "Earnings setup",
        "research_question": "Build an AAPL earnings setup.",
        "primary": {"identity": {"symbol": "AAPL"}, "confirmation": _confirmation_bundle(74), "quote": {"price": 200}, "fundamentals": {"revenue_growth": 10}},
        "source_discovery": {
            "required_for_major_earnings_setup": True,
            "official_earnings_release": {"status": "missing", "required": True},
            "sec_filing": {"status": "missing", "required": True},
        },
        "external_research": {"official_facts": {}},
    }

    validation = service.validate_article(article, context)
    codes = {warning["code"] for warning in validation["warnings"]}

    assert validation["status"] == "failed"
    assert "missing_official_earnings_source" in codes
    assert "official_earnings_retrieval_failed" in codes
    assert "missing_sec_or_ir_source" in codes


def test_aapl_earnings_setup_validation_blocks_stale_year_substitution(monkeypatch):
    monkeypatch.setattr(service, "_sec_company_record", lambda symbol: {"ticker": symbol, "cik_str": 320193, "title": "Apple Inc."})
    monkeypatch.setattr(service, "_sec_company_facts", lambda _cik: {})
    external = service.discover_external_research(
        "AAPL",
        {"symbol": "AAPL", "company_name": "Apple Inc."},
        mode="Standard",
        desired_angle="Earnings setup",
        research_question="Build an AAPL earnings setup.",
    )
    article = {
        "title": "AAPL stale setup",
        "slug": "aapl-stale-setup",
        "summary": "Research only. Not investment advice.",
        "preview_body": "Research only. Not investment advice.",
        "walnut_call": "Mixed",
        "sections": [{"heading": "Executive thesis", "body_markdown": "This uses Q2 2025 instead. Research only. Not investment advice. https://www.apple.com/ca/newsroom/2026/04/apple-reports-second-quarter-results/ https://investor.apple.com/sec-filings/default.aspx " + "word " * 220}],
        "source_links": external["reviewed_sources"],
    }
    context = {
        "desired_angle": "Earnings setup",
        "research_question": "Build an AAPL earnings setup.",
        "primary": {"identity": {"symbol": "AAPL"}, "confirmation": _confirmation_bundle(74), "quote": {"price": 200}, "fundamentals": {"revenue_growth": 10}},
        "source_discovery": external["source_discovery"],
        "external_research": external,
    }

    validation = service.validate_article(article, context)

    assert validation["status"] == "failed"
    assert any(warning["code"] == "stale_year_substitution" for warning in validation["warnings"])


def test_confirmation_score_requested_requires_loaded_score():
    article = {
        "title": "MU score missing",
        "slug": "mu-score-missing",
        "summary": "Research only. Not investment advice.",
        "preview_body": "Research only. Not investment advice.",
        "sections": [{"heading": "Executive thesis", "body_markdown": "Research only. Not investment advice. https://www.sec.gov/edgar/search/#/q=MU https://www.nasdaq.com/market-activity/stocks/mu " + "word " * 220}],
        "source_links": [
            {"label": "SEC", "url": "https://www.sec.gov/edgar/search/#/q=MU", "source_type": "filing_search"},
            {"label": "Nasdaq", "url": "https://www.nasdaq.com/market-activity/stocks/mu", "source_type": "reputable_market_source"},
        ],
    }

    validation = service.validate_article(article, {"primary": {"identity": {"symbol": "MU"}, "confirmation": {}}, "include_confirmation_score": True})

    assert validation["status"] == "failed"
    assert any(warning["code"] == "confirmation_score_unavailable" for warning in validation["warnings"])


def test_confirmation_score_disabled_blocks_score_language():
    article = {
        "title": "MU score included",
        "slug": "mu-score-included",
        "summary": "Research only. Not investment advice.",
        "preview_body": "Research only. Not investment advice.",
        "sections": [{"heading": "Executive thesis", "body_markdown": "Walnut's proprietary confirmation score is 79/100. Research only. Not investment advice. https://www.sec.gov/edgar/search/#/q=MU https://www.nasdaq.com/market-activity/stocks/mu " + "word " * 220}],
        "source_links": [
            {"label": "SEC", "url": "https://www.sec.gov/edgar/search/#/q=MU", "source_type": "filing_search"},
            {"label": "Nasdaq", "url": "https://www.nasdaq.com/market-activity/stocks/mu", "source_type": "reputable_market_source"},
        ],
    }

    validation = service.validate_article(article, {"primary": {"identity": {"symbol": "MU"}, "confirmation": _confirmation_bundle(79)}, "include_confirmation_score": False})

    assert validation["status"] == "failed"
    assert any(warning["code"] == "confirmation_score_not_requested" for warning in validation["warnings"])


def test_confirmation_score_publish_filler_is_removed_when_score_requested():
    cleaned = service.sanitize_research_brief_article(
        {
            "title": "AAPL score filler",
            "slug": "aapl-score-filler",
            "subtitle": "Research only. Not investment advice.",
            "summary": "Research only. Not investment advice.",
            "preview_body": "Research only. Not investment advice.",
            "judgment": "mixed",
            "sections": [
                {
                    "key": "body",
                    "heading": "Executive thesis",
                    "body_markdown": "We do not publish our proprietary confirmation score for AAPL in this brief. Research only. Not investment advice. https://www.sec.gov/edgar/search/#/q=AAPL https://investor.example.com/ " + "word " * 220,
                }
            ],
            "source_links": [
                {"label": "SEC", "url": "https://www.sec.gov/edgar/search/#/q=AAPL", "source_type": "filing_search"},
                {"label": "IR", "url": "https://investor.example.com/", "source_type": "company"},
            ],
        },
        {"include_confirmation_score": True},
        {"primary": {"identity": {"symbol": "AAPL"}, "confirmation": _confirmation_bundle(74)}, "include_confirmation_score": True},
    )
    body = "\n\n".join(section["body_markdown"] for section in cleaned["sections"])

    assert "we do not publish our proprietary confirmation score" not in body.lower()
    assert "Our proprietary confirmation score is 74/100" in body


def test_cross_source_confirmation_validation_keeps_score_separate():
    article = {
        "title": "MU conflated",
        "slug": "mu-conflated",
        "summary": "Research only. Not investment advice.",
        "preview_body": "Research only. Not investment advice.",
        "sections": [{"heading": "Cross-source confirmations", "body_markdown": "The confirmation score is based on fundamentals and reported institutional activity. Research only. Not investment advice. https://www.sec.gov/edgar/search/#/q=MU https://www.nasdaq.com/market-activity/stocks/mu " + "word " * 220}],
        "source_links": [
            {"label": "SEC", "url": "https://www.sec.gov/edgar/search/#/q=MU", "source_type": "filing_search"},
            {"label": "Nasdaq", "url": "https://www.nasdaq.com/market-activity/stocks/mu", "source_type": "reputable_market_source"},
        ],
    }

    validation = service.validate_article(
        article,
        {
            "primary": {"identity": {"symbol": "MU"}, "confirmation": _confirmation_bundle(79)},
            "include_confirmation_score": True,
            "include_cross_source_confirmations": True,
        },
    )

    assert validation["status"] == "failed"
    assert any(warning["code"] == "confirmation_score_conflated" for warning in validation["warnings"])


def test_source_links_zero_blocks_publish_validation(tmp_path, monkeypatch):
    monkeypatch.setenv(service.STORE_ENV, str(tmp_path / "drafts.json"))
    context = {"primary": {"identity": {"symbol": "MU"}}}
    article = {
        "title": "MU source test",
        "slug": "mu-source-test",
        "summary": "Research only. Not investment advice.",
        "preview_body": "Research only. Not investment advice.",
        "sections": [{"body_markdown": "Research only. Not investment advice. " + "word " * 220}],
        "source_links": [],
    }

    validation = service.validate_article(article, context)

    assert validation["status"] == "failed"
    assert any(warning["code"] == "missing_source_links" for warning in validation["warnings"])


def test_external_research_mode_attaches_source_discovery(monkeypatch):
    def fake_get(url, **_kwargs):
        class Response:
            status_code = 200

            def json(self):
                if "company_tickers" in url:
                    return {"0": {"ticker": "MU", "title": "MICRON TECHNOLOGY INC", "cik_str": 723125}}
                return {"facts": {"us-gaap": {"Revenues": {"units": {"USD": [{"val": 1_000_000, "end": "2026-01-01", "filed": "2026-02-01", "form": "10-Q"}]}}}}}

        return Response()

    monkeypatch.setattr(service.requests, "get", fake_get)

    research = service.discover_external_research("MU", {"company_name": "Micron"}, mode="Standard")

    assert research["mode"] == "Standard"
    assert any("sec.gov" in source["url"] for source in research["reviewed_sources"])
    assert research["official_facts"]["revenue"]["value"] == 1_000_000


def test_context_filters_missing_notes_against_walnut_available_data(tmp_path, monkeypatch):
    monkeypatch.setenv(service.STORE_ENV, str(tmp_path / "drafts.json"))

    def fake_discovery(*_args, **_kwargs):
        return {
            "mode": "Standard",
            "reviewed_sources": [],
            "source_notes": [],
            "official_facts": {},
            "missing_data_notes": [
                "gross margin: Not found in reviewed sources",
                "debt: Not found in reviewed sources",
                "revenue consensus: Not found in reviewed sources",
                "eps consensus: Not found in reviewed sources",
                "reported institutional activity: Not found in reviewed sources",
                "price/volume and technicals: Not found in reviewed sources",
                "guidance: Not found in reviewed sources",
            ],
        }

    monkeypatch.setattr(service, "discover_external_research", fake_discovery)
    db = _session()
    _seed_ticker(db, "MU")
    _seed_financials_cache(db, "MU")
    _seed_event(db, "MU", "institutional_accumulation")

    context = service.assemble_research_context(db, service.validate_config(_payload(external_research_mode="Standard").model_dump()))

    assert context["data_availability"]["current price"] is True
    assert context["data_availability"]["volume"] is True
    assert context["data_availability"]["gross margin"] is True
    assert context["data_availability"]["debt"] is True
    assert context["data_availability"]["revenue consensus"] is True
    assert context["data_availability"]["eps consensus"] is True
    assert context["data_availability"]["reported institutional activity"] is True
    assert context["primary"]["financials"]["forecasts"]["nextQuarter"]["revenueEstimate"] == 101_000_000_000
    assert context["missing_data_notes"] == ["guidance: Not found in reviewed sources"]


def test_prompt_restricts_missing_limitations_to_filtered_notes():
    config = service.validate_config(_payload().model_dump())
    prompt = service._prompt(config, {"primary": {"identity": {"symbol": "MU"}}, "missing_data_notes": [], "data_availability": {"current price": True}})

    assert "Treat data_availability as authoritative" in prompt
    assert "Only list fields from missing_data_notes as missing" in prompt
    assert "experienced investor explaining the setup" in prompt
    assert "Avoid generic AI phrasing" in prompt
    assert "AI-watermark words" in prompt
    assert "bullish score requires a bullish call" in prompt


def test_validation_fails_when_draft_marks_available_data_missing():
    context = {
        "primary": {"identity": {"symbol": "MU"}},
        "data_availability": {
            "current price": True,
            "volume": True,
            "reported institutional activity": True,
        },
    }
    article = {
        "title": "MU bad limitations",
        "slug": "mu-bad-limitations",
        "summary": "Research only. Not investment advice.",
        "preview_body": "Research only. Not investment advice.",
        "sections": [
            {
                "body_markdown": (
                    "### Data limitations\n\n"
                    "Not found in reviewed sources: current MU price, volume data, and reported institutional activity. "
                    "Research only. Not investment advice. https://www.sec.gov/edgar/search/#/q=MU "
                    "https://www.nasdaq.com/market-activity/stocks/mu "
                    + "word " * 220
                )
            }
        ],
        "source_links": [
            {"label": "SEC", "url": "https://www.sec.gov/edgar/search/#/q=MU", "source_type": "filing_search"},
            {"label": "Nasdaq", "url": "https://www.nasdaq.com/market-activity/stocks/mu", "source_type": "reputable_market_source"},
        ],
    }

    validation = service.validate_article(article, context)

    assert validation["status"] == "failed"
    warning = next(item for item in validation["warnings"] if item["code"] == "available_data_marked_missing")
    assert "current price" in warning["message"]
    assert "volume" in warning["message"]
    assert "reported institutional activity" in warning["message"]


def test_sanitizer_removes_generated_missing_claim_for_available_walnut_data():
    context = {
        "data_availability": {"reported institutional activity": True},
    }
    article = {
        "title": "NBIS research brief",
        "sections": [
            {
                "heading": "Data limitations",
                "body_markdown": (
                    "Reported institutional activity was not found in reviewed sources. "
                    "The earnings release remains the primary source."
                ),
            }
        ],
    }

    cleaned = service._remove_available_data_missing_claims_from_article(article, context)

    body = cleaned["sections"][0]["body_markdown"]
    assert "institutional activity" not in body.lower()
    assert body == "The earnings release remains the primary source."


def _earnings_article(body: str, symbol: str = "AAPL") -> dict:
    return {
        "title": f"{symbol} earnings setup",
        "slug": f"{symbol.lower()}-earnings-setup",
        "subtitle": "Research only. Not investment advice.",
        "summary": "Research only. Not investment advice.",
        "preview_body": "Research only. Not investment advice.",
        "judgment": "mixed",
        "walnut_call": "Mixed",
        "confidence": "medium",
        "confirmation_score_included": False,
        "primary_ticker": symbol,
        "comparison_tickers": [],
        "category": "Technology",
        "reading_minutes": 6,
        "sections": [{"key": "call", "heading": "The call", "body_markdown": body + " " + "word " * 220}],
        "key_points": [],
        "catalysts": [],
        "risks": [],
        "watch_items": [],
        "data_freshness": [],
        "missing_data_notes": [],
        "source_links": [
            {"label": "SEC", "url": f"https://www.sec.gov/edgar/search/#/q={symbol}", "source_type": "filing_search"},
            {"label": "Investor relations", "url": "https://investor.example.com/", "source_type": "company"},
        ],
        "suggested_card": {"title": f"{symbol} earnings setup", "description": "Research only.", "judgment": "mixed", "tickers": [symbol]},
        "seo": {"title": f"{symbol} earnings setup", "description": "Research only."},
    }


def _earnings_context(symbol: str, *, missing_notes: list[str] | None = None, fundamentals: dict | None = None) -> dict:
    fundamentals = fundamentals or {
        "revenue_growth": 6.0,
        "gross_margin": 46.0,
        "operating_margin": 31.0,
        "forward_pe": 31.0,
        "volume": 50_000_000,
        "avg_volume": 48_000_000,
    }
    return {
        "desired_angle": "Earnings setup",
        "research_question": f"{symbol} earnings setup",
        "section_format": "Walnut Research Brief",
        "primary": {
            "identity": {"symbol": symbol},
            "quote": {"price": 220.0, "market_cap": 3_000_000_000_000, "as_of": "2026-07-20T20:00:00+00:00"},
            "market_state": {"price": 220.0, "volume": fundamentals.get("volume"), "avg_volume": fundamentals.get("avg_volume")},
            "fundamentals": fundamentals,
            "financials": {},
            "confirmation": {"score": 74, "direction": "bullish", "sources": {"fundamentals": {"present": True, "direction": "bullish"}}},
        },
        "data_availability": {
            "current price": True,
            "volume": True,
            "revenue": True,
            "eps consensus": False,
            "valuation data": True,
        },
        "missing_data_notes": missing_notes or [],
    }


def test_aapl_earnings_setup_keeps_bullish_score_direction_when_expensive():
    body = (
        "Walnut judgment: mixed / wait for the print\n\n"
        "Apple is a high-quality franchise with Services growth, buybacks, resilience, and institutional safety. "
        "The market is already paying a premium multiple, while AI contribution remains less measurable than peers. "
        "Research only. Not investment advice. https://www.sec.gov/edgar/search/#/q=AAPL https://investor.example.com/"
    )
    article = _earnings_article(body, "AAPL")

    cleaned = service.sanitize_research_brief_article(article, {"desired_angle": "Earnings setup"}, _earnings_context("AAPL"))
    text = "\n\n".join(section["body_markdown"] for section in cleaned["sections"])

    assert "Our call: Bullish but expensive" in text
    assert "**" not in text
    assert "Setup:" not in text
    assert "mixed / wait for the print" not in text
    assert cleaned["walnut_call"] == "Bullish but expensive"


def test_meta_earnings_setup_keeps_bullish_score_direction_with_capex_risk():
    body = (
        "Meta's ad business is strong and the core business still looks constructive. "
        "The market debate is AI capex, free cash flow conversion, and Reality Labs losses. "
        "Research only. Not investment advice. https://www.sec.gov/edgar/search/#/q=META https://investor.example.com/"
    )
    article = _earnings_article(body, "META")

    cleaned = service.sanitize_research_brief_article(article, {"desired_angle": "Earnings setup"}, _earnings_context("META"))
    text = "\n\n".join(section["body_markdown"] for section in cleaned["sections"])

    assert "Our call: Bullish with capex risk" in text
    assert "**" not in text
    assert "Setup:" not in text
    assert "free cash flow" in text


def test_earnings_setup_missing_some_data_lowers_confidence_without_forcing_wait():
    body = (
        "The business is constructive with strong revenue growth and gross margin. "
        "The main issue is valuation, not unavailable data. "
        "Research only. Not investment advice. https://www.sec.gov/edgar/search/#/q=TEST https://investor.example.com/"
    )
    article = _earnings_article(body, "TEST")

    cleaned = service.sanitize_research_brief_article(
        article,
        {"desired_angle": "Earnings setup"},
        _earnings_context("TEST", missing_notes=["TEST: EPS consensus unavailable", "TEST: guidance unavailable"]),
    )
    text = "\n\n".join(section["body_markdown"] for section in cleaned["sections"])

    assert "Our call:" in text
    assert "**" not in text
    assert "Setup:" not in text
    assert cleaned["confidence"] == "medium"


def test_earnings_setup_keeps_admin_edited_call_copy_plain_and_rounded():
    body = (
        "**Walnut call: Mixed with capex risk**\n\n"
        "Manual admin edit should survive. Revenue growth was 6.425511782832739% and gross margin was 47.862405358827935%. "
        "Walnut call: Mixed with capex risk\n\n"
        "Walnut call: Bullish with capex risk\n\n"
        "Research only. Not investment advice. https://www.sec.gov/edgar/search/#/q=AAPL https://investor.example.com/ "
        + "word " * 220
    )
    article = _earnings_article(body, "AAPL")

    cleaned = service.sanitize_research_brief_article(article, {"desired_angle": "Earnings setup"}, _earnings_context("AAPL"))
    text = "\n\n".join(section["body_markdown"] for section in cleaned["sections"])

    assert "Our call:" in text
    assert text.count("Our call:") == 1
    assert "Walnut call:" not in text
    assert "Manual admin edit should survive." in text
    assert "**" not in text
    assert "6.425511782832739%" not in text
    assert "47.862405358827935%" not in text
    assert "6.4%" in text
    assert "47.9%" in text


def test_manual_article_save_does_not_rewrite_admin_body_or_confirmation_score():
    body = (
        "Our call: Bullish with capex risk\n\n"
        "Manual admin edit should be treated as authoritative after generation. "
        "Our proprietary confirmation score is 83/100. This score is separate from the underlying data. "
        "Research only. Not investment advice. https://www.sec.gov/edgar/search/#/q=AAPL https://investor.example.com/ "
        + "word " * 220
    )
    article = _earnings_article(body, "AAPL")
    article["sections"] = [
        {
            "key": "executive_summary_tldr",
            "heading": "Executive summary (tldr)",
            "body_markdown": "Apple's core business enters fiscal Q3 from a position of strength.",
        },
        {
            "key": "the_call",
            "heading": "The call",
            "body_markdown": body,
        },
    ]
    article["walnut_call"] = "Mixed with capex risk"

    cleaned = service.sanitize_research_brief_article(
        article,
        {"desired_angle": "Earnings setup", "include_confirmation_score": False},
        _earnings_context("AAPL"),
        repair_generated_sections=False,
    )
    text = "\n\n".join(section["body_markdown"] for section in cleaned["sections"])

    assert cleaned["sections"][0]["heading"] == "Executive summary (tldr)"
    assert "Our call: Bullish with capex risk" in text
    assert "Our proprietary confirmation score is 83/100" in text
    assert "Manual admin edit should be treated as authoritative after generation." in text
    assert cleaned["walnut_call"] == "Bullish with capex risk"


def test_validation_allows_manual_confirmation_score_when_config_flag_is_stale():
    article = _earnings_article(
        "Our call: Bullish with capex risk\n\n"
        "Our proprietary confirmation score is 83/100. This score is separate from the underlying data. "
        "Research only. Not investment advice. https://www.sec.gov/edgar/search/#/q=AAPL https://investor.example.com/",
        "AAPL",
    )
    article["confirmation_score_included"] = True
    article["walnut_call"] = "Bullish with capex risk"
    context = _earnings_context("AAPL")
    context["include_confirmation_score"] = False

    validation = service.validate_article(article, context)
    codes = {warning["code"] for warning in validation["warnings"]}

    assert "confirmation_score_not_requested" not in codes
    assert "confirmation_score_missing_from_body" not in codes


def test_earnings_setup_validation_rejects_old_setup_label_as_call():
    article = _earnings_article(
        "Walnut judgment: vibes only\n\nResearch only. Not investment advice. https://www.sec.gov/edgar/search/#/q=AAPL https://investor.example.com/",
        "AAPL",
    )
    article["walnut_call"] = "Capex-risk setup"

    validation = service.validate_article(article, _earnings_context("AAPL"))

    assert validation["status"] == "failed"
    codes = {warning["code"] for warning in validation["warnings"]}
    assert {"invalid_walnut_call", "setup_label_used_as_call", "earnings_walnut_call"}.issubset(codes)


META_BAD_DRAFT_MARKDOWN = """
## Intro / hook

## Bull case
## Why the bull case still works
Meta still has a constructive fundamental setup.

## The call
## The call: wait for the print, with a constructive fundamental bias
A Q2 EPS figure of approximately $7.18 was referenced in the research request as a Public.com estimate.
META price/volume and technical data are marked available in the research configuration, but no numerical META price, volume, support/resistance, or directional technical output was supplied for publication.
The supplied materials flag a tax-benefit impact.

## Sources
## Sources
- Meta Investor Relations - financial results and company disclosures
- SEC EDGAR - Meta Platforms filings

## What to watch next
## What to watch next
Watch ad impressions, average price per ad, AI capex, and free cash flow.

## Data freshness and limitations
No reviewed consensus source was supplied. The supplied context includes reported Congressional transactions and provided comparison confirmation.
""".strip()


def test_research_brief_markdown_cleanup_repairs_meta_draft_structure_and_language():
    cleaned = service.clean_research_brief_markdown(
        META_BAD_DRAFT_MARKDOWN,
        "Reddit DD - Bull Case / Bear Case / The Data / The Call",
    )
    lowered = cleaned.lower()

    assert "## Intro / hook" not in cleaned
    assert cleaned.count("## The call") == 1
    assert cleaned.count("## Sources") == 1
    assert cleaned.count("## What to watch next") == 1
    assert "research request" not in lowered
    assert "supplied materials" not in lowered
    assert "research configuration" not in lowered
    assert "marked available in the research configuration" not in lowered
    assert "provided comparison confirmation" not in lowered
    assert "$7.18" not in cleaned
    assert "Current EPS consensus estimates were not verified in reviewed sources" in cleaned
    assert not service._markdown_structure_issues(cleaned)


def test_research_brief_copy_uses_first_person_walnut_voice():
    cleaned = service.sanitize_research_brief_copy(
        "### Walnut's take\n\n"
        "Walnut's confirmation score is 79/100. Walnut data is supportive, but Walnut's view is mixed."
    )

    assert "### Our take" in cleaned
    assert "Our confirmation score is 79/100." in cleaned
    assert "Our data is supportive" in cleaned
    assert "our view is mixed" in cleaned
    assert "Walnut's" not in cleaned
    assert "Walnut data" not in cleaned


def test_research_brief_copy_removes_stiff_ai_transitions():
    cleaned = service.sanitize_research_brief_copy(
        "It is important to note that margin quality matters. "
        "Overall, the setup is constructive but expensive. "
        "This article will examine the revenue base. "
        "Investors should monitor guidance and free cash flow."
    )

    assert "It is important to note" not in cleaned
    assert "Overall," not in cleaned
    assert "This article will examine" not in cleaned
    assert "Investors should monitor" not in cleaned
    assert "Margin quality matters." in cleaned
    assert "The setup is constructive but expensive." in cleaned
    assert "This brief examines the revenue base." in cleaned
    assert "Watch guidance and free cash flow." in cleaned


def test_research_numeric_claims_are_publication_formatted():
    cleaned = service.sanitize_research_brief_copy(
        "Revenue growth was 6.425511782832739%. Gross margin was 47.862405358827935%. "
        "Operating margin was 32.643396050876966%. The multiple was 2.394827x and revenue was $95400000000."
    )

    assert "6.4%" in cleaned
    assert "47.9%" in cleaned
    assert "32.6%" in cleaned
    assert "2.4x" in cleaned
    assert "$95.4 billion" in cleaned
    assert "6.425511782832739%" not in cleaned


def test_numeric_formatting_validation_blocks_raw_float_artifacts():
    article = {
        "title": "Bad numeric formatting",
        "slug": "bad-numeric-formatting",
        "summary": "Research only. Not investment advice.",
        "preview_body": "Research only. Not investment advice.",
        "sections": [
            {
                "heading": "Executive thesis",
                "body_markdown": "Revenue growth was 6.425511782832739%. Research only. Not investment advice. https://www.sec.gov/edgar/search/#/q=AAPL https://investor.example.com/ " + "word " * 220,
            }
        ],
        "source_links": [
            {"label": "SEC", "url": "https://www.sec.gov/edgar/search/#/q=AAPL", "source_type": "filing_search"},
            {"label": "IR", "url": "https://investor.example.com/", "source_type": "company"},
        ],
    }

    validation = service.validate_article(article, {"primary": {"identity": {"symbol": "AAPL"}}})

    assert validation["status"] == "failed"
    assert any(warning["code"] == "numeric_formatting" for warning in validation["warnings"])


def test_research_brief_article_sanitizer_marks_repaired_and_blocks_no_internal_language():
    article = {
        "title": "META earnings setup",
        "slug": "meta-earnings-setup",
        "subtitle": "Research only. Not investment advice.",
        "summary": "Research only. Not investment advice.",
        "preview_body": "Research only. Not investment advice.",
        "sections": [
            {
                "key": "body",
                "heading": "Intro / hook",
                "body_markdown": META_BAD_DRAFT_MARKDOWN
                + "\n\nResearch only. Not investment advice. https://www.sec.gov/edgar/search/#/q=META https://investor.fb.com/ "
                + "word " * 220,
            }
        ],
        "source_links": [
            {"label": "SEC EDGAR", "url": "https://www.sec.gov/edgar/search/#/q=META", "source_type": "filing_search"},
            {"label": "Meta Investor Relations", "url": "https://investor.fb.com/", "source_type": "company"},
        ],
    }

    cleaned = service.sanitize_research_brief_article(article, {"section_format": "Reddit DD - Bull Case / Bear Case / The Data / The Call"})
    body = "\n\n".join(section["body_markdown"] for section in cleaned["sections"])
    validation = service.validate_article(cleaned, {"primary": {"identity": {"symbol": "META"}}})

    assert cleaned["_copy_sanitizer_repairs"] == 1
    assert "research request" not in body.lower()
    assert "supplied context" not in body.lower()
    assert "research configuration" not in body.lower()
    assert validation["labels"]["structure"] == "repaired"
    assert validation["labels"]["internal_language"] == "repaired"
    assert validation["labels"]["missing_data_language"] == "repaired"
    assert not any(warning["code"] in {"internal_workflow_language", "markdown_structure"} for warning in validation["warnings"])


def test_validation_labels_fail_on_unsanitized_internal_and_structure_language():
    article = {
        "title": "META bad draft",
        "slug": "meta-bad-draft",
        "summary": "Research only. Not investment advice.",
        "preview_body": "Research only. Not investment advice.",
        "sections": [
            {
                "heading": "Executive thesis",
                "body_markdown": (
                    "## Intro / hook\n\n## Sources\n## Sources\n"
                    "The Q1 operating figures are drawn from the supplied research context and research configuration. "
                    "No reviewed consensus source was supplied. Research only. Not investment advice. "
                    "https://www.sec.gov/edgar/search/#/q=META https://investor.fb.com/ "
                    + "word " * 220
                ),
            }
        ],
        "source_links": [
            {"label": "SEC EDGAR", "url": "https://www.sec.gov/edgar/search/#/q=META", "source_type": "filing_search"},
            {"label": "Meta Investor Relations", "url": "https://investor.fb.com/", "source_type": "company"},
        ],
    }

    validation = service.validate_article(article, {"primary": {"identity": {"symbol": "META"}}})

    assert validation["status"] == "failed"
    assert validation["labels"]["structure"] == "failed"
    assert validation["labels"]["internal_language"] == "failed"
    assert validation["labels"]["missing_data_language"] == "failed"


def test_missing_data_and_internal_terms_fail_validation(tmp_path, monkeypatch):
    monkeypatch.setenv(service.STORE_ENV, str(tmp_path / "drafts.json"))
    context = {"primary": {"identity": {"symbol": "MU"}}}
    article = {
        "title": "MU bad wording",
        "slug": "mu-bad-wording",
        "summary": "Research only. Not investment advice.",
        "preview_body": "Research only. Not investment advice.",
        "sections": [
            {
                "body_markdown": (
                    "Gross margin: not supplied for MU. EBITDA: not supplied for MU. The provider cache says confirmation score equals fundamentals. "
                    "Research only. Not investment advice. https://www.sec.gov/edgar/search/#/q=MU https://www.nasdaq.com/market-activity/stocks/mu "
                    + "word " * 220
                )
            }
        ],
        "source_links": [
            {"label": "SEC", "url": "https://www.sec.gov/edgar/search/#/q=MU", "source_type": "filing_search"},
            {"label": "Nasdaq", "url": "https://www.nasdaq.com/market-activity/stocks/mu", "source_type": "reputable_market_source"},
        ],
    }

    validation = service.validate_article(article, context)

    codes = {warning["code"] for warning in validation["warnings"]}
    assert {"not_supplied_language", "internal_wording", "confirmation_score_blended"}.issubset(codes)


def test_numeric_formatter_rounds_raw_percentage_decimals_without_damaging_clean_values():
    cleaned = service.sanitize_research_brief_copy(
        "Revenue grew +17.123456% YoY, EPS grew 22.000000%, margin was 44.25%, and FCF yield was 2.01%."
    )

    assert "+17.1%" in cleaned
    assert "22%" in cleaned
    assert "44.25%" in cleaned
    assert "2.01%" in cleaned
    assert "17.123456%" not in cleaned


def test_generated_article_numeric_precision_is_repaired_before_validation():
    article = {
        "title": "AAPL decimal cleanup",
        "slug": "aapl-decimal-cleanup",
        "summary": "Research only. Not investment advice.",
        "preview_body": "Research only. Not investment advice.",
        "sections": [
            {
                "heading": "Executive thesis",
                "body_markdown": "Revenue grew 17.123456% while EPS rose 22.000000%. Research only. Not investment advice. https://www.sec.gov/edgar/search/#/q=AAPL https://investor.apple.com/earnings-results/default.aspx " + "word " * 220,
            }
        ],
        "source_links": [
            {"label": "SEC", "url": "https://www.sec.gov/edgar/search/#/q=AAPL", "source_type": "filing_search"},
            {"label": "Apple IR", "url": "https://investor.apple.com/earnings-results/default.aspx", "source_type": "official_company_ir"},
        ],
    }

    cleaned = service.sanitize_research_brief_article(article, {}, {"primary": {"identity": {"symbol": "AAPL"}}})
    body = "\n\n".join(section["body_markdown"] for section in cleaned["sections"])
    validation = service.validate_article(cleaned, {"primary": {"identity": {"symbol": "AAPL"}}})

    assert "17.1%" in body
    assert "22%" in body
    assert "17.123456%" not in body
    assert not any(warning["code"] == "numeric_formatting" for warning in validation["warnings"])


def test_reddit_dd_section_format_prompt_contains_required_sections():
    config = service.validate_config(_payload(section_format="Reddit DD - Issue / Risk / Data / Conclusion").model_dump())
    prompt = service._prompt(config, {"primary": {"identity": {"symbol": "MU"}}, "missing_data_notes": []})

    assert "The issue" in prompt
    assert "The risk / opportunity" in prompt
    assert "The data" in prompt
    assert "Conclusion" in prompt


def test_comparison_tickers_are_normalized_from_single_and_comma_separated_inputs():
    single = service.validate_config(_payload(comparison_tickers=["GOOGL"]).model_dump())
    comma = service.validate_config(_payload(comparison_tickers=["googl, amzn, MSFT, googl,,"]).model_dump())
    legacy = service.validate_config(_payload(comparison_ticker="googl,amzn,msft").model_dump())

    assert single["comparison_tickers"] == ["GOOGL"]
    assert single["comparison_ticker"] == "GOOGL"
    assert comma["comparison_tickers"] == ["GOOGL", "AMZN", "MSFT"]
    assert legacy["comparison_tickers"] == ["GOOGL", "AMZN", "MSFT"]


def test_comparison_tickers_reject_too_many_and_primary_duplicates():
    with pytest.raises(HTTPException) as too_many:
        service.validate_config(_payload(comparison_tickers=["A,B,C,D,E,F"]).model_dump())
    assert too_many.value.status_code == 422
    assert "limited to 5" in too_many.value.detail

    with pytest.raises(HTTPException) as duplicate:
        service.validate_config(_payload(ticker="mu", comparison_tickers=["GOOGL, MU"]).model_dump())
    assert duplicate.value.status_code == 422
    assert "Primary ticker" in duplicate.value.detail


def test_comparison_tickers_validate_each_symbol_individually(tmp_path, monkeypatch):
    monkeypatch.setenv(service.STORE_ENV, str(tmp_path / "drafts.json"))
    db = _session()
    _seed_ticker(db, "MU")
    _seed_ticker(db, "GOOGL")
    _seed_ticker(db, "AMZN")

    with pytest.raises(HTTPException) as exc:
        service.assemble_research_context(db, service.validate_config(_payload(comparison_tickers=["GOOGL, AMZN, MSFT"]).model_dump()))

    assert exc.value.status_code == 422
    assert exc.value.detail == "MSFT is not currently supported as a comparison ticker."


def test_research_prompt_receives_comparison_tickers_array(tmp_path, monkeypatch):
    monkeypatch.setenv(service.STORE_ENV, str(tmp_path / "drafts.json"))
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
    monkeypatch.setattr(service, "_start_research_brief_job_worker", lambda job_id: None)
    captured = {}

    def fake_post(*args, **kwargs):
        captured["input"] = kwargs["json"]["input"]
        return _fake_openai_response(*args, **kwargs)

    monkeypatch.setattr(service.requests, "post", fake_post)
    db = _session()
    for symbol in ["MU", "GOOGL", "AMZN", "MSFT"]:
        _seed_ticker(db, symbol)
    admin = _user(db, "admin@example.com", role="admin")

    job = admin_research_brief_generate(_payload(comparison_tickers=["googl,amzn,msft"]), _request_for_user(admin), Response(), db=db)
    service.run_research_brief_generation_job(job["job_id"], db)
    draft = service.get_research_brief_generation_job_draft(job["job_id"], db)

    assert draft["comparison_tickers"] == ["GOOGL", "AMZN", "MSFT"]
    assert draft["config"]["comparison_tickers"] == ["GOOGL", "AMZN", "MSFT"]
    assert '"comparison_tickers": [\n    "GOOGL",\n    "AMZN",\n    "MSFT"\n  ]' in captured["input"]
    assert '"comparison_ticker": "GOOGL,AMZN,MSFT"' not in captured["input"]


def test_context_marks_missing_data_without_treating_it_as_zero(tmp_path, monkeypatch):
    monkeypatch.setenv(service.STORE_ENV, str(tmp_path / "drafts.json"))
    db = _session()
    db.add(Security(symbol="TINY", name="Tiny Co", asset_class="stock", sector="Technology"))
    db.commit()

    context = service.assemble_research_context(db, service.validate_config(_payload(ticker="TINY").model_dump()))

    assert "TINY: fundamentals unavailable" in context["missing_data_notes"]
    assert context["primary"]["fundamentals"] is None


def test_context_fetches_primary_confirmation_and_current_market_state(tmp_path, monkeypatch):
    monkeypatch.setenv(service.STORE_ENV, str(tmp_path / "drafts.json"))
    calls = []

    def fake_confirmation(db, symbols, lookback_days):
        calls.append((symbols, lookback_days))
        return {"AAPL": _confirmation_bundle(72)}

    monkeypatch.setattr(service, "get_confirmation_score_bundles_for_tickers", fake_confirmation)
    db = _session()
    _seed_ticker(db, "AAPL")

    context = service.assemble_research_context(db, service.validate_config(_payload(ticker="AAPL", desired_angle="Earnings setup").model_dump()))

    assert calls == [(["AAPL"], 30)]
    assert context["primary"]["confirmation"]["score"] == 72
    assert context["primary"]["market_state"]["price"] == 125.0
    assert context["primary"]["market_state"]["volume"] == 50_000_000


def test_duplicate_static_mu_slug_blocks_publication(tmp_path, monkeypatch):
    monkeypatch.setenv(service.STORE_ENV, str(tmp_path / "drafts.json"))
    context = {"primary": {"identity": {"symbol": "MU"}}}
    article = {
        "title": "MU duplicate",
        "slug": "mu-dd",
        "summary": "Research only. Not investment advice.",
        "sections": [{"body_markdown": "Research only. Not investment advice. " + "word " * 220}],
    }

    validation = service.validate_article(article, context)

    assert validation["status"] == "failed"
    assert any(warning["code"] == "duplicate_slug" for warning in validation["warnings"])


def test_publish_unpublish_delete_lifecycle_uses_local_store(tmp_path, monkeypatch):
    monkeypatch.setenv(service.STORE_ENV, str(tmp_path / "drafts.json"))
    monkeypatch.setenv(service.MOCK_ENV, "1")
    db = _session()
    _seed_ticker(db)
    admin = _user(db, "admin@example.com", role="admin")
    draft = service.generate_research_brief(db, admin, _payload().model_dump())

    published = service.publish_draft(admin, draft["id"], confirm=True)
    assert published["status"] == "published"
    assert service.published_cards()["items"][0]["route"] == f"/research/{published['article']['slug']}"

    unpublished = service.unpublish_draft(admin, draft["id"], confirm=True)
    assert unpublished["status"] == "unpublished"

    deleted = service.delete_draft(admin, draft["id"], confirm_text="DELETE")
    assert deleted["ok"] is True


def test_republishing_edited_brief_updates_public_article_and_strips_bold_markers(tmp_path, monkeypatch):
    monkeypatch.setenv(service.STORE_ENV, str(tmp_path / "drafts.json"))
    monkeypatch.setenv(service.MOCK_ENV, "1")
    db = _session()
    _seed_ticker(db)
    admin = _user(db, "admin@example.com", role="admin")
    draft = service.generate_research_brief(db, admin, _payload().model_dump())
    published = service.publish_draft(admin, draft["id"], confirm=True)
    slug = published["article"]["slug"]

    service.unpublish_draft(admin, draft["id"], confirm=True)
    article = deepcopy(published["article"])
    article["sections"][0]["body_markdown"] = (
        "Edited **bold** article body. Research only. Not investment advice. "
        "https://www.sec.gov/edgar/search/#/q=MU https://www.nasdaq.com/market-activity/stocks/mu "
        + "word " * 220
    )
    service.update_draft(admin, draft["id"], article, status="unpublished")
    service.publish_draft(admin, draft["id"], confirm=True)

    public = service.published_article(slug)
    body = "\n\n".join(section["body_markdown"] for section in public["article"]["sections"])
    assert "Edited bold article body" in body
    assert "**" not in body


def test_saving_published_brief_keeps_public_article_updated(tmp_path, monkeypatch):
    monkeypatch.setenv(service.STORE_ENV, str(tmp_path / "drafts.json"))
    monkeypatch.setenv(service.MOCK_ENV, "1")
    db = _session()
    _seed_ticker(db)
    admin = _user(db, "admin@example.com", role="admin")
    draft = service.generate_research_brief(db, admin, _payload().model_dump())
    published = service.publish_draft(admin, draft["id"], confirm=True)
    article = deepcopy(published["article"])
    article["sections"][0]["body_markdown"] = (
        "Published edit should stay public. Research only. Not investment advice. "
        "https://www.sec.gov/edgar/search/#/q=MU https://www.nasdaq.com/market-activity/stocks/mu "
        + "word " * 220
    )

    saved = service.update_draft(admin, draft["id"], article, status="draft")
    public = service.published_article(published["article"]["slug"])

    assert saved["status"] == "published"
    assert public["status"] == "published"
    assert "Published edit should stay public" in public["article"]["sections"][0]["body_markdown"]


def test_db_backed_saving_published_brief_keeps_edited_body(tmp_path, monkeypatch):
    monkeypatch.setenv(service.STORE_ENV, str(tmp_path / "drafts.json"))
    monkeypatch.setenv(service.MOCK_ENV, "1")
    db = _session()
    _seed_ticker(db)
    admin = _user(db, "admin@example.com", role="admin")
    draft = service.generate_research_brief(db, admin, _payload().model_dump())
    published = service.publish_draft(admin, draft["id"], confirm=True, db=db)
    article = deepcopy(published["article"])
    article["sections"][0]["body_markdown"] = (
        "DB-backed published edit should stay saved. Research only. Not investment advice. "
        "https://www.sec.gov/edgar/search/#/q=MU https://www.nasdaq.com/market-activity/stocks/mu "
        + "word " * 220
    )

    saved = service.update_draft(admin, draft["id"], article, status="published", db=db)
    reloaded = service.get_draft(draft["id"], db=db)
    listed = service.list_drafts(db=db)["items"][0]

    assert saved["status"] == "published"
    assert "DB-backed published edit should stay saved" in saved["article"]["sections"][0]["body_markdown"]
    assert "DB-backed published edit should stay saved" in reloaded["article"]["sections"][0]["body_markdown"]
    assert "DB-backed published edit should stay saved" in listed["article"]["sections"][0]["body_markdown"]


def test_db_backed_save_persists_edited_comparison_tickers(tmp_path, monkeypatch):
    monkeypatch.setenv(service.STORE_ENV, str(tmp_path / "drafts.json"))
    monkeypatch.setenv(service.MOCK_ENV, "1")
    db = _session()
    for symbol in ["MU", "GOOGL", "AMZN", "MSFT", "NVDA", "AMAT", "ASML"]:
        _seed_ticker(db, symbol)
    admin = _user(db, "admin@example.com", role="admin")
    draft = service.generate_research_brief(db, admin, _payload(comparison_tickers=["GOOGL,AMZN,MSFT"]).model_dump())
    article = deepcopy(draft["article"])
    article["comparison_tickers"] = ["GOOGL", "AMZN", "MSFT"]
    article["suggested_card"]["tickers"] = ["MU", "GOOGL", "AMZN", "MSFT"]
    config_patch = {**draft["config"], "comparison_ticker": "NVDA", "comparison_tickers": ["NVDA", "AMAT", "ASML"]}

    saved = service.update_draft(admin, draft["id"], article, db=db, config_patch=config_patch)
    reloaded = service.get_draft(draft["id"], db=db)

    assert saved["comparison_tickers"] == ["NVDA", "AMAT", "ASML"]
    assert saved["config"]["comparison_tickers"] == ["NVDA", "AMAT", "ASML"]
    assert saved["article"]["comparison_tickers"] == ["NVDA", "AMAT", "ASML"]
    assert saved["article"]["suggested_card"]["tickers"] == ["MU", "NVDA", "AMAT", "ASML"]
    assert reloaded["comparison_tickers"] == ["NVDA", "AMAT", "ASML"]
    assert reloaded["config"]["comparison_tickers"] == ["NVDA", "AMAT", "ASML"]


def test_premium_preview_uses_section_count_and_strips_hidden_sections():
    article = {
        "premium_required": True,
        "required_plan": "premium",
        "preview_section_count": 1,
        "sections": [
            {"key": "intro", "heading": "Intro", "body_markdown": "Public intro."},
            {"key": "base", "heading": "Base Case", "body_markdown": "Gated base case."},
        ],
        "key_points": [],
        "catalysts": ["Hidden catalyst"],
        "risks": ["Hidden risk"],
        "watch_items": ["Hidden watch item"],
        "source_links": [{"label": "Hidden", "url": "https://example.com", "source_type": "source"}],
    }

    preview = service._preview_research_article(article)
    body = "\n\n".join(section["body_markdown"] for section in preview["sections"])

    assert [section["heading"] for section in preview["sections"]] == ["Intro"]
    assert "Public intro" in body
    assert "Gated base case" not in body
    assert preview["catalysts"] == []
    assert preview["risks"] == []
    assert preview["watch_items"] == []
    assert preview["source_links"] == []


def test_premium_preview_marker_overrides_count_and_does_not_leak_marker_or_hidden_text():
    article = {
        "premium_required": True,
        "required_plan": "premium",
        "preview_section_count": 3,
        "sections": [
            {"key": "intro", "heading": "Intro", "body_markdown": "Public intro."},
            {
                "key": "method",
                "heading": "Method",
                "body_markdown": "Public method.\n\n<!-- walnut:paywall -->\n\nHidden base case.",
            },
            {"key": "implications", "heading": "Investment Implications", "body_markdown": "Hidden implications."},
        ],
        "key_points": [],
        "catalysts": [],
        "risks": [],
        "watch_items": [],
        "source_links": [],
    }

    preview = service._preview_research_article(article)
    preview_body = "\n\n".join(section["body_markdown"] for section in preview["sections"])
    full = service._article_without_paywall_markers(article)
    full_body = "\n\n".join(section["body_markdown"] for section in full["sections"])

    assert [section["heading"] for section in preview["sections"]] == ["Intro", "Method"]
    assert "Public method" in preview_body
    assert "Hidden base case" not in preview_body
    assert "Hidden implications" not in preview_body
    assert "walnut:paywall" not in preview_body
    assert "Hidden base case" in full_body
    assert "walnut:paywall" not in full_body
