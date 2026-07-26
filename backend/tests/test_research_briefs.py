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
                            "Walnut data should be read separately from the underlying fundamentals and tape. "
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


def test_model_options_default_to_luna_terra_sol(monkeypatch):
    monkeypatch.delenv(service.RESEARCH_BRIEF_MODEL_DEFAULT, raising=False)
    monkeypatch.delenv(service.RESEARCH_BRIEF_MODEL_OPTIONS, raising=False)

    assert service.research_brief_model_options(None) == ["gpt-5.6-luna", "gpt-5.6-terra", "gpt-5.6-sol"]
    assert service.research_brief_model(None) == "gpt-5.6-terra"
    assert service.research_brief_model_labels(None)["gpt-5.6-luna"] == "GPT-5.6 Luna"


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
    assert "Our proprietary confirmation score is 79/100" in body
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
    assert "professional but human analyst voice" in prompt
    assert "Avoid generic AI phrasing" in prompt


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


def test_aapl_earnings_setup_uses_expensive_defensive_not_wait():
    body = (
        "Walnut judgment: mixed / wait for the print\n\n"
        "Apple is a high-quality franchise with Services growth, buybacks, resilience, and institutional safety. "
        "The market is already paying a premium multiple, while AI contribution remains less measurable than peers. "
        "Research only. Not investment advice. https://www.sec.gov/edgar/search/#/q=AAPL https://investor.example.com/"
    )
    article = _earnings_article(body, "AAPL")

    cleaned = service.sanitize_research_brief_article(article, {"desired_angle": "Earnings setup"}, _earnings_context("AAPL"))
    text = "\n\n".join(section["body_markdown"] for section in cleaned["sections"])

    assert "Our call: Neutral but expensive" in text
    assert "**" not in text
    assert "Setup:" not in text
    assert "mixed / wait for the print" not in text
    assert cleaned["walnut_call"] == "Neutral but expensive"


def test_meta_earnings_setup_uses_capex_risk_when_core_ads_are_strong():
    body = (
        "Meta's ad business is strong and the core business still looks constructive. "
        "The market debate is AI capex, free cash flow conversion, and Reality Labs losses. "
        "Research only. Not investment advice. https://www.sec.gov/edgar/search/#/q=META https://investor.example.com/"
    )
    article = _earnings_article(body, "META")

    cleaned = service.sanitize_research_brief_article(article, {"desired_angle": "Earnings setup"}, _earnings_context("META"))
    text = "\n\n".join(section["body_markdown"] for section in cleaned["sections"])

    assert "Our call: Mixed with capex risk" in text
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
