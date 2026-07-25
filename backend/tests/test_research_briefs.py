from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest
from fastapi import HTTPException, Response
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from starlette.requests import Request

from app.auth import SESSION_COOKIE_NAME, sign_session_payload
from app.db import Base
from app.models import FundamentalsCache, QuoteCache, Security, TickerMeta, UserAccount
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
            revenue_growth=42.5,
            eps_growth=18.0,
            gross_margin=61.2,
            forward_pe=24.0,
            debt_to_equity=0.3,
        )
    )
    db.add(QuoteCache(symbol=symbol, price=125.0, asof_ts=datetime(2026, 7, 20, 16, 0), market_cap=100_000_000_000))
    db.commit()


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
                            "The Walnut confirmation score is separate from the underlying fundamentals and tape. "
                            "Research only. Not investment advice. Sources: https://www.sec.gov/edgar/search/#/q=MU and https://www.nasdaq.com/market-activity/stocks/mu. "
                            + " ".join(["Evidence remains specific."] * 120)
                        ),
                    }
                ],
                "key_points": ["Use underlying data before confirmation score."],
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
