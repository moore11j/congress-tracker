from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest
from fastapi import HTTPException
from sqlalchemy import create_engine
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
                            "Research only. Not investment advice. "
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
        admin_research_brief_generate(_payload(), _request_for_user(user), db)

    assert exc.value.status_code == 403


def test_research_brief_generation_uses_responses_and_saves_draft(tmp_path, monkeypatch):
    monkeypatch.setenv(service.STORE_ENV, str(tmp_path / "drafts.json"))
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
    monkeypatch.setattr(service.requests, "post", _fake_openai_response)
    db = _session()
    _seed_ticker(db)
    admin = _user(db, "admin@example.com", role="admin")

    draft = admin_research_brief_generate(_payload(), _request_for_user(admin), db)

    assert draft["status"] == "draft"
    assert draft["article"]["slug"] == "mu-generated-test"
    assert draft["validation"]["status"] == "passed"
    saved = service.list_drafts()["items"]
    assert saved[0]["id"] == draft["id"]


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
