from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from starlette.requests import Request
from fastapi import HTTPException

from app.auth import SESSION_COOKIE_NAME, sign_session_payload
from app.db import Base, ensure_reddit_ads_assistant_schema
from app.models import UserAccount
from app.routers.reddit_ads_assistant import (
    RedditAdDraftPayload,
    admin_reddit_ads_approve_draft,
    admin_reddit_ads_create_draft,
    admin_reddit_ads_drafts,
    admin_reddit_ads_extension_token,
    admin_reddit_ads_regenerate_draft,
    extension_reddit_ads_drafts,
    extension_reddit_ads_fill_action,
    ExtensionFillActionPayload,
)
from app.services import reddit_ads_assistant as service


def _session():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    ensure_reddit_ads_assistant_schema(engine)
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    return Session()


def _user(db, email: str, *, role: str = "user") -> UserAccount:
    user = UserAccount(email=email, role=role, entitlement_tier="pro")
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


def _request_for_user(user: UserAccount) -> Request:
    token = sign_session_payload({"uid": user.id, "email": user.email})
    return Request({"type": "http", "method": "POST", "path": "/", "headers": [(b"cookie", f"{SESSION_COOKIE_NAME}={token}".encode())]})


def _extension_request(token: str) -> Request:
    return Request({"type": "http", "method": "GET", "path": "/", "headers": [(b"authorization", f"Bearer {token}".encode())]})


def _payload(**overrides) -> RedditAdDraftPayload:
    payload = {
        "campaign_objective": "Premium subscriptions",
        "audience": "AI investors",
        "geography": "United States",
        "product_angle": "See where multiple sources of evidence agree",
        "plan": "Premium",
        "tone": "Official Walnut",
        "destination": "Ticker page",
        "destination_url": "https://walnutmarkets.com/ticker/NVDA",
        "ticker_symbols": ["NVDA", "MU"],
        "research_urls": ["https://walnutmarkets.com/research/ai-earnings-dd"],
        "generate": True,
    }
    payload.update(overrides)
    return RedditAdDraftPayload(**payload)


def _fake_generation(_db, inputs):
    return (
        {
            "headline": f"{inputs['plan']} evidence for {', '.join(inputs['ticker_symbols'])}",
            "primary_text": "Find stronger setups with price, filings, catalysts, risks, and confirmation score kept separate.",
            "short_description": "Make better stock decisions.",
            "cta": "Research NVDA",
            "destination_url": inputs["destination_url"],
            "suggested_subreddit_interests": ["AI investors", "semiconductors"],
            "suggested_image_concept": "Approved Walnut ticker screenshot using the official logo asset.",
            "optional_disclosure": "Investing involves risk.",
            "variations": [
                {"headline": "Variation A", "primary_text": "Hook, insight, judgment, question.", "cta": "Open Walnut"},
                {"headline": "Variation B", "primary_text": "Hook, insight, judgment, question.", "cta": "Research"},
                {"headline": "Variation C", "primary_text": "Hook, insight, judgment, question.", "cta": "Compare"},
            ],
        },
        {"ok": True},
    )


def test_reddit_ads_drafts_require_admin():
    db = _session()
    user = _user(db, "user@example.com")
    try:
        admin_reddit_ads_drafts(request=_request_for_user(user), db=db)
    except HTTPException as exc:
        assert exc.status_code == 403
    else:
        raise AssertionError("Expected non-admin Reddit Ads Assistant access to be rejected.")


def test_reddit_ads_generation_preserves_tone_plan_and_multiple_tickers(monkeypatch):
    db = _session()
    admin = _user(db, "admin@example.com", role="admin")
    monkeypatch.setattr(service, "generate_ad_copy", _fake_generation)

    draft = admin_reddit_ads_create_draft(_payload(tone="ValueInvesting", plan="Pro"), _request_for_user(admin), db)

    assert draft["tone"] == "ValueInvesting"
    assert draft["plan"] == "Pro"
    assert draft["ticker_symbols"] == ["NVDA", "MU"]
    assert draft["final_draft"]["headline"] == "Pro evidence for NVDA, MU"
    assert len(draft["final_draft"]["variations"]) == 3


def test_reddit_ads_url_allowlist_and_custom_url():
    allowed = service.validate_destination_url("https://walnutmarkets.com/pricing", custom_selected=False)
    assert allowed == "https://walnutmarkets.com/pricing"
    try:
        service.validate_destination_url("https://example.com/ad", custom_selected=False)
    except HTTPException as exc:
        assert exc.status_code == 422
    else:
        raise AssertionError("Expected non-Walnut URL to be rejected.")
    assert service.validate_destination_url("https://example.com/ad", custom_selected=True) == "https://example.com/ad"


def test_reddit_ads_claim_warning_blocks_approval(monkeypatch):
    db = _session()
    admin = _user(db, "admin@example.com", role="admin")

    def risky_generation(_db, inputs):
        generated, raw = _fake_generation(_db, inputs)
        generated["primary_text"] = "Guaranteed returns for careful investors."
        return generated, raw

    monkeypatch.setattr(service, "generate_ad_copy", risky_generation)
    draft = admin_reddit_ads_create_draft(_payload(), _request_for_user(admin), db)

    assert any(warning["status"] == "Claim requires evidence" for warning in draft["compliance_warnings"])
    try:
        admin_reddit_ads_approve_draft(draft["id"], _request_for_user(admin), db)
    except HTTPException as exc:
        assert exc.status_code == 422
    else:
        raise AssertionError("Expected risky claim to block approval.")


def test_reddit_ads_approval_and_extension_auth(monkeypatch):
    db = _session()
    admin = _user(db, "admin@example.com", role="admin")
    monkeypatch.setattr(service, "generate_ad_copy", _fake_generation)

    draft = admin_reddit_ads_create_draft(_payload(), _request_for_user(admin), db)
    approved = admin_reddit_ads_approve_draft(draft["id"], _request_for_user(admin), db)
    token = admin_reddit_ads_extension_token(_request_for_user(admin), db)["token"]
    extension_payload = extension_reddit_ads_drafts(_extension_request(token), db)

    assert approved["status"] == "approved"
    assert extension_payload["items"][0]["id"] == draft["id"]
    assert extension_payload["items"][0]["final_draft"]["cta"] == "Research NVDA"


def test_reddit_ads_regenerate_clears_approval(monkeypatch):
    db = _session()
    admin = _user(db, "admin@example.com", role="admin")
    monkeypatch.setattr(service, "generate_ad_copy", _fake_generation)
    draft = admin_reddit_ads_create_draft(_payload(), _request_for_user(admin), db)
    approved = admin_reddit_ads_approve_draft(draft["id"], _request_for_user(admin), db)
    assert approved["status"] == "approved"

    regenerated = admin_reddit_ads_regenerate_draft(draft["id"], _request_for_user(admin), db)

    assert regenerated["status"] == "draft"
    assert regenerated["approved_at"] is None
    assert regenerated["approver_email"] is None


def test_reddit_ads_extension_fill_action_never_submits(monkeypatch):
    db = _session()
    admin = _user(db, "admin@example.com", role="admin")
    monkeypatch.setattr(service, "generate_ad_copy", _fake_generation)
    draft = admin_reddit_ads_create_draft(_payload(), _request_for_user(admin), db)
    admin_reddit_ads_approve_draft(draft["id"], _request_for_user(admin), db)
    token = admin_reddit_ads_extension_token(_request_for_user(admin), db)["token"]

    result = extension_reddit_ads_fill_action(
        draft["id"],
        ExtensionFillActionPayload(fields=["headline", "primaryText", "destinationUrl"]),
        _extension_request(token),
        db,
    )

    assert result["ok"] is True
    assert result["submitted"] is False
    assert result["fields"] == ["headline", "primaryText", "destinationUrl"]
