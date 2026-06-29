from __future__ import annotations

from datetime import datetime, timezone

import pytest
from fastapi import HTTPException
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from starlette.requests import Request

from app.auth import SESSION_COOKIE_NAME, sign_session_payload
from app.db import Base
from app.models import AppSetting, FeatureGate, QuoteCache, SavedScreen, SavedScreenSnapshot, TickerMeta, UserAccount
from app.routers.saved_screens import (
    SavedScreenCreatePayload,
    create_saved_screen,
    list_saved_screen_events,
    refresh_saved_screens_monitoring,
)
from app.routers.screener import stock_screener, stock_screener_export
from app.services.confirmation_score import confirmation_score_bundle_from_source_contexts
from app.services.saved_screen_monitoring import refresh_due_saved_screen_monitoring


@pytest.fixture(autouse=True)
def _allow_provider_screener_fallback(monkeypatch):
    monkeypatch.setenv("SCREENER_PROVIDER_FALLBACK", "1")
    monkeypatch.setenv("SCREENER_RESPONSE_CACHE_TTL_SECONDS", "0")


def _session():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    Base.metadata.create_all(engine)
    return SessionLocal()


def _request(tier: str | None = None) -> Request:
    headers = []
    if tier:
        headers.append((b"x-ct-entitlement-tier", tier.encode("utf-8")))
    return Request({"type": "http", "method": "GET", "path": "/", "headers": headers, "query_string": b""})


def _user(db: Session, email: str, *, tier: str = "free", role: str = "user") -> UserAccount:
    user = UserAccount(email=email, role=role, entitlement_tier=tier)
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


def _request_for_user(user: UserAccount) -> Request:
    token = sign_session_payload({"uid": user.id, "email": user.email})
    return Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/",
            "headers": [(b"cookie", f"{SESSION_COOKIE_NAME}={token}".encode())],
            "query_string": b"",
        }
    )


def _fake_screener_row(symbol: str) -> dict[str, object]:
    return {
        "symbol": symbol,
        "companyName": f"{symbol} Corp",
        "sector": "Technology",
        "industry": "Software - Infrastructure",
        "marketCap": 20_000_000_000,
        "price": 42,
        "volume": 1_500_000,
        "beta": 1.1,
        "country": "US",
        "exchangeShortName": "NASDAQ",
    }


def _full_source_bundle(symbol: str) -> dict:
    return confirmation_score_bundle_from_source_contexts(
        symbol,
        source_contexts={
            "congress": {
                "status": "active",
                "direction": "bullish",
                "buy_count": 2,
                "sell_count": 0,
                "net_flow": 350_000,
                "title": "Congress buying active",
            },
            "insiders": {
                "status": "active",
                "direction": "bullish",
                "buy_count": 1,
                "sell_count": 0,
                "net_flow": 125_000,
                "title": "Insider buying active",
            },
            "signals": {
                "status": "active",
                "direction": "bullish",
                "recent_count": 1,
                "latest_score": 82,
                "title": "Signal conviction active",
            },
            "price_volume": {
                "status": "active",
                "direction": "bullish",
                "score": 74,
                "price_points": 45,
                "latest_volume": 1_500_000,
                "title": "Bullish tape confirmation",
            },
                "government_contracts": {
                    "status": "active",
                    "contract_count": 2,
                    "contract_value": 18_000_000,
                    "latest_date": "2026-06-20",
                    "title": "Government contracts active",
                },
            "options_flow": {
                "status": "active",
                "direction": "bullish",
                "score": 91,
                "freshness_days": 1,
                "title": "Options flow confirming",
            },
            "institutional_activity": {
                "status": "active",
                "direction": "bullish",
                "freshness_days": 2,
                "title": "Institutional activity active",
            },
        },
    )


def _fake_confirmation_context(db, symbols, **kwargs):
    return {
        "bundles": {symbol: _full_source_bundle(symbol) for symbol in symbols},
        "government_contracts_summaries": {
            symbol: {
                "status": "ok",
                "active": True,
                "contract_count": 2,
                "total_award_amount": 18_000_000,
                "largest_award_amount": 12_000_000,
                "latest_award_date": "2026-06-20",
                "top_agency": "NASA",
                "direction": "bullish",
                "score_contribution": 10,
            }
            for symbol in symbols
        },
        "options_flow_summaries": {
            symbol: {
                "active": True,
                "score": 91,
                "direction": "bullish",
                "intensity": "high",
                "call_put_premium_ratio": 2.4,
                "total_premium": 3_250_000,
                "latest_flow_date": "2026-06-20",
                "source": "massive",
                "status": "ok",
            }
            for symbol in symbols
        },
        "institutional_activity_summaries": {
            symbol: {
                "active": True,
                "direction": "bullish",
                "net_activity": 4_500_000,
                "institution_count": 4,
                "total_value": 9_000_000,
                "latest_activity_date": "2026-06-20",
                "source": "fmp",
                "status": "ok",
            }
            for symbol in symbols
        },
        "overlay_availability": {
            "government_contracts": {"enabled": True, "status": "ok", "filterable": True},
            "options_flow": {"enabled": True, "status": "ok", "filterable": True},
            "institutional_activity": {"enabled": True, "status": "ok", "filterable": True},
        },
    }


def _install_overlay_screener_fixtures(monkeypatch):
    monkeypatch.setattr(
        "app.services.screener.fetch_company_screener",
        lambda *, filters, limit: [_fake_screener_row("OPT")],
    )
    monkeypatch.setattr("app.services.screener.build_confirmation_score_context", _fake_confirmation_context)


def test_free_screener_intelligence_filters_require_premium(monkeypatch):
    monkeypatch.setenv("CT_DEFAULT_TIER", "free")
    monkeypatch.setenv("CT_ALLOW_ENTITLEMENT_HEADER", "1")
    monkeypatch.setattr(
        "app.services.screener.fetch_company_screener",
        lambda *, filters, limit: [_fake_screener_row("AAA")],
    )
    db = _session()
    try:
        try:
            stock_screener(request=_request("free"), db=db, confirmation_score_min=50)
        except HTTPException as exc:
            assert exc.status_code == 402
            assert exc.detail["feature"] == "screener_intelligence"
        else:
            raise AssertionError("Expected premium-required response")
    finally:
        db.close()


def test_free_screener_basic_access_is_capped_and_redacted(monkeypatch):
    monkeypatch.setenv("CT_DEFAULT_TIER", "free")
    monkeypatch.setenv("CT_ALLOW_ENTITLEMENT_HEADER", "1")
    monkeypatch.setattr(
        "app.services.screener.fetch_company_screener",
        lambda *, filters, limit: [_fake_screener_row(f"T{idx:03d}") for idx in range(60)],
    )
    db = _session()
    try:
        response = stock_screener(request=_request("free"), db=db, page_size=50)
        assert response["result_cap"] == 25
        assert response["returned"] == 25
        assert response["access"]["intelligence_locked"] is True
        assert isinstance(response["items"][0]["confirmation"]["score"], int)
        assert response["items"][0]["confirmation"]["score"] >= 0
    finally:
        db.close()


def test_free_screener_total_available_counts_before_result_cap(monkeypatch):
    monkeypatch.setenv("CT_DEFAULT_TIER", "free")
    monkeypatch.setenv("CT_ALLOW_ENTITLEMENT_HEADER", "1")
    monkeypatch.setattr(
        "app.services.screener.fetch_company_screener",
        lambda *, filters, limit: (_ for _ in ()).throw(AssertionError("cache-backed screener should not call provider")),
    )
    db = _session()
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    try:
        for index in range(30):
            symbol = f"CAP{index:02d}"
            db.add(
                TickerMeta(
                    symbol=symbol,
                    company_name=f"Cap Test {index}",
                    exchange="NASDAQ",
                    sector="Technology",
                    industry="Software - Infrastructure",
                    country="US",
                )
            )
            db.add(QuoteCache(symbol=symbol, price=25 + index, asof_ts=now))
        db.commit()

        response = stock_screener(
            request=_request("free"),
            db=db,
            page_size=25,
            sector="Technology",
            price_min=1,
            price_max=100,
        )

        assert response["result_cap"] == 25
        assert response["returned"] == 25
        assert response["total_available"] == 30
        assert response["has_next"] is False
    finally:
        db.close()


def test_free_and_premium_screener_redact_options_flow_but_keep_confirmation_score(monkeypatch):
    monkeypatch.setenv("CT_DEFAULT_TIER", "free")
    monkeypatch.setenv("CT_ALLOW_ENTITLEMENT_HEADER", "1")
    _install_overlay_screener_fixtures(monkeypatch)
    db = _session()
    try:
        for tier in ("free", "premium"):
            response = stock_screener(request=_request(tier), db=db, page_size=5)
            row = response["items"][0]
            assert row["confirmation"]["score"] > 0
            assert row["options_flow_status"] == "pro_locked"
            assert row["options_flow_active"] is None
            assert row["options_flow_score"] is None
            assert row["options_flow_total_premium"] is None
            assert row["options_flow_locked"] is True
            assert response["overlay_availability"]["options_flow"]["status"] == "pro_locked"
            assert response["overlay_availability"]["options_flow"]["filterable"] is False
    finally:
        db.close()


def test_pro_screener_returns_options_flow_when_provider_data_exists(monkeypatch):
    monkeypatch.setenv("CT_DEFAULT_TIER", "free")
    monkeypatch.setenv("CT_ALLOW_ENTITLEMENT_HEADER", "1")
    _install_overlay_screener_fixtures(monkeypatch)
    db = _session()
    try:
        response = stock_screener(request=_request("pro"), db=db, page_size=5)
        row = response["items"][0]
        assert row["confirmation"]["score"] > 0
        assert row["options_flow_status"] == "ok"
        assert row["options_flow_active"] is True
        assert row["options_flow_score"] == 91
        assert row["options_flow_total_premium"] == 3_250_000
        assert "options_flow_locked" not in row
        assert response["overlay_availability"]["options_flow"]["status"] == "ok"
        assert response["overlay_availability"]["options_flow"]["filterable"] is True
    finally:
        db.close()


def test_free_and_premium_screener_redact_institutional_activity_but_keep_confirmation_score(monkeypatch):
    monkeypatch.setenv("CT_DEFAULT_TIER", "free")
    monkeypatch.setenv("CT_ALLOW_ENTITLEMENT_HEADER", "1")
    _install_overlay_screener_fixtures(monkeypatch)
    db = _session()
    try:
        for tier in ("free", "premium"):
            response = stock_screener(request=_request(tier), db=db, page_size=5)
            row = response["items"][0]
            assert row["confirmation"]["score"] > 0
            assert row["institutional_activity_status"] == "pro_locked"
            assert row["institutional_activity_active"] is None
            assert row["institutional_activity_net_activity"] is None
            assert row["institutional_activity_institution_count"] is None
            assert row["institutional_activity_locked"] is True
            assert response["overlay_availability"]["institutional_activity"]["status"] == "pro_locked"
            assert response["overlay_availability"]["institutional_activity"]["filterable"] is False
    finally:
        db.close()


def test_pro_screener_returns_institutional_activity_when_provider_data_exists(monkeypatch):
    monkeypatch.setenv("CT_DEFAULT_TIER", "free")
    monkeypatch.setenv("CT_ALLOW_ENTITLEMENT_HEADER", "1")
    _install_overlay_screener_fixtures(monkeypatch)
    db = _session()
    try:
        response = stock_screener(request=_request("pro"), db=db, page_size=5)
        row = response["items"][0]
        assert row["confirmation"]["score"] > 0
        assert row["institutional_activity_status"] == "ok"
        assert row["institutional_activity_active"] is True
        assert row["institutional_activity_net_activity"] == 4_500_000
        assert row["institutional_activity_institution_count"] == 4
        assert "institutional_activity_locked" not in row
        assert response["overlay_availability"]["institutional_activity"]["status"] == "ok"
        assert response["overlay_availability"]["institutional_activity"]["filterable"] is True
    finally:
        db.close()


def test_admin_screener_returns_signal_options_and_institutional_overlays(monkeypatch):
    monkeypatch.setenv("CT_DEFAULT_TIER", "free")
    _install_overlay_screener_fixtures(monkeypatch)
    db = _session()
    try:
        admin = _user(db, "admin-screener@example.com", tier="free", role="admin")

        response = stock_screener(
            request=_request_for_user(admin),
            db=db,
            page_size=5,
            confirmation_score_min=1,
            options_flow_active=True,
            options_flow_min_score=50,
            institutional_activity_active=True,
            institutional_activity_min_value=1,
        )
        row = response["items"][0]

        assert response["access"]["tier"] == "admin"
        assert response["access"]["intelligence_locked"] is False
        assert response["access"]["options_flow_locked"] is False
        assert response["access"]["institutional_activity_locked"] is False
        assert response["overlay_availability"]["options_flow"]["status"] == "ok"
        assert response["overlay_availability"]["institutional_activity"]["status"] == "ok"
        assert row["confirmation"]["score"] > 0
        assert row["options_flow_status"] == "ok"
        assert row["options_flow_active"] is True
        assert row["institutional_activity_status"] == "ok"
        assert row["institutional_activity_active"] is True
        assert "options_flow_locked" not in row
        assert "institutional_activity_locked" not in row
    finally:
        db.close()


def test_screener_accepts_small_page_size_with_plan_caps(monkeypatch):
    monkeypatch.setenv("CT_DEFAULT_TIER", "free")
    monkeypatch.setenv("CT_ALLOW_ENTITLEMENT_HEADER", "1")
    monkeypatch.setattr(
        "app.services.screener.fetch_company_screener",
        lambda *, filters, limit: [_fake_screener_row(f"T{idx:03d}") for idx in range(12)],
    )
    db = _session()
    try:
        response = stock_screener(request=_request("free"), db=db, page_size=5)
        assert response["page_size"] == 5
        assert response["returned"] == 5
        assert response["has_next"] is True
    finally:
        db.close()


def test_screener_export_lock_message_uses_configured_required_plan(monkeypatch):
    monkeypatch.setenv("CT_DEFAULT_TIER", "free")
    monkeypatch.setenv("CT_ALLOW_ENTITLEMENT_HEADER", "1")
    monkeypatch.setattr(
        "app.services.screener.fetch_company_screener",
        lambda *, filters, limit: [_fake_screener_row("AAA")],
    )
    db = _session()
    try:
        db.add(FeatureGate(feature_key="screener_csv_export", required_tier="pro", description="Export screener results."))
        db.commit()
        try:
            stock_screener_export(request=_request("premium"), db=db)
        except HTTPException as exc:
            assert exc.status_code == 402
            assert exc.detail["feature"] == "screener_csv_export"
            assert exc.detail["message"] == "CSV export is a Pro feature."
        else:
            raise AssertionError("Expected pro-required response")

        response = stock_screener(request=_request("premium"), db=db, page_size=5)
        assert response["access"]["csv_export_locked"] is True
        assert response["access"]["csv_export_required_plan"] == "pro"

        export_response = stock_screener_export(request=_request("pro"), db=db, filename_prefix=None)
        assert export_response.headers["x-screener-exported-rows"] == "1"
    finally:
        db.close()


def test_saved_screen_limit_uses_screener_saved_screens(monkeypatch):
    monkeypatch.setenv("CT_DEFAULT_TIER", "free")
    db = _session()
    try:
        user = _user(db, "free@example.com", tier="free")
        db.add_all(
            [
                AppSetting(key="saved_screens_free_limit", value="3"),
                AppSetting(key="saved_views_free_limit", value="99"),
                SavedScreen(user_id=user.id, name="Existing 1", params_json="{}"),
                SavedScreen(user_id=user.id, name="Existing 2", params_json="{}"),
                SavedScreen(user_id=user.id, name="Existing 3", params_json="{}"),
            ]
        )
        db.commit()

        try:
            create_saved_screen(
                SavedScreenCreatePayload(name="Overflow", params={"sector": "Technology"}),
                _request_for_user(user),
                db,
            )
        except HTTPException as exc:
            assert exc.status_code == 402
            assert exc.detail["feature"] == "screener_saved_screens"
        else:
            raise AssertionError("Expected premium-required response")
    finally:
        db.close()


def test_saved_screen_monitoring_endpoints_are_premium_only(monkeypatch):
    monkeypatch.setenv("CT_DEFAULT_TIER", "free")
    db = _session()
    try:
        user = _user(db, "free-monitoring@example.com", tier="free")
        db.add(SavedScreen(user_id=user.id, name="Core", params_json="{}"))
        db.commit()

        for fn in (list_saved_screen_events, refresh_saved_screens_monitoring):
            try:
                fn(request=_request_for_user(user), db=db)
            except HTTPException as exc:
                assert exc.status_code == 402
                assert exc.detail["feature"] == "screener_monitoring"
            else:
                raise AssertionError("Expected premium-required response")
    finally:
        db.close()


def test_background_saved_screen_monitoring_skips_free_users(monkeypatch):
    monkeypatch.setenv("CT_DEFAULT_TIER", "free")
    monkeypatch.setattr(
        "app.services.saved_screen_monitoring.build_screener_rows",
        lambda *_args, **_kwargs: [
            {
                "symbol": "AAPL",
                "confirmation": {
                    "score": 64,
                    "band": "strong",
                    "direction": "bullish",
                    "status": "2-source bullish",
                    "source_count": 2,
                },
                "why_now": {"state": "strong"},
            }
        ],
    )
    db = _session()
    try:
        free_user = _user(db, "free-bg@example.com", tier="free")
        premium_user = _user(db, "premium-bg@example.com", tier="premium")
        db.add_all(
            [
                SavedScreen(user_id=free_user.id, name="Free screen", params_json="{}"),
                SavedScreen(user_id=premium_user.id, name="Premium screen", params_json="{}"),
            ]
        )
        db.commit()

        result = refresh_due_saved_screen_monitoring(db)
        db.commit()

        assert result["refreshed"] == 1
        assert db.query(SavedScreenSnapshot).count() == 1
    finally:
        db.close()
