from __future__ import annotations

from fastapi import HTTPException
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from starlette.requests import Request

from app.auth import sign_session_payload
from app.db import Base
from app.models import SavedScreen, SavedScreenSnapshot, UserAccount
from app.routers.saved_screens import (
    SavedScreenCreatePayload,
    create_saved_screen,
    list_saved_screen_events,
    refresh_saved_screens_monitoring,
)
from app.routers.screener import stock_screener, stock_screener_export
from app.services.saved_screen_monitoring import refresh_due_saved_screen_monitoring


def _session():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    Base.metadata.create_all(engine)
    return SessionLocal()


def _request(tier: str | None = None) -> Request:
    headers = []
    if tier:
        headers.append((b"x-ct-entitlement-tier", tier.encode("utf-8")))
    return Request({"type": "http", "method": "GET", "path": "/", "headers": headers})


def _user(db: Session, email: str, *, tier: str = "free") -> UserAccount:
    user = UserAccount(email=email, role="user", entitlement_tier=tier)
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


def _request_for_user(user: UserAccount) -> Request:
    token = sign_session_payload({"uid": user.id, "email": user.email})
    return Request(
        {"type": "http", "method": "POST", "path": "/", "headers": [(b"authorization", f"Bearer {token}".encode())]}
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
            stock_screener(request=_request("free"), db=db, congress_activity="has_activity")
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
        assert response["items"][0]["confirmation"]["locked"] is True
        assert response["items"][0]["why_now"]["locked"] is True
    finally:
        db.close()


def test_free_screener_export_requires_premium(monkeypatch):
    monkeypatch.setenv("CT_DEFAULT_TIER", "free")
    monkeypatch.setenv("CT_ALLOW_ENTITLEMENT_HEADER", "1")
    monkeypatch.setattr(
        "app.services.screener.fetch_company_screener",
        lambda *, filters, limit: [_fake_screener_row("AAA")],
    )
    db = _session()
    try:
        try:
            stock_screener_export(request=_request("free"), db=db)
        except HTTPException as exc:
            assert exc.status_code == 402
            assert exc.detail["feature"] == "screener_csv_export"
        else:
            raise AssertionError("Expected premium-required response")
    finally:
        db.close()


def test_saved_screen_limit_uses_screener_saved_screens(monkeypatch):
    monkeypatch.setenv("CT_DEFAULT_TIER", "free")
    db = _session()
    try:
        user = _user(db, "free@example.com", tier="free")
        db.add(SavedScreen(user_id=user.id, name="Existing", params_json="{}"))
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
