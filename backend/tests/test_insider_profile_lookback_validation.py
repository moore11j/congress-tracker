from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.db import Base, get_db
from app.auth import sign_session_payload
from app.main import app
from app.models import Event, UserAccount
from app.routers import events as events_router


def _session_factory():
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False)


def _install_db_override(Session) -> None:
    def override_get_db():
        db = Session()
        try:
            yield db
        finally:
            db.close()

    app.dependency_overrides[get_db] = override_get_db


async def _call_app(path: str, headers: list[tuple[bytes, bytes]] | None = None) -> tuple[int, dict]:
    raw_path, _, query_string = path.partition("?")
    messages = []

    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message):
        messages.append(message)

    await app(
        {
            "type": "http",
            "http_version": "1.1",
            "method": "GET",
            "path": raw_path,
            "raw_path": raw_path.encode("ascii"),
            "query_string": query_string.encode("ascii"),
            "headers": [(b"host", b"testserver"), *(headers or [])],
            "client": ("203.0.113.77", 12345),
            "server": ("testserver", 80),
            "scheme": "http",
        },
        receive,
        send,
    )
    started = next(message for message in messages if message["type"] == "http.response.start")
    body = b"".join(message.get("body", b"") for message in messages if message["type"] == "http.response.body")
    return int(started["status"]), json.loads(body or b"{}")


@pytest.fixture
def api_db(monkeypatch):
    monkeypatch.setenv("RATE_LIMIT_ENABLED", "false")
    Session = _session_factory()
    _install_db_override(Session)
    try:
        yield Session
    finally:
        app.dependency_overrides.clear()


@pytest.mark.parametrize(
    ("path", "response_key"),
    [
        ("/api/insiders/0000919484/summary", "lookback_days"),
        ("/api/insiders/0000919484/alpha-summary", "lookback_days"),
        ("/api/insiders/0000919484/trades", "lookback_days"),
        ("/api/insiders/0000919484/top-tickers", "lookback_days"),
        ("/api/insiders/0000919484/stock-chart", "days"),
    ],
)
@pytest.mark.parametrize("lookback_days", [180, 1095])
def test_insider_profile_endpoints_accept_extended_lookbacks(api_db, path: str, response_key: str, lookback_days: int):
    status, body = asyncio.run(_call_app(f"{path}?lookback_days={lookback_days}"))

    assert status == 200
    assert body[response_key] == lookback_days


@pytest.mark.parametrize(
    "path",
    [
        "/api/insiders/0000919484/summary",
        "/api/insiders/0000919484/alpha-summary",
        "/api/insiders/0000919484/trades",
        "/api/insiders/0000919484/top-tickers",
    ],
)
def test_insider_profile_event_endpoints_reject_oversized_lookbacks(api_db, path: str):
    status, body = asyncio.run(_call_app(f"{path}?lookback_days=2000"))

    assert status == 400
    assert body["detail"] == "Invalid lookback_days. Allowed values: 30, 90, 180, 365, 1095."


def test_insider_stock_chart_rejects_oversized_lookback(api_db):
    status, body = asyncio.run(_call_app("/api/insiders/0000919484/stock-chart?lookback_days=2000"))

    assert status == 422
    assert body["detail"][0]["type"] == "less_than_equal"
    assert body["detail"][0]["ctx"]["le"] == 1095


def test_insider_recent_trades_public_route_returns_same_rows_for_guest_and_free_user(api_db, monkeypatch):
    monkeypatch.setattr(events_router, "get_current_prices_meta_db", lambda *_args, **_kwargs: {})

    ts = datetime.now(timezone.utc)
    with api_db() as db:
        user = UserAccount(email="free@example.com", role="user", entitlement_tier="free")
        db.add(user)
        db.add(
            Event(
                id=1001,
                event_type="insider_trade",
                ts=ts,
                event_date=ts,
                symbol="PLTR",
                source="fmp",
                trade_type="sale",
                amount_min=1000,
                amount_max=5000,
                payload_json=json.dumps(
                    {
                        "symbol": "PLTR",
                        "transaction_date": ts.date().isoformat(),
                        "reporting_cik": "0001824159",
                        "insider_name": "Sankar Shyam",
                        "company_name": "Palantir Technologies Inc",
                    }
                ),
            )
        )
        db.commit()
        token = sign_session_payload({"uid": user.id, "email": user.email})

    path = "/api/insiders/0001824159/trades?lookback_days=90&limit=20&page=0"
    guest_status, guest_body = asyncio.run(_call_app(path))
    free_status, free_body = asyncio.run(_call_app(path, [(b"authorization", f"Bearer {token}".encode("ascii"))]))

    assert guest_status == 200
    assert free_status == 200
    assert guest_body["total"] == 1
    assert guest_body["page"] == 0
    assert guest_body["limit"] == 20
    assert guest_body["items"][0]["symbol"] == "PLTR"
    assert free_body == guest_body
