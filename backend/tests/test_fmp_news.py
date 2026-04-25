from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from starlette.requests import Request

from app.auth import sign_session_payload
from app.db import Base
from app.main import list_insights_news, ticker_news
from app.models import Security, UserAccount, Watchlist, WatchlistItem
from app.services.fmp_news import clear_news_cache


class _FakeResponse:
    def __init__(self, status_code: int, payload, text: str = ""):
        self.status_code = status_code
        self._payload = payload
        self.text = text

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


def _session():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    Base.metadata.create_all(engine)
    return Session()


def _anonymous_request() -> Request:
    return Request({"type": "http", "method": "GET", "path": "/", "headers": []})


def _request_for_user(user: UserAccount) -> Request:
    token = sign_session_payload({"uid": user.id, "email": user.email})
    return Request(
        {
            "type": "http",
            "method": "GET",
            "path": "/",
            "headers": [(b"authorization", f"Bearer {token}".encode())],
        }
    )


def test_ticker_news_returns_normalized_items_and_caches_provider_calls(monkeypatch):
    db = _session()
    clear_news_cache()
    calls = {"count": 0}

    def fake_get(url, params=None, timeout=30):
        calls["count"] += 1
        assert params["symbols"] == "AAPL"
        return _FakeResponse(
            200,
            [
                {
                    "symbol": "AAPL",
                    "publishedDate": "2026-04-25T15:30:00Z",
                    "site": "Reuters",
                    "title": "Apple launches a new product line",
                    "url": "https://example.com/apple-product",
                    "text": "Apple expanded its hardware lineup.",
                    "image": "https://example.com/apple.jpg",
                }
            ],
        )

    monkeypatch.setenv("FMP_API_KEY", "test-key")
    monkeypatch.setattr("app.services.fmp_news.requests.get", fake_get)

    first = ticker_news("AAPL", db, limit=8)
    second = ticker_news("AAPL", db, limit=8)

    assert calls["count"] == 1
    assert first["status"] == "ok"
    assert second["status"] == "ok"
    assert first["items"][0] == {
        "symbol": "AAPL",
        "related_symbols": ["AAPL"],
        "title": "Apple launches a new product line",
        "site": "Reuters",
        "published_at": "2026-04-25T15:30:00+00:00",
        "url": "https://example.com/apple-product",
        "image_url": "https://example.com/apple.jpg",
        "summary": "Apple expanded its hardware lineup.",
        "source": "fmp",
        "source_type": "stock_news",
    }


def test_insights_news_returns_unavailable_when_plan_limited(monkeypatch):
    db = _session()
    clear_news_cache()

    def fake_get(url, params=None, timeout=30):
        return _FakeResponse(402, [], text="Payment Required")

    monkeypatch.setenv("FMP_API_KEY", "test-key")
    monkeypatch.setattr("app.services.fmp_news.requests.get", fake_get)

    response = list_insights_news(_anonymous_request(), db, category="market", limit=25, page=0, offset=None)

    assert response["items"] == []
    assert response["status"] == "unavailable"
    assert response["message"] == "News is unavailable under the current data plan."


def test_watchlist_insights_uses_current_user_watchlist_symbols(monkeypatch):
    db = _session()
    clear_news_cache()

    user = UserAccount(email="reader@example.com", role="user", entitlement_tier="free")
    db.add(user)
    db.flush()
    watchlist = Watchlist(name="Tech", owner_user_id=user.id)
    aapl = Security(symbol="AAPL", name="Apple", asset_class="stock", sector=None)
    msft = Security(symbol="MSFT", name="Microsoft", asset_class="stock", sector=None)
    db.add_all([watchlist, aapl, msft])
    db.flush()
    db.add_all(
        [
            WatchlistItem(watchlist_id=watchlist.id, security_id=aapl.id),
            WatchlistItem(watchlist_id=watchlist.id, security_id=msft.id),
        ]
    )
    db.commit()

    captured = {"symbols": None}

    def fake_get(url, params=None, timeout=30):
        captured["symbols"] = params.get("symbols") or params.get("tickers")
        return _FakeResponse(
            200,
            [
                {
                    "symbols": "AAPL,MSFT",
                    "publishedDate": "2026-04-25T13:00:00Z",
                    "site": "Bloomberg",
                    "title": "Big tech headlines",
                    "url": "https://example.com/big-tech",
                    "text": "Both Apple and Microsoft were in focus.",
                }
            ],
        )

    monkeypatch.setenv("FMP_API_KEY", "test-key")
    monkeypatch.setattr("app.services.fmp_news.requests.get", fake_get)

    response = list_insights_news(_request_for_user(user), db, category="watchlist", limit=25, page=0, offset=None)

    assert captured["symbols"] == "AAPL,MSFT"
    assert response["status"] == "ok"
    assert response["items"][0]["related_symbols"] == ["AAPL", "MSFT"]
