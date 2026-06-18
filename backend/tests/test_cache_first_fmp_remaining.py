from __future__ import annotations

from datetime import datetime, timedelta

import requests
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import app.main as main_module
from app.db import Base
from app.models import TickerMeta
from app.request_priority import reset_request_context, set_request_context
from app.services.fmp_news import clear_news_cache, get_general_news, get_press_releases, get_sec_filings, get_stock_news
from app.services.provider_usage import provider_usage_summary, reset_provider_usage
from app.services.ticker_financials import clear_financials_cache, get_ticker_financials
from app.services.ticker_meta import get_ticker_meta


class _FakeResponse:
    def __init__(self, status_code: int, payload, text: str = ""):
        self.status_code = status_code
        self._payload = payload
        self.text = text

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}")


def _db():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    Session = sessionmaker(bind=engine, future=True)
    return Session()


def test_news_public_cache_hit_makes_zero_fmp_calls(monkeypatch):
    clear_news_cache()
    reset_provider_usage()
    monkeypatch.setenv("FMP_API_KEY", "test-key")
    monkeypatch.setenv("FMP_PERSIST_USAGE_EVENTS", "0")

    def warm_get(url, params=None, timeout=30):
        return _FakeResponse(
            200,
            [
                {
                    "symbol": "AAPL",
                    "title": "Apple raises dividend",
                    "publishedDate": "2026-06-01T12:00:00Z",
                    "url": "https://example.com/aapl",
                    "site": "Reuters",
                }
            ],
        )

    monkeypatch.setattr("app.services.fmp_news.requests.get", warm_get)
    assert get_stock_news(symbol="AAPL", page=0, limit=5)["status"] == "ok"

    def fail_get(*_args, **_kwargs):
        raise AssertionError("FMP should not be called on public cache hit")

    monkeypatch.setattr("app.services.fmp_news.requests.get", fail_get)
    token = set_request_context({"path": "/api/tickers/AAPL/news", "priority": "heavy"})
    try:
        payload = get_stock_news(symbol="AAPL", page=0, limit=5)
    finally:
        reset_request_context(token)

    assert payload["status"] == "ok"
    assert payload["items"][0]["title"] == "Apple raises dividend"
    usage = provider_usage_summary()
    assert any(row["name"] == "news:stock" and row["kind"] == "cache_hit" for row in usage["top_categories"])
    assert any(row["category"] == "news:stock" and row["items_written"] == 1 for row in usage["content_writes"])
    reset_provider_usage()


def test_news_public_cache_miss_enqueues_without_fmp_when_sync_disabled(monkeypatch):
    clear_news_cache()
    monkeypatch.setenv("FMP_API_KEY", "test-key")
    monkeypatch.setenv("FMP_ALLOW_SYNC_USER_FETCH", "false")
    monkeypatch.setenv("FMP_PERSIST_USAGE_EVENTS", "0")
    jobs = []
    calls = {"count": 0}

    def fail_get(*_args, **_kwargs):
        calls["count"] += 1
        raise AssertionError("FMP should not be called on public cache miss")

    def fake_enqueue(**kwargs):
        jobs.append(kwargs)
        return True

    monkeypatch.setattr("app.services.fmp_news.requests.get", fail_get)
    monkeypatch.setattr("app.services.fmp_news.enqueue_data_enrichment_job", fake_enqueue)
    token = set_request_context({"path": "/api/tickers/AAPL/news", "priority": "heavy"})
    try:
        payload = get_stock_news(symbol="AAPL", page=0, limit=5)
    finally:
        reset_request_context(token)

    assert payload["status"] == "warming"
    assert payload["items"] == []
    assert payload["cache_status"] == "warming"
    assert "message" not in payload
    assert "reason" not in payload
    assert "unavailable" not in payload
    assert "data" not in payload
    assert calls["count"] == 0
    assert jobs and jobs[0]["job_type"] == "news_stock"
    assert jobs[0]["symbol"] == "AAPL"


def test_press_public_cache_miss_enqueues_without_fmp(monkeypatch):
    clear_news_cache()
    monkeypatch.setenv("FMP_API_KEY", "test-key")
    monkeypatch.setenv("FMP_ALLOW_SYNC_USER_FETCH", "true")
    monkeypatch.setenv("FMP_PERSIST_USAGE_EVENTS", "0")
    jobs = []
    calls = {"count": 0}

    def fail_get(*_args, **_kwargs):
        calls["count"] += 1
        raise AssertionError("FMP should not be called on public press cache miss")

    def fake_enqueue(**kwargs):
        jobs.append(kwargs)
        return True

    monkeypatch.setattr("app.services.fmp_news.requests.get", fail_get)
    monkeypatch.setattr("app.services.fmp_news.enqueue_data_enrichment_job", fake_enqueue)
    token = set_request_context({"path": "/api/tickers/AAPL/press-releases", "priority": "heavy"})
    try:
        payload = get_press_releases(symbol="AAPL", page=0, limit=5)
    finally:
        reset_request_context(token)

    assert payload["status"] == "warming"
    assert payload["items"] == []
    assert calls["count"] == 0
    assert jobs and jobs[0]["job_type"] == "press_releases"
    assert jobs[0]["symbol"] == "AAPL"


def test_sec_filings_public_cache_miss_enqueues_without_fmp(monkeypatch):
    clear_news_cache()
    monkeypatch.setenv("FMP_API_KEY", "test-key")
    monkeypatch.setenv("FMP_ALLOW_SYNC_USER_FETCH", "true")
    monkeypatch.setenv("FMP_PERSIST_USAGE_EVENTS", "0")
    jobs = []
    calls = {"count": 0}

    def fail_get(*_args, **_kwargs):
        calls["count"] += 1
        raise AssertionError("FMP should not be called on public filings cache miss")

    def fake_enqueue(**kwargs):
        jobs.append(kwargs)
        return True

    monkeypatch.setattr("app.services.fmp_news.requests.get", fail_get)
    monkeypatch.setattr("app.services.fmp_news.enqueue_data_enrichment_job", fake_enqueue)
    token = set_request_context({"path": "/api/tickers/AAPL/sec-filings", "priority": "heavy"})
    try:
        payload = get_sec_filings(symbol="AAPL", from_date="2026-06-01", to_date="2026-06-11", page=0, limit=5)
    finally:
        reset_request_context(token)

    assert payload["status"] == "warming"
    assert payload["items"] == []
    assert calls["count"] == 0
    assert jobs and jobs[0]["job_type"] == "sec_filings"
    assert jobs[0]["payload"]["from_date"] == "2026-06-01"


def test_financials_public_cache_miss_enqueues_without_fmp_when_sync_disabled(monkeypatch):
    clear_financials_cache()
    monkeypatch.setenv("FMP_API_KEY", "test-key")
    monkeypatch.setenv("FMP_ALLOW_SYNC_USER_FETCH", "false")
    monkeypatch.setenv("FMP_PERSIST_USAGE_EVENTS", "0")
    jobs = []
    calls = {"count": 0}

    def fail_get(*_args, **_kwargs):
        calls["count"] += 1
        raise AssertionError("FMP should not be called on public financials miss")

    def fake_enqueue(**kwargs):
        jobs.append(kwargs)
        return True

    monkeypatch.setattr("app.services.ticker_financials.requests.get", fail_get)
    monkeypatch.setattr("app.services.ticker_financials.enqueue_data_enrichment_job", fake_enqueue)
    token = set_request_context({"path": "/api/tickers/AAPL/financials", "priority": "heavy"})
    try:
        payload = get_ticker_financials("AAPL")
    finally:
        reset_request_context(token)

    assert payload["status"] == "warming"
    assert payload["cache_status"] == "warming"
    assert "message" not in payload
    assert "reason" not in payload
    assert "unavailable" not in payload
    assert "data" not in payload
    assert calls["count"] == 0
    assert jobs and jobs[0]["job_type"] == "ticker_financials"
    assert jobs[0]["symbol"] == "AAPL"


def test_financials_background_context_can_fetch(monkeypatch):
    clear_financials_cache()
    monkeypatch.setenv("FMP_API_KEY", "test-key")
    monkeypatch.setenv("FMP_ALLOW_SYNC_USER_FETCH", "false")
    monkeypatch.setenv("FMP_PERSIST_USAGE_EVENTS", "0")
    calls = {"count": 0}

    def fake_get(url, params=None, timeout=30):
        calls["count"] += 1
        return _FakeResponse(200, [])

    monkeypatch.setattr("app.services.ticker_financials.requests.get", fake_get)

    payload = get_ticker_financials("AAPL")

    assert payload["status"] == "unavailable"
    assert calls["count"] > 0


def test_news_provider_failure_serves_stale_cache_if_available(monkeypatch):
    clear_news_cache()
    monkeypatch.setenv("FMP_API_KEY", "test-key")
    monkeypatch.setenv("FMP_PERSIST_USAGE_EVENTS", "0")
    now = {"value": 1_000.0}
    jobs = []

    def fake_time():
        return now["value"]

    def warm_get(url, params=None, timeout=30):
        return _FakeResponse(
            200,
            [
                {
                    "title": "Cached macro headline",
                    "publishedDate": "2026-06-01T12:00:00Z",
                    "url": "https://example.com/macro",
                    "site": "Reuters",
                }
            ],
        )

    monkeypatch.setattr("app.services.fmp_news.time.time", fake_time)
    monkeypatch.setattr("app.services.fmp_news.requests.get", warm_get)
    assert get_general_news(page=0, limit=5)["status"] == "ok"

    now["value"] += 16 * 60

    def fail_get(*_args, **_kwargs):
        raise requests.Timeout("provider down")

    def fake_enqueue(**kwargs):
        jobs.append(kwargs)
        return True

    monkeypatch.setattr("app.services.fmp_news.requests.get", fail_get)
    monkeypatch.setattr("app.services.fmp_news.enqueue_data_enrichment_job", fake_enqueue)

    payload = get_general_news(page=0, limit=5)

    assert payload["status"] == "ok"
    assert payload["stale"] is True
    assert payload["items"][0]["title"] == "Cached macro headline"
    assert jobs and jobs[0]["job_type"] == "news_general"


def test_ticker_meta_public_miss_enqueues_without_fmp_when_sync_disabled(monkeypatch):
    db = _db()
    monkeypatch.setenv("FMP_API_KEY", "test-key")
    monkeypatch.setenv("FMP_ALLOW_SYNC_USER_FETCH", "false")
    monkeypatch.setenv("FMP_PERSIST_USAGE_EVENTS", "0")
    jobs = []
    calls = {"count": 0}

    def fail_get(*_args, **_kwargs):
        calls["count"] += 1
        raise AssertionError("FMP should not be called on public ticker_meta miss")

    def fake_enqueue(**kwargs):
        jobs.append(kwargs)
        return True

    monkeypatch.setattr("app.services.ticker_meta.requests.get", fail_get)
    monkeypatch.setattr("app.services.ticker_meta.enqueue_data_enrichment_job", fake_enqueue)
    token = set_request_context({"path": "/api/tickers/AAPL", "priority": "heavy"})
    try:
        payload = get_ticker_meta(db, ["AAPL"], allow_refresh=True)
    finally:
        reset_request_context(token)
        db.close()

    assert payload == {}
    assert calls["count"] == 0
    assert jobs and jobs[0]["job_type"] == "ticker_meta"
    assert jobs[0]["symbol"] == "AAPL"


def test_ticker_meta_background_context_can_fetch(monkeypatch):
    db = _db()
    monkeypatch.setenv("FMP_API_KEY", "test-key")
    monkeypatch.setenv("FMP_ALLOW_SYNC_USER_FETCH", "false")
    monkeypatch.setenv("FMP_PERSIST_USAGE_EVENTS", "0")
    calls = {"count": 0}

    def fake_get(url, params=None, timeout=30):
        calls["count"] += 1
        if url.endswith("/stable/search-symbol"):
            return _FakeResponse(200, [{"symbol": "AAPL", "name": "Apple Inc.", "exchange": "NASDAQ"}])
        if url.endswith("/stable/profile"):
            return _FakeResponse(
                200,
                [
                    {
                        "symbol": "AAPL",
                        "companyName": "Apple Inc.",
                        "exchangeShortName": "NASDAQ",
                        "sector": "Technology",
                        "industry": "Consumer Electronics",
                        "country": "US",
                    }
                ],
            )
        if "/profile/" in url:
            return _FakeResponse(
                200,
                [
                    {
                        "symbol": "AAPL",
                        "companyName": "Apple Inc.",
                        "exchangeShortName": "NASDAQ",
                        "sector": "Technology",
                        "industry": "Consumer Electronics",
                        "country": "US",
                    }
                ],
            )
        return _FakeResponse(200, [])

    monkeypatch.setattr("app.services.ticker_meta.requests.get", fake_get)
    try:
        payload = get_ticker_meta(db, ["AAPL"], allow_refresh=True)
    finally:
        db.close()

    assert payload["AAPL"] == {
        "company_name": "Apple Inc.",
        "exchange": "NASDAQ",
        "sector": "Technology",
        "industry": "Consumer Electronics",
        "country": "US",
    }
    assert calls["count"] == 2


def test_ticker_meta_background_upsert_does_not_overwrite_identity_with_nulls(monkeypatch):
    db = _db()
    monkeypatch.setenv("FMP_API_KEY", "test-key")
    monkeypatch.setenv("FMP_ALLOW_SYNC_USER_FETCH", "false")
    monkeypatch.setenv("FMP_PERSIST_USAGE_EVENTS", "0")
    db.add(
        TickerMeta(
            symbol="MSTR",
            company_name="Strategy Inc",
            exchange="NASDAQ",
            sector="Technology",
            industry="Software - Application",
            country="US",
            updated_at=datetime.utcnow() - timedelta(days=30),
        )
    )
    db.commit()

    def fake_get(url, params=None, timeout=30):
        if url.endswith("/stable/search-symbol"):
            return _FakeResponse(200, [{"symbol": "MSTR", "name": "Strategy Inc", "exchange": "NASDAQ"}])
        if url.endswith("/stable/profile"):
            return _FakeResponse(200, [{"symbol": "MSTR", "companyName": "Strategy Inc"}])
        if "/profile/" in url:
            return _FakeResponse(200, [{"symbol": "MSTR", "companyName": "Strategy Inc"}])
        return _FakeResponse(200, [])

    monkeypatch.setattr("app.services.ticker_meta.requests.get", fake_get)
    try:
        payload = get_ticker_meta(db, ["MSTR"], allow_refresh=True)
    finally:
        db.close()

    assert payload["MSTR"]["sector"] == "Technology"
    assert payload["MSTR"]["industry"] == "Software - Application"
    assert payload["MSTR"]["country"] == "US"


def test_background_profile_snapshot_missing_key_uses_background_reason(monkeypatch):
    reset_provider_usage()
    main_module._TICKER_PROFILE_SNAPSHOT_CACHE.clear()
    monkeypatch.delenv("FMP_API_KEY", raising=False)
    monkeypatch.setenv("FMP_PERSIST_USAGE_EVENTS", "0")
    monkeypatch.setattr(main_module, "enqueue_data_enrichment_job", lambda **_kwargs: True)

    token = set_request_context({"path": "background", "priority": "normal", "job_type": "profile"})
    try:
        payload = main_module._company_profile_snapshot_from_fmp("NBIS")
    finally:
        reset_request_context(token)

    reasons = {row["reason"]: row["count"] for row in provider_usage_summary(limit=10)["fallback_reasons"]}
    assert payload == {}
    assert reasons.get("background_provider_disabled", 0) >= 1
    assert reasons.get("provider_disabled", 0) == 0
    assert reasons.get("page_fetch_blocked", 0) == 0


def test_ticker_meta_public_stale_row_is_served_and_enqueued(monkeypatch):
    db = _db()
    monkeypatch.setenv("FMP_API_KEY", "test-key")
    monkeypatch.setenv("FMP_ALLOW_SYNC_USER_FETCH", "false")
    monkeypatch.setenv("FMP_PERSIST_USAGE_EVENTS", "0")
    jobs = []
    calls = {"count": 0}
    db.add(
        TickerMeta(
            symbol="AAPL",
            company_name="Apple Inc.",
            exchange="NASDAQ",
            updated_at=datetime.utcnow() - timedelta(days=30),
        )
    )
    db.commit()

    def fail_get(*_args, **_kwargs):
        calls["count"] += 1
        raise AssertionError("FMP should not be called when stale metadata can be served")

    def fake_enqueue(**kwargs):
        jobs.append(kwargs)
        return True

    monkeypatch.setattr("app.services.ticker_meta.requests.get", fail_get)
    monkeypatch.setattr("app.services.ticker_meta.enqueue_data_enrichment_job", fake_enqueue)
    token = set_request_context({"path": "/api/tickers/AAPL", "priority": "heavy"})
    try:
        payload = get_ticker_meta(db, ["AAPL"], allow_refresh=True)
    finally:
        reset_request_context(token)
        db.close()

    assert payload["AAPL"]["company_name"] == "Apple Inc."
    assert calls["count"] == 0
    assert jobs and jobs[0]["job_type"] == "ticker_meta"


def test_ticker_meta_public_sparse_identity_row_is_served_and_enqueued(monkeypatch):
    db = _db()
    monkeypatch.setenv("FMP_API_KEY", "test-key")
    monkeypatch.setenv("FMP_ALLOW_SYNC_USER_FETCH", "false")
    monkeypatch.setenv("FMP_PERSIST_USAGE_EVENTS", "0")
    jobs = []
    calls = {"count": 0}
    db.add(
        TickerMeta(
            symbol="NBIS",
            company_name="Nebius Group N.V.",
            exchange="NASDAQ",
            updated_at=datetime.utcnow(),
        )
    )
    db.commit()

    def fail_get(*_args, **_kwargs):
        calls["count"] += 1
        raise AssertionError("FMP should not be called when sparse metadata can be served")

    def fake_enqueue(**kwargs):
        jobs.append(kwargs)
        return True

    monkeypatch.setattr("app.services.ticker_meta.requests.get", fail_get)
    monkeypatch.setattr("app.services.ticker_meta.enqueue_data_enrichment_job", fake_enqueue)
    token = set_request_context({"path": "/api/tickers/NBIS", "priority": "heavy"})
    try:
        payload = get_ticker_meta(db, ["NBIS"], allow_refresh=True)
    finally:
        reset_request_context(token)
        db.close()

    assert payload["NBIS"]["company_name"] == "Nebius Group N.V."
    assert payload["NBIS"]["exchange"] == "NASDAQ"
    assert calls["count"] == 0
    assert jobs and jobs[0]["job_type"] == "ticker_meta"
    assert jobs[0]["symbol"] == "NBIS"
    assert jobs[0]["reason"] == "missing_profile_identity"


def test_ticker_meta_background_sparse_identity_row_refreshes(monkeypatch):
    db = _db()
    monkeypatch.setenv("FMP_API_KEY", "test-key")
    monkeypatch.setenv("FMP_ALLOW_SYNC_USER_FETCH", "false")
    monkeypatch.setenv("FMP_PERSIST_USAGE_EVENTS", "0")
    calls = {"count": 0}
    db.add(
        TickerMeta(
            symbol="NBIS",
            company_name="Nebius Group N.V.",
            exchange="NASDAQ",
            updated_at=datetime.utcnow(),
        )
    )
    db.commit()

    def fake_get(url, params=None, timeout=30):
        calls["count"] += 1
        if url.endswith("/stable/search-symbol"):
            return _FakeResponse(200, [{"symbol": "NBIS", "name": "Nebius Group N.V.", "exchange": "NASDAQ"}])
        if url.endswith("/stable/profile"):
            return _FakeResponse(
                200,
                [
                    {
                        "symbol": "NBIS",
                        "companyName": "Nebius Group N.V.",
                        "exchangeShortName": "NASDAQ",
                        "sector": "Technology",
                        "industry": "Information Technology Services",
                        "country": "NL",
                    }
                ],
            )
        if "/profile/" in url:
            return _FakeResponse(
                200,
                [
                    {
                        "symbol": "NBIS",
                        "companyName": "Nebius Group N.V.",
                        "exchangeShortName": "NASDAQ",
                        "sector": "Technology",
                        "industry": "Information Technology Services",
                        "country": "NL",
                    }
                ],
            )
        return _FakeResponse(200, [])

    monkeypatch.setattr("app.services.ticker_meta.requests.get", fake_get)
    token = set_request_context({"path": "background", "priority": "normal", "job_type": "ticker_meta"})
    try:
        payload = get_ticker_meta(db, ["NBIS"], allow_refresh=True)
    finally:
        reset_request_context(token)
        db.close()

    assert payload["NBIS"] == {
        "company_name": "Nebius Group N.V.",
        "exchange": "NASDAQ",
        "sector": "Technology",
        "industry": "Information Technology Services",
        "country": "NL",
    }
    assert calls["count"] == 2
