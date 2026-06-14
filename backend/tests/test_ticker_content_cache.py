from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import app.main as main_module
import app.services.ticker_content_cache as content_cache
from app.db import Base
from app.main import admin_ticker_content_debug, admin_ticker_debug, ticker_news, ticker_profile, ticker_sec_filings
from app.models import PriceCache, Security, TickerMeta
from app.request_priority import reset_request_context, set_request_context
from app.services.fmp_news import clear_news_cache
from app.services.ticker_content_cache import db_ticker_content_cache_set
from app.services.ticker_hydration import ticker_hydration_status


def _session(monkeypatch):
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    monkeypatch.setenv("TICKER_CONTENT_SQLITE_CACHE", "1")
    monkeypatch.setattr(content_cache, "SessionLocal", Session)
    return Session()


def test_ticker_news_reads_durable_cache_without_provider_call(monkeypatch):
    db = _session(monkeypatch)
    clear_news_cache()
    try:
        db_ticker_content_cache_set(
            "news",
            "AAPL",
            {
                "items": [
                    {
                        "symbol": "AAPL",
                        "title": "Apple durable headline",
                        "site": "Reuters",
                        "published_at": "2026-06-14T12:00:00Z",
                        "url": "https://example.com/aapl-news",
                        "source": "fmp_stock_news",
                    }
                ],
                "status": "ok",
                "page": 0,
                "limit": 20,
                "has_next": False,
            },
            session=db,
        )

        def fail_get(*args, **kwargs):
            raise AssertionError("provider should not be called for durable cache hit")

        monkeypatch.setattr("app.services.fmp_news.requests.get", fail_get)
        token = set_request_context({"path": "/api/tickers/AAPL/news"})
        try:
            response = ticker_news("aapl", page=0, limit=1)
        finally:
            reset_request_context(token)

        assert response["status"] == "ok"
        assert response["item_count"] == 1
        assert response["items"][0]["title"] == "Apple durable headline"
    finally:
        db.close()


def test_ticker_news_reads_latest_durable_cache_without_exact_window_match(monkeypatch):
    db = _session(monkeypatch)
    clear_news_cache()
    try:
        db_ticker_content_cache_set(
            "news",
            "AAPL",
            {
                "items": [
                    {
                        "symbol": "AAPL",
                        "title": "Apple latest cached headline",
                        "site": "Reuters",
                        "published_at": "2026-06-14T12:00:00Z",
                        "url": "https://example.com/aapl-latest-news",
                        "source": "fmp_stock_news",
                    }
                ],
                "status": "ok",
                "page": 0,
                "limit": 20,
                "has_next": False,
            },
            window_key="limit=20:from=2026-06-01",
            session=db,
        )

        monkeypatch.setattr(
            "app.services.fmp_news.requests.get",
            lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("provider should not be called")),
        )
        token = set_request_context({"path": "/api/tickers/AAPL/news"})
        try:
            response = ticker_news("aapl", page=0, limit=5)
        finally:
            reset_request_context(token)

        assert response["status"] == "ok"
        assert response["item_count"] == 1
        assert response["items"][0]["title"] == "Apple latest cached headline"
    finally:
        db.close()


def test_ticker_sec_filings_reads_durable_365d_cache(monkeypatch):
    db = _session(monkeypatch)
    clear_news_cache()
    try:
        db_ticker_content_cache_set(
            "sec_filings",
            "AAPL",
            {
                "items": [
                    {
                        "symbol": "AAPL",
                        "form_type": "10-Q",
                        "title": "Quarterly Report",
                        "filing_date": "2026-05-01",
                        "url": "https://example.com/aapl-10q",
                        "source": "fmp_sec_filings",
                    }
                ],
                "status": "ok",
                "page": 0,
                "limit": 100,
                "has_next": False,
            },
            window_key="365d",
            session=db,
        )

        def fail_get(*args, **kwargs):
            raise AssertionError("provider should not be called for durable filing cache hit")

        monkeypatch.setattr("app.services.fmp_news.requests.get", fail_get)
        token = set_request_context({"path": "/api/tickers/AAPL/sec-filings"})
        try:
            response = ticker_sec_filings("AAPL", from_date=None, to_date=None, page=0, limit=1)
        finally:
            reset_request_context(token)

        assert response["status"] == "ok"
        assert response["item_count"] == 1
        assert response["items"][0]["form_type"] == "10-Q"
        assert response["window_days"] in {364, 365, 366}
    finally:
        db.close()


def test_ticker_sec_filings_reads_latest_cache_without_exact_window_match(monkeypatch):
    db = _session(monkeypatch)
    clear_news_cache()
    try:
        db_ticker_content_cache_set(
            "sec_filings",
            "NBIS",
            {
                "items": [
                    {
                        "symbol": "NBIS",
                        "form_type": "6-K",
                        "title": "Foreign issuer report",
                        "filing_date": "2026-06-01",
                        "url": "https://example.com/nbis-6k",
                        "source": "fmp_sec_filings",
                    }
                ],
                "status": "ok",
                "page": 0,
                "limit": 20,
                "has_next": False,
            },
            window_key="from=2026-05-01:to=2026-06-01:limit=20",
            session=db,
        )

        monkeypatch.setattr(
            "app.services.fmp_news.requests.get",
            lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("provider should not be called")),
        )
        token = set_request_context({"path": "/api/tickers/NBIS/sec-filings"})
        try:
            response = ticker_sec_filings("NBIS", from_date="2026-01-01", to_date="2026-06-14", page=0, limit=100)
        finally:
            reset_request_context(token)

        assert response["status"] == "ok"
        assert response["item_count"] == 1
        assert response["items"][0]["form_type"] == "6-K"
    finally:
        db.close()


def test_ticker_shell_returns_partial_profile_when_widget_guard_is_saturated_and_provider_fails(monkeypatch):
    db = _session(monkeypatch)
    acquired = []
    try:
        main_module._TICKER_PROFILE_RESPONSE_CACHE.clear()
        db.add(Security(symbol="NBIS", name="Nebius Group N.V.", asset_class="stock", sector="Technology"))
        db.add(TickerMeta(symbol="NBIS", company_name="Nebius Group N.V.", exchange="NASDAQ"))
        db.commit()

        while main_module._TICKER_WIDGET_SEMAPHORE.acquire(blocking=False):
            acquired.append(True)

        monkeypatch.setattr(
            main_module.requests,
            "get",
            lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("ticker shell should not call provider")),
        )
        response = ticker_profile("NBIS", db=db)

        assert response["ticker"]["symbol"] == "NBIS"
        assert response["ticker"]["name"] == "Nebius Group N.V."
        assert response["ticker"]["exchange"] == "NASDAQ"
        assert response["top_members"] == []
        assert response["confirmation_score_bundle"] is None
    finally:
        for _ in acquired:
            main_module._TICKER_WIDGET_SEMAPHORE.release()
        db.close()


def test_ticker_shell_returns_200_style_shell_for_missing_symbol(monkeypatch):
    db = _session(monkeypatch)
    try:
        main_module._TICKER_PROFILE_RESPONSE_CACHE.clear()
        response = ticker_profile("AAPL", db=db)

        assert response["ticker"]["symbol"] == "AAPL"
        assert response["ticker"]["name"] == "AAPL"
        assert response["ticker"]["metadata_status"] == "loading"
    finally:
        db.close()


def test_hydration_optional_content_uses_durable_cache(monkeypatch):
    db = _session(monkeypatch)
    try:
        db_ticker_content_cache_set(
            "news",
            "NBIS",
            {
                "items": [{"symbol": "NBIS", "title": "Nebius headline", "url": "https://example.com/nbis"}],
                "status": "ok",
                "page": 0,
                "limit": 20,
                "has_next": False,
            },
            session=db,
        )

        status = ticker_hydration_status(db, "nbis")

        assert status["optional"]["news"] == "ok"
        assert "news" not in status["missing_sections"]
    finally:
        db.close()


def test_admin_ticker_debug_reports_content_cache_and_jobs(monkeypatch):
    db = _session(monkeypatch)
    try:
        db.add(PriceCache(symbol="AAPL", date="2026-06-14", close=200.0, volume=1_000_000))
        db_ticker_content_cache_set(
            "news",
            "AAPL",
            {
                "items": [{"symbol": "AAPL", "title": "Apple cache debug", "url": "https://example.com/aapl"}],
                "status": "ok",
                "page": 0,
                "limit": 20,
                "has_next": False,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            },
            session=db,
        )
        db.commit()
        monkeypatch.setattr(main_module, "require_admin_user", lambda *args, **kwargs: object())

        response = admin_ticker_debug("aapl", request=object(), db=db)

        assert response["normalized_symbol"] == "AAPL"
        assert response["news_cache"]["rows_found"] == 1
        assert response["news_cache"]["top_items"][0]["title"] == "Apple cache debug"
        assert response["technical_price_volume_input_status"]["price_points_90d"] == 1
    finally:
        db.close()


def test_admin_ticker_content_debug_reports_cache_and_endpoint_counts(monkeypatch):
    db = _session(monkeypatch)
    try:
        db_ticker_content_cache_set(
            "news",
            "AAPL",
            {
                "items": [{"symbol": "AAPL", "title": "Apple cache debug", "url": "https://example.com/aapl"}],
                "status": "ok",
                "page": 0,
                "limit": 20,
                "has_next": False,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            },
            session=db,
        )
        db_ticker_content_cache_set(
            "press_releases",
            "AAPL",
            {
                "items": [{"symbol": "AAPL", "title": "Apple press debug", "url": "https://example.com/aapl-press"}],
                "status": "ok",
                "page": 0,
                "limit": 20,
                "has_next": False,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            },
            session=db,
        )
        db_ticker_content_cache_set(
            "sec_filings",
            "AAPL",
            {
                "items": [{"symbol": "AAPL", "form_type": "10-Q", "filing_date": "2026-05-01", "url": "https://example.com/aapl-10q"}],
                "status": "ok",
                "page": 0,
                "limit": 100,
                "has_next": False,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            },
            window_key="custom-window",
            session=db,
        )
        monkeypatch.setattr(main_module, "require_admin_user", lambda *args, **kwargs: object())

        response = admin_ticker_content_debug("aapl", request=object(), db=db)

        assert response["normalized_symbol"] == "AAPL"
        assert response["news"]["cache"]["rows_found"] == 1
        assert response["news"]["endpoint_item_count"] == 1
        assert response["sec_filings"]["cache"]["rows_found"] == 1
        assert response["sec_filings"]["endpoint_item_count"] == 1
        assert response["press_releases"]["cache"]["rows_found"] == 1
        assert response["press_releases"]["endpoint_item_count"] == 1
        assert "recent_jobs" in response
        assert "cik_mapping" in response
    finally:
        db.close()
