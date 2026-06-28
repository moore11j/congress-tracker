from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from fastapi import HTTPException

from app.db import Base
from app.main import _build_ticker_chart_bundle, ticker_chart_bundle
from app.models import FundamentalsCache, PriceCache
from app.request_priority import reset_request_context, set_request_context
from app.services.cache_policy import CachePolicy, is_fresh, is_stale_but_usable
from app.services.fmp_news import clear_news_cache, get_general_news
from app.services.market_data_cache import get_or_refresh_market_data
from app.services.provider_usage import provider_usage_summary, reset_provider_usage
from app.services.ticker_financials import clear_financials_cache, get_ticker_financials


def _session():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    Session = sessionmaker(bind=engine, future=True)
    return Session()


def test_cache_policy_fresh_and_stale_windows():
    now = datetime.now(timezone.utc)
    policy = CachePolicy("test", ttl_seconds=60, stale_seconds=3600)

    assert is_fresh({"as_of": now.isoformat()}, policy) is True
    assert is_stale_but_usable({"as_of": (now - timedelta(minutes=10)).isoformat()}, policy) is True
    assert is_stale_but_usable({"as_of": (now - timedelta(hours=2)).isoformat()}, policy) is False


def test_page_load_fmp_disabled_returns_news_fallback_without_provider_call(monkeypatch):
    clear_news_cache()
    reset_provider_usage()
    monkeypatch.setenv("FMP_API_KEY", "test-key")
    monkeypatch.setenv("FMP_PERSIST_USAGE_EVENTS", "0")
    monkeypatch.delenv("FMP_LIVE_USER_ROUTES_ENABLED", raising=False)
    monkeypatch.delenv("FMP_CACHE_ONLY_USER_ROUTES", raising=False)

    def fail_get(*args, **kwargs):
        raise AssertionError("FMP should not be called on passive page load")

    monkeypatch.setattr("app.services.fmp_news.requests.get", fail_get)
    token = set_request_context({"path": "/api/insights/news", "priority": "normal"})
    try:
        response = get_general_news(page=0, limit=5)
    finally:
        reset_request_context(token)

    assert response["status"] == "warming"
    assert response["cache_status"] == "warming"
    assert "message" not in response
    assert "data" not in response
    assert "unavailable" not in response
    assert "reason" not in response


def test_page_load_financials_disabled_returns_structured_fallback_without_provider_call(monkeypatch):
    clear_financials_cache()
    reset_provider_usage()
    monkeypatch.setenv("FMP_API_KEY", "test-key")
    monkeypatch.setenv("FMP_PERSIST_USAGE_EVENTS", "0")

    def fail_get(*args, **kwargs):
        raise AssertionError("FMP should not be called on passive page load")

    monkeypatch.setattr("app.services.ticker_financials.requests.get", fail_get)
    token = set_request_context({"path": "/api/tickers/AAPL/financials", "priority": "heavy"})
    try:
        response = get_ticker_financials("AAPL")
    finally:
        reset_request_context(token)

    assert response["status"] == "warming"
    assert response["cache_status"] == "warming"
    assert "message" not in response
    assert "data" not in response
    assert "unavailable" not in response
    assert "reason" not in response


def test_ticker_chart_uses_cached_prices_when_page_load_fmp_disabled(monkeypatch):
    db = _session()
    reset_provider_usage()
    monkeypatch.setenv("FMP_API_KEY", "test-key")
    monkeypatch.setenv("FMP_PERSIST_USAGE_EVENTS", "0")
    monkeypatch.setattr("app.main._query_unified_signals", lambda **kwargs: [])
    monkeypatch.setattr("app.main.enqueue_data_enrichment_job", lambda **kwargs: True)
    today = datetime.now(timezone.utc).date()
    prior = today - timedelta(days=1)
    for symbol, close in [("AAPL", 190.0), ("AAPL", 195.0), ("^GSPC", 5100.0), ("^GSPC", 5150.0)]:
        day = prior if close in {190.0, 5100.0} else today
        db.add(PriceCache(symbol=symbol, date=day.isoformat(), close=close))
    db.commit()

    def fail_get(*args, **kwargs):
        raise AssertionError("FMP should not be called for chart snapshots on passive page load")

    monkeypatch.setattr("app.main.requests.get", fail_get)
    token = set_request_context({"path": "/api/tickers/AAPL/chart-bundle", "priority": "heavy"})
    try:
        response = _build_ticker_chart_bundle("AAPL", 30, db)
    finally:
        reset_request_context(token)

    assert response["prices"][-1] == {"date": today.isoformat(), "close": 195.0}
    assert response["quote"]["current_price"] == 195.0


def test_ticker_chart_uses_cached_fundamentals_without_page_load_fmp(monkeypatch):
    db = _session()
    reset_provider_usage()
    monkeypatch.setenv("FMP_API_KEY", "test-key")
    monkeypatch.setenv("FMP_PERSIST_USAGE_EVENTS", "0")
    monkeypatch.setattr("app.main._query_unified_signals", lambda **kwargs: [])
    today = datetime.now(timezone.utc).date()
    prior = today - timedelta(days=1)
    for symbol, close in [("MSTR", 340.0), ("MSTR", 350.0), ("^GSPC", 5100.0), ("^GSPC", 5150.0)]:
        day = prior if close in {340.0, 5100.0} else today
        db.add(PriceCache(symbol=symbol, date=day.isoformat(), close=close))
    db.add(
        FundamentalsCache(
            symbol="MSTR",
            provider="fmp",
            status="ok",
            fetched_at=datetime.now(timezone.utc),
            market_cap=92_000_000_000,
            price=351.25,
            volume=7_500_000,
            avg_volume=6_800_000,
            trailing_pe=42.5,
            beta=1.87,
        )
    )
    db.commit()

    def fail_get(*args, **kwargs):
        raise AssertionError("FMP should not be called when durable fundamentals cache is warm")

    def fail_enqueue(**kwargs):
        raise AssertionError(f"warm fundamentals should not enqueue refresh jobs: {kwargs}")

    monkeypatch.setattr("app.main.requests.get", fail_get)
    monkeypatch.setattr("app.main.enqueue_data_enrichment_job", fail_enqueue)
    token = set_request_context({"path": "/api/tickers/MSTR/chart-bundle", "priority": "heavy"})
    try:
        response = _build_ticker_chart_bundle("MSTR", 30, db)
    finally:
        reset_request_context(token)

    assert response["quote"]["current_price"] == 351.25
    assert response["quote"]["market_cap"] == 92_000_000_000
    assert response["quote"]["day_volume"] == 7_500_000
    assert response["quote"]["average_volume"] == 6_800_000
    assert response["quote"]["trailing_pe"] == 42.5
    assert response["quote"]["beta"] == 1.87


def test_ticker_chart_cold_fundamentals_miss_enqueues_without_page_load_fmp(monkeypatch):
    db = _session()
    reset_provider_usage()
    monkeypatch.setenv("FMP_API_KEY", "test-key")
    monkeypatch.setenv("FMP_PERSIST_USAGE_EVENTS", "0")
    monkeypatch.setattr("app.main._query_unified_signals", lambda **kwargs: [])
    today = datetime.now(timezone.utc).date()
    prior = today - timedelta(days=1)
    for symbol, close in [("SDRL", 8.0), ("SDRL", 8.5), ("^GSPC", 5100.0), ("^GSPC", 5150.0)]:
        day = prior if close in {8.0, 5100.0} else today
        db.add(PriceCache(symbol=symbol, date=day.isoformat(), close=close))
    db.commit()
    jobs = []

    def fail_get(*args, **kwargs):
        raise AssertionError("FMP should not be called on a cold ticker page load")

    def capture_enqueue(**kwargs):
        jobs.append(kwargs)
        return True

    monkeypatch.setattr("app.main.requests.get", fail_get)
    monkeypatch.setattr("app.main.enqueue_data_enrichment_job", capture_enqueue)
    token = set_request_context({"path": "/api/tickers/SDRL/chart-bundle", "priority": "heavy"})
    try:
        response = _build_ticker_chart_bundle("SDRL", 30, db)
    finally:
        reset_request_context(token)

    assert response["quote"]["current_price"] == 8.5
    assert response["quote"]["market_cap"] is None
    assert {job["job_type"] for job in jobs} >= {"fundamentals", "quote"}
    assert all(job["symbol"] == "SDRL" for job in jobs)


def test_ticker_chart_route_returns_cached_payload_when_heavy_route_saturated(monkeypatch):
    db = _session()
    monkeypatch.setenv("FMP_PERSIST_USAGE_EVENTS", "0")
    monkeypatch.setattr("app.main._query_unified_signals", lambda **kwargs: [])
    monkeypatch.setattr("app.main.enqueue_data_enrichment_job", lambda **kwargs: True)
    today = datetime.now(timezone.utc).date()
    prior = today - timedelta(days=1)
    for symbol, day, close in [
        ("BMNR", prior, 10.0),
        ("BMNR", today, 11.0),
        ("^GSPC", prior, 5100.0),
        ("^GSPC", today, 5110.0),
    ]:
        db.add(PriceCache(symbol=symbol, date=day.isoformat(), close=close))
    db.commit()

    def saturated(*_args, **_kwargs):
        raise HTTPException(status_code=503, detail="Endpoint temporarily busy; please retry shortly.")

    monkeypatch.setattr("app.main._coalesced_ticker_chart_bundle", saturated)
    token = set_request_context({"path": "/api/tickers/BMNR/chart-bundle", "priority": "heavy"})
    try:
        response = ticker_chart_bundle("BMNR", days=30, db=db)
    finally:
        reset_request_context(token)

    assert response["symbol"] == "BMNR"
    assert response["prices"][-1] == {"date": today.isoformat(), "close": 11.0}
    assert response["quote"]["current_price"] == 11.0


def test_market_data_cache_returns_stale_on_provider_failure(monkeypatch):
    reset_provider_usage()
    monkeypatch.setenv("FMP_PERSIST_USAGE_EVENTS", "0")
    policy = CachePolicy("demo", ttl_seconds=60, stale_seconds=3600, hot_ttl_seconds=0)
    stale_record = {"data": {"price": 10}, "as_of": (datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat()}

    result = get_or_refresh_market_data(
        None,
        key="demo:stale",
        category="demo",
        cache_loader=lambda db: stale_record,
        provider_fetcher=lambda: (_ for _ in ()).throw(RuntimeError("provider down")),
        cache_writer=lambda db, data: None,
        policy=policy,
        source="page_load",
        allow_live_fetch=True,
        allow_stale=True,
    )

    assert result["cache_status"] == "stale"
    assert result["data"] == {"price": 10}
    assert result["stale"] is True


def test_provider_usage_summary_reports_warning_status(monkeypatch):
    reset_provider_usage()
    monkeypatch.setenv("FMP_PERSIST_USAGE_EVENTS", "0")
    monkeypatch.setenv("FMP_CALLS_PER_MINUTE_SOFT_LIMIT", "1")
    token = set_request_context({"path": "background", "priority": "normal"})
    try:
        from app.services.provider_usage import ensure_fmp_live_allowed

        ensure_fmp_live_allowed(category="test", symbol="AAPL")
        try:
            ensure_fmp_live_allowed(category="test", symbol="MSFT")
        except Exception:
            pass
    finally:
        reset_request_context(token)

    summary = provider_usage_summary()
    assert summary["status"] == "critical"
    assert summary["totals"]["throttles"] >= 1
    reset_provider_usage()


def test_provider_usage_default_plan_assumption_is_enterprise_500(monkeypatch):
    reset_provider_usage()
    monkeypatch.delenv("FMP_PLAN_CALLS_PER_MINUTE", raising=False)
    monkeypatch.delenv("FMP_CALLS_PER_MINUTE", raising=False)
    monkeypatch.delenv("FMP_SOFT_LIMIT_PER_MINUTE", raising=False)
    monkeypatch.delenv("FMP_HARD_LIMIT_PER_MINUTE", raising=False)
    monkeypatch.delenv("FMP_CALLS_PER_MINUTE_SOFT_LIMIT", raising=False)
    monkeypatch.delenv("FMP_CALLS_PER_MINUTE_HARD_LIMIT", raising=False)

    summary = provider_usage_summary()

    assert summary["configured_calls_per_minute"] == 500
    assert summary["budget"]["plan_calls_per_minute"] == 500
    assert summary["budget"]["hard_limit_per_minute"] == 500
    reset_provider_usage()


def test_provider_usage_honors_soft_and_hard_budget_aliases(monkeypatch):
    reset_provider_usage()
    monkeypatch.setenv("FMP_PERSIST_USAGE_EVENTS", "0")
    monkeypatch.setenv("FMP_PLAN_CALLS_PER_MINUTE", "300")
    monkeypatch.setenv("FMP_SOFT_LIMIT_PER_MINUTE", "90")
    monkeypatch.setenv("FMP_HARD_LIMIT_PER_MINUTE", "2")
    token = set_request_context({"path": "background", "priority": "normal"})
    try:
        from app.services.provider_usage import ensure_fmp_live_allowed

        ensure_fmp_live_allowed(category="test", symbol="AAPL")
        ensure_fmp_live_allowed(category="test", symbol="MSFT")
        try:
            ensure_fmp_live_allowed(category="test", symbol="NVDA")
        except Exception:
            pass
    finally:
        reset_request_context(token)

    summary = provider_usage_summary()
    assert summary["configured_calls_per_minute"] == 300
    assert summary["budget"]["soft_limit_per_minute"] == 90
    assert summary["budget"]["hard_limit_per_minute"] == 2
    assert summary["totals"]["throttles"] == 1
    assert summary["recent_throttles"][0]["budget_tier"] == "hard"
    reset_provider_usage()
