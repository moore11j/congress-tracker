from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import app.services.ticker_hydration as hydration_module
from app.db import Base
from app.models import FundamentalsCache, PriceCache, Security
from app.services.ticker_hydration import request_ticker_hydration, ticker_hydration_status


def _db():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    return Session()


def _price_rows(symbol: str, count: int) -> list[PriceCache]:
    today = datetime.now(timezone.utc).date()
    return [
        PriceCache(
            symbol=symbol,
            date=(today - timedelta(days=count - index - 1)).isoformat(),
            close=100 + index,
            volume=1_000_000 + index,
        )
        for index in range(count)
    ]


def test_ticker_hydration_status_reports_ok_and_missing_states():
    db = _db()
    try:
        db.add(Security(symbol="NBIS", name="Nebius Group N.V.", asset_class="stock"))
        db.add_all(_price_rows("NBIS", 75))
        db.add(
            FundamentalsCache(
                symbol="NBIS",
                provider="fmp",
                status="ok",
                fetched_at=datetime.now(timezone.utc),
                price=31.25,
                market_cap=8_000_000_000,
                volume=5_000_000,
                avg_volume=4_500_000,
                beta=1.4,
                trailing_pe=22.5,
            )
        )
        db.commit()

        status = ticker_hydration_status(db, "nbis")

        assert status["symbol"] == "NBIS"
        assert status["critical"]["profile"] == "ok"
        assert status["critical"]["quote"] == "ok"
        assert status["critical"]["chart_30d"] == "ok"
        assert status["critical"]["chart_365d"] == "ok"
        assert status["critical"]["fundamentals"] == "ok"
        assert status["critical"]["technicals"] == "ok"
        assert status["optional"]["news"] == "missing"
        assert "news" in status["missing_sections"]
        assert status["should_request_hydration"] is True
        assert status["queued_jobs_count"] == 0
    finally:
        db.close()


def test_request_ticker_hydration_queues_missing_jobs(monkeypatch):
    db = _db()
    captured: list[dict] = []
    monkeypatch.delenv("FMP_ALLOW_BOUNDED_TICKER_REFRESH", raising=False)

    def fake_enqueue(**kwargs):
        captured.append(kwargs)
        return True

    monkeypatch.setattr(hydration_module, "enqueue_data_enrichment_job", fake_enqueue)
    try:
        result = request_ticker_hydration(db, "NBIS", reason="ticker_page_view", priority=20)

        job_types = {job["job_type"] for job in captured}
        assert {"quote", "ticker_meta", "price_series", "fundamentals", "technical_indicators"} <= job_types
        assert {"news_stock", "ticker_financials", "press_releases", "sec_filings"} <= job_types
        assert any(job["job_type"] == "price_series" and ":" in (job.get("window_key") or "") for job in captured)
        assert result["jobs_enqueued_by_type"]["ticker_financials"] == 1
        assert result["already_pending_count"] == 0
        assert result["skipped_invalid_count"] == 0
        assert result["refreshed"]["attempted"] is False
    finally:
        db.close()


def test_bounded_ticker_refresh_respects_max_calls_and_skips_heavy_endpoints(monkeypatch):
    db = _db()
    calls: list[str] = []
    hydration_module._SYMBOL_LOCKS.clear()
    monkeypatch.setenv("FMP_ALLOW_BOUNDED_TICKER_REFRESH", "true")
    monkeypatch.setenv("FMP_TICKER_REFRESH_MAX_CALLS_PER_SYMBOL", "2")
    monkeypatch.setenv("FMP_TICKER_REFRESH_LOCK_TTL_SECONDS", "1")
    monkeypatch.delenv("FMP_TICKER_REFRESH_WATCHLIST_ONLY", raising=False)
    monkeypatch.setattr(hydration_module, "enqueue_data_enrichment_job", lambda **kwargs: False)

    def quote_refresh(db_arg, symbols, **kwargs):
        calls.append("quote")
        return {}

    def profile_refresh(db_arg, symbols, **kwargs):
        calls.append("profile")
        return {}

    def chart_refresh(db_arg, symbol, start_date, end_date, **kwargs):
        calls.append("chart")
        return {}

    def fundamentals_refresh(symbol):
        calls.append("fundamentals")
        return type("Result", (), {"status": "failed", "values": {}})()

    monkeypatch.setattr("app.services.quote_lookup.get_current_prices_meta_db", quote_refresh)
    monkeypatch.setattr("app.services.ticker_meta.get_ticker_meta", profile_refresh)
    monkeypatch.setattr("app.services.price_lookup.get_daily_close_series_with_fallback", chart_refresh)
    monkeypatch.setattr("app.services.fundamentals_cache.fetch_fundamentals_for_symbol", fundamentals_refresh)

    try:
        result = request_ticker_hydration(db, "NBIS", reason="ticker_page_view", priority=20)

        assert result["refreshed"]["attempted"] is True
        assert result["refreshed"]["calls"] == 2
        assert calls == ["quote", "profile"]
        assert "news_stock" not in calls
        assert "ticker_financials" not in calls
    finally:
        db.close()
