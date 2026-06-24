from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import app.services.data_enrichment_queue as queue_module
from app.db import Base
from app.models import Event, PageViewEvent, Security, Watchlist, WatchlistItem
from app.services.data_enrichment_queue import enqueue_priority_ticker_prewarm_jobs


def _db():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    return Session()


def test_priority_ticker_prewarm_includes_watchlist_and_default_symbols(monkeypatch):
    db = _db()
    captured = []
    try:
        bmnr = Security(symbol="BMNR", name="Bimini Holdings", asset_class="stock")
        db.add(bmnr)
        db.flush()
        watchlist = Watchlist(name="Core", owner_user_id=42)
        db.add(watchlist)
        db.flush()
        db.add(WatchlistItem(watchlist_id=watchlist.id, security_id=bmnr.id))
        db.add(Event(event_type="insider_trade", ts=datetime.now(timezone.utc), symbol="MSTR", source="test", payload_json="{}"))
        db.commit()

        def fake_enqueue(**kwargs):
            captured.append(kwargs)
            return True

        monkeypatch.setattr(queue_module, "enqueue_data_enrichment_job", fake_enqueue)

        result = enqueue_priority_ticker_prewarm_jobs(db, symbol_limit=3, popular_limit=1)

        assert "BMNR" in result["symbols"]
        assert "MSTR" in result["symbols"]
        assert "NBIS" in result["symbols"]
        assert result["symbol_count"] <= 3
        bmnr_job_types = {job["job_type"] for job in captured if job.get("symbol") == "BMNR"}
        assert {"quote", "ticker_meta", "price_series", "fundamentals", "ticker_financials", "news_stock", "press_releases", "sec_filings", "technical_indicators"} <= bmnr_job_types
        assert any(job["job_type"] == "price_series" and ":" in job["window_key"] for job in captured if job.get("symbol") == "BMNR")
        assert result["enqueued_by_type"]["technical_indicators"] >= 1
    finally:
        db.close()


def test_priority_ticker_prewarm_skips_placeholder_symbols_before_selection(monkeypatch, caplog):
    db = _db()
    captured = []
    try:
        placeholder = Security(symbol="[SYMBOL]", name="Placeholder", asset_class="stock")
        valid = Security(symbol="BMNR", name="Bimini Holdings", asset_class="stock")
        db.add_all([placeholder, valid])
        db.flush()
        watchlist = Watchlist(name="Core", owner_user_id=42)
        db.add(watchlist)
        db.flush()
        db.add_all(
            [
                WatchlistItem(watchlist_id=watchlist.id, security_id=placeholder.id),
                WatchlistItem(watchlist_id=watchlist.id, security_id=valid.id),
                Event(event_type="insider_trade", ts=datetime.now(timezone.utc), symbol="UNKNOWN", source="test", payload_json="{}"),
                Event(event_type="insider_trade", ts=datetime.now(timezone.utc), symbol="MSTR", source="test", payload_json="{}"),
                PageViewEvent(
                    user_id=None,
                    path="/ticker/[SYMBOL]",
                    normalized_path="/ticker/[SYMBOL]",
                    route_group="ticker",
                    is_authenticated=False,
                ),
            ]
        )
        db.commit()

        def fake_enqueue(**kwargs):
            captured.append(kwargs)
            return True

        monkeypatch.setenv("PRIORITY_TICKER_PREWARM_LANDING_SYMBOLS", "SYMBOL,NBIS")
        monkeypatch.setattr(queue_module, "enqueue_data_enrichment_job", fake_enqueue)

        with caplog.at_level("INFO", logger=queue_module.logger.name):
            result = enqueue_priority_ticker_prewarm_jobs(db, symbol_limit=6, popular_limit=3)

        assert "[SYMBOL]" not in result["symbols"]
        assert "UNKNOWN" not in result["symbols"]
        assert "SYMBOL" not in result["symbols"]
        assert {"BMNR", "MSTR", "NBIS"} <= set(result["symbols"])
        assert all(job.get("symbol") not in {"[SYMBOL]", "UNKNOWN", "SYMBOL"} for job in captured)
        assert result["attempted"] == result["symbol_count"] * 10
        assert "prewarm_ticker_invalid_symbol_skipped source=watchlist symbol=[SYMBOL]" in caplog.text
        assert "prewarm_ticker_invalid_symbol_skipped source=recently_viewed symbol=[SYMBOL]" not in caplog.text
        assert "prewarm_ticker_invalid_symbol_skipped source=popular symbol=UNKNOWN" in caplog.text
        assert "prewarm_ticker_invalid_symbol_skipped source=landing symbol=SYMBOL" in caplog.text
    finally:
        db.close()


def test_recently_viewed_ticker_symbols_extracts_real_path_and_ignores_templates(caplog):
    db = _db()
    now = datetime.now(timezone.utc)
    try:
        db.add_all(
            [
                PageViewEvent(
                    user_id=None,
                    path="/ticker/AAPL",
                    normalized_path="/ticker/[symbol]",
                    route_group="ticker",
                    is_authenticated=False,
                    created_at=now - timedelta(seconds=1),
                ),
                PageViewEvent(
                    user_id=None,
                    path="/ticker/[symbol]",
                    normalized_path="/ticker/[symbol]",
                    route_group="ticker",
                    is_authenticated=False,
                    created_at=now - timedelta(seconds=2),
                ),
                PageViewEvent(
                    user_id=None,
                    path="/portfolio",
                    normalized_path="/ticker/[symbol]",
                    route_group="ticker",
                    is_authenticated=False,
                    created_at=now - timedelta(seconds=3),
                ),
                PageViewEvent(
                    user_id=None,
                    path="/ticker/:symbol",
                    normalized_path="/ticker/[symbol]",
                    route_group="ticker",
                    is_authenticated=False,
                    created_at=now - timedelta(seconds=4),
                ),
                PageViewEvent(
                    user_id=None,
                    path="",
                    normalized_path="/ticker/[symbol]",
                    route_group="ticker",
                    is_authenticated=False,
                    created_at=now - timedelta(seconds=5),
                ),
                PageViewEvent(
                    user_id=None,
                    path="/ticker/NVDA?tab=financials",
                    normalized_path="/ticker/[symbol]",
                    route_group="ticker",
                    is_authenticated=False,
                    created_at=now - timedelta(seconds=6),
                ),
                PageViewEvent(
                    user_id=None,
                    path="/ticker/AAPL",
                    normalized_path="/ticker/[symbol]",
                    route_group="ticker",
                    is_authenticated=False,
                    created_at=now - timedelta(seconds=7),
                ),
            ]
        )
        db.commit()

        with caplog.at_level("INFO", logger=queue_module.logger.name):
            symbols = queue_module._recently_viewed_ticker_symbols(db, limit=10)

        assert symbols == ["AAPL", "NVDA"]
        assert "[symbol]" not in symbols
        assert "prewarm_ticker_invalid_symbol_skipped source=recently_viewed symbol=[symbol]" not in caplog.text
    finally:
        db.close()


def test_priority_ticker_prewarm_counts_real_recently_viewed_tickers_without_template_noise(monkeypatch, caplog):
    db = _db()
    captured = []
    now = datetime.now(timezone.utc)
    try:
        db.add_all(
            [
                PageViewEvent(
                    user_id=None,
                    path="/ticker/AMD",
                    normalized_path="/ticker/[symbol]",
                    route_group="ticker",
                    is_authenticated=False,
                    created_at=now,
                ),
                PageViewEvent(
                    user_id=None,
                    path="/ticker/[symbol]",
                    normalized_path="/ticker/[symbol]",
                    route_group="ticker",
                    is_authenticated=False,
                    created_at=now - timedelta(seconds=1),
                ),
            ]
        )
        db.commit()

        def fake_enqueue(**kwargs):
            captured.append(kwargs)
            return True

        monkeypatch.setattr(queue_module, "enqueue_data_enrichment_job", fake_enqueue)

        with caplog.at_level("INFO", logger=queue_module.logger.name):
            result = enqueue_priority_ticker_prewarm_jobs(db, symbol_limit=2, popular_limit=0)

        assert result["recently_viewed_symbol_count"] == 1
        assert result["symbols"][0] == "AMD"
        assert all(job.get("symbol") != "[symbol]" for job in captured)
        assert "prewarm_ticker_invalid_symbol_skipped source=recently_viewed symbol=[symbol]" not in caplog.text
    finally:
        db.close()


def test_priority_ticker_prewarm_reports_enqueue_skip_reasons(monkeypatch):
    db = _db()
    try:
        db.add(Event(event_type="insider_trade", ts=datetime.now(timezone.utc), symbol="MSTR", source="test", payload_json="{}"))
        db.commit()

        monkeypatch.setattr(queue_module, "enqueue_data_enrichment_job", lambda **kwargs: False)
        monkeypatch.setattr(queue_module, "_enqueue_skip_reason", lambda **kwargs: "skipped_existing_pending")

        result = enqueue_priority_ticker_prewarm_jobs(db, symbol_limit=1, popular_limit=1)

        assert result["attempted"] == 10
        assert result["enqueued"] == 0
        assert result["skipped_existing_pending"] == 10
        assert result["skip_reasons"] == {"skipped_existing_pending": 10}
        assert result["skip_reasons_by_type"]["price_series"]["skipped_existing_pending"] == 2
    finally:
        db.close()
