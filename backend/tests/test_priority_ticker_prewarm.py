from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import app.services.data_enrichment_queue as queue_module
from app.db import Base
from app.models import Event, Security, Watchlist, WatchlistItem
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
