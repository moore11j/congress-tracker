from __future__ import annotations

from datetime import date, datetime, timezone

from sqlalchemy import func, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

import app.ingest_insider_trades as ingest_module
from app.db import Base
from app.models import DataEnrichmentJob, Event, InsiderTransaction, TradeOutcome


def _session_factory():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False)


def _fmp_row() -> dict:
    return {
        "symbol": "RBLX",
        "filingDate": "2026-07-07",
        "transactionDate": "2026-07-06",
        "reportingCik": "0001835037",
        "insiderName": "Reinstra Mark",
        "transactionType": "S-Sale",
        "securitiesTransacted": 1100,
        "price": 57.9424,
        "companyName": "Roblox Corp",
        "securityName": "Class A Common Stock",
        "officerTitle": "Chief Legal Officer",
    }


def test_insider_ingest_refreshes_feed_pnl_after_event_commit(monkeypatch) -> None:
    SessionLocal = _session_factory()
    monkeypatch.setattr(ingest_module, "SessionLocal", SessionLocal)
    monkeypatch.setattr(
        ingest_module,
        "fetch_insider_trades",
        lambda page, limit: [_fmp_row()] if page == 0 else [],
    )

    refresh_calls: list[list[int]] = []

    def fake_refresh(db, *, event_ids: list[int]) -> dict:
        refresh_calls.append(list(event_ids))
        event = db.get(Event, event_ids[0])
        assert event is not None
        assert event.symbol == "RBLX"
        assert event.event_type == "insider_trade"
        db.add(
            TradeOutcome(
                event_id=event.id,
                symbol=event.symbol,
                trade_type=event.trade_type,
                source=event.source,
                trade_date=date(2026, 7, 6),
                entry_price=57.9424,
                entry_price_date=date(2026, 7, 6),
                current_price=56.71,
                current_price_date=date(2026, 7, 8),
                return_pct=2.1269398575,
                scoring_status="ok",
                methodology_version="feed_pnl_cache_v1",
                computed_at=datetime.now(timezone.utc),
            )
        )
        db.commit()
        return {
            "events_requested": 1,
            "events_scanned": 1,
            "symbols_requested": 1,
            "symbols_refreshed": 1,
            "pnl_refreshed": 1,
            "pnl_missing_inputs": 0,
            "pnl_failed": 0,
            "symbols_affected": ["RBLX"],
        }

    monkeypatch.setattr(ingest_module, "refresh_feed_pnl_events_now", fake_refresh)

    result = ingest_module.ingest_insider_trades(days=30, page_limit=1, per_page=100)

    assert result["status"] == "ok"
    assert result["inserted_events"] == 1
    assert result["feed_pnl_refresh"]["status"] == "ok"
    assert result["feed_pnl_refresh"]["pnl_refreshed"] == 1
    assert refresh_calls and len(refresh_calls[0]) == 1

    db = SessionLocal()
    try:
        assert db.execute(select(func.count()).select_from(Event)).scalar_one() == 1
        assert db.execute(select(func.count()).select_from(InsiderTransaction)).scalar_one() == 1
        outcome = db.execute(select(TradeOutcome)).scalar_one()
        assert outcome.symbol == "RBLX"
        assert outcome.current_price == 56.71
        assert outcome.scoring_status == "ok"
        job_types = {row.job_type for row in db.execute(select(DataEnrichmentJob)).scalars()}
        assert job_types == {"price_eod", "pnl_refresh", "quote"}
    finally:
        db.close()


def test_insider_ingest_dedupes_events_and_does_not_refresh_duplicate(monkeypatch) -> None:
    SessionLocal = _session_factory()
    monkeypatch.setattr(ingest_module, "SessionLocal", SessionLocal)
    monkeypatch.setattr(
        ingest_module,
        "fetch_insider_trades",
        lambda page, limit: [_fmp_row()] if page == 0 else [],
    )
    refresh_calls = 0

    def fake_refresh(db, *, event_ids: list[int]) -> dict:
        nonlocal refresh_calls
        refresh_calls += 1
        return {"events_requested": len(event_ids), "events_scanned": len(event_ids), "pnl_refreshed": len(event_ids)}

    monkeypatch.setattr(ingest_module, "refresh_feed_pnl_events_now", fake_refresh)

    first = ingest_module.ingest_insider_trades(days=30, page_limit=1, per_page=100)
    second = ingest_module.ingest_insider_trades(days=30, page_limit=1, per_page=100)

    assert first["inserted_events"] == 1
    assert second["inserted_events"] == 0
    assert second["skipped"] == 1
    assert second["feed_pnl_refresh"]["status"] == "skipped"
    assert refresh_calls == 1

    db = SessionLocal()
    try:
        assert db.execute(select(func.count()).select_from(Event)).scalar_one() == 1
        assert db.execute(select(func.count()).select_from(InsiderTransaction)).scalar_one() == 1
    finally:
        db.close()
