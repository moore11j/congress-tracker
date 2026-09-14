from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from sqlalchemy import func, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

import app.ingest_insider_trades as ingest_module
from app.db import Base
from app.models import DataEnrichmentJob, Event, InsiderTransaction, InsiderTransactionNormalized, SecForm4Filing, TradeOutcome
from app.services.profile_overviews import insiders_overview, profiles_summary


def _session_factory():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False)


def _fmp_row() -> dict:
    return {
        "symbol": "RBLX",
        "filingDate": date.today().isoformat(),
        "transactionDate": date.today().isoformat(),
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
    bump_calls: list[str] = []

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
    monkeypatch.setattr(
        ingest_module,
        "try_bump_feed_events_epoch",
        lambda *, reason: bump_calls.append(reason) or {"status": "ok", "epoch": "1", "reason": reason},
    )

    result = ingest_module.ingest_insider_trades(days=30, page_limit=1, per_page=100)

    assert result["status"] == "ok"
    assert result["inserted_events"] == 1
    assert result["inserted_normalized"] == 1
    assert result["feed_pnl_refresh"]["status"] == "ok"
    assert result["feed_pnl_refresh"]["pnl_refreshed"] == 1
    assert result["feed_cache_epoch"]["status"] == "ok"
    assert refresh_calls and len(refresh_calls[0]) == 1
    assert bump_calls == ["insider_ingest"]

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
    bump_calls: list[str] = []

    def fake_refresh(db, *, event_ids: list[int]) -> dict:
        nonlocal refresh_calls
        refresh_calls += 1
        return {"events_requested": len(event_ids), "events_scanned": len(event_ids), "pnl_refreshed": len(event_ids)}

    monkeypatch.setattr(ingest_module, "refresh_feed_pnl_events_now", fake_refresh)
    monkeypatch.setattr(
        ingest_module,
        "try_bump_feed_events_epoch",
        lambda *, reason: bump_calls.append(reason) or {"status": "ok", "epoch": str(len(bump_calls)), "reason": reason},
    )

    first = ingest_module.ingest_insider_trades(days=30, page_limit=1, per_page=100)
    second = ingest_module.ingest_insider_trades(days=30, page_limit=1, per_page=100)

    assert first["inserted_events"] == 1
    assert second["inserted_events"] == 0
    assert second["inserted_normalized"] == 0
    assert second["skipped"] == 1
    assert second["feed_pnl_refresh"]["status"] == "skipped"
    assert second["feed_cache_epoch"]["status"] == "skipped"
    assert refresh_calls == 1
    assert bump_calls == ["insider_ingest"]

    db = SessionLocal()
    try:
        assert db.execute(select(func.count()).select_from(Event)).scalar_one() == 1
        assert db.execute(select(func.count()).select_from(InsiderTransaction)).scalar_one() == 1
    finally:
        db.close()


def _configure_profile_ingest(monkeypatch, rows):
    factory = _session_factory()
    monkeypatch.setattr(ingest_module, "SessionLocal", factory)
    monkeypatch.setattr(ingest_module, "fetch_insider_trades", lambda page, limit: rows if page == 0 else [])
    monkeypatch.setattr(ingest_module, "_refresh_inserted_feed_pnl", lambda ids: {"status": "skipped"})
    monkeypatch.setattr(ingest_module, "try_bump_feed_events_epoch", lambda **kwargs: {"status": "ok"})
    return factory


def test_ingest_updates_both_profile_dashboards_without_backfill(monkeypatch):
    sale = _fmp_row()
    purchase = {**sale, "transactionType": "P-Purchase", "securitiesTransacted": 100, "price": 10}
    grant = {**sale, "transactionType": "A-Award", "securitiesTransacted": 200, "price": 10}
    factory = _configure_profile_ingest(monkeypatch, [sale, purchase, grant, sale])

    result = ingest_module.ingest_insider_trades(page_limit=1)

    assert result["inserted_normalized"] == 3
    assert result["inserted_events"] == 3
    with factory() as db:
        overview = insiders_overview(db)
        assert overview["summary"][0]["value"] == 2
        month = next(row for row in overview["monthly_activity"] if row["period"] == date.today().strftime("%b %y"))
        assert month["trades"] == 2
        assert month["buy_value"] == 1000
        assert month["sell_value"] == pytest.approx(1100 * 57.9424)
        assert len(overview["recent_notable_trades"]) == 2
        summary = profiles_summary(db)
        card = next(card for card in summary["cards"] if card["kind"] == "insiders")
        assert card["metrics"][0]["value"] == 2
        assert card["metrics"][1]["value"] == 1
        assert card["trend"]["points"][-1]["value"] == 2


def test_duplicate_feed_trade_repairs_missing_profile_record(monkeypatch):
    factory = _configure_profile_ingest(monkeypatch, [_fmp_row()])
    ingest_module.ingest_insider_trades(page_limit=1)
    with factory() as db:
        db.query(InsiderTransactionNormalized).delete()
        db.commit()

    repaired = ingest_module.ingest_insider_trades(page_limit=1)
    repeated = ingest_module.ingest_insider_trades(page_limit=1)

    assert repaired["inserted_events"] == repaired["inserted_raw"] == 0
    assert repaired["inserted_normalized"] == 1
    assert repeated["inserted_normalized"] == 0
    with factory() as db:
        assert db.query(Event).count() == 1
        assert db.query(InsiderTransactionNormalized).count() == 1
        assert db.query(SecForm4Filing).count() == 1


def test_profile_write_failure_rolls_back_feed_and_raw_trade(monkeypatch):
    factory = _configure_profile_ingest(monkeypatch, [_fmp_row()])

    def fail_sync(db, row):
        raise RuntimeError("profile write failed")

    monkeypatch.setattr(ingest_module, "sync_insider_transaction_normalized", fail_sync)
    with pytest.raises(RuntimeError, match="profile write failed"):
        ingest_module.ingest_insider_trades(page_limit=1)
    with factory() as db:
        assert db.query(Event).count() == 0
        assert db.query(InsiderTransaction).count() == 0
        assert db.query(InsiderTransactionNormalized).count() == 0
