from __future__ import annotations

import json
from datetime import date, datetime, timezone

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.models import (
    AppSetting,
    DataEnrichmentJob,
    Event,
    Filing,
    GovernmentContractAction,
    Member,
    PriceCache,
    Security,
    TradeOutcome,
    Transaction,
)
from app.routers.events import list_events
from app.services.congress_outcome_coverage import repair_recent_congress_outcomes


def _session(tables=None):
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    Base.metadata.create_all(
        engine,
        tables=tables
        or [
            Event.__table__,
            TradeOutcome.__table__,
            GovernmentContractAction.__table__,
        ],
    )
    return Session()


def _recent_ingest_tables():
    return [
        AppSetting.__table__,
        Member.__table__,
        Security.__table__,
        Filing.__table__,
        Transaction.__table__,
        Event.__table__,
        TradeOutcome.__table__,
        DataEnrichmentJob.__table__,
    ]


def _congress_event(
    *,
    event_id: int = 1,
    symbol: str = "FCBN",
    asset_class: str = "stock",
    security_name: str = "First Citizens BancShares Inc",
) -> Event:
    event_dt = datetime(2026, 5, 19, tzinfo=timezone.utc)
    return Event(
        id=event_id,
        event_type="congress_trade",
        ts=event_dt,
        event_date=event_dt,
        symbol=symbol,
        source="senate_fmp",
        member_name="John Fetterman",
        member_bioguide_id="F000479",
        chamber="senate",
        party="D",
        trade_type="purchase",
        transaction_type="purchase",
        amount_min=1001,
        amount_max=15000,
        impact_score=0,
        payload_json=json.dumps(
            {
                "transaction_id": event_id * 10,
                "symbol": symbol,
                "ticker": symbol,
                "company_name": security_name,
                "security_name": security_name,
                "asset_class": asset_class,
                "trade_date": "2026-05-01",
                "report_date": "2026-05-19",
                "transaction_type": "purchase",
            }
        ),
    )


def _seed_congress_transaction(
    db,
    *,
    tx_id: int = 101,
    symbol: str = "AAPL",
    trade_date: date = date(2026, 5, 1),
    report_date: date = date(2026, 5, 19),
) -> None:
    member = Member(
        id=tx_id,
        bioguide_id=f"T{tx_id:06}",
        first_name="Test",
        last_name="Member",
        chamber="senate",
        party="I",
        state="CA",
    )
    security = Security(
        id=tx_id,
        symbol=symbol,
        name=f"{symbol} Corporation",
        asset_class="stock",
        sector="Technology",
    )
    filing = Filing(
        id=tx_id,
        member_id=member.id,
        source="senate_fmp",
        filing_date=report_date,
        document_url=f"https://example.test/{tx_id}",
        document_hash=f"doc-{tx_id}",
    )
    tx = Transaction(
        id=tx_id,
        filing_id=filing.id,
        member_id=member.id,
        security_id=security.id,
        owner_type="self",
        transaction_type="purchase",
        trade_date=trade_date,
        report_date=report_date,
        amount_range_min=1001,
        amount_range_max=15000,
        description=f"{symbol} Corporation",
    )
    db.add_all([member, security, filing, tx])
    db.commit()


def _ingest_metrics(*, inserted: int = 0, filings_scanned: int = 0) -> dict[str, int]:
    return {
        "inserted": inserted,
        "skipped": 0,
        "skipped_old": 0,
        "filings_scanned": filings_scanned,
        "non_equity_symbol_skipped": 0,
    }


def _patch_outcome_prices(monkeypatch, *, entry=10.0, current=12.0):
    def fake_entry(_db, symbol, target_date, **_kwargs):
        if symbol == "^GSPC":
            return {"close": 100.0, "status": "ok", "error": None, "symbol": symbol}
        if entry is None:
            return {"close": None, "status": "no_data", "error": f"No entry close for {symbol}", "symbol": symbol}
        return {"close": entry, "status": "ok", "error": None, "symbol": symbol}

    def fake_current(_db, symbols, **_kwargs):
        result = {}
        for symbol in symbols:
            if symbol == "^GSPC":
                result[symbol] = {"price": 110.0, "asof_ts": datetime(2026, 5, 20, tzinfo=timezone.utc)}
            elif current is not None:
                result[symbol] = {"price": current, "asof_ts": datetime(2026, 5, 20, tzinfo=timezone.utc)}
        return result

    monkeypatch.setattr("app.services.member_performance.get_eod_close_with_meta", fake_entry)
    monkeypatch.setattr("app.services.member_performance.get_current_prices_meta_db", fake_current)


def test_events_endpoint_uses_persisted_congress_outcome_fields(monkeypatch):
    db = _session()
    try:
        monkeypatch.setattr("app.routers.events.get_current_prices_meta_db", lambda *_args, **_kwargs: {})
        monkeypatch.setattr("app.routers.events.get_eod_close", lambda *_args, **_kwargs: None)
        monkeypatch.setattr("app.routers.events.get_confirmation_metrics_for_symbols", lambda *_args, **_kwargs: {})
        event = _congress_event(symbol="JPM", security_name="JPMorgan Chase & Co")
        db.add(event)
        db.flush()
        db.add(
            TradeOutcome(
                event_id=event.id,
                member_id="F000479",
                member_name="John Fetterman",
                symbol="JPM",
                trade_type="purchase",
                source="senate_fmp",
                trade_date=date(2026, 5, 1),
                entry_price=200.0,
                current_price=220.0,
                benchmark_symbol="^GSPC",
                return_pct=10.0,
                alpha_pct=4.0,
                amount_min=1001,
                amount_max=15000,
                scoring_status="ok",
                methodology_version="congress_v1",
                computed_at=datetime.now(timezone.utc),
            )
        )
        db.commit()

        page = list_events(db=db, member="John Fetterman", mode="congress", limit=10)

        assert page.items[0].estimated_price == 200.0
        assert page.items[0].price == 200.0
        assert page.items[0].current_price == 220.0
        assert page.items[0].pnl_pct == pytest.approx(10.0)
        assert page.items[0].return_pct == pytest.approx(10.0)
    finally:
        db.close()


def test_repair_detects_applies_and_is_idempotent_for_missing_congress_outcome(monkeypatch):
    db = _session()
    try:
        _patch_outcome_prices(monkeypatch)
        db.add(_congress_event())
        db.commit()

        dry = repair_recent_congress_outcomes(db, since_report_date=date(2026, 5, 1), dry_run=True)
        assert dry["eligible_missing_outcomes"] == 1
        assert dry["rows"][0]["symbol"] == "FCBN"
        assert dry["rows"][0]["proposed_estimated_price"] == 10.0
        assert round(dry["rows"][0]["proposed_pnl_pct"], 6) == 20.0

        applied = repair_recent_congress_outcomes(db, since_report_date=date(2026, 5, 1), dry_run=False)
        assert applied["inserted"] == 1
        outcome = db.execute(select(TradeOutcome)).scalar_one()
        assert outcome.event_id == 1
        assert outcome.entry_price == 10.0
        assert outcome.return_pct == pytest.approx(20.0)

        again = repair_recent_congress_outcomes(db, since_report_date=date(2026, 5, 1), dry_run=True)
        assert again["eligible_missing_outcomes"] == 0
    finally:
        db.close()


def test_repair_excludes_corporate_bond_from_equity_pnl(monkeypatch):
    db = _session()
    try:
        _patch_outcome_prices(monkeypatch)
        db.add(_congress_event(symbol="ATH", asset_class="Corporate Bond", security_name="Athene Corporate Bond"))
        db.commit()

        dry = repair_recent_congress_outcomes(db, since_report_date=date(2026, 5, 1), dry_run=True)

        assert dry["eligible_missing_outcomes"] == 0
        assert dry["rows"][0]["safe_to_apply"] is False
        assert dry["rows"][0]["skip_reason"] == "not_equity_outcome_eligible"
    finally:
        db.close()


def test_missing_price_data_records_clear_skip_reason(monkeypatch):
    db = _session()
    try:
        _patch_outcome_prices(monkeypatch, entry=None, current=12.0)
        db.add(_congress_event())
        db.commit()

        dry = repair_recent_congress_outcomes(db, since_report_date=date(2026, 5, 1), dry_run=True)

        assert dry["rows"][0]["proposed_estimated_price"] is None
        assert dry["rows"][0]["proposed_pnl_pct"] is None
        assert dry["rows"][0]["skip_reason"] == "no_data"
    finally:
        db.close()


def test_repair_computes_trade_outcome_when_price_cache_exists(monkeypatch):
    db = _session([Event.__table__, TradeOutcome.__table__, PriceCache.__table__])
    try:
        db.add(_congress_event(symbol="AAPL", security_name="Apple Inc"))
        db.add_all(
            [
                PriceCache(symbol="AAPL", date="2026-05-01", close=10.0),
                PriceCache(symbol="SPY", date="2026-05-01", close=100.0),
            ]
        )
        db.commit()

        def fake_current(_db, symbols, **_kwargs):
            asof = datetime(2026, 5, 20, tzinfo=timezone.utc)
            return {
                symbol: {"price": 110.0 if symbol == "SPY" else 12.0, "asof_ts": asof, "status": "ok"}
                for symbol in symbols
            }

        monkeypatch.setattr("app.services.member_performance.get_current_prices_meta_db", fake_current)

        applied = repair_recent_congress_outcomes(
            db,
            since_report_date=date(2026, 5, 1),
            dry_run=False,
            benchmark_symbol="SPY",
        )

        assert applied["inserted"] == 1
        outcome = db.execute(select(TradeOutcome)).scalar_one()
        assert outcome.entry_price == 10.0
        assert outcome.return_pct == pytest.approx(20.0)
        assert outcome.benchmark_entry_price == 100.0
        assert outcome.scoring_status == "ok"
    finally:
        db.close()


def test_recent_ingest_survives_rollback_prone_outcome_repair_and_dedupes(monkeypatch):
    db = _session(_recent_ingest_tables())
    Session = sessionmaker(bind=db.get_bind(), autoflush=False, autocommit=False)
    _seed_congress_transaction(db, symbol="AAPL")
    db.close()

    import app.ingest_congress_recent as recent_module

    monkeypatch.setattr(recent_module, "SessionLocal", Session)
    monkeypatch.setattr(recent_module, "ingest_house", lambda **_kwargs: _ingest_metrics())
    monkeypatch.setattr(recent_module, "ingest_senate", lambda **_kwargs: _ingest_metrics(inserted=1, filings_scanned=1))

    rolled_back = {"value": False}

    def rollback_entry(db_session, symbol, _target_date, **_kwargs):
        if symbol == "AAPL" and not rolled_back["value"]:
            rolled_back["value"] = True
            db_session.rollback()
            return {
                "close": None,
                "status": "provider_unavailable",
                "error": "missing_api_key",
                "symbol": symbol,
            }
        return {"close": 100.0, "status": "ok", "error": None, "symbol": symbol}

    def fake_current(_db, symbols, **_kwargs):
        asof = datetime(2026, 5, 20, tzinfo=timezone.utc)
        return {
            symbol: {
                "price": 110.0 if symbol == "^GSPC" else 12.0,
                "asof_ts": asof,
                "status": "ok",
            }
            for symbol in symbols
        }

    monkeypatch.setattr("app.services.member_performance.get_eod_close_with_meta", rollback_entry)
    monkeypatch.setattr("app.services.member_performance.get_current_prices_meta_db", fake_current)

    first = recent_module.run_recent_congress_ingest(days=9999, pages=1, limit=1, sleep_s=0)
    second = recent_module.run_recent_congress_ingest(days=9999, pages=1, limit=1, sleep_s=0)

    assert rolled_back["value"] is True
    assert first["events_inserted"] == 1
    assert first["outcome_coverage"]["inserted"] == 1
    assert second["events_inserted"] == 0

    with Session() as check:
        events = check.execute(select(Event)).scalars().all()
        jobs = check.execute(select(DataEnrichmentJob)).scalars().all()
        outcomes = check.execute(select(TradeOutcome)).scalars().all()
        status = check.get(AppSetting, recent_module.CONGRESS_RECENT_STATUS_KEY)

        assert len(events) == 1
        assert events[0].symbol == "AAPL"
        assert sorted(job.job_type for job in jobs) == ["pnl_refresh", "price_eod", "quote"]
        assert len(outcomes) == 1
        assert outcomes[0].event_id == events[0].id
        assert outcomes[0].scoring_status == "provider_unavailable"
        assert status is not None


def test_recent_ingest_dry_run_persists_nothing(monkeypatch):
    db = _session(_recent_ingest_tables())
    Session = sessionmaker(bind=db.get_bind(), autoflush=False, autocommit=False)
    _seed_congress_transaction(db, symbol="AAPL")
    db.close()

    import app.ingest_congress_recent as recent_module

    monkeypatch.setattr(recent_module, "SessionLocal", Session)
    monkeypatch.setattr(recent_module, "ingest_house", lambda **_kwargs: _ingest_metrics())
    monkeypatch.setattr(recent_module, "ingest_senate", lambda **_kwargs: _ingest_metrics(inserted=1, filings_scanned=1))

    result = recent_module.run_recent_congress_ingest(days=9999, pages=1, limit=1, sleep_s=0, dry_run=True)

    assert result["dry_run"] is True
    assert result["outcome_coverage"] == {"skipped": "dry_run"}
    with Session() as check:
        assert check.execute(select(Event)).scalars().all() == []
        assert check.execute(select(TradeOutcome)).scalars().all() == []
        assert check.execute(select(DataEnrichmentJob)).scalars().all() == []
        assert check.get(AppSetting, recent_module.CONGRESS_RECENT_STATUS_KEY) is None


def test_recent_ingest_runs_outcome_coverage_for_new_events(monkeypatch):
    tables = [
        AppSetting.__table__,
        Member.__table__,
        Security.__table__,
        Filing.__table__,
        Transaction.__table__,
        Event.__table__,
        TradeOutcome.__table__,
    ]
    db = _session(tables)
    Session = sessionmaker(bind=db.get_bind(), autoflush=False, autocommit=False)
    db.close()

    import app.ingest_congress_recent as recent_module

    _patch_outcome_prices(monkeypatch)
    monkeypatch.setattr(recent_module, "SessionLocal", Session)
    monkeypatch.setattr(recent_module, "ingest_house", lambda **_kwargs: {"inserted": 0, "skipped": 0, "skipped_old": 0, "filings_scanned": 0, "non_equity_symbol_skipped": 0})
    monkeypatch.setattr(recent_module, "ingest_senate", lambda **_kwargs: {"inserted": 1, "skipped": 0, "skipped_old": 0, "filings_scanned": 1, "non_equity_symbol_skipped": 0})

    def fake_insert(db_session, **_kwargs):
        db_session.add(_congress_event())
        return 1

    monkeypatch.setattr(recent_module, "insert_missing_congress_events_from_transactions", fake_insert)

    result = recent_module.run_recent_congress_ingest(days=9999, pages=1, limit=1, sleep_s=0)

    with Session() as check:
        outcome = check.execute(select(TradeOutcome)).scalar_one()
        assert result["outcome_coverage"]["inserted"] == 1
        assert outcome.symbol == "FCBN"
        assert outcome.entry_price == 10.0
