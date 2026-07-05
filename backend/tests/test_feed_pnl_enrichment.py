from __future__ import annotations

import json
from datetime import datetime, timezone

from sqlalchemy import create_engine, event as sqlalchemy_event, select
from sqlalchemy.orm import Session, sessionmaker

import app.services.data_enrichment_queue as queue_module
from app.db import Base
from app.models import DataEnrichmentJob, Event, PriceCache, QuoteCache, TradeOutcome
from app.routers.events import list_events
from app.services.feed_pnl_enrichment import enqueue_feed_pnl_enrichment_for_event, process_feed_pnl_refresh_job


def _session_factory():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False)


def _event(event_id: int, event_type: str = "congress_trade", **kwargs) -> Event:
    now = datetime(2026, 6, 16, tzinfo=timezone.utc)
    payload = kwargs.pop("payload", {"trade_date": "2026-06-15", "asset_class": "equity"})
    return Event(
        id=event_id,
        event_type=event_type,
        ts=kwargs.pop("ts", now),
        event_date=kwargs.pop("event_date", now),
        symbol=kwargs.pop("symbol", "ACEL"),
        source=kwargs.pop("source", "test"),
        member_name=kwargs.pop("member_name", "Member"),
        member_bioguide_id=kwargs.pop("member_bioguide_id", "M1"),
        trade_type=kwargs.pop("trade_type", "purchase"),
        amount_min=kwargs.pop("amount_min", 1_000),
        amount_max=kwargs.pop("amount_max", 15_000),
        impact_score=0,
        payload_json=json.dumps(payload),
        **kwargs,
    )


def _stub_feed_dependencies(monkeypatch) -> None:
    monkeypatch.setattr("app.routers.events.get_confirmation_metrics_for_symbols", lambda *_args, **_kwargs: {})
    monkeypatch.setattr("app.routers.events._ticker_meta_with_security_names", lambda *_args, **_kwargs: {})
    monkeypatch.setattr("app.routers.events.get_cik_meta", lambda *_args, **_kwargs: {})


def test_new_insider_event_enqueues_targeted_feed_pnl_jobs_without_duplicates() -> None:
    SessionLocal = _session_factory()
    db = SessionLocal()
    try:
        event = _event(
            101,
            "insider_trade",
            symbol="ADSK",
            trade_type="sale",
            member_name=None,
            member_bioguide_id=None,
            payload={
                "symbol": "ADSK",
                "transaction_date": "2026-06-15",
                "transaction_type": "S-Sale",
                "is_market_trade": True,
                "insider_name": "Jane CFO",
                "reporting_cik": "0000000001",
            },
        )
        db.add(event)
        db.flush()

        first = enqueue_feed_pnl_enrichment_for_event(db, event, use_current_session=True)
        second = enqueue_feed_pnl_enrichment_for_event(db, event, use_current_session=True)
        db.commit()

        rows = db.execute(select(DataEnrichmentJob).order_by(DataEnrichmentJob.job_type)).scalars().all()
        assert {row.job_type for row in rows} == {"price_eod", "pnl_refresh", "quote"}
        assert first["quote_enqueued"] is True
        assert first["price_eod_enqueued"] is True
        assert first["pnl_refresh_enqueued"] is True
        assert second["quote_enqueued"] is False
        assert second["price_eod_enqueued"] is False
        assert second["pnl_refresh_enqueued"] is False
        assert len(rows) == 3
        assert {row.symbol for row in rows} == {"ADSK"}
        assert {row.date_key for row in rows if row.date_key} == {"2026-06-15"}
    finally:
        db.close()


def test_new_congress_event_enqueues_targeted_feed_pnl_jobs() -> None:
    SessionLocal = _session_factory()
    db = SessionLocal()
    try:
        event = _event(102, "congress_trade", symbol="BLND")
        db.add(event)
        db.flush()

        result = enqueue_feed_pnl_enrichment_for_event(db, event, use_current_session=True)
        db.commit()

        rows = db.execute(select(DataEnrichmentJob)).scalars().all()
        assert result["eligible"] is True
        assert {row.job_type for row in rows} == {"price_eod", "pnl_refresh", "quote"}
        assert {row.symbol for row in rows} == {"BLND"}
        priorities = {row.job_type: row.priority for row in rows}
        assert priorities == {"quote": 5, "price_eod": 6, "pnl_refresh": 7}
    finally:
        db.close()


def test_congress_etf_fund_event_enqueues_targeted_feed_pnl_jobs() -> None:
    SessionLocal = _session_factory()
    db = SessionLocal()
    try:
        event = _event(
            103,
            "congress_trade",
            symbol="IWM",
            payload={
                "symbol": "IWM",
                "ticker": "IWM",
                "trade_date": "2026-05-27",
                "asset_class": "etf_fund",
                "security_name": "iShares Trust - iShares Russell 2000 ETF",
                "transaction_type": "sale",
            },
        )
        db.add(event)
        db.flush()

        result = enqueue_feed_pnl_enrichment_for_event(db, event, use_current_session=True)
        db.commit()

        rows = db.execute(select(DataEnrichmentJob)).scalars().all()
        assert result["eligible"] is True
        assert result["skipped_reason"] is None
        assert {row.job_type for row in rows} == {"price_eod", "pnl_refresh", "quote"}
        assert {row.symbol for row in rows} == {"IWM"}
        assert {row.date_key for row in rows if row.date_key} == {"2026-05-27"}
    finally:
        db.close()


def test_structurally_unpriceable_event_writes_outcome_instead_of_updating() -> None:
    SessionLocal = _session_factory()
    db = SessionLocal()
    try:
        event = _event(
            104,
            "congress_trade",
            symbol=None,
            payload={
                "trade_date": "2026-05-27",
                "asset_class": "other",
                "security_name": "Unresolved security",
                "transaction_type": "purchase",
            },
        )
        db.add(event)
        db.flush()

        result = enqueue_feed_pnl_enrichment_for_event(db, event, use_current_session=True)
        db.commit()

        outcome = db.execute(select(TradeOutcome).where(TradeOutcome.event_id == 104)).scalar_one()
        assert result["eligible"] is False
        assert result["structural_outcome_written"] is True
        assert result["skipped_reason"] == "no_symbol"
        assert outcome.return_pct is None
        assert outcome.scoring_status == "no_symbol"
        assert outcome.methodology_version == "feed_pnl_cache_v1"
        assert db.execute(select(DataEnrichmentJob)).scalars().all() == []
    finally:
        db.close()


def test_quote_and_price_eod_workers_populate_caches(monkeypatch) -> None:
    SessionLocal = _session_factory()
    monkeypatch.setattr(queue_module, "SessionLocal", SessionLocal)
    monkeypatch.setenv("ENRICHMENT_QUEUE_ENABLED", "true")

    def fake_quote(db: Session, symbols: list[str], **_kwargs):
        for symbol in symbols:
            db.merge(QuoteCache(symbol=symbol, price=20.0, asof_ts=datetime(2026, 6, 16)))
        return {symbol: {"price": 20.0, "asof_ts": datetime(2026, 6, 16)} for symbol in symbols}

    def fake_eod(db: Session, symbol: str, date_key: str, **_kwargs):
        db.merge(PriceCache(symbol=symbol, date=date_key, close=10.0))
        return {"close": 10.0, "status": "ok", "date": date_key}

    monkeypatch.setattr("app.services.quote_lookup.get_current_prices_meta_db", fake_quote)
    monkeypatch.setattr("app.services.price_lookup.get_eod_close_with_meta", fake_eod)

    db = SessionLocal()
    try:
        db.add_all(
            [
                DataEnrichmentJob(
                    job_type="quote",
                    symbol="ACEL",
                    dedupe_key="quote|ACEL||",
                    priority=1,
                    status="queued",
                    attempts=0,
                    max_attempts=3,
                    source="test",
                    reason="test",
                    next_run_at=datetime.now(timezone.utc),
                ),
                DataEnrichmentJob(
                    job_type="price_eod",
                    symbol="ACEL",
                    date_key="2026-06-15",
                    dedupe_key="price_eod|ACEL|2026-06-15|",
                    priority=2,
                    status="queued",
                    attempts=0,
                    max_attempts=3,
                    source="test",
                    reason="test",
                    next_run_at=datetime.now(timezone.utc),
                ),
            ]
        )
        db.commit()
    finally:
        db.close()

    summary = queue_module.process_data_enrichment_jobs(limit=2)

    db = SessionLocal()
    try:
        assert summary["succeeded"] == 2
        assert db.get(QuoteCache, "ACEL").price == 20.0
        assert db.get(PriceCache, ("ACEL", "2026-06-15")).close == 10.0
    finally:
        db.close()


def test_event_scoped_pnl_refresh_writes_trade_outcome_when_inputs_exist(monkeypatch) -> None:
    SessionLocal = _session_factory()
    monkeypatch.setattr(queue_module, "SessionLocal", SessionLocal)
    monkeypatch.setenv("ENRICHMENT_QUEUE_ENABLED", "true")

    db = SessionLocal()
    try:
        db.add(_event(201, "congress_trade", symbol="ACEL"))
        db.add(QuoteCache(symbol="ACEL", price=13.0, asof_ts=datetime(2026, 6, 16)))
        db.add(PriceCache(symbol="ACEL", date="2026-06-15", close=10.0))
        db.add(
            DataEnrichmentJob(
                job_type="pnl_refresh",
                symbol="ACEL",
                date_key="2026-06-15",
                window_key="event:201",
                dedupe_key="pnl_refresh|ACEL|2026-06-15|event:201",
                priority=1,
                status="queued",
                attempts=0,
                max_attempts=3,
                source="test",
                reason="test",
                payload_json=json.dumps({"event_id": 201}),
                next_run_at=datetime.now(timezone.utc),
            )
        )
        db.commit()
    finally:
        db.close()

    summary = queue_module.process_data_enrichment_jobs(limit=1)

    db = SessionLocal()
    try:
        outcome = db.execute(select(TradeOutcome).where(TradeOutcome.event_id == 201)).scalar_one()
        assert summary == {"processed": 1, "succeeded": 1, "failed": 0, "skipped": 0}
        assert round(outcome.return_pct or 0, 2) == 30.0
        assert outcome.scoring_status == "ok"
        assert outcome.methodology_version == "feed_pnl_cache_v1"
    finally:
        db.close()


def test_event_scoped_pnl_refresh_updates_existing_insider_outcome_without_methodology_downgrade(monkeypatch) -> None:
    SessionLocal = _session_factory()
    monkeypatch.setattr(queue_module, "SessionLocal", SessionLocal)
    monkeypatch.setenv("ENRICHMENT_QUEUE_ENABLED", "true")

    def fake_quote(db: Session, symbols: list[str], **_kwargs):
        for symbol in symbols:
            db.merge(QuoteCache(symbol=symbol, price=117.91, asof_ts=datetime(2026, 7, 2)))
        return {symbol: {"price": 117.91, "asof_ts": datetime(2026, 7, 2)} for symbol in symbols}

    monkeypatch.setattr("app.services.quote_lookup.get_current_prices_meta_db", fake_quote)

    db = SessionLocal()
    try:
        db.add(
            _event(
                203,
                "insider_trade",
                symbol="MGRC",
                trade_type="sale",
                event_date=datetime(2026, 7, 1, tzinfo=timezone.utc),
                payload={
                    "symbol": "MGRC",
                    "transaction_date": "2026-07-01",
                    "transaction_type": "S-Sale",
                    "is_market_trade": True,
                    "price": 121.2318,
                    "insider_name": "Joseph F Hanna",
                    "reporting_cik": "0000000001",
                },
            )
        )
        db.add(PriceCache(symbol="MGRC", date="2026-07-01", close=119.89))
        db.add(
            TradeOutcome(
                event_id=203,
                member_id="M1",
                member_name="Member",
                symbol="MGRC",
                trade_type="sale",
                trade_date=datetime(2026, 7, 1, tzinfo=timezone.utc).date(),
                entry_price=119.89,
                current_price=78.67,
                current_price_date=datetime(2026, 7, 3, tzinfo=timezone.utc).date(),
                return_pct=34.39,
                benchmark_symbol="^GSPC",
                benchmark_return_pct=1.2,
                alpha_pct=33.19,
                scoring_status="ok",
                methodology_version="insider_v1",
            )
        )
        db.commit()

        process_feed_pnl_refresh_job(db, event_id=203)
        db.commit()

        outcome = db.execute(select(TradeOutcome).where(TradeOutcome.event_id == 203)).scalar_one()
        assert outcome.methodology_version == "insider_v1"
        assert outcome.entry_price == 121.2318
        assert outcome.current_price == 117.91
        assert outcome.entry_price_date == datetime(2026, 7, 1, tzinfo=timezone.utc).date()
        assert outcome.current_price_date == datetime(2026, 7, 2, tzinfo=timezone.utc).date()
        assert round(outcome.return_pct or 0, 2) == 2.74
        assert outcome.benchmark_return_pct == 1.2
        assert outcome.alpha_pct == 33.19
    finally:
        db.close()


def test_event_scoped_pnl_refresh_stays_retryable_when_inputs_missing(monkeypatch) -> None:
    SessionLocal = _session_factory()
    monkeypatch.setattr(queue_module, "SessionLocal", SessionLocal)
    monkeypatch.setenv("ENRICHMENT_QUEUE_ENABLED", "true")

    db = SessionLocal()
    try:
        db.add(_event(202, "congress_trade", symbol="RARE"))
        db.add(
            DataEnrichmentJob(
                job_type="pnl_refresh",
                symbol="RARE",
                date_key="2026-06-15",
                window_key="event:202",
                dedupe_key="pnl_refresh|RARE|2026-06-15|event:202",
                priority=1,
                status="queued",
                attempts=0,
                max_attempts=3,
                source="test",
                reason="test",
                payload_json=json.dumps({"event_id": 202}),
                next_run_at=datetime.now(timezone.utc),
            )
        )
        db.commit()
    finally:
        db.close()

    summary = queue_module.process_data_enrichment_jobs(limit=1)

    db = SessionLocal()
    try:
        job = db.execute(
            select(DataEnrichmentJob).where(DataEnrichmentJob.job_type == "pnl_refresh")
        ).scalar_one()
        assert summary["failed"] == 1
        assert job.status == "queued"
        assert job.reason == "feed_pnl_input_missing"
        assert job.attempts == 1
        assert db.execute(select(TradeOutcome).where(TradeOutcome.event_id == 202)).scalar_one_or_none() is None
        assert {
            row.job_type
            for row in db.execute(select(DataEnrichmentJob).where(DataEnrichmentJob.job_type != "pnl_refresh")).scalars()
        } == {"quote", "price_eod"}
    finally:
        db.close()


def test_events_endpoint_returns_updating_without_provider_when_outcome_missing(monkeypatch) -> None:
    db = _session_factory()()
    try:
        _stub_feed_dependencies(monkeypatch)
        monkeypatch.setattr("app.routers.events.get_current_prices_meta_db", lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("provider called")))
        monkeypatch.setattr("app.routers.events.get_eod_close", lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("provider called")))
        enqueued: list[int] = []
        monkeypatch.setattr(
            "app.routers.events.enqueue_feed_pnl_enrichment_for_events",
            lambda _db, events, **_kwargs: enqueued.extend(event.id for event in events) or {"events": len(events)},
        )
        db.add(_event(301, "congress_trade", symbol="ORKA"))
        db.commit()

        page = list_events(db=db, mode="all", limit=10, enrich_prices=True)

        assert [item.id for item in page.items] == [301]
        assert page.items[0].pnl_pct is None
        assert page.items[0].outcome_status is None
        assert enqueued == [301]
    finally:
        db.close()


def test_events_endpoint_enrich_prices_false_skips_feed_pnl_enqueue(monkeypatch) -> None:
    db = _session_factory()()
    try:
        _stub_feed_dependencies(monkeypatch)
        monkeypatch.setattr("app.routers.events.get_current_prices_meta_db", lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("provider called")))
        monkeypatch.setattr("app.routers.events.get_eod_close", lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("provider called")))
        monkeypatch.setattr("app.routers.events._load_trade_outcomes_for_events", lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("outcomes called")))
        monkeypatch.setattr("app.routers.events.enqueue_feed_pnl_enrichment_for_events", lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("enqueue called")))
        db.add(_event(351, "congress_trade", symbol="ORKA"))
        db.commit()

        page = list_events(db=db, mode="all", limit=10, page_size=10, enrich_prices=False)

        assert [item.id for item in page.items] == [351]
        assert page.items[0].pnl_pct is None
        assert page.items[0].current_price is None
        assert page.total is None
        assert page.limit == 10
        assert page.offset == 0
    finally:
        db.close()


def test_events_endpoint_batches_missing_outcome_enqueue(monkeypatch) -> None:
    db = _session_factory()()
    try:
        _stub_feed_dependencies(monkeypatch)
        monkeypatch.setattr("app.routers.events.get_current_prices_meta_db", lambda *_args, **_kwargs: {})
        monkeypatch.setattr("app.routers.events.get_eod_close", lambda *_args, **_kwargs: None)
        batches: list[list[int]] = []

        def _capture_batch(_db, events, **kwargs):
            batches.append([event.id for event in events])
            assert kwargs["use_current_session"] is True
            return {"events": len(events)}

        monkeypatch.setattr("app.routers.events.enqueue_feed_pnl_enrichment_for_events", _capture_batch)
        db.add_all([
            _event(352, "congress_trade", symbol="ORKA"),
            _event(353, "insider_trade", symbol="BLND"),
        ])
        db.commit()

        page = list_events(db=db, mode="all", limit=10, enrich_prices=True)

        assert sorted(item.id for item in page.items) == [352, 353]
        assert len(batches) == 1
        assert sorted(batches[0]) == [352, 353]
    finally:
        db.close()


def test_events_endpoint_include_total_false_skips_count_query(monkeypatch) -> None:
    SessionLocal = _session_factory()
    engine = SessionLocal.kw["bind"]
    statements: list[str] = []

    def _capture_sql(_conn, _cursor, statement, _parameters, _context, _executemany):
        statements.append(statement.lower())

    sqlalchemy_event.listen(engine, "before_cursor_execute", _capture_sql)
    db = SessionLocal()
    try:
        _stub_feed_dependencies(monkeypatch)
        db.add(
            _event(
                354,
                "congress_trade",
                symbol=None,
                member_bioguide_id=None,
                amount_min=None,
                amount_max=None,
                payload={"asset_class": "other"},
            )
        )
        db.commit()
        statements.clear()

        page = list_events(db=db, mode="all", limit=10, include_total=False, enrich_prices=False)

        assert [item.id for item in page.items] == [354]
        assert page.total is None
        assert page.has_more is False
        assert not any("count(" in statement or "count(*)" in statement for statement in statements)
    finally:
        db.close()
        sqlalchemy_event.remove(engine, "before_cursor_execute", _capture_sql)


def test_events_endpoint_include_total_false_returns_has_more_without_count(monkeypatch) -> None:
    SessionLocal = _session_factory()
    engine = SessionLocal.kw["bind"]
    statements: list[str] = []

    def _capture_sql(_conn, _cursor, statement, _parameters, _context, _executemany):
        statements.append(statement.lower())

    sqlalchemy_event.listen(engine, "before_cursor_execute", _capture_sql)
    db = SessionLocal()
    try:
        _stub_feed_dependencies(monkeypatch)
        db.add_all([
            _event(
                355,
                "congress_trade",
                symbol=None,
                member_bioguide_id=None,
                amount_min=None,
                amount_max=None,
                payload={"asset_class": "other"},
            ),
            _event(
                356,
                "congress_trade",
                symbol=None,
                member_bioguide_id=None,
                amount_min=None,
                amount_max=None,
                payload={"asset_class": "other"},
            ),
        ])
        db.commit()
        statements.clear()

        page = list_events(db=db, mode="all", limit=1, include_total=False, enrich_prices=False)

        assert len(page.items) == 1
        assert page.total is None
        assert page.has_more is True
        assert not any("count(" in statement or "count(*)" in statement for statement in statements)
    finally:
        db.close()
        sqlalchemy_event.remove(engine, "before_cursor_execute", _capture_sql)


def test_events_endpoint_treats_retryable_missing_outcome_as_updating(monkeypatch) -> None:
    db = _session_factory()()
    try:
        _stub_feed_dependencies(monkeypatch)
        monkeypatch.setattr("app.routers.events.get_current_prices_meta_db", lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("provider called")))
        monkeypatch.setattr("app.routers.events.get_eod_close", lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("provider called")))
        enqueued: list[int] = []
        monkeypatch.setattr(
            "app.routers.events.enqueue_feed_pnl_enrichment_for_events",
            lambda _db, events, **_kwargs: enqueued.extend(event.id for event in events) or {"events": len(events)},
        )
        db.add(_event(303, "insider_trade", symbol="BLND"))
        db.add(
            TradeOutcome(
                event_id=303,
                member_id="M1",
                member_name="Member",
                symbol="BLND",
                trade_type="purchase",
                trade_date=datetime(2026, 6, 15, tzinfo=timezone.utc).date(),
                entry_price=1.68,
                current_price=None,
                return_pct=None,
                benchmark_symbol="^GSPC",
                scoring_status="provider_429",
                methodology_version="insider_v1",
            )
        )
        db.commit()

        page = list_events(db=db, mode="all", limit=10, enrich_prices=True)

        assert [item.id for item in page.items] == [303]
        assert page.items[0].pnl_pct is None
        assert page.items[0].outcome_status is None
        assert enqueued == [303]
    finally:
        db.close()


def test_events_endpoint_returns_pnl_once_trade_outcome_exists(monkeypatch) -> None:
    db = _session_factory()()
    try:
        _stub_feed_dependencies(monkeypatch)
        monkeypatch.setattr("app.routers.events.get_current_prices_meta_db", lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("provider called")))
        monkeypatch.setattr("app.routers.events.get_eod_close", lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("provider called")))
        db.add(_event(302, "congress_trade", symbol="LINC"))
        db.add(
            TradeOutcome(
                event_id=302,
                member_id="M1",
                member_name="Member",
                symbol="LINC",
                trade_type="purchase",
                trade_date=datetime(2026, 6, 15, tzinfo=timezone.utc).date(),
                entry_price=10.0,
                current_price=12.5,
                return_pct=25.0,
                benchmark_symbol="^GSPC",
                scoring_status="ok",
                methodology_version="feed_pnl_cache_v1",
            )
        )
        db.commit()

        page = list_events(db=db, mode="all", limit=10, enrich_prices=True)

        assert page.items[0].pnl_pct == 25.0
        assert page.items[0].outcome_status == "ok"
    finally:
        db.close()
