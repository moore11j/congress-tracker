from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone

from sqlalchemy import create_engine, func, select
from sqlalchemy.dialects import postgresql, sqlite
from sqlalchemy.orm import Session, sessionmaker

import app.compute_replicated_portfolios as compute_module
import app.backfill_price_cache as backfill_module
from app.db import Base
from app.models import Event, PriceCache, ReplicatedPortfolioPoint, ReplicatedPortfolioPosition, ReplicatedPortfolioRun
from app.routers.events import insider_portfolio_performance
from app.services.replicated_portfolios import (
    PortfolioTradeEvent,
    load_replicated_portfolio_events,
    run_replicated_portfolio_simulation,
    simulate_replicated_portfolio,
    skip_reason_summary,
)


def _session():
    engine = create_engine("sqlite:///:memory:", future=True)
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    Base.metadata.create_all(bind=engine)
    return SessionLocal()


def _session_factory():
    engine = create_engine("sqlite:///:memory:", future=True)
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    Base.metadata.create_all(bind=engine)
    return engine, SessionLocal


def _event(
    *,
    event_id: int,
    symbol: str,
    side: str,
    transaction_date: date,
    public_date: date | None = None,
    issuer_cik: str | None = None,
    reporting_cik: str = "0000001111",
) -> PortfolioTradeEvent:
    return PortfolioTradeEvent(
        event_id=event_id,
        entity_type="insider",
        entity_id=reporting_cik,
        symbol=symbol,
        side=side,
        transaction_date=transaction_date,
        public_date=public_date or transaction_date,
        issuer_cik=issuer_cik,
        issuer_symbol=symbol,
    )


def _date_keys(start: date, end: date) -> list[str]:
    return [(start + timedelta(days=offset)).isoformat() for offset in range((end - start).days + 1)]


def test_buy_only_portfolio_continues_moving_daily_after_purchase():
    simulation = simulate_replicated_portfolio(
        events=[_event(event_id=1, symbol="AAPL", side="purchase", transaction_date=date(2026, 1, 2))],
        price_histories={"AAPL": {"2026-01-02": 100.0, "2026-01-03": 110.0, "2026-01-04": 121.0}},
        benchmark_history={"2026-01-02": 100.0, "2026-01-03": 100.0, "2026-01-04": 100.0},
        start_date=date(2026, 1, 2),
        end_date=date(2026, 1, 4),
        mode="realistic_disclosure_lag",
    )

    values = [point.strategy_value for point in simulation.points]
    assert values == [100000.0, 110000.0, 121000.0]
    assert [point.active_positions for point in simulation.points] == [1, 1, 1]


def test_1095_day_run_does_not_collapse_to_first_trade_date_when_benchmark_exists():
    db = _session()
    try:
        start = date(2023, 1, 1)
        end = start + timedelta(days=1095)
        trade_day = end - timedelta(days=20)
        ts = datetime.combine(trade_day, datetime.min.time(), tzinfo=timezone.utc)
        db.add(
            Event(
                id=501,
                event_type="congress_trade",
                ts=ts,
                event_date=ts,
                symbol="AAPL",
                source="test",
                trade_type="purchase",
                member_bioguide_id="M001",
                payload_json=json.dumps(
                    {
                        "symbol": "AAPL",
                        "trade_date": trade_day.isoformat(),
                        "report_date": trade_day.isoformat(),
                        "asset_class": "equity",
                    }
                ),
                amount_min=1000,
                amount_max=15000,
            )
        )
        for offset, day in enumerate(_date_keys(start, end)):
            db.add(PriceCache(symbol="^GSPC", date=day, close=100.0 + offset))
        for offset, day in enumerate(_date_keys(trade_day, end)):
            db.add(PriceCache(symbol="AAPL", date=day, close=100.0 + offset))
        db.commit()

        simulation = run_replicated_portfolio_simulation(
            db,
            entity_type="congress_member",
            entity_id="M001",
            lookback_days=1095,
            mode="realistic_disclosure_lag",
            benchmark="^GSPC",
            end_date=end,
        )

        assert simulation.points[0].asof_date == start
        assert simulation.points[-1].asof_date == end
        assert simulation.summary.points_count == 1096
        assert simulation.coverage.benchmark_points_loaded == 1096
        assert simulation.coverage.actual_start_date == start
        assert simulation.coverage.calendar_source == "benchmark"
    finally:
        db.close()


def test_curve_does_not_go_flat_while_holdings_remain_open():
    simulation = simulate_replicated_portfolio(
        events=[_event(event_id=2, symbol="MSFT", side="purchase", transaction_date=date(2026, 1, 2))],
        price_histories={"MSFT": {"2026-01-02": 50.0, "2026-01-03": 60.0, "2026-01-04": 45.0}},
        benchmark_history={"2026-01-02": 100.0, "2026-01-03": 100.0, "2026-01-04": 100.0},
        start_date=date(2026, 1, 2),
        end_date=date(2026, 1, 4),
        mode="realistic_disclosure_lag",
    )

    returns = [point.strategy_return_pct for point in simulation.points]
    assert returns == [0.0, 20.0, -10.0]
    assert simulation.positions[0].status == "open"


def test_sell_closes_matching_position_and_stops_exposure():
    simulation = simulate_replicated_portfolio(
        events=[
            _event(event_id=3, symbol="NVDA", side="purchase", transaction_date=date(2026, 1, 2)),
            _event(event_id=4, symbol="NVDA", side="sale", transaction_date=date(2026, 1, 3)),
        ],
        price_histories={"NVDA": {"2026-01-02": 100.0, "2026-01-03": 120.0, "2026-01-04": 240.0}},
        benchmark_history={"2026-01-02": 100.0, "2026-01-03": 100.0, "2026-01-04": 100.0},
        start_date=date(2026, 1, 2),
        end_date=date(2026, 1, 4),
        mode="realistic_disclosure_lag",
    )

    assert simulation.positions[0].status == "closed"
    assert simulation.positions[0].exit_date == date(2026, 1, 3)
    assert simulation.points[-1].strategy_value == 120000.0
    assert simulation.points[-1].active_positions == 0
    assert simulation.points[-1].cash_pct == 100.0


def test_disclosure_lag_mode_uses_public_date_not_transaction_date():
    event = _event(
        event_id=5,
        symbol="AAPL",
        side="purchase",
        transaction_date=date(2026, 1, 2),
        public_date=date(2026, 1, 4),
    )
    simulation = simulate_replicated_portfolio(
        events=[event],
        price_histories={"AAPL": {"2026-01-02": 100.0, "2026-01-03": 150.0, "2026-01-04": 200.0}},
        benchmark_history={"2026-01-02": 100.0, "2026-01-03": 100.0, "2026-01-04": 100.0},
        start_date=date(2026, 1, 2),
        end_date=date(2026, 1, 4),
        mode="realistic_disclosure_lag",
    )

    assert simulation.positions[0].entry_date == date(2026, 1, 4)
    assert simulation.positions[0].entry_price == 200.0
    assert simulation.points[-1].strategy_value == 100000.0


def test_theoretical_mode_uses_transaction_date():
    event = _event(
        event_id=6,
        symbol="AAPL",
        side="purchase",
        transaction_date=date(2026, 1, 2),
        public_date=date(2026, 1, 4),
    )
    simulation = simulate_replicated_portfolio(
        events=[event],
        price_histories={"AAPL": {"2026-01-02": 100.0, "2026-01-03": 150.0, "2026-01-04": 200.0}},
        benchmark_history={"2026-01-02": 100.0, "2026-01-03": 100.0, "2026-01-04": 100.0},
        start_date=date(2026, 1, 2),
        end_date=date(2026, 1, 4),
        mode="theoretical_transaction_date",
    )

    assert simulation.positions[0].entry_date == date(2026, 1, 2)
    assert simulation.points[-1].strategy_value == 200000.0


def test_unpriceable_events_are_skipped_with_reason():
    simulation = simulate_replicated_portfolio(
        events=[_event(event_id=7, symbol="ZZZZ", side="purchase", transaction_date=date(2026, 1, 2))],
        price_histories={},
        benchmark_history={"2026-01-02": 100.0, "2026-01-03": 100.0},
        start_date=date(2026, 1, 2),
        end_date=date(2026, 1, 3),
        mode="realistic_disclosure_lag",
    )

    assert simulation.positions == []
    assert simulation.skipped[0].reason == "missing_price_history"


def test_insider_form4_purchase_and_sale_side_parsing_works():
    db = _session()
    try:
        ts = datetime(2026, 1, 10, tzinfo=timezone.utc)
        db.add_all(
            [
                Event(
                    id=20,
                    event_type="insider_trade",
                    ts=ts,
                    event_date=ts,
                    symbol="AAPL",
                    source="sec_form4",
                    trade_type=None,
                    payload_json=json.dumps(
                        {
                            "symbol": "AAPL",
                            "transaction_date": "2026-01-09",
                            "filing_date": "2026-01-10",
                            "reporting_cik": "0000001111",
                            "raw": {
                                "companyCik": "0000320193",
                                "transactionCoding": {"transactionCode": "P"},
                                "transactionAmounts": {"transactionAcquiredDisposedCode": "A"},
                            },
                        }
                    ),
                ),
                Event(
                    id=21,
                    event_type="insider_trade",
                    ts=ts,
                    event_date=ts,
                    symbol="AAPL",
                    source="sec_form4",
                    trade_type=None,
                    payload_json=json.dumps(
                        {
                            "symbol": "AAPL",
                            "transaction_date": "2026-01-11",
                            "filing_date": "2026-01-12",
                            "reporting_cik": "0000001111",
                            "raw": {
                                "companyCik": "0000320193",
                                "transactionCoding": {"transactionCode": "S"},
                                "transactionAmounts": {"transactionAcquiredDisposedCode": "D"},
                            },
                        }
                    ),
                ),
            ]
        )
        db.commit()

        events, skipped = load_replicated_portfolio_events(
            db,
            entity_type="insider",
            entity_id="0000001111",
            lookback_days=1095,
            issuer="0000320193",
            end_date=date(2026, 1, 12),
        )

        assert skipped == []
        assert [event.side for event in events] == ["purchase", "sale"]
    finally:
        db.close()


def test_future_dated_insider_event_is_skipped():
    db = _session()
    try:
        ts = datetime(2026, 1, 10, tzinfo=timezone.utc)
        db.add(
            Event(
                id=22,
                event_type="insider_trade",
                ts=ts,
                event_date=datetime(2030, 1, 1, tzinfo=timezone.utc),
                symbol="AAPL",
                source="sec_form4",
                trade_type=None,
                payload_json=json.dumps(
                    {
                        "symbol": "AAPL",
                        "transaction_date": "2030-01-01",
                        "filing_date": "2026-01-10",
                        "reporting_cik": "0000001111",
                        "raw": {
                            "companyCik": "0000320193",
                            "transactionCoding": {"transactionCode": {"value": "P"}},
                            "transactionAmounts": {"transactionAcquiredDisposedCode": {"value": "A"}},
                        },
                    }
                ),
            )
        )
        db.commit()

        events, skipped = load_replicated_portfolio_events(
            db,
            entity_type="insider",
            entity_id="0000001111",
            lookback_days=1095,
            issuer="0000320193",
            end_date=date(2026, 5, 21),
        )

        assert events == []
        assert skipped[0].reason == "future_transaction_date"
    finally:
        db.close()


def test_reit_with_valid_symbol_is_eligible_and_marked_to_market():
    db = _session()
    try:
        ts = datetime(2026, 1, 2, tzinfo=timezone.utc)
        db.add(
            Event(
                id=30,
                event_type="congress_trade",
                ts=ts,
                event_date=ts,
                symbol="O",
                source="test",
                trade_type="purchase",
                member_bioguide_id="M002",
                payload_json=json.dumps(
                    {
                        "symbol": "O",
                        "trade_date": "2026-01-02",
                        "report_date": "2026-01-02",
                        "asset_class": "REIT",
                        "security_description": "Realty Income Corp public REIT",
                    }
                ),
                amount_min=1000,
                amount_max=15000,
            )
        )
        db.commit()

        events, skipped = load_replicated_portfolio_events(
            db,
            entity_type="congress_member",
            entity_id="M002",
            lookback_days=30,
            end_date=date(2026, 1, 5),
        )
        simulation = simulate_replicated_portfolio(
            events=events,
            price_histories={"O": {"2026-01-02": 50.0, "2026-01-03": 55.0}},
            benchmark_history={"2026-01-02": 100.0, "2026-01-03": 100.0},
            start_date=date(2026, 1, 2),
            end_date=date(2026, 1, 3),
            mode="realistic_disclosure_lag",
        )

        assert skipped == []
        assert events[0].symbol == "O"
        assert simulation.positions[0].status == "open"
        assert simulation.points[-1].strategy_value == 110000.0
    finally:
        db.close()


def test_unsupported_options_remain_skipped_with_clear_reason():
    db = _session()
    try:
        ts = datetime(2026, 1, 2, tzinfo=timezone.utc)
        db.add(
            Event(
                id=31,
                event_type="congress_trade",
                ts=ts,
                event_date=ts,
                symbol="AAPL",
                source="test",
                trade_type="purchase",
                member_bioguide_id="M003",
                payload_json=json.dumps(
                    {
                        "symbol": "AAPL",
                        "trade_date": "2026-01-02",
                        "report_date": "2026-01-02",
                        "asset_class": "Stock Option",
                        "security_description": "AAPL call option",
                    }
                ),
                amount_min=1000,
                amount_max=15000,
            )
        )
        db.commit()

        events, skipped = load_replicated_portfolio_events(
            db,
            entity_type="congress_member",
            entity_id="M003",
            lookback_days=30,
            end_date=date(2026, 1, 5),
        )

        assert events == []
        assert skipped[0].reason == "options"
        assert skip_reason_summary(skipped) == {"options": 1}
    finally:
        db.close()


def test_insider_issuer_scoping_does_not_mix_same_reporting_cik_across_issuers():
    db = _session()
    try:
        ts = datetime(2026, 1, 10, tzinfo=timezone.utc)
        db.add_all(
            [
                Event(
                    id=10,
                    event_type="insider_trade",
                    ts=ts,
                    event_date=ts,
                    symbol="AAPL",
                    source="fmp",
                    trade_type="purchase",
                    payload_json=json.dumps(
                        {
                            "symbol": "AAPL",
                            "transaction_date": "2026-01-09",
                            "filing_date": "2026-01-10",
                            "reporting_cik": "0000001111",
                            "raw": {"companyCik": "0000320193"},
                        }
                    ),
                ),
                Event(
                    id=11,
                    event_type="insider_trade",
                    ts=ts,
                    event_date=ts,
                    symbol="NKE",
                    source="fmp",
                    trade_type="purchase",
                    payload_json=json.dumps(
                        {
                            "symbol": "NKE",
                            "transaction_date": "2026-01-09",
                            "filing_date": "2026-01-10",
                            "reporting_cik": "0000001111",
                            "raw": {"companyCik": "0000320187"},
                        }
                    ),
                ),
            ]
        )
        db.commit()

        scoped, skipped = load_replicated_portfolio_events(
            db,
            entity_type="insider",
            entity_id="0000001111",
            lookback_days=1095,
            issuer="0000320193",
            end_date=date(2026, 1, 11),
        )

        assert skipped == []
        assert [event.symbol for event in scoped] == ["AAPL"]
        assert scoped[0].issuer_cik == "0000320193"
    finally:
        db.close()


def test_portfolio_endpoint_returns_persisted_run_without_writes():
    db = _session()
    try:
        run = ReplicatedPortfolioRun(
            entity_type="insider",
            entity_id="0000001111",
            issuer_cik="0000320193",
            mode="realistic_disclosure_lag",
            lookback_days=1095,
            benchmark_symbol="^GSPC",
            start_date=date(2023, 1, 1),
            end_date=date(2026, 1, 1),
            ending_value=125000.0,
            benchmark_ending_value=110000.0,
            total_return_pct=25.0,
            benchmark_return_pct=10.0,
            alpha_pct=15.0,
            cagr_pct=7.7,
            max_drawdown_pct=5.0,
            volatility_pct=12.0,
            sharpe_ratio=1.1,
            win_rate_pct=100.0,
            average_exposure_pct=80.0,
            ending_cash_pct=20.0,
            points_count=1,
            positions_count=1,
            skipped_events_count=0,
        )
        db.add(run)
        db.flush()
        db.add(ReplicatedPortfolioPoint(run_id=run.id, asof_date=date(2026, 1, 1), strategy_value=125000.0, benchmark_value=110000.0, strategy_return_pct=25.0, benchmark_return_pct=10.0, alpha_pct=15.0, daily_return_pct=0.0, active_positions=1, exposure_pct=80.0, cash_pct=20.0))
        db.add(ReplicatedPortfolioPosition(run_id=run.id, source_event_id=123, symbol="AAPL", side="purchase", entry_date=date(2025, 1, 1), entry_price=100.0, shares=10.0, market_value=1250.0, return_pct=25.0, status="open"))
        db.add(PriceCache(symbol="AAPL", date="2026-01-01", close=125.0))
        db.commit()

        before_runs = db.scalar(select(func.count()).select_from(ReplicatedPortfolioRun))
        before_points = db.scalar(select(func.count()).select_from(ReplicatedPortfolioPoint))
        before_prices = db.scalar(select(func.count()).select_from(PriceCache))

        response = insider_portfolio_performance(
            "0000001111",
            db=db,
            lookback_days=1095,
            mode="realistic_disclosure_lag",
            issuer="0000320193",
        )

        assert response["persisted_only"] is True
        assert response["run_id"] == run.id
        assert response["summary"]["total_return_pct"] == 25.0
        assert response["points"][0]["strategy_value"] == 125000.0
        assert db.scalar(select(func.count()).select_from(ReplicatedPortfolioRun)) == before_runs
        assert db.scalar(select(func.count()).select_from(ReplicatedPortfolioPoint)) == before_points
        assert db.scalar(select(func.count()).select_from(PriceCache)) == before_prices
    finally:
        db.close()


def test_summary_only_does_not_dump_full_skipped_event_arrays(monkeypatch):
    engine, SessionLocal = _session_factory()
    monkeypatch.setattr(compute_module, "engine", engine)
    monkeypatch.setattr(compute_module, "SessionLocal", SessionLocal)
    with SessionLocal() as db:
        ts = datetime(2026, 1, 2, tzinfo=timezone.utc)
        db.add(
            Event(
                id=601,
                event_type="congress_trade",
                ts=ts,
                event_date=ts,
                symbol="AAPL",
                source="test",
                trade_type="purchase",
                member_bioguide_id="M_SUMMARY",
                member_name="Summary Tester",
                payload_json=json.dumps(
                    {
                        "symbol": "AAPL",
                        "trade_date": "2026-01-02",
                        "report_date": "2026-01-02",
                        "asset_class": "Stock Option",
                    }
                ),
                amount_min=1000,
                amount_max=15000,
            )
        )
        db.add(PriceCache(symbol="^GSPC", date="2026-01-02", close=100.0))
        db.commit()

    report = compute_module.run_compute(
        entity_type="congress",
        entity_id="M_SUMMARY",
        lookback_days=365,
        mode="realistic_disclosure_lag",
        limit=1,
        dry_run=True,
        benchmark="^GSPC",
        summary_only=True,
    )

    row = report["results"][0]
    assert "skipped" not in row
    assert row["top_skip_reasons"] == {"options": 1}
    assert row["entity_name"] == "Summary Tester"


def test_targeted_entity_id_limits_compute_to_requested_entity(monkeypatch):
    engine, SessionLocal = _session_factory()
    monkeypatch.setattr(compute_module, "engine", engine)
    monkeypatch.setattr(compute_module, "SessionLocal", SessionLocal)
    with SessionLocal() as db:
        for member_id, event_id, symbol in [("M_TARGET", 701, "AAPL"), ("M_OTHER", 702, "MSFT")]:
            ts = datetime(2026, 1, 2, tzinfo=timezone.utc)
            db.add(
                Event(
                    id=event_id,
                    event_type="congress_trade",
                    ts=ts,
                    event_date=ts,
                    symbol=symbol,
                    source="test",
                    trade_type="purchase",
                    member_bioguide_id=member_id,
                    payload_json=json.dumps(
                        {
                            "symbol": symbol,
                            "trade_date": "2026-01-02",
                            "report_date": "2026-01-02",
                            "asset_class": "equity",
                        }
                    ),
                    amount_min=1000,
                    amount_max=15000,
                )
            )
            db.add(PriceCache(symbol=symbol, date="2026-01-02", close=100.0))
        db.add(PriceCache(symbol="^GSPC", date="2026-01-02", close=100.0))
        db.commit()

    report = compute_module.run_compute(
        entity_type="congress",
        entity_id="M_TARGET",
        lookback_days=365,
        mode="realistic_disclosure_lag",
        limit=5,
        dry_run=True,
        benchmark="^GSPC",
        summary_only=True,
    )

    assert [row["entity_id"] for row in report["results"]] == ["M_TARGET"]
    assert report["results"][0]["events_used"] == 1


def test_insider_inspect_mode_surfaces_raw_side_fields_and_normalized_side(monkeypatch):
    engine, SessionLocal = _session_factory()
    monkeypatch.setattr(compute_module, "engine", engine)
    monkeypatch.setattr(compute_module, "SessionLocal", SessionLocal)
    with SessionLocal() as db:
        ts = datetime(2026, 1, 10, tzinfo=timezone.utc)
        db.add(
            Event(
                id=801,
                event_type="insider_trade",
                ts=ts,
                event_date=ts,
                symbol="AAPL",
                source="sec_form4",
                trade_type=None,
                payload_json=json.dumps(
                    {
                        "symbol": "AAPL",
                        "transaction_date": "2026-01-09",
                        "filing_date": "2026-01-10",
                        "reporting_cik": "0000001111",
                        "raw": {
                            "companyCik": "0000320193",
                            "transactionCoding": {"transactionCode": {"value": "P"}},
                            "transactionAmounts": {
                                "transactionAcquiredDisposedCode": {"value": "A"},
                                "transactionShares": {"value": 42},
                            },
                        },
                    }
                ),
            )
        )
        db.commit()

    report = compute_module.run_inspect_events(
        entity_type="insider",
        entity_id="0000001111",
        issuer_cik="0000320193",
        issuer_symbol=None,
        lookback_days=1095,
        limit=20,
    )

    item = report["items"][0]
    assert item["event_id"] == 801
    assert item["transaction_code"] == "P"
    assert item["acquisition_disposition_code"] == "A"
    assert item["normalized_side"] == "purchase"
    assert item["raw_side_fields"]
    assert item["transaction_amount_fields"]


def test_coverage_only_reports_benchmark_cache_window(monkeypatch):
    engine, SessionLocal = _session_factory()
    monkeypatch.setattr(compute_module, "engine", engine)
    monkeypatch.setattr(compute_module, "SessionLocal", SessionLocal)
    with SessionLocal() as db:
        db.add_all(
            [
                PriceCache(symbol="SPY", date="2026-01-02", close=100.0),
                PriceCache(symbol="SPY", date="2026-01-03", close=101.0),
            ]
        )
        db.commit()

    report = compute_module.run_coverage_only(benchmark="SPY", lookback_days=1095)

    assert report["benchmark_symbol"] == "SPY"
    assert report["cache_first_date"] == "2026-01-02"
    assert report["cache_last_date"] == "2026-01-03"
    assert report["cache_rows_total"] == 2
    assert report["is_sparse"] is True
    assert report["missing_weekdays_estimate"] > 0
    assert report["largest_missing_date_ranges"]


def test_insider_candidate_selection_ignores_invalid_only_entities(monkeypatch):
    engine, SessionLocal = _session_factory()
    monkeypatch.setattr(compute_module, "engine", engine)
    monkeypatch.setattr(compute_module, "SessionLocal", SessionLocal)
    with SessionLocal() as db:
        ts = datetime(2026, 1, 10, tzinfo=timezone.utc)
        db.add_all(
            [
                Event(
                    id=901,
                    event_type="insider_trade",
                    ts=ts,
                    event_date=ts,
                    symbol="BAD",
                    source="sec_form4",
                    payload_json=json.dumps(
                        {
                            "symbol": "BAD",
                            "transaction_date": "2030-01-01",
                            "filing_date": "2026-01-10",
                            "reporting_cik": "0000009999",
                            "raw": {
                                "companyCik": "0000000001",
                                "transactionCoding": {"transactionCode": {"value": "P"}},
                            },
                        }
                    ),
                ),
                Event(
                    id=902,
                    event_type="insider_trade",
                    ts=ts,
                    event_date=ts,
                    symbol="AAPL",
                    source="sec_form4",
                    payload_json=json.dumps(
                        {
                            "symbol": "AAPL",
                            "transaction_date": "2026-01-09",
                            "filing_date": "2026-01-10",
                            "reporting_cik": "0000001111",
                            "raw": {
                                "companyCik": "0000320193",
                                "transactionCoding": {"transactionCode": {"value": "P"}},
                            },
                        }
                    ),
                ),
            ]
        )
        db.commit()
        monkeypatch.setattr(
            compute_module,
            "load_replicated_portfolio_events",
            lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("candidate scan must not load portfolios")),
        )
        candidates = compute_module._candidate_insiders(db, limit=5, lookback_days=1095)

    assert candidates.entity_ids == ["0000001111"]
    assert candidates.candidates_scanned == 2
    assert candidates.candidates_selected == 1
    assert candidates.events_parsed == 2


def test_insider_candidate_selection_respects_scan_bounds(monkeypatch):
    engine, SessionLocal = _session_factory()
    monkeypatch.setattr(compute_module, "engine", engine)
    monkeypatch.setattr(compute_module, "SessionLocal", SessionLocal)
    with SessionLocal() as db:
        ts = datetime(2026, 1, 10, tzinfo=timezone.utc)
        for index in range(12):
            cik = f"{index + 1:010d}"
            db.add(
                Event(
                    id=920 + index,
                    event_type="insider_trade",
                    ts=ts,
                    event_date=ts,
                    symbol=f"S{index}",
                    source="sec_form4",
                    payload_json=json.dumps(
                        {
                            "symbol": f"S{index}",
                            "transaction_date": "2026-01-09",
                            "filing_date": "2026-01-10",
                            "reporting_cik": cik,
                            "raw": {
                                "companyCik": "0000320193",
                                "transactionCoding": {"transactionCode": {"value": "P"}},
                            },
                        }
                    ),
                )
            )
        db.commit()
        candidates = compute_module._candidate_insiders(
            db,
            limit=10,
            lookback_days=1095,
            candidate_scan_limit=3,
            max_events_per_candidate=1,
        )

    assert len(candidates.entity_ids) == 3
    assert candidates.candidates_scanned == 3
    assert candidates.candidates_selected == 3
    assert candidates.events_parsed == 3
    assert candidates.candidate_scan_limit_hit is True


def test_insider_candidate_prefilter_query_is_dialect_safe():
    cutoff = datetime(2025, 1, 1, tzinfo=timezone.utc)
    now_dt = datetime(2026, 1, 1, tzinfo=timezone.utc)
    query = compute_module._insider_candidate_rows_query(
        cutoff=cutoff,
        now_dt=now_dt,
        issuer_cik="0000320193",
        issuer_symbol="AAPL",
        row_limit=25,
    )

    postgres_sql = str(query.compile(dialect=postgresql.dialect())).lower()
    sqlite_sql = str(query.compile(dialect=sqlite.dialect())).lower()

    assert "json_extract" not in postgres_sql
    assert "json_extract" not in sqlite_sql
    assert "events.payload_json" in postgres_sql
    assert "limit" in postgres_sql


def test_targeted_insider_entity_id_bypasses_candidate_scan(monkeypatch):
    engine, SessionLocal = _session_factory()
    monkeypatch.setattr(compute_module, "engine", engine)
    monkeypatch.setattr(compute_module, "SessionLocal", SessionLocal)
    monkeypatch.setattr(
        compute_module,
        "_candidate_insiders",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("targeted entity must bypass broad candidate scan")),
    )
    with SessionLocal() as db:
        ts = datetime(2026, 1, 10, tzinfo=timezone.utc)
        db.add(
            Event(
                id=940,
                event_type="insider_trade",
                ts=ts,
                event_date=ts,
                symbol="AAPL",
                source="sec_form4",
                payload_json=json.dumps(
                    {
                        "symbol": "AAPL",
                        "transaction_date": "2026-01-09",
                        "filing_date": "2026-01-10",
                        "reporting_cik": "0000001111",
                        "raw": {
                            "companyCik": "0000320193",
                            "transactionCoding": {"transactionCode": {"value": "P"}},
                        },
                    }
                ),
            )
        )
        db.add(PriceCache(symbol="AAPL", date="2026-01-10", close=100.0))
        db.add(PriceCache(symbol="^GSPC", date="2026-01-10", close=100.0))
        db.commit()

    report = compute_module.run_compute(
        entity_type="insider",
        entity_id="0000001111",
        lookback_days=365,
        mode="realistic_disclosure_lag",
        limit=10,
        dry_run=True,
        benchmark="^GSPC",
        summary_only=True,
    )

    assert [row["entity_id"] for row in report["results"]] == ["0000001111"]
    assert report["candidates_scanned"] == 1
    assert report["events_prefiltered"] == 0
    assert report["events_parsed"] == 0
    assert report["results"][0]["candidates_selected"] == 1


def test_summary_only_missing_price_symbol_summary(monkeypatch):
    engine, SessionLocal = _session_factory()
    monkeypatch.setattr(compute_module, "engine", engine)
    monkeypatch.setattr(compute_module, "SessionLocal", SessionLocal)
    with SessionLocal() as db:
        ts = datetime(2026, 1, 2, tzinfo=timezone.utc)
        db.add(
            Event(
                id=903,
                event_type="congress_trade",
                ts=ts,
                event_date=ts,
                symbol="AAPL",
                source="test",
                trade_type="purchase",
                member_bioguide_id="M_MISSING",
                payload_json=json.dumps(
                    {
                        "symbol": "AAPL",
                        "trade_date": "2026-01-02",
                        "report_date": "2026-01-02",
                        "asset_class": "equity",
                    }
                ),
                amount_min=1000,
                amount_max=15000,
            )
        )
        db.add(PriceCache(symbol="^GSPC", date="2026-01-02", close=100.0))
        db.commit()

    report = compute_module.run_compute(
        entity_type="congress",
        entity_id="M_MISSING",
        lookback_days=365,
        mode="realistic_disclosure_lag",
        limit=1,
        dry_run=True,
        benchmark="^GSPC",
        summary_only=True,
    )

    row = report["results"][0]
    assert row["missing_price_symbols_count"] == 1
    assert row["top_missing_price_symbols"] == {"AAPL": 1}
    assert row["top_skip_reasons"] == {"missing_price": 1}


def test_summary_only_caps_coverage_and_symbol_diagnostics(monkeypatch):
    engine, SessionLocal = _session_factory()
    monkeypatch.setattr(compute_module, "engine", engine)
    monkeypatch.setattr(compute_module, "SessionLocal", SessionLocal)
    with SessionLocal() as db:
        ts = datetime(2026, 1, 2, tzinfo=timezone.utc)
        for index in range(15):
            symbol = f"SYM{index}"
            db.add(
                Event(
                    id=950 + index,
                    event_type="congress_trade",
                    ts=ts,
                    event_date=ts,
                    symbol=symbol,
                    source="test",
                    trade_type="purchase",
                    member_bioguide_id="M_COMPACT",
                    payload_json=json.dumps(
                        {
                            "symbol": symbol,
                            "trade_date": "2026-01-02",
                            "report_date": "2026-01-02",
                            "asset_class": "equity",
                        }
                    ),
                    amount_min=1000,
                    amount_max=15000,
                )
            )
            db.add(PriceCache(symbol=symbol, date="2026-01-02", close=100.0 + index))
        db.add(PriceCache(symbol="^GSPC", date="2026-01-02", close=100.0))
        db.commit()

    report = compute_module.run_compute(
        entity_type="congress",
        entity_id="M_COMPACT",
        lookback_days=365,
        mode="realistic_disclosure_lag",
        limit=1,
        dry_run=True,
        benchmark="^GSPC",
        summary_only=True,
    )
    row = report["results"][0]

    assert row["coverage_limitations_count"] > 10
    assert len(row["coverage_limitations"]) == 10
    assert len(row["symbol_coverage_summary"]) == 10


def test_summary_only_verbose_includes_full_diagnostics(monkeypatch):
    engine, SessionLocal = _session_factory()
    monkeypatch.setattr(compute_module, "engine", engine)
    monkeypatch.setattr(compute_module, "SessionLocal", SessionLocal)
    with SessionLocal() as db:
        ts = datetime(2026, 1, 2, tzinfo=timezone.utc)
        for index in range(12):
            symbol = f"V{index}"
            db.add(
                Event(
                    id=980 + index,
                    event_type="congress_trade",
                    ts=ts,
                    event_date=ts,
                    symbol=symbol,
                    source="test",
                    trade_type="purchase",
                    member_bioguide_id="M_VERBOSE",
                    payload_json=json.dumps(
                        {
                            "symbol": symbol,
                            "trade_date": "2026-01-02",
                            "report_date": "2026-01-02",
                            "asset_class": "equity",
                        }
                    ),
                    amount_min=1000,
                    amount_max=15000,
                )
            )
            db.add(PriceCache(symbol=symbol, date="2026-01-02", close=100.0 + index))
        db.add(PriceCache(symbol="^GSPC", date="2026-01-02", close=100.0))
        db.commit()

    report = compute_module.run_compute(
        entity_type="congress",
        entity_id="M_VERBOSE",
        lookback_days=365,
        mode="realistic_disclosure_lag",
        limit=1,
        dry_run=True,
        benchmark="^GSPC",
        summary_only=True,
        verbose=True,
    )
    row = report["results"][0]

    assert row["coverage_limitations_count"] == len(row["coverage_limitations"])
    assert len(row["symbol_coverage_summary"]) == 12


def test_backfill_price_cache_dry_run_and_apply(monkeypatch):
    engine, SessionLocal = _session_factory()
    monkeypatch.setattr(backfill_module, "engine", engine)
    monkeypatch.setattr(backfill_module, "SessionLocal", SessionLocal)
    monkeypatch.setattr(
        backfill_module,
        "_fetch_provider_eod_close_series",
        lambda symbol, start, end: ({"2026-01-02": 100.0, "2026-01-03": 101.0}, symbol),
    )

    dry = backfill_module.backfill_price_cache(
        symbols=["^GSPC"],
        start_date="2026-01-02",
        end_date="2026-01-03",
        dry_run=True,
    )
    with SessionLocal() as db:
        assert db.scalar(select(func.count()).select_from(PriceCache)) == 0

    applied = backfill_module.backfill_price_cache(
        symbols=["^GSPC"],
        start_date="2026-01-02",
        end_date="2026-01-03",
        dry_run=False,
    )
    with SessionLocal() as db:
        assert db.scalar(select(func.count()).select_from(PriceCache)) == 2

    assert dry["rows"][0]["rows_missing"] == 2
    assert dry["rows"][0]["rows_inserted_or_updated"] == 0
    assert applied["rows"][0]["rows_inserted_or_updated"] == 2
