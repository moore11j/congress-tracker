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
    PortfolioCoverage,
    PortfolioCurveDiagnostics,
    PortfolioPoint,
    PortfolioSimulation,
    PortfolioSkip,
    PortfolioSummary,
    PortfolioTradeEvent,
    _DailyCurveQuality,
    _build_curve_diagnostics,
    load_replicated_portfolio_events,
    normalize_skip_reason,
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


def _add_congress_portfolio_fixture(db: Session, *, member_id: str, event_id: int, symbol: str = "AAPL", member_name: str | None = None) -> date:
    day = datetime.now(timezone.utc).date()
    ts = datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc)
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
            member_name=member_name,
            payload_json=json.dumps(
                {
                    "symbol": symbol,
                    "trade_date": day.isoformat(),
                    "report_date": day.isoformat(),
                    "asset_class": "equity",
                }
            ),
            amount_min=1000,
            amount_max=15000,
        )
    )
    db.merge(PriceCache(symbol=symbol, date=day.isoformat(), close=100.0))
    db.merge(PriceCache(symbol="^GSPC", date=day.isoformat(), close=100.0))
    db.flush()
    return day


def _add_existing_portfolio_run(
    db: Session,
    *,
    entity_id: str,
    lookback_days: int = 365,
    skipped_symbols: list[str] | None = None,
) -> ReplicatedPortfolioRun:
    day = datetime.now(timezone.utc).date()
    run = ReplicatedPortfolioRun(
        entity_type="congress_member",
        entity_id=entity_id,
        mode="realistic_disclosure_lag",
        lookback_days=lookback_days,
        benchmark_symbol="^GSPC",
        start_date=day - timedelta(days=lookback_days),
        end_date=day,
        ending_value=110000.0,
        benchmark_ending_value=100000.0,
        total_return_pct=10.0,
        benchmark_return_pct=0.0,
        alpha_pct=10.0,
        points_count=1,
        positions_count=1,
        skipped_events_count=len(skipped_symbols or []),
        status="ok",
    )
    db.add(run)
    db.flush()
    db.add(ReplicatedPortfolioPoint(run_id=run.id, asof_date=day, strategy_value=110000.0, benchmark_value=100000.0, strategy_return_pct=10.0))
    db.add(
        ReplicatedPortfolioPosition(
            run_id=run.id,
            source_event_id=None,
            symbol="AAPL",
            side="purchase",
            status="open",
        )
    )
    for index, symbol in enumerate(skipped_symbols or []):
        db.add(
            ReplicatedPortfolioPosition(
                run_id=run.id,
                source_event_id=9000 + index,
                symbol=symbol,
                side="purchase",
                status="skipped",
                skip_reason="missing_price",
            )
        )
    db.commit()
    db.refresh(run)
    return run


def _fake_portfolio_simulation(
    *,
    status: str,
    avg_priced: float,
    pct_gap: float,
    suggested_symbols: list[str] | None = None,
    suggested_start: date | None = None,
    suggested_end: date | None = None,
) -> PortfolioSimulation:
    today = datetime.now(timezone.utc).date()
    summary = PortfolioSummary(
        starting_value=100000.0,
        ending_value=100000.0,
        benchmark_ending_value=100000.0,
        total_return_pct=0.0,
        benchmark_return_pct=0.0,
        alpha_pct=0.0,
        cagr_pct=0.0,
        max_drawdown_pct=0.0,
        volatility_pct=0.0,
        sharpe_ratio=None,
        win_rate_pct=0.0,
        average_exposure_pct=0.0,
        ending_cash_pct=100.0,
        points_count=0,
        positions_count=1,
        skipped_events_count=0,
    )
    coverage = PortfolioCoverage(
        requested_start_date=today - timedelta(days=365),
        requested_end_date=today,
        warmup_start_date=None,
        warmup_days=0,
        actual_start_date=None,
        actual_end_date=None,
        calendar_points=0,
        calendar_source="test",
        benchmark_symbol="^GSPC",
        benchmark_points_loaded=0,
        benchmark_first_date=None,
        benchmark_last_date=None,
        symbols_loaded=0,
        symbol_points_loaded={},
        symbol_first_dates={},
        symbol_last_dates={},
        limitations=[],
    )
    diagnostics = PortfolioCurveDiagnostics(
        flat_segment_count=0,
        longest_flat_segment_days=0,
        longest_problematic_flat_segment_days=20 if status == "poor" else 0,
        average_exposure_pct=0.0,
        min_exposure_pct=0.0,
        max_exposure_pct=0.0,
        days_with_zero_exposure=0,
        days_with_active_positions_but_zero_exposure=0,
        days_with_active_positions_but_no_valued_positions=0,
        pct_position_days_with_price_gaps=pct_gap,
        pct_invested_value_with_price_gaps=pct_gap,
        avg_priced_invested_value_pct=avg_priced,
        min_priced_invested_value_pct=avg_priced,
        days_below_90pct_priced_value=1 if avg_priced < 90 else 0,
        days_below_75pct_priced_value=1 if avg_priced < 75 else 0,
        days_below_50pct_priced_value=1 if avg_priced < 50 else 0,
        stale_price_fill_count=1 if pct_gap else 0,
        missing_price_fill_count=1 if pct_gap else 0,
        positions_marked_to_market_count=0,
        positions_using_stale_price_count=0,
        pct_days_with_price_gaps=pct_gap,
        curve_quality_status=status,
        curve_quality_notes=["test diagnostics"],
        flat_segments=[],
        suggested_backfill_symbols=suggested_symbols or [],
        suggested_backfill_start_date=suggested_start,
        suggested_backfill_end_date=suggested_end,
    )
    return PortfolioSimulation(
        summary=summary,
        points=[],
        positions=[],
        skipped=[],
        coverage=coverage,
        curve_diagnostics=diagnostics,
        daily_quality=[],
    )


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


def test_warmup_purchase_contributes_value_on_first_requested_day():
    simulation = simulate_replicated_portfolio(
        events=[_event(event_id=701, symbol="AAPL", side="purchase", transaction_date=date(2025, 6, 1))],
        price_histories={
            "AAPL": {
                "2025-06-01": 100.0,
                "2026-01-01": 200.0,
                "2026-01-02": 220.0,
            }
        },
        benchmark_history={
            "2025-06-01": 100.0,
            "2026-01-01": 100.0,
            "2026-01-02": 100.0,
        },
        start_date=date(2026, 1, 1),
        end_date=date(2026, 1, 2),
        mode="realistic_disclosure_lag",
        warmup_start_date=date(2025, 6, 1),
    )

    assert [point.asof_date for point in simulation.points] == [date(2026, 1, 1), date(2026, 1, 2)]
    assert simulation.points[0].strategy_value == 100000.0
    assert simulation.points[0].active_positions == 1
    assert simulation.points[0].exposure_pct == 100.0
    assert simulation.points[1].strategy_value == 110000.0
    assert simulation.coverage.warmup_days == 214


def test_short_lookback_uses_warmup_events_to_reconstruct_opening_holdings():
    db = _session()
    try:
        end = date(2026, 1, 31)
        start = end - timedelta(days=30)
        trade_day = date(2025, 6, 1)
        ts = datetime.combine(trade_day, datetime.min.time(), tzinfo=timezone.utc)
        db.add(
            Event(
                id=702,
                event_type="congress_trade",
                ts=ts,
                event_date=ts,
                symbol="AAPL",
                source="test",
                trade_type="purchase",
                member_bioguide_id="M_WARM",
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
        for offset, day in enumerate(_date_keys(trade_day, end)):
            db.merge(PriceCache(symbol="AAPL", date=day, close=100.0 + offset))
            db.merge(PriceCache(symbol="^GSPC", date=day, close=100.0))
        db.commit()

        simulation = run_replicated_portfolio_simulation(
            db,
            entity_type="congress_member",
            entity_id="M_WARM",
            lookback_days=30,
            mode="realistic_disclosure_lag",
            benchmark="^GSPC",
            end_date=end,
        )

        assert simulation.points[0].asof_date == start
        assert simulation.points[0].active_positions == 1
        assert simulation.points[0].strategy_value == 100000.0
        assert simulation.summary.positions_count == 1
        assert simulation.coverage.warmup_days == 1095
    finally:
        db.close()


def test_1095_day_run_does_not_apply_default_warmup_to_prior_trade():
    db = _session()
    try:
        end = date(2026, 1, 31)
        start = end - timedelta(days=1095)
        prior_trade_day = start - timedelta(days=20)
        ts = datetime.combine(prior_trade_day, datetime.min.time(), tzinfo=timezone.utc)
        db.add(
            Event(
                id=703,
                event_type="congress_trade",
                ts=ts,
                event_date=ts,
                symbol="AAPL",
                source="test",
                trade_type="purchase",
                member_bioguide_id="M_3Y",
                payload_json=json.dumps(
                    {
                        "symbol": "AAPL",
                        "trade_date": prior_trade_day.isoformat(),
                        "report_date": prior_trade_day.isoformat(),
                        "asset_class": "equity",
                    }
                ),
                amount_min=1000,
                amount_max=15000,
            )
        )
        for day in _date_keys(start, end):
            db.merge(PriceCache(symbol="AAPL", date=day, close=100.0))
            db.merge(PriceCache(symbol="^GSPC", date=day, close=100.0))
        db.commit()

        simulation = run_replicated_portfolio_simulation(
            db,
            entity_type="congress_member",
            entity_id="M_3Y",
            lookback_days=1095,
            mode="realistic_disclosure_lag",
            benchmark="^GSPC",
            end_date=end,
        )

        assert simulation.summary.positions_count == 0
        assert simulation.points[0].active_positions == 0
        assert simulation.coverage.warmup_days == 0
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


def test_daily_curve_uses_same_day_price_when_available():
    simulation = simulate_replicated_portfolio(
        events=[_event(event_id=201, symbol="AAPL", side="purchase", transaction_date=date(2026, 1, 2))],
        price_histories={"AAPL": {"2026-01-02": 100.0, "2026-01-03": 130.0}},
        benchmark_history={"2026-01-02": 100.0, "2026-01-03": 100.0},
        start_date=date(2026, 1, 2),
        end_date=date(2026, 1, 3),
        mode="realistic_disclosure_lag",
    )

    assert [point.strategy_value for point in simulation.points] == [100000.0, 130000.0]
    assert simulation.curve_diagnostics.stale_price_fill_count == 0
    assert simulation.curve_diagnostics.missing_price_fill_count == 0


def test_bounded_prior_close_fill_marks_stale_price():
    simulation = simulate_replicated_portfolio(
        events=[_event(event_id=202, symbol="AAPL", side="purchase", transaction_date=date(2026, 1, 2))],
        price_histories={"AAPL": {"2026-01-02": 100.0, "2026-01-04": 120.0}},
        benchmark_history={"2026-01-02": 100.0, "2026-01-03": 100.0, "2026-01-04": 100.0},
        start_date=date(2026, 1, 2),
        end_date=date(2026, 1, 4),
        mode="realistic_disclosure_lag",
        max_stale_price_trading_days=5,
    )

    assert [point.strategy_value for point in simulation.points] == [100000.0, 100000.0, 120000.0]
    assert simulation.curve_diagnostics.stale_price_fill_count == 1
    assert simulation.curve_diagnostics.positions_using_stale_price_count == 1
    assert simulation.curve_diagnostics.curve_quality_status == "warning"


def test_stale_price_beyond_tolerance_triggers_curve_quality_warning():
    simulation = simulate_replicated_portfolio(
        events=[_event(event_id=203, symbol="AAPL", side="purchase", transaction_date=date(2026, 1, 2))],
        price_histories={"AAPL": {"2026-01-02": 100.0}},
        benchmark_history={day: 100.0 for day in _date_keys(date(2026, 1, 2), date(2026, 1, 10))},
        start_date=date(2026, 1, 2),
        end_date=date(2026, 1, 10),
        mode="realistic_disclosure_lag",
        max_stale_price_trading_days=2,
    )

    assert simulation.curve_diagnostics.missing_price_fill_count > 0
    assert simulation.curve_diagnostics.curve_quality_status == "poor"
    assert any("lacked a bounded prior close" in note for note in simulation.curve_diagnostics.curve_quality_notes)


def test_zero_position_window_produces_intentional_flat_curve_note():
    simulation = simulate_replicated_portfolio(
        events=[],
        price_histories={},
        benchmark_history={day: 100.0 for day in _date_keys(date(2026, 1, 2), date(2026, 1, 6))},
        start_date=date(2026, 1, 2),
        end_date=date(2026, 1, 6),
        mode="realistic_disclosure_lag",
    )

    assert simulation.summary.positions_count == 0
    assert simulation.curve_diagnostics.curve_quality_status == "good"
    assert "No simulated holdings were active in this window." in simulation.curve_diagnostics.curve_quality_notes


def test_active_positions_with_zero_exposure_are_flagged():
    points = [
        PortfolioPoint(
            asof_date=date.fromisoformat(day),
            strategy_value=100000.0,
            benchmark_value=100000.0,
            strategy_return_pct=0.0,
            benchmark_return_pct=0.0,
            alpha_pct=0.0,
            daily_return_pct=0.0,
            active_positions=1,
            exposure_pct=0.0,
            cash_pct=100.0,
        )
        for day in _date_keys(date(2026, 1, 2), date(2026, 1, 6))
    ]
    daily_quality = [
        _DailyCurveQuality(
            day=point.asof_date.isoformat(),
            active_symbols=["AAPL"],
            stale_symbols=[],
            missing_symbols=[],
            marked_to_market_count=1,
            portfolio_value=100000.0,
            cash_value=100000.0,
            invested_value=0.0,
            exposure_pct=0.0,
            valued_positions_count=1,
            zero_value_positions_count=1,
            shares_nonzero_count=0,
            market_value_nonzero_count=0,
            top_positions_by_market_value=[],
            top_zero_value_symbols=["AAPL"],
        )
        for point in points
    ]
    diagnostics = _build_curve_diagnostics(
        points=points,
        daily_quality=daily_quality,
        positions_count=1,
        stale_price_fill_count=0,
        missing_price_fill_count=0,
        positions_marked_to_market_count=5,
        stale_position_keys=set(),
    )

    assert diagnostics.days_with_active_positions_but_zero_exposure == 5
    assert diagnostics.curve_quality_status == "warning"
    longest = max(diagnostics.flat_segments, key=lambda item: item.trading_days)
    assert longest.zero_value_positions_count == 1
    assert longest.total_shares_nonzero_count == 0
    assert longest.total_market_value_nonzero_count == 0
    assert longest.top_zero_value_symbols == ["AAPL"]


def test_flat_segment_diagnostics_distinguish_missing_prices_from_no_holdings():
    missing_price_simulation = simulate_replicated_portfolio(
        events=[_event(event_id=705, symbol="MSFT", side="purchase", transaction_date=date(2026, 1, 2))],
        price_histories={"MSFT": {"2026-01-02": 100.0}},
        benchmark_history={day: 100.0 for day in _date_keys(date(2026, 1, 2), date(2026, 1, 6))},
        start_date=date(2026, 1, 2),
        end_date=date(2026, 1, 6),
        mode="realistic_disclosure_lag",
        max_stale_price_trading_days=0,
    )
    missing_segment = max(missing_price_simulation.curve_diagnostics.flat_segments, key=lambda item: item.trading_days)

    no_holdings_simulation = simulate_replicated_portfolio(
        events=[],
        price_histories={},
        benchmark_history={day: 100.0 for day in _date_keys(date(2026, 1, 2), date(2026, 1, 6))},
        start_date=date(2026, 1, 2),
        end_date=date(2026, 1, 6),
        mode="realistic_disclosure_lag",
    )
    no_holdings_segment = max(no_holdings_simulation.curve_diagnostics.flat_segments, key=lambda item: item.trading_days)

    assert "MSFT" in missing_segment.missing_symbols
    assert missing_segment.legitimate_no_holdings is False
    assert no_holdings_segment.legitimate_no_holdings is True
    assert no_holdings_segment.active_positions_count == 0


def test_portfolio_build_up_segment_is_classified_without_poor_status():
    points = [
        PortfolioPoint(
            asof_date=date(2026, 1, 2),
            strategy_value=100000.0,
            benchmark_value=100000.0,
            strategy_return_pct=0.0,
            benchmark_return_pct=0.0,
            alpha_pct=0.0,
            daily_return_pct=0.0,
            active_positions=2,
            exposure_pct=0.0,
            cash_pct=100.0,
        ),
        PortfolioPoint(
            asof_date=date(2026, 1, 3),
            strategy_value=100000.0,
            benchmark_value=100000.0,
            strategy_return_pct=0.0,
            benchmark_return_pct=0.0,
            alpha_pct=0.0,
            daily_return_pct=0.0,
            active_positions=2,
            exposure_pct=100.0,
            cash_pct=0.0,
        ),
    ]
    daily_quality = [
        _DailyCurveQuality(
            day="2026-01-02",
            active_symbols=["AAPL", "MSFT"],
            stale_symbols=[],
            missing_symbols=[],
            marked_to_market_count=2,
            active_positions_count=2,
            portfolio_value=100000.0,
            cash_value=100000.0,
            invested_value=0.0,
            exposure_pct=0.0,
            valued_positions_count=2,
            priced_invested_value_pct=100.0,
        ),
        _DailyCurveQuality(
            day="2026-01-03",
            active_symbols=["AAPL", "MSFT"],
            stale_symbols=[],
            missing_symbols=[],
            marked_to_market_count=2,
            active_positions_count=2,
            portfolio_value=100000.0,
            cash_value=0.0,
            invested_value=100000.0,
            exposure_pct=100.0,
            valued_positions_count=2,
            priced_invested_value=100000.0,
            priced_invested_value_pct=100.0,
        ),
    ]

    diagnostics = _build_curve_diagnostics(
        points=points,
        daily_quality=daily_quality,
        positions_count=2,
        stale_price_fill_count=0,
        missing_price_fill_count=0,
        positions_marked_to_market_count=4,
        stale_position_keys=set(),
    )

    assert diagnostics.flat_segments[0].segment_type == "portfolio_build_up"
    assert diagnostics.curve_quality_status != "poor"


def test_tiny_missing_price_position_does_not_make_curve_poor():
    points = [
        PortfolioPoint(
            asof_date=date.fromisoformat(day),
            strategy_value=100000.0,
            benchmark_value=100000.0,
            strategy_return_pct=0.0,
            benchmark_return_pct=0.0,
            alpha_pct=0.0,
            daily_return_pct=0.0,
            active_positions=2,
            exposure_pct=100.0,
            cash_pct=0.0,
        )
        for day in _date_keys(date(2026, 1, 2), date(2026, 1, 6))
    ]
    daily_quality = [
        _DailyCurveQuality(
            day=point.asof_date.isoformat(),
            active_symbols=["BIG", "TINY"],
            stale_symbols=[],
            missing_symbols=["TINY"],
            marked_to_market_count=2,
            active_positions_count=2,
            portfolio_value=100000.0,
            invested_value=100000.0,
            exposure_pct=100.0,
            valued_positions_count=2,
            priced_invested_value=99000.0,
            missing_invested_value=1000.0,
            price_gap_invested_value=1000.0,
            priced_invested_value_pct=99.0,
            price_gap_value_by_symbol={"TINY": 1000.0},
        )
        for point in points
    ]

    diagnostics = _build_curve_diagnostics(
        points=points,
        daily_quality=daily_quality,
        positions_count=2,
        stale_price_fill_count=0,
        missing_price_fill_count=5,
        positions_marked_to_market_count=10,
        stale_position_keys=set(),
    )

    assert diagnostics.avg_priced_invested_value_pct == 99.0
    assert diagnostics.curve_quality_status != "poor"


def test_large_missing_value_exposure_marks_curve_poor():
    points = [
        PortfolioPoint(
            asof_date=date.fromisoformat(day),
            strategy_value=100000.0,
            benchmark_value=100000.0,
            strategy_return_pct=0.0,
            benchmark_return_pct=0.0,
            alpha_pct=0.0,
            daily_return_pct=0.0,
            active_positions=2,
            exposure_pct=100.0,
            cash_pct=0.0,
        )
        for day in _date_keys(date(2026, 1, 2), date(2026, 1, 6))
    ]
    daily_quality = [
        _DailyCurveQuality(
            day=point.asof_date.isoformat(),
            active_symbols=["BIG", "GAP"],
            stale_symbols=[],
            missing_symbols=["GAP"],
            marked_to_market_count=2,
            active_positions_count=2,
            portfolio_value=100000.0,
            invested_value=100000.0,
            exposure_pct=100.0,
            valued_positions_count=2,
            priced_invested_value=60000.0,
            missing_invested_value=40000.0,
            price_gap_invested_value=40000.0,
            priced_invested_value_pct=60.0,
            price_gap_value_by_symbol={"GAP": 40000.0},
        )
        for point in points
    ]

    diagnostics = _build_curve_diagnostics(
        points=points,
        daily_quality=daily_quality,
        positions_count=2,
        stale_price_fill_count=0,
        missing_price_fill_count=5,
        positions_marked_to_market_count=10,
        stale_position_keys=set(),
    )

    assert diagnostics.avg_priced_invested_value_pct == 60.0
    assert diagnostics.days_below_75pct_priced_value == 5
    assert diagnostics.curve_quality_status == "poor"


def test_suggested_backfill_ranking_prioritizes_market_value_impact():
    points = [
        PortfolioPoint(
            asof_date=date.fromisoformat(day),
            strategy_value=100000.0,
            benchmark_value=100000.0,
            strategy_return_pct=0.0,
            benchmark_return_pct=0.0,
            alpha_pct=0.0,
            daily_return_pct=0.0,
            active_positions=3,
            exposure_pct=100.0,
            cash_pct=0.0,
        )
        for day in _date_keys(date(2026, 1, 2), date(2026, 1, 4))
    ]
    daily_quality = [
        _DailyCurveQuality(
            day="2026-01-02",
            active_symbols=["HIGH", "LOW"],
            stale_symbols=["LOW"],
            missing_symbols=[],
            marked_to_market_count=2,
            active_positions_count=2,
            portfolio_value=100000.0,
            invested_value=100000.0,
            priced_invested_value=99900.0,
            stale_invested_value=100.0,
            price_gap_invested_value=100.0,
            priced_invested_value_pct=99.9,
            price_gap_value_by_symbol={"LOW": 100.0},
        ),
        _DailyCurveQuality(
            day="2026-01-03",
            active_symbols=["HIGH", "LOW"],
            stale_symbols=["LOW"],
            missing_symbols=[],
            marked_to_market_count=2,
            active_positions_count=2,
            portfolio_value=100000.0,
            invested_value=100000.0,
            priced_invested_value=99900.0,
            stale_invested_value=100.0,
            price_gap_invested_value=100.0,
            priced_invested_value_pct=99.9,
            price_gap_value_by_symbol={"LOW": 100.0},
        ),
        _DailyCurveQuality(
            day="2026-01-04",
            active_symbols=["HIGH", "LOW"],
            stale_symbols=["HIGH"],
            missing_symbols=[],
            marked_to_market_count=2,
            active_positions_count=2,
            portfolio_value=100000.0,
            invested_value=100000.0,
            priced_invested_value=50000.0,
            stale_invested_value=50000.0,
            price_gap_invested_value=50000.0,
            priced_invested_value_pct=50.0,
            price_gap_value_by_symbol={"HIGH": 50000.0},
        ),
    ]

    diagnostics = _build_curve_diagnostics(
        points=points,
        daily_quality=daily_quality,
        positions_count=3,
        stale_price_fill_count=4,
        missing_price_fill_count=0,
        positions_marked_to_market_count=6,
        stale_position_keys=set(),
    )

    assert diagnostics.suggested_backfill_symbols[0] == "HIGH"


def test_nonzero_positions_with_long_flat_segment_warns():
    simulation = simulate_replicated_portfolio(
        events=[_event(event_id=204, symbol="MSFT", side="purchase", transaction_date=date(2026, 1, 2))],
        price_histories={"MSFT": {day: 50.0 for day in _date_keys(date(2026, 1, 2), date(2026, 1, 10))}},
        benchmark_history={day: 100.0 for day in _date_keys(date(2026, 1, 2), date(2026, 1, 10))},
        start_date=date(2026, 1, 2),
        end_date=date(2026, 1, 10),
        mode="realistic_disclosure_lag",
    )

    assert simulation.summary.positions_count == 1
    assert simulation.curve_diagnostics.longest_flat_segment_days == 9
    assert simulation.curve_diagnostics.flat_segments[0].segment_type == "true_flat_value"
    assert simulation.curve_diagnostics.curve_quality_status == "good"


def test_curve_diagnostics_identify_longest_flat_segment():
    simulation = simulate_replicated_portfolio(
        events=[_event(event_id=205, symbol="NVDA", side="purchase", transaction_date=date(2026, 1, 2))],
        price_histories={
            "NVDA": {
                "2026-01-02": 100.0,
                "2026-01-03": 100.0,
                "2026-01-04": 100.0,
                "2026-01-05": 110.0,
                "2026-01-06": 110.0,
            }
        },
        benchmark_history={day: 100.0 for day in _date_keys(date(2026, 1, 2), date(2026, 1, 6))},
        start_date=date(2026, 1, 2),
        end_date=date(2026, 1, 6),
        mode="realistic_disclosure_lag",
    )

    longest = max(simulation.curve_diagnostics.flat_segments, key=lambda item: item.trading_days)
    assert longest.start_date == date(2026, 1, 2)
    assert longest.end_date == date(2026, 1, 4)
    assert longest.trading_days == 3


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
        db.add(PriceCache(symbol="AAPL", date="2026-01-10", close=100.0))
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
        db.add(PriceCache(symbol="AAPL", date="2026-01-10", close=100.0))
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


def test_member_portfolio_endpoint_returns_persisted_run_without_writes():
    from app.main import member_portfolio_performance

    db = _session()
    try:
        run = ReplicatedPortfolioRun(
            entity_type="congress_member",
            entity_id="M_PORT",
            mode="realistic_disclosure_lag",
            lookback_days=1095,
            benchmark_symbol="^GSPC",
            start_date=date(2023, 1, 1),
            end_date=date(2026, 1, 1),
            ending_value=131356.529,
            benchmark_ending_value=110000.0,
            total_return_pct=31.356529,
            benchmark_return_pct=10.0,
            alpha_pct=21.356529,
            cagr_pct=9.533521,
            max_drawdown_pct=5.0,
            volatility_pct=12.0,
            sharpe_ratio=1.16994,
            win_rate_pct=100.0,
            average_exposure_pct=80.0,
            ending_cash_pct=20.0,
            points_count=3,
            positions_count=1,
            skipped_events_count=0,
        )
        db.add(run)
        db.flush()
        for offset in range(3):
            db.add(
                ReplicatedPortfolioPoint(
                    run_id=run.id,
                    asof_date=date(2025, 12, 30) + timedelta(days=offset),
                    strategy_value=100000.0 + offset,
                    benchmark_value=100000.0,
                    strategy_return_pct=float(offset),
                    benchmark_return_pct=0.0,
                    alpha_pct=float(offset),
                    daily_return_pct=0.0,
                    active_positions=1,
                    exposure_pct=80.0,
                    cash_pct=20.0,
                )
            )
        db.add(PriceCache(symbol="AAPL", date="2026-01-01", close=125.0))
        db.commit()

        before_runs = db.scalar(select(func.count()).select_from(ReplicatedPortfolioRun))
        before_points = db.scalar(select(func.count()).select_from(ReplicatedPortfolioPoint))
        before_prices = db.scalar(select(func.count()).select_from(PriceCache))

        response = member_portfolio_performance(
            "M_PORT",
            db=db,
            lookback_days=1095,
            mode="realistic_disclosure_lag",
        )

        assert response["persisted_only"] is True
        assert response["run_id"] == run.id
        assert response["summary"]["points_count"] == 3
        assert len(response["points"]) == 3
        assert response["summary"]["total_return_pct"] == 31.356529
        assert response["summary"]["cagr_pct"] == 9.533521
        assert response["summary"]["sharpe_ratio"] == 1.16994
        assert response["curve_quality_status"] in {"good", "warning", "poor"}
        assert "longest_flat_segment_days" in response
        assert "pct_days_with_price_gaps" in response
        assert "data_coverage_notes" in response
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


def test_apply_output_is_compact_by_default(monkeypatch):
    engine, SessionLocal = _session_factory()
    monkeypatch.setattr(compute_module, "engine", engine)
    monkeypatch.setattr(compute_module, "SessionLocal", SessionLocal)
    with SessionLocal() as db:
        ts = datetime(2026, 1, 2, tzinfo=timezone.utc)
        for index in range(12):
            symbol = f"AP{index}"
            db.add(
                Event(
                    id=620 + index,
                    event_type="congress_trade",
                    ts=ts,
                    event_date=ts,
                    symbol=symbol,
                    source="test",
                    trade_type="purchase",
                    member_bioguide_id="M_APPLY",
                    member_name="Apply Tester",
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
        entity_id="M_APPLY",
        lookback_days=365,
        mode="realistic_disclosure_lag",
        limit=1,
        dry_run=False,
        benchmark="^GSPC",
    )
    row = report["results"][0]

    assert row["run_id"]
    assert row["entity_type"] == "congress_member"
    assert row["entity_id"] == "M_APPLY"
    assert row["entity_name"] == "Apply Tester"
    assert row["persisted_points"] == 1
    assert row["positions_count"] == 12
    assert "coverage" not in row
    assert "summary" not in row
    assert "symbol_coverage_summary" not in row
    assert "skipped" not in row
    assert set(
        [
            "total_return_pct",
            "alpha_pct",
            "benchmark_return_pct",
            "top_skip_reasons",
            "missing_price_symbols_count",
            "top_missing_price_symbols",
        ]
    ).issubset(row)


def test_apply_output_verbose_includes_full_coverage(monkeypatch):
    engine, SessionLocal = _session_factory()
    monkeypatch.setattr(compute_module, "engine", engine)
    monkeypatch.setattr(compute_module, "SessionLocal", SessionLocal)
    with SessionLocal() as db:
        ts = datetime(2026, 1, 2, tzinfo=timezone.utc)
        db.add(
            Event(
                id=640,
                event_type="congress_trade",
                ts=ts,
                event_date=ts,
                symbol="AAPL",
                source="test",
                trade_type="purchase",
                member_bioguide_id="M_APPLY_VERBOSE",
                member_name="Verbose Apply",
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
        db.add(PriceCache(symbol="AAPL", date="2026-01-02", close=100.0))
        db.add(PriceCache(symbol="^GSPC", date="2026-01-02", close=100.0))
        db.commit()

    report = compute_module.run_compute(
        entity_type="congress",
        entity_id="M_APPLY_VERBOSE",
        lookback_days=365,
        mode="realistic_disclosure_lag",
        limit=1,
        dry_run=False,
        benchmark="^GSPC",
        verbose=True,
    )
    row = report["results"][0]

    assert row["run_id"]
    assert row["persisted_points"] == 1
    assert "coverage" in row
    assert "symbol_points_loaded" in row["coverage"]
    assert "summary" in row
    assert "symbol_coverage_summary" in row


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


def test_standard_lookback_set_expands_for_targeted_batch(monkeypatch):
    engine, SessionLocal = _session_factory()
    monkeypatch.setattr(compute_module, "engine", engine)
    monkeypatch.setattr(compute_module, "SessionLocal", SessionLocal)
    with SessionLocal() as db:
        _add_congress_portfolio_fixture(db, member_id="M_STD", event_id=1201, member_name="Standard Tester")
        db.commit()

    report = compute_module.run_compute(
        entity_type="congress",
        entity_ids="M_STD",
        lookback_days=1095,
        lookback_set="standard",
        mode="realistic_disclosure_lag",
        limit=1,
        dry_run=True,
        benchmark="^GSPC",
    )

    assert report["lookbacks_requested"] == [30, 90, 180, 365, 1095]
    assert [row["lookback_days"] for row in report["results"]] == [30, 90, 180, 365, 1095]
    assert report["summary"]["runs_planned"] == 5
    assert report["summary"]["would_create"] == 5


def test_comma_separated_lookback_days_work(monkeypatch):
    engine, SessionLocal = _session_factory()
    monkeypatch.setattr(compute_module, "engine", engine)
    monkeypatch.setattr(compute_module, "SessionLocal", SessionLocal)
    with SessionLocal() as db:
        _add_congress_portfolio_fixture(db, member_id="M_CSV", event_id=1202)
        db.commit()

    report = compute_module.run_compute(
        entity_type="congress",
        entity_id="M_CSV",
        lookback_days="30,90,365",
        mode="realistic_disclosure_lag",
        limit=1,
        dry_run=True,
        benchmark="^GSPC",
    )

    assert report["lookbacks_requested"] == [30, 90, 365]
    assert report["summary"]["lookbacks_requested"] == 3
    assert [row["status"] for row in report["results"]] == ["would_create", "would_create", "would_create"]


def test_entity_ids_list_targets_only_requested_members(monkeypatch):
    engine, SessionLocal = _session_factory()
    monkeypatch.setattr(compute_module, "engine", engine)
    monkeypatch.setattr(compute_module, "SessionLocal", SessionLocal)
    with SessionLocal() as db:
        _add_congress_portfolio_fixture(db, member_id="M_LIST_A", event_id=1203, symbol="AAA")
        _add_congress_portfolio_fixture(db, member_id="M_LIST_B", event_id=1204, symbol="BBB")
        _add_congress_portfolio_fixture(db, member_id="M_LIST_C", event_id=1205, symbol="CCC")
        db.commit()

    report = compute_module.run_compute(
        entity_type="congress",
        entity_ids="M_LIST_A,M_LIST_B",
        lookback_days=365,
        mode="realistic_disclosure_lag",
        limit=10,
        dry_run=True,
        benchmark="^GSPC",
    )

    assert [row["entity_id"] for row in report["results"]] == ["M_LIST_A", "M_LIST_B"]
    assert report["summary"]["entities_requested"] == 2
    assert report["summary"]["runs_planned"] == 2


def test_existing_runs_are_skipped_by_default_without_compute(monkeypatch):
    engine, SessionLocal = _session_factory()
    monkeypatch.setattr(compute_module, "engine", engine)
    monkeypatch.setattr(compute_module, "SessionLocal", SessionLocal)
    monkeypatch.setattr(
        compute_module,
        "run_replicated_portfolio_simulation",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("existing runs should skip compute")),
    )
    with SessionLocal() as db:
        existing = _add_existing_portfolio_run(db, entity_id="M_EXISTS", lookback_days=365)

    report = compute_module.run_compute(
        entity_type="congress",
        entity_id="M_EXISTS",
        lookback_days=365,
        mode="realistic_disclosure_lag",
        limit=1,
        dry_run=False,
        benchmark="^GSPC",
    )

    row = report["results"][0]
    assert row["status"] == "skipped_existing"
    assert row["run_id"] == existing.id
    assert report["summary"]["skipped_existing"] == 1


def test_compact_planned_result_handles_skipped_persisted_position(monkeypatch):
    engine, SessionLocal = _session_factory()
    monkeypatch.setattr(compute_module, "engine", engine)
    monkeypatch.setattr(compute_module, "SessionLocal", SessionLocal)
    with SessionLocal() as db:
        _add_existing_portfolio_run(db, entity_id="M_SKIP_POS", lookback_days=365, skipped_symbols=["MISS"])

    report = compute_module.run_compute(
        entity_type="congress",
        entity_id="M_SKIP_POS",
        lookback_days=365,
        mode="realistic_disclosure_lag",
        limit=1,
        dry_run=True,
        benchmark="^GSPC",
    )

    row = report["results"][0]
    assert row["status"] == "skipped_existing"
    assert row["missing_price_symbols_count"] == 1
    assert row["top_skip_reasons"] == {"missing_price": 1}


def test_compact_planned_result_populates_top_missing_price_symbols_from_positions(monkeypatch):
    engine, SessionLocal = _session_factory()
    monkeypatch.setattr(compute_module, "engine", engine)
    monkeypatch.setattr(compute_module, "SessionLocal", SessionLocal)
    with SessionLocal() as db:
        _add_existing_portfolio_run(
            db,
            entity_id="M_SKIP_TOP",
            lookback_days=365,
            skipped_symbols=["MISS", "MISS", "ALSO"],
        )

    report = compute_module.run_compute(
        entity_type="congress",
        entity_id="M_SKIP_TOP",
        lookback_days=365,
        mode="realistic_disclosure_lag",
        limit=1,
        dry_run=True,
        benchmark="^GSPC",
    )

    row = report["results"][0]
    assert row["missing_price_symbols_count"] == 2
    assert row["top_missing_price_symbols"] == {"MISS": 2, "ALSO": 1}


def test_batch_dry_run_across_multiple_existing_lookbacks_does_not_crash(monkeypatch):
    engine, SessionLocal = _session_factory()
    monkeypatch.setattr(compute_module, "engine", engine)
    monkeypatch.setattr(compute_module, "SessionLocal", SessionLocal)
    monkeypatch.setattr(
        compute_module,
        "run_replicated_portfolio_simulation",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("existing lookbacks should skip compute")),
    )
    with SessionLocal() as db:
        _add_existing_portfolio_run(db, entity_id="M_BATCH_SKIP", lookback_days=30, skipped_symbols=["MISS"])
        _add_existing_portfolio_run(db, entity_id="M_BATCH_SKIP", lookback_days=90, skipped_symbols=["MISS"])
        _add_existing_portfolio_run(db, entity_id="M_BATCH_SKIP", lookback_days=180, skipped_symbols=["MISS"])

    report = compute_module.run_compute(
        entity_type="congress",
        entity_id="M_BATCH_SKIP",
        lookback_days="30,90,180",
        mode="realistic_disclosure_lag",
        limit=1,
        dry_run=True,
        benchmark="^GSPC",
    )

    assert [row["lookback_days"] for row in report["results"]] == [30, 90, 180]
    assert [row["status"] for row in report["results"]] == ["skipped_existing", "skipped_existing", "skipped_existing"]
    assert report["summary"]["skipped_existing"] == 3


def test_existing_skip_object_normalization_is_unchanged():
    skip = PortfolioSkip(event_id=1, symbol="AAPL", side="purchase", reason="missing_price_history")

    assert normalize_skip_reason(skip) == "missing_price"
    assert skip_reason_summary([skip]) == {"missing_price": 1}


def test_replace_existing_only_when_explicitly_passed(monkeypatch):
    engine, SessionLocal = _session_factory()
    monkeypatch.setattr(compute_module, "engine", engine)
    monkeypatch.setattr(compute_module, "SessionLocal", SessionLocal)
    with SessionLocal() as db:
        _add_congress_portfolio_fixture(db, member_id="M_REPLACE", event_id=1206)
        _add_existing_portfolio_run(db, entity_id="M_REPLACE", lookback_days=365)

    skipped = compute_module.run_compute(
        entity_type="congress",
        entity_id="M_REPLACE",
        lookback_days=365,
        mode="realistic_disclosure_lag",
        limit=1,
        dry_run=False,
        benchmark="^GSPC",
    )
    assert skipped["results"][0]["status"] == "skipped_existing"

    replaced = compute_module.run_compute(
        entity_type="congress",
        entity_id="M_REPLACE",
        lookback_days=365,
        mode="realistic_disclosure_lag",
        limit=1,
        dry_run=False,
        benchmark="^GSPC",
        replace_existing=True,
    )

    with SessionLocal() as db:
        run_ids = [row[0] for row in db.execute(select(ReplicatedPortfolioRun.id)).all()]
    assert replaced["results"][0]["status"] == "created"
    assert len(run_ids) == 1


def test_dry_run_writes_nothing(monkeypatch):
    engine, SessionLocal = _session_factory()
    monkeypatch.setattr(compute_module, "engine", engine)
    monkeypatch.setattr(compute_module, "SessionLocal", SessionLocal)
    with SessionLocal() as db:
        _add_congress_portfolio_fixture(db, member_id="M_DRY", event_id=1207)
        db.commit()
        before = (
            db.scalar(select(func.count()).select_from(ReplicatedPortfolioRun)),
            db.scalar(select(func.count()).select_from(ReplicatedPortfolioPoint)),
            db.scalar(select(func.count()).select_from(ReplicatedPortfolioPosition)),
        )

    report = compute_module.run_compute(
        entity_type="congress",
        entity_id="M_DRY",
        lookback_days=365,
        mode="realistic_disclosure_lag",
        limit=1,
        dry_run=True,
        benchmark="^GSPC",
    )

    with SessionLocal() as db:
        after = (
            db.scalar(select(func.count()).select_from(ReplicatedPortfolioRun)),
            db.scalar(select(func.count()).select_from(ReplicatedPortfolioPoint)),
            db.scalar(select(func.count()).select_from(ReplicatedPortfolioPosition)),
        )
    assert report["results"][0]["status"] == "would_create"
    assert after == before


def test_apply_writes_only_replicated_portfolio_tables(monkeypatch):
    engine, SessionLocal = _session_factory()
    monkeypatch.setattr(compute_module, "engine", engine)
    monkeypatch.setattr(compute_module, "SessionLocal", SessionLocal)
    with SessionLocal() as db:
        _add_congress_portfolio_fixture(db, member_id="M_APPLY_ONLY", event_id=1208)
        db.commit()
        before_events = db.scalar(select(func.count()).select_from(Event))
        before_prices = db.scalar(select(func.count()).select_from(PriceCache))

    report = compute_module.run_compute(
        entity_type="congress",
        entity_id="M_APPLY_ONLY",
        lookback_days=365,
        mode="realistic_disclosure_lag",
        limit=1,
        dry_run=False,
        benchmark="^GSPC",
    )

    with SessionLocal() as db:
        assert db.scalar(select(func.count()).select_from(Event)) == before_events
        assert db.scalar(select(func.count()).select_from(PriceCache)) == before_prices
        assert db.scalar(select(func.count()).select_from(ReplicatedPortfolioRun)) == 1
        assert db.scalar(select(func.count()).select_from(ReplicatedPortfolioPoint)) >= 1
        assert db.scalar(select(func.count()).select_from(ReplicatedPortfolioPosition)) >= 1
    assert report["results"][0]["status"] == "created"


def test_compact_output_hides_coverage_internals_unless_verbose(monkeypatch):
    engine, SessionLocal = _session_factory()
    monkeypatch.setattr(compute_module, "engine", engine)
    monkeypatch.setattr(compute_module, "SessionLocal", SessionLocal)
    with SessionLocal() as db:
        _add_congress_portfolio_fixture(db, member_id="M_COMPACT_BATCH", event_id=1209)
        db.commit()

    compact = compute_module.run_compute(
        entity_type="congress",
        entity_id="M_COMPACT_BATCH",
        lookback_days=365,
        mode="realistic_disclosure_lag",
        limit=1,
        dry_run=True,
        benchmark="^GSPC",
    )
    verbose = compute_module.run_compute(
        entity_type="congress",
        entity_id="M_COMPACT_BATCH",
        lookback_days=365,
        mode="realistic_disclosure_lag",
        limit=1,
        dry_run=True,
        benchmark="^GSPC",
        verbose=True,
    )

    assert "coverage" not in compact["results"][0]
    assert "symbol_coverage_summary" not in compact["results"][0]
    assert "summary" not in compact["results"][0]
    assert "coverage" in verbose["results"][0]
    assert "symbol_coverage_summary" in verbose["results"][0]
    assert "summary" in verbose["results"][0]


def test_legacy_single_entity_single_lookback_shape_still_works(monkeypatch):
    engine, SessionLocal = _session_factory()
    monkeypatch.setattr(compute_module, "engine", engine)
    monkeypatch.setattr(compute_module, "SessionLocal", SessionLocal)
    with SessionLocal() as db:
        _add_congress_portfolio_fixture(db, member_id="M_LEGACY", event_id=1210)
        db.commit()

    report = compute_module.run_compute(
        entity_type="congress",
        entity_id="M_LEGACY",
        lookback_days=365,
        mode="realistic_disclosure_lag",
        limit=1,
        dry_run=True,
        benchmark="^GSPC",
    )

    assert report["lookback_days"] == 365
    assert len(report["results"]) == 1
    assert report["results"][0]["entity_id"] == "M_LEGACY"
    assert report["results"][0]["lookback_days"] == 365


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
        db.add(PriceCache(symbol="AAPL", date="2026-01-10", close=100.0))
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
            db.add(PriceCache(symbol=f"S{index}", date="2026-01-10", close=100.0))
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


def test_insider_candidate_selection_ranks_priceable_ahead_of_missing_price(monkeypatch):
    engine, SessionLocal = _session_factory()
    monkeypatch.setattr(compute_module, "engine", engine)
    monkeypatch.setattr(compute_module, "SessionLocal", SessionLocal)
    with SessionLocal() as db:
        ts = datetime(2026, 1, 10, tzinfo=timezone.utc)
        db.add_all(
            [
                Event(
                    id=960,
                    event_type="insider_trade",
                    ts=ts,
                    event_date=ts,
                    symbol="MISS",
                    source="sec_form4",
                    payload_json=json.dumps(
                        {
                            "symbol": "MISS",
                            "transaction_date": "2026-01-09",
                            "filing_date": "2026-01-10",
                            "reporting_cik": "0000009999",
                            "raw": {"transactionCoding": {"transactionCode": {"value": "P"}}},
                        }
                    ),
                ),
                Event(
                    id=961,
                    event_type="insider_trade",
                    ts=ts - timedelta(days=1),
                    event_date=ts - timedelta(days=1),
                    symbol="GOOD",
                    source="sec_form4",
                    payload_json=json.dumps(
                        {
                            "symbol": "GOOD",
                            "transaction_date": "2026-01-08",
                            "filing_date": "2026-01-09",
                            "reporting_cik": "0000002222",
                            "raw": {"transactionCoding": {"transactionCode": {"value": "P"}}},
                        }
                    ),
                ),
            ]
        )
        db.add(PriceCache(symbol="GOOD", date="2026-01-09", close=25.0))
        db.commit()
        candidates = compute_module._candidate_insiders(db, limit=2, lookback_days=1095)

    assert candidates.entity_ids == ["0000002222"]
    assert candidates.metrics_for("0000002222")["candidate_priceable_event_estimate"] == 1
    assert candidates.metrics_for("0000009999")["candidate_priceable_event_estimate"] == 0


def test_insider_candidate_selection_ranks_named_candidate_ahead_when_similar(monkeypatch):
    engine, SessionLocal = _session_factory()
    monkeypatch.setattr(compute_module, "engine", engine)
    monkeypatch.setattr(compute_module, "SessionLocal", SessionLocal)
    with SessionLocal() as db:
        ts = datetime(2026, 1, 10, tzinfo=timezone.utc)
        db.add_all(
            [
                Event(
                    id=962,
                    event_type="insider_trade",
                    ts=ts,
                    event_date=ts,
                    symbol="ANON",
                    source="sec_form4",
                    payload_json=json.dumps(
                        {
                            "symbol": "ANON",
                            "transaction_date": "2026-01-09",
                            "filing_date": "2026-01-10",
                            "reporting_cik": "0000003333",
                            "raw": {"transactionCoding": {"transactionCode": {"value": "P"}}},
                        }
                    ),
                ),
                Event(
                    id=963,
                    event_type="insider_trade",
                    ts=ts,
                    event_date=ts,
                    symbol="NAMED",
                    source="sec_form4",
                    payload_json=json.dumps(
                        {
                            "symbol": "NAMED",
                            "transaction_date": "2026-01-09",
                            "filing_date": "2026-01-10",
                            "reporting_cik": "0000004444",
                            "reportingOwnerName": "Named Insider",
                            "raw": {"transactionCoding": {"transactionCode": {"value": "P"}}},
                        }
                    ),
                ),
            ]
        )
        db.add_all(
            [
                PriceCache(symbol="ANON", date="2026-01-10", close=10.0),
                PriceCache(symbol="NAMED", date="2026-01-10", close=10.0),
                PriceCache(symbol="^GSPC", date="2026-01-10", close=100.0),
            ]
        )
        db.commit()
        candidates = compute_module._candidate_insiders(db, limit=2, lookback_days=1095)

    assert candidates.entity_ids[0] == "0000004444"
    assert candidates.metrics_for("0000004444")["candidate_name_found"] is True
    assert candidates.metrics_for("0000004444")["entity_name"] == "Named Insider"

    report = compute_module.run_compute(
        entity_type="insider",
        lookback_days=1095,
        mode="realistic_disclosure_lag",
        limit=1,
        dry_run=True,
        benchmark="^GSPC",
        summary_only=True,
    )
    row = report["results"][0]
    assert row["entity_id"] == "0000004444"
    assert row["entity_name"] == "Named Insider"
    assert row["candidate_quality_score"] > 0
    assert row["candidate_valid_side_events"] == 1
    assert row["candidate_priceable_event_estimate"] == 1
    assert row["candidate_name_found"] is True


def test_insider_candidate_selection_deprioritizes_zero_priceable_candidates(monkeypatch):
    engine, SessionLocal = _session_factory()
    monkeypatch.setattr(compute_module, "engine", engine)
    monkeypatch.setattr(compute_module, "SessionLocal", SessionLocal)
    with SessionLocal() as db:
        ts = datetime(2026, 1, 10, tzinfo=timezone.utc)
        db.add(
            Event(
                id=964,
                event_type="insider_trade",
                ts=ts,
                event_date=ts,
                symbol="NOPRICE",
                source="sec_form4",
                payload_json=json.dumps(
                    {
                        "symbol": "NOPRICE",
                        "transaction_date": "2026-01-09",
                        "filing_date": "2026-01-10",
                        "reporting_cik": "0000005555",
                        "raw": {"transactionCoding": {"transactionCode": {"value": "P"}}},
                    }
                ),
            )
        )
        db.commit()
        candidates = compute_module._candidate_insiders(db, limit=1, lookback_days=1095)

    assert candidates.entity_ids == []
    assert candidates.metrics_for("0000005555")["candidate_valid_side_events"] == 1
    assert candidates.metrics_for("0000005555")["candidate_priceable_event_estimate"] == 0


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

    assert "coverage_limitations" not in row
    assert "coverage_limitations_count" not in row
    assert "symbol_coverage_summary" not in row


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


def test_price_preflight_dry_run_writes_no_price_cache(monkeypatch):
    engine, SessionLocal = _session_factory()
    monkeypatch.setattr(compute_module, "engine", engine)
    monkeypatch.setattr(compute_module, "SessionLocal", SessionLocal)
    start = date(2026, 1, 2)
    end = date(2026, 1, 9)
    simulation = _fake_portfolio_simulation(
        status="poor",
        avg_priced=40.0,
        pct_gap=60.0,
        suggested_symbols=["HIGH"],
        suggested_start=start,
        suggested_end=end,
    )
    provider_calls: list[str] = []
    monkeypatch.setattr(compute_module, "load_replicated_portfolio_events", lambda *args, **kwargs: ([], []))
    monkeypatch.setattr(compute_module, "run_replicated_portfolio_simulation", lambda *args, **kwargs: simulation)
    monkeypatch.setattr(
        compute_module,
        "_fetch_provider_eod_close_series",
        lambda symbol, start_date, end_date: provider_calls.append(symbol) or ({"2026-01-02": 100.0, "2026-01-09": 101.0}, symbol),
    )

    report = compute_module.run_compute(
        entity_type="congress",
        entity_id="M_PREFLIGHT_DRY",
        lookback_days=365,
        mode="realistic_disclosure_lag",
        limit=1,
        dry_run=True,
        benchmark="^GSPC",
        price_preflight=True,
    )

    with SessionLocal() as db:
        assert db.scalar(select(func.count()).select_from(PriceCache)) == 0
    row = report["results"][0]
    assert row["preflight_passes_attempted"] == 0
    assert row["preflight_symbols_backfilled"] == []
    assert row["preflight_stopped_reason"] == "dry_run_no_price_writes"
    assert row["preflight_suggested_passes"][0]["symbols"] == ["HIGH"]
    assert provider_calls == []


def test_apply_price_preflight_backfills_only_with_backfill_flag(monkeypatch):
    engine, SessionLocal = _session_factory()
    monkeypatch.setattr(compute_module, "engine", engine)
    monkeypatch.setattr(compute_module, "SessionLocal", SessionLocal)
    start = date(2026, 1, 2)
    end = date(2026, 1, 9)
    poor = _fake_portfolio_simulation(
        status="poor",
        avg_priced=35.0,
        pct_gap=65.0,
        suggested_symbols=["HIGH"],
        suggested_start=start,
        suggested_end=end,
    )
    warning = _fake_portfolio_simulation(status="warning", avg_priced=90.0, pct_gap=10.0)
    monkeypatch.setattr(compute_module, "load_replicated_portfolio_events", lambda *args, **kwargs: ([], []))
    monkeypatch.setattr(compute_module, "_fetch_provider_eod_close_series", lambda symbol, start_date, end_date: ({"2026-01-02": 100.0}, symbol))

    monkeypatch.setattr(compute_module, "run_replicated_portfolio_simulation", lambda *args, **kwargs: poor)
    without_backfill = compute_module.run_compute(
        entity_type="congress",
        entity_id="M_PREFLIGHT_NO_BACKFILL",
        lookback_days=365,
        mode="realistic_disclosure_lag",
        limit=1,
        dry_run=False,
        benchmark="^GSPC",
        price_preflight=True,
    )
    with SessionLocal() as db:
        assert db.scalar(select(func.count()).select_from(PriceCache)) == 0

    simulations = iter([poor, warning])
    monkeypatch.setattr(compute_module, "run_replicated_portfolio_simulation", lambda *args, **kwargs: next(simulations))
    with_backfill = compute_module.run_compute(
        entity_type="congress",
        entity_id="M_PREFLIGHT_BACKFILL",
        lookback_days=365,
        mode="realistic_disclosure_lag",
        limit=1,
        dry_run=False,
        benchmark="^GSPC",
        price_preflight=True,
        price_preflight_backfill=True,
    )

    with SessionLocal() as db:
        assert db.scalar(select(func.count()).select_from(PriceCache).where(PriceCache.symbol == "HIGH")) == 1
    assert without_backfill["results"][0]["preflight_passes_attempted"] == 0
    assert with_backfill["results"][0]["preflight_passes_attempted"] == 1
    assert with_backfill["results"][0]["preflight_symbols_backfilled"] == ["HIGH"]
    assert with_backfill["results"][0]["preflight_stopped_reason"] == "curve_quality_warning"


def test_price_preflight_respects_max_passes_and_symbol_order(monkeypatch):
    engine, SessionLocal = _session_factory()
    monkeypatch.setattr(compute_module, "engine", engine)
    monkeypatch.setattr(compute_module, "SessionLocal", SessionLocal)
    start = date(2026, 1, 2)
    end = date(2026, 1, 9)
    poor = _fake_portfolio_simulation(
        status="poor",
        avg_priced=20.0,
        pct_gap=80.0,
        suggested_symbols=["HIGH", "LOW"],
        suggested_start=start,
        suggested_end=end,
    )
    provider_calls: list[str] = []
    monkeypatch.setattr(compute_module, "load_replicated_portfolio_events", lambda *args, **kwargs: ([], []))
    monkeypatch.setattr(compute_module, "run_replicated_portfolio_simulation", lambda *args, **kwargs: poor)
    monkeypatch.setattr(
        compute_module,
        "_fetch_provider_eod_close_series",
        lambda symbol, start_date, end_date: provider_calls.append(symbol) or ({"2026-01-02": 100.0, "2026-01-09": 101.0}, symbol),
    )

    report = compute_module.run_compute(
        entity_type="congress",
        entity_id="M_PREFLIGHT_MAX",
        lookback_days=365,
        mode="realistic_disclosure_lag",
        limit=1,
        dry_run=False,
        benchmark="^GSPC",
        price_preflight=True,
        price_preflight_backfill=True,
        price_preflight_max_passes=2,
        price_preflight_max_symbols=1,
    )

    row = report["results"][0]
    assert row["preflight_passes_attempted"] == 2
    assert row["preflight_stopped_reason"] == "max_passes_reached"
    assert [item["symbols"] for item in row["preflight_suggested_passes"]] == [["HIGH"], ["HIGH"]]
    assert provider_calls == ["HIGH", "HIGH"]


def test_price_preflight_stops_when_warning_or_good_reached(monkeypatch):
    engine, SessionLocal = _session_factory()
    monkeypatch.setattr(compute_module, "engine", engine)
    monkeypatch.setattr(compute_module, "SessionLocal", SessionLocal)
    start = date(2026, 1, 2)
    end = date(2026, 1, 9)
    simulations = iter(
        [
            _fake_portfolio_simulation(
                status="poor",
                avg_priced=20.0,
                pct_gap=80.0,
                suggested_symbols=["HIGH"],
                suggested_start=start,
                suggested_end=end,
            ),
            _fake_portfolio_simulation(status="good", avg_priced=100.0, pct_gap=0.0),
        ]
    )
    monkeypatch.setattr(compute_module, "load_replicated_portfolio_events", lambda *args, **kwargs: ([], []))
    monkeypatch.setattr(compute_module, "run_replicated_portfolio_simulation", lambda *args, **kwargs: next(simulations))
    monkeypatch.setattr(compute_module, "_fetch_provider_eod_close_series", lambda symbol, start_date, end_date: ({"2026-01-02": 100.0}, symbol))

    report = compute_module.run_compute(
        entity_type="congress",
        entity_id="M_PREFLIGHT_GOOD",
        lookback_days=365,
        mode="realistic_disclosure_lag",
        limit=1,
        dry_run=False,
        benchmark="^GSPC",
        price_preflight=True,
        price_preflight_backfill=True,
        price_preflight_max_passes=2,
    )

    row = report["results"][0]
    assert row["preflight_passes_attempted"] == 1
    assert row["final_curve_quality_status"] == "good"
    assert row["preflight_stopped_reason"] == "curve_quality_good"


def test_price_preflight_does_not_repeatedly_retry_terminal_provider_history(monkeypatch):
    engine, SessionLocal = _session_factory()
    monkeypatch.setattr(compute_module, "engine", engine)
    monkeypatch.setattr(compute_module, "SessionLocal", SessionLocal)
    start = date(2026, 1, 2)
    end = date(2026, 1, 9)
    poor = _fake_portfolio_simulation(
        status="poor",
        avg_priced=20.0,
        pct_gap=80.0,
        suggested_symbols=["WBA"],
        suggested_start=start,
        suggested_end=end,
    )
    provider_calls: list[str] = []
    monkeypatch.setattr(compute_module, "load_replicated_portfolio_events", lambda *args, **kwargs: ([], []))
    monkeypatch.setattr(compute_module, "run_replicated_portfolio_simulation", lambda *args, **kwargs: poor)
    monkeypatch.setattr(
        compute_module,
        "_fetch_provider_eod_close_series",
        lambda symbol, start_date, end_date: provider_calls.append(symbol) or ({"2026-01-02": 100.0}, symbol),
    )

    report = compute_module.run_compute(
        entity_type="congress",
        entity_id="M_PREFLIGHT_TERMINAL",
        lookback_days=365,
        mode="realistic_disclosure_lag",
        limit=1,
        dry_run=False,
        benchmark="^GSPC",
        price_preflight=True,
        price_preflight_backfill=True,
        price_preflight_max_passes=3,
    )

    row = report["results"][0]
    assert row["preflight_passes_attempted"] == 1
    assert row["preflight_stopped_reason"] == "no_retryable_suggested_symbols"
    assert provider_calls == ["WBA"]
    assert "provider history ended" in row["preflight_terminal_provider_notes"][0]


def test_compute_output_unchanged_without_price_preflight(monkeypatch):
    engine, SessionLocal = _session_factory()
    monkeypatch.setattr(compute_module, "engine", engine)
    monkeypatch.setattr(compute_module, "SessionLocal", SessionLocal)
    with SessionLocal() as db:
        _add_congress_portfolio_fixture(db, member_id="M_NO_PREFLIGHT", event_id=2500)
        db.commit()

    report = compute_module.run_compute(
        entity_type="congress",
        entity_id="M_NO_PREFLIGHT",
        lookback_days=365,
        mode="realistic_disclosure_lag",
        limit=1,
        dry_run=True,
        benchmark="^GSPC",
    )

    row = report["results"][0]
    assert "preflight_passes_attempted" not in row
    assert "initial_curve_quality_status" not in row


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
