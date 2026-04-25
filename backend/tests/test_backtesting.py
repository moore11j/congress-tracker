from __future__ import annotations

import json
from datetime import date, datetime, timezone

from fastapi import HTTPException
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from starlette.requests import Request

from app.auth import sign_session_payload
from app.db import Base
from app.models import Event, Member, PriceCache, Security, UserAccount, Watchlist, WatchlistItem
from app.routers.backtests import backtest_run
from app.routers.events import suggest_member_insider
from app.services.backtesting.engine import build_equity_timeline, run_backtest
from app.services.backtesting.metrics import compute_max_drawdown_pct
from app.services.backtesting.models import MAX_CUSTOM_TICKERS, BacktestStrategyConfig, ResolvedPosition


def _session():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    Base.metadata.create_all(bind=engine)
    return SessionLocal()


def _request_for_user(user: UserAccount) -> Request:
    token = sign_session_payload({"uid": user.id, "email": user.email})
    return Request({"type": "http", "method": "POST", "path": "/", "headers": [(b"authorization", f"Bearer {token}".encode())]})


def _user(db: Session, email: str, *, tier: str = "premium") -> UserAccount:
    user = UserAccount(email=email, role="user", entitlement_tier=tier)
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


def _price(db: Session, symbol: str, day: str, close: float) -> None:
    db.add(PriceCache(symbol=symbol, date=day, close=close))


def _watchlist(db: Session, *, user_id: int, name: str, symbols: list[str]) -> Watchlist:
    watchlist = Watchlist(name=name, owner_user_id=user_id)
    db.add(watchlist)
    db.flush()
    for symbol in symbols:
        security = Security(symbol=symbol, name=symbol, asset_class="stock", sector=None)
        db.add(security)
        db.flush()
        db.add(WatchlistItem(watchlist_id=watchlist.id, security_id=security.id))
    db.commit()
    db.refresh(watchlist)
    return watchlist


def _simulation(positions: list[ResolvedPosition], price_histories: dict[str, dict[str, float]], benchmark: dict[str, float]):
    return build_equity_timeline(
        positions=positions,
        price_histories=price_histories,
        benchmark_history=benchmark,
        start_date=min(position.entry_date for position in positions),
        end_date=max(position.exit_date for position in positions),
        start_balance=100.0,
        contribution_amount=0.0,
        contribution_frequency="none",
        rebalancing_frequency="monthly",
    )


def test_two_active_tickers_allocate_roughly_fifty_fifty():
    simulation = _simulation(
        positions=[
            ResolvedPosition(symbol="AAPL", entry_date=date(2024, 1, 2), exit_date=date(2024, 1, 3), entry_price=100.0, exit_price=200.0, return_pct=100.0),
            ResolvedPosition(symbol="MSFT", entry_date=date(2024, 1, 2), exit_date=date(2024, 1, 3), entry_price=100.0, exit_price=200.0, return_pct=100.0),
        ],
        price_histories={
            "AAPL": {"2024-01-02": 100.0, "2024-01-03": 200.0},
            "MSFT": {"2024-01-02": 100.0, "2024-01-03": 200.0},
        },
        benchmark={"2024-01-02": 100.0, "2024-01-03": 100.0},
    )

    jan_02 = next(point for point in simulation.timeline if point.date == "2024-01-02")
    assert jan_02.invested_pct == 100.0
    assert round(simulation.diagnostics.max_position_weight_observed, 2) == 50.0
    assert round(simulation.timeline[-1].strategy_value, 4) == 200.0


def test_single_active_ticker_can_reach_full_concentration():
    simulation = _simulation(
        positions=[
            ResolvedPosition(symbol="NVDA", entry_date=date(2024, 1, 2), exit_date=date(2024, 1, 3), entry_price=100.0, exit_price=600.0, return_pct=500.0),
        ],
        price_histories={"NVDA": {"2024-01-02": 100.0, "2024-01-03": 600.0}},
        benchmark={"2024-01-02": 100.0, "2024-01-03": 100.0},
    )

    jan_02 = next(point for point in simulation.timeline if point.date == "2024-01-02")
    assert jan_02.invested_pct == 100.0
    assert round(simulation.diagnostics.max_position_weight_observed, 2) == 100.0
    assert round(simulation.timeline[-1].strategy_value, 4) == 600.0


def test_three_active_tickers_allocate_roughly_thirds():
    simulation = _simulation(
        positions=[
            ResolvedPosition(symbol="AAPL", entry_date=date(2024, 1, 2), exit_date=date(2024, 1, 3), entry_price=100.0, exit_price=100.0, return_pct=0.0),
            ResolvedPosition(symbol="MSFT", entry_date=date(2024, 1, 2), exit_date=date(2024, 1, 3), entry_price=100.0, exit_price=100.0, return_pct=0.0),
            ResolvedPosition(symbol="NVDA", entry_date=date(2024, 1, 2), exit_date=date(2024, 1, 3), entry_price=100.0, exit_price=100.0, return_pct=0.0),
        ],
        price_histories={
            "AAPL": {"2024-01-02": 100.0, "2024-01-03": 100.0},
            "MSFT": {"2024-01-02": 100.0, "2024-01-03": 100.0},
            "NVDA": {"2024-01-02": 100.0, "2024-01-03": 100.0},
        },
        benchmark={"2024-01-02": 100.0, "2024-01-03": 100.0},
    )

    assert round(simulation.diagnostics.max_position_weight_observed, 2) == 33.33


def test_contributions_do_not_inflate_time_weighted_return():
    simulation = build_equity_timeline(
        positions=[],
        price_histories={},
        benchmark_history={"2024-01-02": 100.0, "2024-02-02": 100.0},
        start_date=date(2024, 1, 2),
        end_date=date(2024, 2, 2),
        start_balance=100.0,
        contribution_amount=50.0,
        contribution_frequency="monthly",
        rebalancing_frequency="monthly",
    )

    assert simulation.total_contributions == 150.0
    assert simulation.timeline[-1].strategy_value == 150.0
    assert simulation.timeline[-1].strategy_return_pct == 0.0


def test_monthly_rebalance_does_not_buy_mid_month_entries_until_next_rebalance():
    simulation = build_equity_timeline(
        positions=[
            ResolvedPosition(symbol="AAPL", entry_date=date(2024, 1, 2), exit_date=date(2024, 2, 28), entry_price=100.0, exit_price=100.0, return_pct=0.0),
            ResolvedPosition(symbol="MSFT", entry_date=date(2024, 1, 15), exit_date=date(2024, 2, 28), entry_price=100.0, exit_price=100.0, return_pct=0.0),
        ],
        price_histories={
            "AAPL": {"2024-01-02": 100.0, "2024-01-15": 100.0, "2024-01-31": 100.0, "2024-02-02": 100.0, "2024-02-28": 100.0},
            "MSFT": {"2024-01-15": 100.0, "2024-01-31": 100.0, "2024-02-02": 100.0, "2024-02-28": 100.0},
        },
        benchmark_history={"2024-01-02": 100.0, "2024-01-15": 100.0, "2024-01-31": 100.0, "2024-02-02": 100.0, "2024-02-28": 100.0},
        start_date=date(2024, 1, 2),
        end_date=date(2024, 2, 28),
        start_balance=100.0,
        contribution_amount=0.0,
        contribution_frequency="none",
        rebalancing_frequency="monthly",
    )

    jan_31 = next(point for point in simulation.timeline if point.date == "2024-01-31")
    feb_02 = next(point for point in simulation.timeline if point.date == "2024-02-02")
    assert jan_31.active_positions == 2
    assert jan_31.invested_pct == 100.0
    assert feb_02.invested_pct == 100.0


def test_forced_exits_sell_positions_and_move_proceeds_to_cash():
    simulation = _simulation(
        positions=[
            ResolvedPosition(symbol="AAPL", entry_date=date(2024, 1, 2), exit_date=date(2024, 1, 10), entry_price=100.0, exit_price=100.0, return_pct=0.0),
        ],
        price_histories={"AAPL": {"2024-01-02": 100.0, "2024-01-10": 100.0}},
        benchmark={"2024-01-02": 100.0, "2024-01-10": 100.0},
    )

    assert simulation.timeline[-1].cash == 100.0
    assert simulation.timeline[-1].invested_pct == 0.0


def test_gross_exposure_never_exceeds_100_percent():
    simulation = _simulation(
        positions=[
            ResolvedPosition(symbol="AAPL", entry_date=date(2024, 1, 2), exit_date=date(2024, 1, 3), entry_price=100.0, exit_price=200.0, return_pct=100.0),
            ResolvedPosition(symbol="MSFT", entry_date=date(2024, 1, 2), exit_date=date(2024, 1, 3), entry_price=100.0, exit_price=200.0, return_pct=100.0),
            ResolvedPosition(symbol="AMZN", entry_date=date(2024, 1, 2), exit_date=date(2024, 1, 3), entry_price=100.0, exit_price=200.0, return_pct=100.0),
            ResolvedPosition(symbol="META", entry_date=date(2024, 1, 2), exit_date=date(2024, 1, 3), entry_price=100.0, exit_price=200.0, return_pct=100.0),
        ],
        price_histories={
            "AAPL": {"2024-01-02": 100.0, "2024-01-03": 200.0},
            "MSFT": {"2024-01-02": 100.0, "2024-01-03": 200.0},
            "AMZN": {"2024-01-02": 100.0, "2024-01-03": 200.0},
            "META": {"2024-01-02": 100.0, "2024-01-03": 200.0},
        },
        benchmark={"2024-01-02": 100.0, "2024-01-03": 100.0},
    )

    assert simulation.diagnostics.max_invested_pct <= 100.0001


def test_signal_backtest_uses_first_close_on_or_after_disclosure_date():
    db = _session()
    try:
        user = _user(db, "premium@example.com")
        db.add(
            Event(
                id=1,
                event_type="insider_trade",
                ts=datetime(2024, 1, 6, 12, 0, tzinfo=timezone.utc),
                event_date=datetime(2024, 1, 6, 12, 0, tzinfo=timezone.utc),
                symbol="AAPL",
                source="insider",
                trade_type="purchase",
                amount_min=1000,
                amount_max=5000,
                payload_json=json.dumps(
                    {
                        "symbol": "AAPL",
                        "filing_date": "2024-01-06",
                        "transaction_date": "2024-01-03",
                        "reporting_cik": "0001234567",
                        "insider_name": "Example Insider",
                    }
                ),
            )
        )
        _price(db, "AAPL", "2024-01-05", 99.0)
        _price(db, "AAPL", "2024-01-08", 110.0)
        _price(db, "AAPL", "2024-02-07", 121.0)
        _price(db, "^GSPC", "2024-01-08", 100.0)
        _price(db, "^GSPC", "2024-02-07", 101.0)
        db.commit()

        result = run_backtest(
            db,
            BacktestStrategyConfig(
                strategy_type="insider",
                source_scope="insider",
                insider_cik="0001234567",
                start_date=date(2024, 1, 1),
                end_date=date(2024, 2, 10),
                hold_days=30,
            ),
            user_id=user.id,
        )

        assert result.positions[0].entry_date == "2024-01-08"
    finally:
        db.close()


def test_custom_tickers_normalizes_uppercase_and_deduplicates():
    config = BacktestStrategyConfig(
        strategy_type="custom_tickers",
        tickers=[" aapl ", "MSFT", "aapl", " msft "],
        start_date=date(2024, 1, 1),
        end_date=date(2024, 2, 1),
        hold_days=30,
    )

    assert config.tickers == ["AAPL", "MSFT"]


def test_custom_tickers_rejects_more_than_v1_limit():
    try:
        BacktestStrategyConfig(
            strategy_type="custom_tickers",
            tickers=[f"TICK{i}" for i in range(MAX_CUSTOM_TICKERS + 1)],
            start_date=date(2024, 1, 1),
            end_date=date(2024, 2, 1),
            hold_days=30,
        )
    except ValueError as exc:
        assert "at most" in str(exc)
    else:
        raise AssertionError("Expected custom ticker limit validation error")


def test_custom_tickers_backtest_builds_static_positions():
    db = _session()
    try:
        user = _user(db, "premium@example.com")
        _price(db, "AAPL", "2024-01-02", 100.0)
        _price(db, "AAPL", "2024-01-05", 110.0)
        _price(db, "MSFT", "2024-01-02", 200.0)
        _price(db, "MSFT", "2024-01-05", 220.0)
        _price(db, "^GSPC", "2024-01-02", 100.0)
        _price(db, "^GSPC", "2024-01-05", 101.0)
        db.commit()

        result = run_backtest(
            db,
            BacktestStrategyConfig(
                strategy_type="custom_tickers",
                tickers=["aapl", "MSFT", "AAPL"],
                start_date=date(2024, 1, 2),
                end_date=date(2024, 1, 5),
                hold_days=30,
            ),
            user_id=user.id,
        )

        assert [position.symbol for position in result.positions] == ["AAPL", "MSFT"]
        assert result.summary.trade_count == 2
    finally:
        db.close()


def test_custom_ticker_allocations_are_respected():
    simulation = build_equity_timeline(
        positions=[
            ResolvedPosition(symbol="AAPL", entry_date=date(2024, 1, 2), exit_date=date(2024, 1, 3), entry_price=100.0, exit_price=100.0, return_pct=0.0),
            ResolvedPosition(symbol="MSFT", entry_date=date(2024, 1, 2), exit_date=date(2024, 1, 3), entry_price=100.0, exit_price=100.0, return_pct=0.0),
        ],
        price_histories={
            "AAPL": {"2024-01-02": 100.0, "2024-01-03": 100.0},
            "MSFT": {"2024-01-02": 100.0, "2024-01-03": 100.0},
        },
        benchmark_history={"2024-01-02": 100.0, "2024-01-03": 100.0},
        start_date=date(2024, 1, 2),
        end_date=date(2024, 1, 3),
        start_balance=100.0,
        contribution_amount=0.0,
        contribution_frequency="none",
        rebalancing_frequency="monthly",
        custom_allocations={"AAPL": 70.0, "MSFT": 30.0},
    )

    assert round(simulation.diagnostics.max_position_weight_observed, 2) == 70.0


def test_custom_ticker_allocations_require_full_hundred_percent():
    try:
        BacktestStrategyConfig(
            strategy_type="custom_tickers",
            tickers=[{"symbol": "AAPL", "allocation_pct": 70}, {"symbol": "MSFT", "allocation_pct": 20}],
            start_date=date(2024, 1, 1),
            end_date=date(2024, 2, 1),
            hold_days=30,
        )
    except ValueError as exc:
        assert "100%" in str(exc)
    else:
        raise AssertionError("Expected custom allocation total validation error")


def test_signal_entry_weekend_uses_next_available_close_and_records_fallback():
    db = _session()
    try:
        user = _user(db, "premium@example.com")
        db.add(
            Event(
                id=11,
                event_type="insider_trade",
                ts=datetime(2024, 1, 6, 12, 0, tzinfo=timezone.utc),
                event_date=datetime(2024, 1, 6, 12, 0, tzinfo=timezone.utc),
                symbol="AAPL",
                source="insider",
                trade_type="purchase",
                amount_min=1000,
                amount_max=5000,
                payload_json=json.dumps({"symbol": "AAPL", "filing_date": "2024-01-06", "reporting_cik": "0001234567"}),
            )
        )
        _price(db, "AAPL", "2024-01-08", 110.0)
        _price(db, "AAPL", "2024-02-05", 121.0)
        _price(db, "^GSPC", "2024-01-08", 100.0)
        _price(db, "^GSPC", "2024-02-05", 101.0)
        db.commit()

        result = run_backtest(
            db,
            BacktestStrategyConfig(
                strategy_type="insider",
                source_scope="insider",
                insider_cik="0001234567",
                start_date=date(2024, 1, 1),
                end_date=date(2024, 2, 10),
                hold_days=30,
            ),
            user_id=user.id,
        )

        assert result.positions[0].entry_date == "2024-01-08"
        assert result.positions[0].price_fallback_used is True
        assert result.summary.price_fallback_positions_count == 1
    finally:
        db.close()


def test_signal_exit_missing_close_uses_prior_fallback_without_skip():
    db = _session()
    try:
        user = _user(db, "premium@example.com")
        db.add(
            Event(
                id=12,
                event_type="insider_trade",
                ts=datetime(2024, 1, 2, 12, 0, tzinfo=timezone.utc),
                event_date=datetime(2024, 1, 2, 12, 0, tzinfo=timezone.utc),
                symbol="AAPL",
                source="insider",
                trade_type="purchase",
                amount_min=1000,
                amount_max=5000,
                payload_json=json.dumps({"symbol": "AAPL", "filing_date": "2024-01-02", "reporting_cik": "0001234567"}),
            )
        )
        _price(db, "AAPL", "2024-01-02", 100.0)
        _price(db, "AAPL", "2024-01-31", 115.0)
        _price(db, "AAPL", "2024-02-06", 120.0)
        _price(db, "^GSPC", "2024-01-02", 100.0)
        _price(db, "^GSPC", "2024-01-31", 101.0)
        _price(db, "^GSPC", "2024-02-06", 102.0)
        db.commit()

        result = run_backtest(
            db,
            BacktestStrategyConfig(
                strategy_type="insider",
                source_scope="insider",
                insider_cik="0001234567",
                start_date=date(2024, 1, 1),
                end_date=date(2024, 2, 10),
                hold_days=30,
            ),
            user_id=user.id,
        )

        assert result.positions[0].exit_date == "2024-01-31"
        assert result.summary.skipped_positions_count == 0
        assert result.summary.price_fallback_positions_count == 1
    finally:
        db.close()


def test_congress_member_autosuggest_prefers_canonical_member_identity():
    db = _session()
    try:
        db.add_all([
            Member(bioguide_id="P000197", first_name="Nancy", last_name="Pelosi", chamber="House", party="D", state="CA"),
            Member(bioguide_id="FMP_HOUSE_CA11_NANCY_PELOSI", first_name="Nancy", last_name="Pelosi", chamber="House", party="D", state="CA"),
        ])
        db.commit()

        response = suggest_member_insider(db=db, q="pelosi", limit=10)

        assert len(response["items"]) == 1
        assert response["items"][0]["bioguide_id"] == "P000197"
    finally:
        db.close()


def test_cagr_uses_time_weighted_return_not_ending_balance_with_contributions():
    db = _session()
    try:
        user = _user(db, "premium@example.com")
        watchlist = Watchlist(name="Empty", owner_user_id=user.id)
        db.add(watchlist)
        _price(db, "^GSPC", "2024-01-02", 100.0)
        _price(db, "^GSPC", "2024-02-02", 100.0)
        _price(db, "^GSPC", "2025-01-02", 100.0)
        db.commit()
        db.refresh(watchlist)

        result = run_backtest(
            db,
            BacktestStrategyConfig(
                strategy_type="watchlist",
                watchlist_id=watchlist.id,
                start_date=date(2024, 1, 2),
                end_date=date(2025, 1, 2),
                hold_days=90,
                start_balance=100.0,
                contribution_amount=10.0,
                contribution_frequency="monthly",
                rebalancing_frequency="monthly",
            ),
            user_id=user.id,
        )

        assert result.summary.strategy_return_pct == 0.0
        assert result.summary.cagr_pct == 0.0
        assert result.summary.total_contributions > result.summary.start_balance
    finally:
        db.close()


def test_max_drawdown_uses_indexed_curve_not_raw_balance_with_contributions():
    db = _session()
    try:
        user = _user(db, "premium@example.com")
        watchlist = _watchlist(db, user_id=user.id, name="Drawdown", symbols=["AAPL"])
        _price(db, "AAPL", "2024-01-02", 100.0)
        _price(db, "AAPL", "2024-01-03", 50.0)
        _price(db, "AAPL", "2024-02-02", 50.0)
        _price(db, "AAPL", "2024-02-05", 25.0)
        _price(db, "^GSPC", "2024-01-02", 100.0)
        _price(db, "^GSPC", "2024-01-03", 100.0)
        _price(db, "^GSPC", "2024-02-02", 100.0)
        _price(db, "^GSPC", "2024-02-05", 100.0)
        db.commit()

        result = run_backtest(
            db,
            BacktestStrategyConfig(
                strategy_type="watchlist",
                watchlist_id=watchlist.id,
                start_date=date(2024, 1, 2),
                end_date=date(2024, 2, 5),
                hold_days=90,
                start_balance=100.0,
                contribution_amount=100.0,
                contribution_frequency="monthly",
                rebalancing_frequency="annually",
            ),
            user_id=user.id,
        )

        assert round(result.summary.max_drawdown_pct, 4) == 58.3333
    finally:
        db.close()


def test_max_drawdown_metric_uses_peak_to_trough_drop():
    assert round(compute_max_drawdown_pct([100.0, 120.0, 90.0, 110.0]), 4) == 25.0


def test_backtest_route_is_premium_gated():
    db = _session()
    try:
        user = _user(db, "free@example.com", tier="free")
        try:
            backtest_run(
                BacktestStrategyConfig(
                    strategy_type="congress",
                    source_scope="all_congress",
                    start_date=date(2024, 1, 1),
                    end_date=date(2024, 3, 1),
                    hold_days=30,
                ),
                _request_for_user(user),
                db,
            )
        except HTTPException as exc:
            assert exc.status_code == 402
            assert exc.detail["feature"] == "backtesting"
        else:
            raise AssertionError("Expected premium-required response")
    finally:
        db.close()
