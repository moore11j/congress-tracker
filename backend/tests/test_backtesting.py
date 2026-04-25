from __future__ import annotations

import json
from datetime import date, datetime, timezone

from fastapi import HTTPException
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from starlette.requests import Request

from app.auth import sign_session_payload
from app.db import Base
from app.models import Event, PriceCache, Security, UserAccount, Watchlist, WatchlistItem
from app.routers.backtests import backtest_run
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
        max_position_weight=0.25,
    )


def test_two_plus_100_positions_cannot_exceed_plus_50_portfolio_return():
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

    assert round(simulation.timeline[-1].strategy_value, 4) == 150.0
    assert simulation.timeline[-1].strategy_return_pct <= 50.0


def test_one_plus_500_position_cannot_contribute_more_than_plus_125():
    simulation = _simulation(
        positions=[
            ResolvedPosition(symbol="NVDA", entry_date=date(2024, 1, 2), exit_date=date(2024, 1, 3), entry_price=100.0, exit_price=600.0, return_pct=500.0),
        ],
        price_histories={"NVDA": {"2024-01-02": 100.0, "2024-01-03": 600.0}},
        benchmark={"2024-01-02": 100.0, "2024-01-03": 100.0},
    )

    assert round(simulation.timeline[-1].strategy_value, 4) == 225.0
    assert simulation.timeline[-1].strategy_return_pct <= 125.0


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
        max_position_weight=0.25,
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
        max_position_weight=0.25,
    )

    jan_31 = next(point for point in simulation.timeline if point.date == "2024-01-31")
    feb_02 = next(point for point in simulation.timeline if point.date == "2024-02-02")
    assert jan_31.active_positions == 2
    assert jan_31.invested_pct == 25.0
    assert feb_02.invested_pct == 50.0


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


def test_max_position_weight_never_exceeds_25_percent():
    simulation = _simulation(
        positions=[
            ResolvedPosition(symbol="NVDA", entry_date=date(2024, 1, 2), exit_date=date(2024, 1, 3), entry_price=100.0, exit_price=600.0, return_pct=500.0),
        ],
        price_histories={"NVDA": {"2024-01-02": 100.0, "2024-01-03": 600.0}},
        benchmark={"2024-01-02": 100.0, "2024-01-03": 100.0},
    )

    assert simulation.diagnostics.max_position_weight_observed <= 25.0001


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

        assert round(result.summary.max_drawdown_pct, 4) == 15.4167
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
