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
from app.services.backtesting.models import BacktestStrategyConfig, ResolvedPosition


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


def test_overlapping_positions_are_weighted_not_summed():
    positions = [
        ResolvedPosition(symbol="AAPL", entry_date=date(2024, 1, 2), exit_date=date(2024, 1, 3), entry_price=100.0, exit_price=120.0, return_pct=20.0),
        ResolvedPosition(symbol="MSFT", entry_date=date(2024, 1, 2), exit_date=date(2024, 1, 3), entry_price=100.0, exit_price=110.0, return_pct=10.0),
    ]
    timeline, daily_returns, total_contributions = build_equity_timeline(
        positions=positions,
        price_histories={
            "AAPL": {"2024-01-02": 100.0, "2024-01-03": 120.0},
            "MSFT": {"2024-01-02": 100.0, "2024-01-03": 110.0},
        },
        benchmark_history={"2024-01-02": 100.0, "2024-01-03": 100.0},
        start_date=date(2024, 1, 2),
        end_date=date(2024, 1, 3),
        start_balance=100.0,
        contribution_amount=0.0,
        contribution_frequency="none",
        rebalancing_frequency="monthly",
    )

    assert total_contributions == 0.0
    assert len(daily_returns) == 1
    assert round(daily_returns[0] * 100, 4) == 15.0
    assert timeline[0].cash == 0.0
    assert timeline[-1].active_positions == 2
    assert round(timeline[-1].strategy_value, 4) == 115.0


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

        assert len(result.positions) == 1
        assert result.positions[0].entry_date == "2024-01-08"
        assert result.positions[0].entry_price == 110.0
    finally:
        db.close()


def test_monthly_contributions_raise_balances_and_benchmark_also_receives_them():
    db = _session()
    try:
        user = _user(db, "premium@example.com")
        watchlist = Watchlist(name="Empty", owner_user_id=user.id)
        db.add(watchlist)
        _price(db, "^GSPC", "2024-01-02", 100.0)
        _price(db, "^GSPC", "2024-02-02", 110.0)
        db.commit()
        db.refresh(watchlist)

        result = run_backtest(
            db,
            BacktestStrategyConfig(
                strategy_type="watchlist",
                watchlist_id=watchlist.id,
                start_date=date(2024, 1, 2),
                end_date=date(2024, 2, 2),
                hold_days=90,
                start_balance=100.0,
                contribution_amount=10.0,
                contribution_frequency="monthly",
                rebalancing_frequency="monthly",
            ),
            user_id=user.id,
        )

        assert result.summary.total_contributions == 10.0
        assert result.summary.ending_balance == 110.0
        assert result.summary.benchmark_ending_balance == 120.0
        assert result.summary.strategy_return_pct == 0.0
        assert result.timeline[-1].benchmark_value == 120.0
    finally:
        db.close()


def test_cagr_uses_time_weighted_return_not_contributions():
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
        assert result.summary.ending_balance > result.summary.start_balance
    finally:
        db.close()


def test_sharpe_is_null_for_flat_curve_and_numeric_when_returns_vary():
    db = _session()
    try:
        user = _user(db, "premium@example.com")
        flat_watchlist = _watchlist(db, user_id=user.id, name="Flat", symbols=["AAPL"])
        _price(db, "AAPL", "2024-01-02", 100.0)
        _price(db, "AAPL", "2024-01-03", 100.0)
        _price(db, "^GSPC", "2024-01-02", 100.0)
        _price(db, "^GSPC", "2024-01-03", 100.0)
        db.commit()

        flat_result = run_backtest(
            db,
            BacktestStrategyConfig(
                strategy_type="watchlist",
                watchlist_id=flat_watchlist.id,
                start_date=date(2024, 1, 2),
                end_date=date(2024, 1, 3),
                hold_days=90,
            ),
            user_id=user.id,
        )

        assert flat_result.summary.sharpe_ratio is None

        varied_watchlist = _watchlist(db, user_id=user.id, name="Trend", symbols=["MSFT"])
        _price(db, "MSFT", "2024-02-01", 100.0)
        _price(db, "MSFT", "2024-02-02", 110.0)
        _price(db, "MSFT", "2024-02-05", 99.0)
        _price(db, "^GSPC", "2024-02-01", 100.0)
        _price(db, "^GSPC", "2024-02-02", 101.0)
        _price(db, "^GSPC", "2024-02-05", 102.0)
        db.commit()

        varied_result = run_backtest(
            db,
            BacktestStrategyConfig(
                strategy_type="watchlist",
                watchlist_id=varied_watchlist.id,
                start_date=date(2024, 2, 1),
                end_date=date(2024, 2, 5),
                hold_days=90,
            ),
            user_id=user.id,
        )

        assert varied_result.summary.sharpe_ratio is not None
    finally:
        db.close()


def test_max_drawdown_metric_uses_peak_to_trough_drop():
    assert round(compute_max_drawdown_pct([100.0, 120.0, 90.0, 110.0]), 4) == 25.0


def test_empty_watchlist_backtest_returns_flat_strategy_gracefully():
    db = _session()
    try:
        user = _user(db, "premium@example.com")
        watchlist = Watchlist(name="Empty", owner_user_id=user.id)
        db.add(watchlist)
        _price(db, "^GSPC", "2024-01-02", 100.0)
        _price(db, "^GSPC", "2024-01-03", 105.0)
        db.commit()
        db.refresh(watchlist)

        result = run_backtest(
            db,
            BacktestStrategyConfig(
                strategy_type="watchlist",
                watchlist_id=watchlist.id,
                start_date=date(2024, 1, 2),
                end_date=date(2024, 1, 3),
                hold_days=90,
            ),
            user_id=user.id,
        )

        assert result.summary.strategy_return_pct == 0.0
        assert result.summary.positions_count == 0
        assert result.timeline[-1].strategy_value == 10000.0
        assert result.timeline[-1].benchmark_value == 10500.0
    finally:
        db.close()


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
