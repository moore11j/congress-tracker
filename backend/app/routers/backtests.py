from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, Depends, Request
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.auth import current_user
from app.db import get_db
from app.entitlements import current_entitlements, require_feature
from app.models import SavedScreen, Watchlist, WatchlistItem
from app.services.backtesting.engine import run_backtest
from app.services.backtesting.models import DEFAULT_BENCHMARK, HOLD_DAY_OPTIONS, BacktestStrategyConfig

router = APIRouter(tags=["backtests"])


@router.get("/backtests/presets")
def backtest_presets(request: Request, db: Session = Depends(get_db)):
    user = current_user(db, request, required=False)
    entitlements = current_entitlements(request, db)

    watchlists: list[dict[str, object]] = []
    saved_screens: list[dict[str, object]] = []
    if user is not None:
        watchlist_rows = db.execute(
            select(
                Watchlist.id,
                Watchlist.name,
                func.count(WatchlistItem.id).label("ticker_count"),
            )
            .select_from(Watchlist)
            .outerjoin(WatchlistItem, WatchlistItem.watchlist_id == Watchlist.id)
            .where(Watchlist.owner_user_id == user.id)
            .group_by(Watchlist.id, Watchlist.name)
            .order_by(Watchlist.name.asc(), Watchlist.id.asc())
        ).all()
        watchlists = [
            {"id": int(row.id), "name": row.name, "ticker_count": int(row.ticker_count or 0)}
            for row in watchlist_rows
        ]

        screen_rows = (
            db.execute(
                select(SavedScreen)
                .where(SavedScreen.user_id == user.id)
                .order_by(SavedScreen.updated_at.desc(), SavedScreen.id.desc())
            )
            .scalars()
            .all()
        )
        saved_screens = [
            {
                "id": int(screen.id),
                "name": screen.name,
                "last_refreshed_at": screen.last_refreshed_at,
                "updated_at": screen.updated_at,
            }
            for screen in screen_rows
        ]

    return {
        "today": datetime.now(timezone.utc).date().isoformat(),
        "defaults": {
            "benchmark": DEFAULT_BENCHMARK,
            "weighting": "equal",
            "hold_days": 90,
            "lookback_days": 365,
            "start_balance": 10000,
            "contribution_amount": 0,
            "contribution_frequency": "none",
            "rebalancing_frequency": "monthly",
            "max_position_weight": 1.0,
        },
        "access": {
            "tier": entitlements.tier,
            "can_run": entitlements.has_feature("backtesting"),
            "signed_in": user is not None,
        },
        "strategy_types": [
            {"key": "watchlist", "label": "Watchlist"},
            {"key": "saved_screen", "label": "Screens"},
            {"key": "congress", "label": "Congress"},
            {"key": "insider", "label": "Insider"},
            {"key": "custom_tickers", "label": "Custom"},
        ],
        "lookback_options": [
            {"days": 30, "label": "30D"},
            {"days": 90, "label": "90D"},
            {"days": 180, "label": "180D"},
            {"days": 365, "label": "1Y"},
            {"days": 1095, "label": "3Y"},
        ],
        "hold_day_options": [{"days": days, "label": str(days)} for days in HOLD_DAY_OPTIONS],
        "benchmark_options": [{"symbol": DEFAULT_BENCHMARK, "label": "S&P 500"}],
        "contribution_frequency_options": [
            {"key": "none", "label": "None"},
            {"key": "monthly", "label": "Monthly"},
            {"key": "quarterly", "label": "Quarterly"},
            {"key": "annually", "label": "Annually"},
        ],
        "rebalancing_frequency_options": [
            {"key": "monthly", "label": "Monthly"},
            {"key": "quarterly", "label": "Quarterly"},
            {"key": "semi_annually", "label": "Semi-annually"},
            {"key": "annually", "label": "Annually"},
        ],
        "source_scopes": {
            "congress": [
                {"key": "all_congress", "label": "All Congress"},
                {"key": "house", "label": "House"},
                {"key": "senate", "label": "Senate"},
                {"key": "member", "label": "Specific Member"},
            ],
            "insider": [
                {"key": "all_insiders", "label": "All Insiders"},
                {"key": "insider", "label": "Specific Insider"},
            ],
        },
        "watchlists": watchlists,
        "saved_screens": saved_screens,
    }


@router.post("/backtests/run")
def backtest_run(
    payload: BacktestStrategyConfig,
    request: Request,
    db: Session = Depends(get_db),
):
    user = current_user(db, request, required=True)
    require_feature(
        current_entitlements(request, db),
        "backtesting",
        message="Portfolio backtesting is included with Premium.",
    )
    return run_backtest(db, payload, user_id=user.id)
