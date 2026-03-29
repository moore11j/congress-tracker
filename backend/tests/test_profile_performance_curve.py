from datetime import date
from types import SimpleNamespace

import pytest

from app.services.profile_performance_curve import build_normalized_profile_curve, build_timeline_dates


def _outcome(*, event_id: int, trade_date: date, return_pct: float, symbol: str = "XYZ"):
    return SimpleNamespace(
        event_id=event_id,
        symbol=symbol,
        trade_type="purchase",
        trade_date=trade_date,
        return_pct=return_pct,
        alpha_pct=None,
        benchmark_return_pct=None,
        holding_days=30,
    )


def test_build_normalized_profile_curve_uses_equal_weight_lots_not_additive_returns():
    timeline = build_timeline_dates(date(2026, 1, 1), date(2026, 1, 3))
    outcomes = [
        _outcome(event_id=1, trade_date=date(2026, 1, 1), return_pct=100.0),
        _outcome(event_id=2, trade_date=date(2026, 1, 2), return_pct=100.0),
    ]
    benchmark = {
        "2026-01-01": 100.0,
        "2026-01-02": 101.0,
        "2026-01-03": 102.0,
    }

    curve = build_normalized_profile_curve(
        outcomes=outcomes,
        timeline_dates=timeline,
        benchmark_close_map=benchmark,
        benchmark_dates=sorted(benchmark.keys()),
    )

    returns_by_day = {row["asof_date"]: row["cumulative_return_pct"] for row in curve.member_series}
    assert returns_by_day["2026-01-01"] == 100.0
    assert returns_by_day["2026-01-02"] == 100.0
    assert returns_by_day["2026-01-03"] == 100.0


def test_build_normalized_profile_curve_emits_full_daily_timeline_and_flat_between_events():
    timeline = build_timeline_dates(date(2026, 1, 1), date(2026, 1, 5))
    outcomes = [_outcome(event_id=7, trade_date=date(2026, 1, 3), return_pct=40.0)]
    benchmark = {
        "2026-01-01": 100.0,
        "2026-01-02": 101.0,
        "2026-01-03": 100.0,
        "2026-01-04": 101.0,
        "2026-01-05": 102.0,
    }

    curve = build_normalized_profile_curve(
        outcomes=outcomes,
        timeline_dates=timeline,
        benchmark_close_map=benchmark,
        benchmark_dates=sorted(benchmark.keys()),
    )

    assert len(curve.member_series) == 5
    assert len(curve.benchmark_series) == 5
    returns = [row["cumulative_return_pct"] for row in curve.member_series]
    assert returns[0] == pytest.approx(0.0)
    assert returns[1] == pytest.approx(0.0)
    assert returns[2] == pytest.approx(40.0)
    assert returns[3] == pytest.approx(40.0)
    assert returns[4] == pytest.approx(40.0)
