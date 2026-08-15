from datetime import date, timedelta

from app.services.strategy_performance_metrics import trailing_snapshot_values


def test_trailing_snapshots_include_risk_metrics_and_annualized_30_day_return():
    start = date(2024, 1, 2)
    points = []
    strategy_value = 100.0
    benchmark_value = 100.0
    for offset in range(401):
        if offset:
            strategy_value *= 1.004 if offset % 2 else 0.999
            benchmark_value *= 1.002 if offset % 3 else 0.999
        points.append(
            {
                "date": (start + timedelta(days=offset)).isoformat(),
                "strategy_value": strategy_value,
                "benchmark_value": benchmark_value,
                "active_lots": 5,
            }
        )

    snapshots = trailing_snapshot_values(
        strategy_id=1,
        run_id=2,
        as_of_date=start + timedelta(days=400),
        points=points,
        baseline_metrics={"total_return_pct": 20.0, "cagr_pct": 18.0, "sharpe": 1.1},
        walnut_score=72.0,
    )
    by_period = {snapshot["period"]: snapshot for snapshot in snapshots}

    assert by_period["30d"]["cagr_pct"] is not None
    assert by_period["30d"]["sharpe"] is not None
    assert by_period["30d"]["max_drawdown_pct"] is not None
    assert by_period["30d"]["annualized_volatility_pct"] is not None
    assert by_period["1y"]["cagr_pct"] is not None
    assert by_period["1y"]["sharpe"] is not None
    assert by_period["2y"]["cagr_pct"] is None
