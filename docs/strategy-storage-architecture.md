# Strategy Storage Architecture

Draft status: storage/read path only. Backtest refresh writers and user-facing pages should be reviewed before launch.

## Goals

- Persist curated strategy definitions separately from generated backtest output.
- Avoid recalculating full strategy histories during normal page loads.
- Keep every published result reproducible with methodology version, code version, dataset versions, fees, slippage, benchmark, universe, execution timing, and diagnostics.
- Support premium/pro gating without hiding methodology disclosures.
- Allow future strategies to be added by configuration and refresh jobs, not hard-coded page logic.

## Tables

- `strategy_definitions`: canonical strategy catalog row. Stores slug, name, category, access tier, methodology text, rule JSON, parameters, universe config, tags, risk notes, confidence, and methodology version.
- `strategy_backtest_runs`: immutable-ish run record for one strategy refresh. Stores run key, run type, status, date range, benchmark, code/data versions, parameters, universe hash, metrics JSON, diagnostics JSON, Walnut Strategy Score, costs, and timing assumptions.
- `strategy_performance_snapshots`: query-friendly metric snapshots by period, such as `30d`, `1y`, `2y`, `3y`, and `max`.
- `strategy_equity_curve_points`: daily strategy and benchmark values plus drawdown and active holding count for charting.
- `strategy_holdings_snapshots`: as-of container for one holdings refresh.
- `strategy_holding_rows`: historical holdings rows tied to a holdings snapshot.
- `strategy_current_holdings`: fast current holdings cache for strategy detail pages.

## Read Path

Backend routes read persisted rows only:

- `GET /api/strategies`
- `GET /api/strategies/{slug}`
- `GET /api/admin/strategies`

The public routes return published strategy definitions. Premium/pro access controls are evaluated from `strategy_definitions.access_tier`; locked users can see catalog/methodology metadata but not equity curves or current holdings.

## Refresh Path

Not implemented in this phase.

Recommended next writer:

1. Load a curated `strategy_definitions` config.
2. Run the correct research engine with point-in-time inputs.
3. Persist one `strategy_backtest_runs` row with deterministic `run_key`.
4. Store period metrics in `strategy_performance_snapshots`.
5. Store daily curve points in `strategy_equity_curve_points`.
6. Store latest holdings in `strategy_holdings_snapshots`, `strategy_holding_rows`, and `strategy_current_holdings`.
7. Mark stale runs as historical, but do not delete reproducibility records.

## Launch Guardrails

- Do not publish a strategy unless its run has a reproducible rule, parameters, data versions, execution timing, costs, and benchmark.
- Do not use current confirmation scores as historical scores unless point-in-time score components are available for that date.
- Do not mix transaction-date and disclosure-date results in the same published record.
- Do not rank production strategies solely by CAGR.
- Do not display a strategy as current if `strategy_current_holdings.as_of_date` or latest run freshness is stale.
