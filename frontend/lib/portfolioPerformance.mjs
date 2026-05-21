export const PORTFOLIO_MODE = "realistic_disclosure_lag";

export const PORTFOLIO_LOOKBACK_OPTIONS = [
  { label: "30D", value: 30 },
  { label: "90D", value: 90 },
  { label: "180D", value: 180 },
  { label: "1Y", value: 365 },
  { label: "3Y", value: 1095 },
];

export const DEFAULT_PORTFOLIO_LOOKBACK_DAYS = 1095;

function finiteNumber(value) {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function normalizedReturnPct(value, startingValue) {
  const current = finiteNumber(value);
  const start = finiteNumber(startingValue);
  if (current == null || start == null || start <= 0) return null;
  return ((current / start) - 1) * 100;
}

function firstFinite(values) {
  for (const value of values) {
    const numeric = finiteNumber(value);
    if (numeric != null) return numeric;
  }
  return null;
}

export function isPortfolioLookbackDays(value) {
  return PORTFOLIO_LOOKBACK_OPTIONS.some((option) => option.value === value);
}

export function normalizeMemberPortfolioChartData(portfolio) {
  const summary = portfolio?.summary ?? null;
  const points = Array.isArray(portfolio?.points) ? portfolio.points : [];
  if (!summary || points.length < 2) {
    return { memberSeries: [], benchmarkSeries: [] };
  }

  const startingPortfolioValue =
    firstFinite([summary.starting_value, points[0]?.strategy_value]) ?? null;
  const startingBenchmarkValue =
    firstFinite(points.map((point) => point?.benchmark_value)) ?? null;

  const memberSeries = [];
  const benchmarkSeries = [];

  points.forEach((point, index) => {
    const portfolioReturnPct = normalizedReturnPct(point?.strategy_value, startingPortfolioValue);
    const benchmarkReturnPct = normalizedReturnPct(point?.benchmark_value, startingBenchmarkValue);
    const asofDate = point?.asof_date ?? null;

    if (asofDate && portfolioReturnPct != null) {
      memberSeries.push({
        event_id: index,
        date: asofDate,
        symbol: null,
        asof_date: asofDate,
        return_pct: portfolioReturnPct,
        alpha_pct: point?.alpha_pct ?? null,
        benchmark_return_pct: benchmarkReturnPct,
        cumulative_return_pct: portfolioReturnPct,
        running_benchmark_return_pct: benchmarkReturnPct,
        cumulative_alpha_pct:
          benchmarkReturnPct == null ? (point?.alpha_pct ?? null) : portfolioReturnPct - benchmarkReturnPct,
        strategy_return_pct: portfolioReturnPct,
        active_positions: point?.active_positions ?? null,
      });
    }

    if (asofDate && benchmarkReturnPct != null) {
      benchmarkSeries.push({
        asof_date: asofDate,
        cumulative_return_pct: benchmarkReturnPct,
      });
    }
  });

  return { memberSeries, benchmarkSeries };
}
