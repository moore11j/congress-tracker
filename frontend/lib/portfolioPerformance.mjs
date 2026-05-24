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

function amountMidpoint(position) {
  const min = finiteNumber(position?.amount_min);
  const max = finiteNumber(position?.amount_max);
  if (min != null && max != null) return (min + max) / 2;
  return max ?? min;
}

function positionValue(position, price) {
  const amount = amountMidpoint(position);
  if (amount != null) return amount;
  const shares = finiteNumber(position?.shares);
  if (shares != null && price != null) return shares * price;
  return finiteNumber(position?.market_value);
}

function normalizedMarkerSide(side) {
  const normalized = String(side ?? "").trim().toLowerCase();
  return ["sale", "sell", "s", "decrease", "disposal", "disposed"].some((token) => normalized.includes(token))
    ? "Sell"
    : "Buy";
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
        strategy_value: finiteNumber(point?.strategy_value),
        benchmark_value: finiteNumber(point?.benchmark_value),
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

export function normalizeMemberPortfolioEventMarkers(portfolio) {
  const positions = Array.isArray(portfolio?.positions) ? portfolio.positions : [];
  const markers = [];
  positions.forEach((position, index) => {
    const status = String(position?.status ?? "").toLowerCase();
    const symbol = position?.symbol ? String(position.symbol).toUpperCase() : null;
    if (!symbol) return;

    if (status === "skipped") {
      const skippedDate = position.report_date ?? position.trade_date ?? null;
      if (!skippedDate) return;
      markers.push({
        id: `${position.source_event_id ?? index}-${symbol}-skipped-${skippedDate}`,
        date: skippedDate,
        symbol,
        side: normalizedMarkerSide(position.side),
        trade_date: position.trade_date ?? skippedDate,
        filing_date: position.report_date ?? skippedDate,
        value: amountMidpoint(position),
        price: null,
        return_pct: null,
        simulation_status: "skipped",
        skip_reason: position.skip_reason ?? null,
        skip_category: position.skip_category ?? null,
      });
      return;
    }

    if (position.entry_date) {
      const entryPrice = finiteNumber(position.entry_price);
      markers.push({
        id: `${position.source_event_id ?? index}-${symbol}-buy-${position.entry_date}`,
        date: position.entry_date,
        symbol,
        side: "Buy",
        trade_date: position.trade_date ?? position.entry_date,
        filing_date: position.report_date ?? position.entry_date,
        value: positionValue(position, entryPrice),
        price: entryPrice,
        return_pct: finiteNumber(position.return_pct),
        simulation_status: "simulated",
        skip_reason: null,
        skip_category: null,
      });
    }

    if (position.exit_date) {
      const exitPrice = finiteNumber(position.exit_price);
      markers.push({
        id: `${position.source_event_id ?? index}-${symbol}-sell-${position.exit_date}`,
        date: position.exit_date,
        symbol,
        side: "Sell",
        trade_date: position.exit_date,
        filing_date: position.exit_date,
        value: positionValue(position, exitPrice),
        price: exitPrice,
        return_pct: finiteNumber(position.return_pct),
        simulation_status: "simulated",
        skip_reason: null,
        skip_category: null,
      });
    }
  });
  return markers
    .sort((a, b) => a.date.localeCompare(b.date) || a.symbol.localeCompare(b.symbol));
}
