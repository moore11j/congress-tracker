import type { StrategyDefinitionPayload, StrategyDetailPayload } from "@/lib/api";

/** Keep directory previews and detail charts on the same dated series. */
export function strategyChartView(strategy: StrategyDetailPayload | null, mode: "daily" | "historical" = "daily") {
  const daily = Boolean(strategy?.prospectiveActive) && mode !== "historical";
  const model = strategy?.modelChart;
  const performance = daily ? model?.performance : strategy?.performance;
  const equityCurve = (daily ? model?.points : strategy?.equityCurve) ?? [];
  const notice = !daily || model?.status === "current" ? null
    : model?.status === "partial_coverage" ? `${model.unfilledSymbolCount ?? 0} tickers had unfilled entries (cash retained); ${model.skippedRebalanceCount ?? 0} rebalances lacked execution prices. ${model.staleMarkCount ?? 0} daily marks use a prior close, at most three sessions old. This is a reconstruction with incomplete price coverage.`
    : model?.status === "awaiting_first_session" ? "This model was activated after the latest completed session. Its daily chart begins after the next market close."
    : model?.status === "price_gap" ? "Chart refresh is waiting for verified market prices. The last complete session is shown; missing returns are not estimated."
    : "Daily chart refresh is pending. The last verified values are shown.";
  return { daily, model, performance, equityCurve, notice };
}

function displayValue(value: unknown) {
  return typeof value === "string" ? value.trim() : "";
}

export function displayStrategyName(name: string) {
  return name.replace(/\s*\(\d+D\)$/i, "");
}

export function displayStrategyUniverse(strategy: StrategyDefinitionPayload) {
  const universe = strategy.universe ?? {};
  const rule = strategy.rule ?? {};
  const industry = [universe.industry, universe.sector, rule.industry, rule.sector]
    .map(displayValue)
    .find(Boolean);
  if (industry) return industry;

  const source = [universe.source, universe.basis, rule.source, rule.candidate_source]
    .map(displayValue)
    .join(" ")
    .toLowerCase();
  if (source.includes("contract") || strategy.category === "government_contract") return "Gov Contractors";
  if (strategy.category === "congress") return "Congress Trades";
  if (strategy.category === "insider") return "Insider Trades";
  return "US Equities";
}
