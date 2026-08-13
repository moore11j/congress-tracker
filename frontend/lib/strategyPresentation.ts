import type { StrategyDefinitionPayload } from "@/lib/api";

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
