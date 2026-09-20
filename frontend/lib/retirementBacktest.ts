import type { BacktestRunRequest, BacktestRunResponse, MemberInsiderSuggestion } from "./api";

// Match the backtester's autocomplete: prefer canonical IDs over legacy aliases.
export function retirementCongressSources(items: MemberInsiderSuggestion[]) {
  const members = new Map<string, MemberInsiderSuggestion>();
  for (const item of items) {
    if (item.category !== "congress" || !item.bioguide_id?.trim()) continue;
    const memberId = item.bioguide_id.trim().toUpperCase();
    const key = `${(item.label || item.value).trim().toLowerCase()}|${(item.chamber ?? "").trim().toLowerCase()}`;
    const existing = members.get(key);
    if (!existing || (existing.bioguide_id?.startsWith("FMP_") && !memberId.startsWith("FMP_"))) members.set(key, { ...item, bioguide_id: memberId });
  }
  return [...members.values()].map(item => ({ id: item.bioguide_id!.trim(), name: item.label || item.value,
    href: `/members/${encodeURIComponent(item.bioguide_id!.trim())}`, kind: "member" }));
}

/** Fixed, reproducible assumptions for Congress retirement scenarios. */
export function congressRetirementBacktest(memberId: string, today: string): BacktestRunRequest {
  const end = new Date(`${today}T00:00:00Z`);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(today) || !Number.isFinite(end.getTime()) || end.toISOString().slice(0, 10) !== today) {
    throw new Error("The backtester returned an invalid date. Please try again.");
  }
  end.setUTCDate(end.getUTCDate() - 1);
  const start = new Date(end);
  start.setUTCDate(start.getUTCDate() - 1095);
  return {
    strategy_type: "congress", source_scope: "member", member_id: memberId,
    start_date: start.toISOString().slice(0, 10), end_date: end.toISOString().slice(0, 10),
    start_balance: 10000, contribution_amount: 0, contribution_frequency: "none",
    hold_days: 90, rebalancing_frequency: "monthly", weighting: "equal",
    max_position_weight: 1, benchmark: "SPY", portfolio_model: "disclosure_date", buy_and_hold: false,
  };
}

export function retirementBacktestReturn(result: BacktestRunResponse) {
  const { summary, timeline } = result;
  const first = timeline[0]?.date, last = timeline.at(-1)?.date;
  const span = (Date.parse(last ?? "") - Date.parse(first ?? "")) / 86400000;
  if (!summary || !Number.isFinite(summary.positions_count) || summary.positions_count < 1 ||
      !timeline.some(point => point.active_positions > 0 && point.invested_pct > 0)) {
    throw new Error("No invested portfolio could be simulated for this member. A cash-only result cannot supply a retirement return.");
  }
  if (!Number.isFinite(span) || span < 365) {
    throw new Error("This member’s backtest has less than one year of priced history. Choose another member or a custom return.");
  }
  const rate = summary.cagr_pct;
  if (typeof rate !== "number" || !Number.isFinite(rate) || rate < -99 || rate > 100) {
    throw new Error("This backtest has no usable CAGR within the calculator’s supported range (-99% to 100%).");
  }
  const warnings: string[] = [];
  if (summary.skipped_positions_count > 0) {
    warnings.push(`${summary.skipped_positions_count} positions skipped: ${(summary.skipped_reasons ?? []).join("; ") || "insufficient price data"}. The result excludes those positions.`);
  }
  if (summary.price_fallback_positions_count > 0) {
    warnings.push(`${summary.price_fallback_positions_count} positions used nearby trading-day prices.`);
  }
  return { rate, period: `${first} to ${last}`, warnings,
    basis: "Simulated Congress-strategy CAGR · disclosure-date entries · 90-day holds · monthly equal-weight rebalancing · $10,000 starting capital, no contributions",
    assumptions: result.assumptions,
  };
}
