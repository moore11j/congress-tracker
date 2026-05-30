export type ScreenerParams = Record<string, string | number>;

export type ScreenerColumnKey =
  | "congress"
  | "insiders"
  | "institutional"
  | "options_flow"
  | "government_contracts"
  | "confirmation"
  | "why_now"
  | "rel_volume"
  | "price_move_pct"
  | "rsi"
  | "macd_state"
  | "trend_state"
  | "trailing_pe"
  | "forward_pe"
  | "price_sales"
  | "ev_ebitda"
  | "gross_margin"
  | "operating_margin"
  | "net_margin"
  | "roe"
  | "roic"
  | "revenue_growth"
  | "eps_growth"
  | "ebitda_growth"
  | "fcf_growth"
  | "debt_equity"
  | "current_ratio"
  | "net_debt_ebitda"
  | "eps_ttm"
  | "fcf"
  | "fcf_margin"
  | "earnings_yield";

export const TECHNICAL_PARAM_KEYS = [
  "rel_volume_min",
  "rel_volume_max",
  "price_move_min",
  "price_move_max",
  "rsi_min",
  "rsi_max",
  "macd_state",
  "trend_state",
] as const;

export const FUNDAMENTAL_PARAM_KEYS = [
  "trailing_pe_min",
  "trailing_pe_max",
  "forward_pe_min",
  "forward_pe_max",
  "price_to_sales_min",
  "price_to_sales_max",
  "ev_to_ebitda_min",
  "ev_to_ebitda_max",
  "gross_margin_min",
  "gross_margin_max",
  "operating_margin_min",
  "operating_margin_max",
  "net_margin_min",
  "net_margin_max",
  "roe_min",
  "roe_max",
  "roic_min",
  "roic_max",
  "revenue_growth_min",
  "revenue_growth_max",
  "eps_growth_min",
  "eps_growth_max",
  "ebitda_growth_min",
  "ebitda_growth_max",
  "fcf_growth_min",
  "fcf_growth_max",
  "debt_to_equity_min",
  "debt_to_equity_max",
  "current_ratio_min",
  "current_ratio_max",
  "net_debt_to_ebitda_min",
  "net_debt_to_ebitda_max",
  "eps_ttm_min",
  "eps_ttm_max",
  "free_cash_flow_min",
  "free_cash_flow_max",
  "fcf_margin_min",
  "fcf_margin_max",
  "earnings_yield_min",
  "earnings_yield_max",
] as const;

export function hasActiveParam(params: ScreenerParams, key: string): boolean {
  const value = params[key];
  if (value === undefined || value === null) return false;
  const cleaned = String(value).trim();
  return cleaned !== "" && cleaned.toLowerCase() !== "any";
}

export function hasAnyActiveParam(params: ScreenerParams, keys: readonly string[]): boolean {
  return keys.some((key) => hasActiveParam(params, key));
}

export function activeScreenerColumns(params: ScreenerParams): ScreenerColumnKey[] {
  const columns: ScreenerColumnKey[] = [];
  const sort = String(params.sort ?? "");

  if (hasActiveParam(params, "congress_activity") || sort === "congress_activity") columns.push("congress");
  if (hasActiveParam(params, "insider_activity") || sort === "insider_activity") columns.push("insiders");
  if (hasAnyActiveParam(params, ["institutional_activity_active", "institutional_activity_direction", "institutional_activity_min_value"])) {
    columns.push("institutional");
  }
  if (hasAnyActiveParam(params, ["options_flow_active", "options_flow_direction", "options_flow_min_score", "options_flow_min_premium"])) {
    columns.push("options_flow");
  }
  if (hasActiveParam(params, "government_contracts_active")) columns.push("government_contracts");
  if (hasAnyActiveParam(params, ["confirmation_score_min", "confirmation_direction", "confirmation_band"]) || sort === "confirmation_score") {
    columns.push("confirmation");
  }
  if (hasAnyActiveParam(params, ["why_now_state", "freshness"]) || sort === "freshness") columns.push("why_now");

  if (hasAnyActiveParam(params, ["rel_volume_min", "rel_volume_max"])) columns.push("rel_volume");
  if (hasAnyActiveParam(params, ["price_move_min", "price_move_max"])) columns.push("price_move_pct");
  if (hasAnyActiveParam(params, ["rsi_min", "rsi_max"])) columns.push("rsi");
  if (hasActiveParam(params, "macd_state")) columns.push("macd_state");
  if (hasActiveParam(params, "trend_state")) columns.push("trend_state");

  const fundamentalPairs: Array<[ScreenerColumnKey, string, string]> = [
    ["trailing_pe", "trailing_pe_min", "trailing_pe_max"],
    ["forward_pe", "forward_pe_min", "forward_pe_max"],
    ["price_sales", "price_to_sales_min", "price_to_sales_max"],
    ["ev_ebitda", "ev_to_ebitda_min", "ev_to_ebitda_max"],
    ["gross_margin", "gross_margin_min", "gross_margin_max"],
    ["operating_margin", "operating_margin_min", "operating_margin_max"],
    ["net_margin", "net_margin_min", "net_margin_max"],
    ["roe", "roe_min", "roe_max"],
    ["roic", "roic_min", "roic_max"],
    ["revenue_growth", "revenue_growth_min", "revenue_growth_max"],
    ["eps_growth", "eps_growth_min", "eps_growth_max"],
    ["ebitda_growth", "ebitda_growth_min", "ebitda_growth_max"],
    ["fcf_growth", "fcf_growth_min", "fcf_growth_max"],
    ["debt_equity", "debt_to_equity_min", "debt_to_equity_max"],
    ["current_ratio", "current_ratio_min", "current_ratio_max"],
    ["net_debt_ebitda", "net_debt_to_ebitda_min", "net_debt_to_ebitda_max"],
    ["eps_ttm", "eps_ttm_min", "eps_ttm_max"],
    ["fcf", "free_cash_flow_min", "free_cash_flow_max"],
    ["fcf_margin", "fcf_margin_min", "fcf_margin_max"],
    ["earnings_yield", "earnings_yield_min", "earnings_yield_max"],
  ];
  fundamentalPairs.forEach(([column, minKey, maxKey]) => {
    if (hasAnyActiveParam(params, [minKey, maxKey])) columns.push(column);
  });

  return columns;
}

export function hasActiveTechnicalFilters(params: ScreenerParams): boolean {
  return hasAnyActiveParam(params, TECHNICAL_PARAM_KEYS);
}

export function hasActiveFundamentalFilters(params: ScreenerParams): boolean {
  return hasAnyActiveParam(params, FUNDAMENTAL_PARAM_KEYS);
}

export function hasActiveIntelligenceFilters(params: ScreenerParams): boolean {
  return hasAnyActiveParam(params, [
    "congress_activity",
    "insider_activity",
    "confirmation_score_min",
    "confirmation_direction",
    "confirmation_band",
    "why_now_state",
    "freshness",
    "government_contracts_active",
    "options_flow_active",
    "options_flow_direction",
    "options_flow_min_score",
    "options_flow_min_premium",
    "institutional_activity_active",
    "institutional_activity_direction",
    "institutional_activity_min_value",
  ]);
}
