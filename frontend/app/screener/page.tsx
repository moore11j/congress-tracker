import Link from "next/link";
import { ClickableScreenerRow } from "@/components/screener/ClickableScreenerRow";
import { CollapsibleFilterSection } from "@/components/screener/CollapsibleFilterSection";
import { FormattedNumberInput } from "@/components/screener/FormattedNumberInput";
import { ScreenerEntitlementRefresh } from "@/components/screener/ScreenerEntitlementRefresh";
import { EntitlementHintRefresh } from "@/components/auth/EntitlementHintRefresh";
import { VerifiedSessionGuard } from "@/components/auth/VerifiedSessionGuard";
import { ScreenerExportButton } from "@/components/screener/ScreenerExportButton";
import { ScreenerResultsAutoScroll } from "@/components/screener/ScreenerResultsAutoScroll";
import { ScreenerResultsClient } from "@/components/screener/ScreenerResultsClient";
import { ScreenerUpgradeOverlay } from "@/components/screener/ScreenerUpgradeOverlay";
import { AddTickerToWatchlist } from "@/components/watchlists/AddTickerToWatchlist";
import { SavedViewsBar } from "@/components/saved-views/SavedViewsBar";
import { UpgradePrompt } from "@/components/billing/UpgradePrompt";
import { getEntitlements, getPlanConfig, type PlanConfig } from "@/lib/api";
import { formatCompanyName } from "@/lib/companyName";
import { defaultPlanConfig } from "@/lib/defaultPlanConfig";
import { defaultEntitlements, entitlementsFromTierHint, hasEntitlement, limitFor } from "@/lib/entitlements";
import {
  FUNDAMENTAL_PARAM_KEYS,
  TECHNICAL_PARAM_KEYS,
  activeScreenerColumns,
  hasActiveFundamentalFilters,
  hasActiveIntelligenceFilters,
  hasActiveTechnicalFilters,
  type ScreenerColumnKey,
} from "@/lib/screenerColumns";
import { buildReturnTo, requirePageAuthState } from "@/lib/serverAuth";
import { withServerTimeout } from "@/lib/serverTimeout";
import {
  activeFilterControlClassName,
  cardClassName,
  ghostButtonClassName,
  selectClassName,
  subtlePrimaryButtonClassName,
  tickerMonoLinkClassName,
} from "@/lib/styles";
import { resultsTableFrameClassName, stickyResultsTableHeaderClassName } from "@/components/ui/resultsTableFrame";
import { tickerHref } from "@/lib/ticker";

export const dynamic = "force-dynamic";

type SearchParams = Record<string, string | string[] | undefined>;

type ScreenerRow = {
  symbol: string;
  company_name: string;
  sector?: string | null;
  industry?: string | null;
  market_cap?: number | null;
  price?: number | null;
  volume?: number | null;
  avg_volume?: number | null;
  rel_volume?: number | null;
  price_move_pct?: number | null;
  rsi?: number | null;
  macd_state?: string | null;
  trend_state?: string | null;
  beta?: number | null;
  country?: string | null;
  exchange?: string | null;
  trailing_pe?: number | null;
  forward_pe?: number | null;
  price_sales?: number | null;
  ev_ebitda?: number | null;
  gross_margin?: number | null;
  operating_margin?: number | null;
  net_margin?: number | null;
  roe?: number | null;
  roic?: number | null;
  revenue_growth?: number | null;
  eps_growth?: number | null;
  ebitda_growth?: number | null;
  fcf_growth?: number | null;
  debt_equity?: number | null;
  current_ratio?: number | null;
  net_debt_ebitda?: number | null;
  eps_ttm?: number | null;
  fcf?: number | null;
  fcf_margin?: number | null;
  earnings_yield?: number | null;
  congress_activity: ActivityOverlay;
  insider_activity: ActivityOverlay;
  confirmation: {
    score: number | null;
    band: "inactive" | "weak" | "moderate" | "strong" | "exceptional" | string;
    direction: "bullish" | "bearish" | "neutral" | "mixed" | string;
    status: string;
    locked?: boolean;
  };
  why_now: {
    state: "early" | "strengthening" | "strong" | "mixed" | "fading" | "inactive" | string;
    headline: string;
    locked?: boolean;
  };
  signal_freshness: {
    freshness_score: number | null;
    freshness_state: "fresh" | "early" | "active" | "maturing" | "stale" | "inactive" | string;
    freshness_label: string;
    locked?: boolean;
  };
  ticker_url?: string;
  government_contracts_status?: string | null;
  government_contracts_active?: boolean | null;
  government_contracts_score_contribution?: number | null;
  government_contracts_count?: number | null;
  government_contracts_total_amount?: number | null;
  government_contracts_largest_amount?: number | null;
  government_contracts_latest_date?: string | null;
  government_contracts_top_agency?: string | null;
  government_contracts_direction?: string | null;
  options_flow_active?: boolean | null;
  options_flow_score?: number | null;
  options_flow_direction?: string | null;
  options_flow_intensity?: string | null;
  options_flow_total_premium?: number | null;
  options_flow_call_put_premium_ratio?: number | null;
  options_flow_latest_date?: string | null;
  options_flow_status?: string | null;
  options_flow_locked?: boolean | null;
  institutional_activity_active?: boolean | null;
  institutional_activity_direction?: string | null;
  institutional_activity_net_activity?: number | null;
  institutional_activity_institution_count?: number | null;
  institutional_activity_total_value?: number | null;
  institutional_activity_ownership_pct?: number | null;
  institutional_activity_holders_increased?: number | null;
  institutional_activity_holders_reduced?: number | null;
  institutional_activity_new_positions?: number | null;
  institutional_activity_exits?: number | null;
  institutional_activity_holder_breadth?: number | null;
  institutional_activity_materiality_score?: number | null;
  institutional_activity_latest_date?: string | null;
  institutional_activity_status?: string | null;
  institutional_activity_locked?: boolean | null;
};

type ActivityOverlay = {
  present: boolean;
  label: string;
  direction?: string | null;
  freshness_days?: number | null;
  locked?: boolean;
};

type ScreenerResponse = {
  items: ScreenerRow[];
  page: number;
  page_size: number;
  returned: number;
  total_available: number;
  has_next: boolean;
  sort: {
    sort_by: string;
    sort_dir: string;
  };
  filters: Record<string, string | number | boolean>;
  lookback_days: number;
  overlay_availability?: {
    government_contracts: OverlayAvailability;
    options_flow: OverlayAvailability;
    institutional_activity: OverlayAvailability;
  };
  ignored_filters?: string[];
  feature_flags?: Record<string, boolean>;
  result_cap?: number;
  access?: {
    tier: "free" | "premium" | "pro" | "admin";
    intelligence_locked: boolean;
    options_flow_locked?: boolean;
    institutional_activity_locked?: boolean;
    presets_locked: boolean;
    saved_screens_limit: number;
    monitoring_locked: boolean;
    csv_export_locked: boolean;
    csv_export_required_plan?: "free" | "premium" | "pro" | string;
    feature_flags?: Record<string, boolean>;
  };
};

type OverlayAvailability = {
  enabled: boolean;
  status: string;
  filterable: boolean;
  locked?: boolean;
  required_plan?: string | null;
};

const PARAM_KEYS = [
  "market_cap_min",
  "market_cap_max",
  "price_min",
  "price_max",
  "volume_min",
  "beta_min",
  "beta_max",
  "dividend_yield_min",
  "dividend_yield_max",
  "sector",
  "industry",
  "country",
  "exchange",
  "congress_activity",
  "insider_activity",
  "confirmation_score_min",
  "confirmation_direction",
  "confirmation_band",
  "why_now_state",
  "freshness",
  "government_contracts_active",
  "government_contracts_min_amount",
  "government_contracts_lookback_days",
  "options_flow_active",
  "options_flow_direction",
  "options_flow_min_score",
  "options_flow_min_premium",
  "options_flow_lookback_days",
  "institutional_activity_active",
  "institutional_activity_type",
  "institutional_activity_direction",
  "institutional_activity_min_value",
  "institutional_activity_min_ownership_pct",
  "institutional_activity_holder_breadth",
  "institutional_activity_lookback",
  "institutional_activity_lookback_days",
  ...TECHNICAL_PARAM_KEYS,
  ...FUNDAMENTAL_PARAM_KEYS,
  "lookback_days",
  "sort",
  "sort_dir",
  "page_size",
] as const;

const SECTORS = [
  "Technology",
  "Healthcare",
  "Financial Services",
  "Industrials",
  "Communication Services",
  "Consumer Cyclical",
  "Consumer Defensive",
  "Energy",
  "Utilities",
  "Real Estate",
  "Basic Materials",
];

const INDUSTRIES = [
  "Semiconductors",
  "Software - Infrastructure",
  "Software - Application",
  "Consumer Electronics",
  "Internet Content & Information",
  "Communication Equipment",
  "Banks - Diversified",
  "Banks - Regional",
  "Asset Management",
  "Credit Services",
  "Insurance - Diversified",
  "Drug Manufacturers - General",
  "Biotechnology",
  "Medical Devices",
  "Healthcare Plans",
  "Aerospace & Defense",
  "Specialty Industrial Machinery",
  "Auto Manufacturers",
  "Oil & Gas Integrated",
  "Utilities - Regulated Electric",
  "REIT - Specialty",
];

const EXCHANGES = ["NASDAQ", "NYSE", "AMEX"];
const COUNTRIES = ["US", "CA", "GB", "DE", "FR", "JP"];
const SORTS = [
  ["relevance", "Relevance"],
  ["confirmation_score", "Confirmation"],
  ["freshness", "Freshness"],
  ["market_cap", "Market cap"],
  ["price", "Price"],
  ["volume", "Volume"],
  ["beta", "Beta"],
  ["congress_activity", "Congress"],
  ["insider_activity", "Insiders"],
  ["symbol", "Symbol"],
] as const;
const ACTIVITY_FILTER_OPTIONS = [
  ["has_activity", "Has recent activity"],
  ["no_activity", "No recent activity"],
  ["buy_leaning", "Buy-leaning"],
  ["sell_leaning", "Sell-leaning"],
] as const;
const CONFIRMATION_SCORE_OPTIONS = [
  ["40", "40+"],
  ["60", "60+"],
  ["80", "80+"],
] as const;
const CONFIRMATION_DIRECTION_OPTIONS = [
  ["bullish", "Bullish"],
  ["bearish", "Bearish"],
  ["mixed", "Conflicted"],
] as const;
const CONFIRMATION_BAND_OPTIONS = [
  ["moderate_plus", "Moderate+"],
  ["strong_plus", "Strong+"],
  ["exceptional", "Exceptional"],
] as const;
const WHY_NOW_OPTIONS = [
  ["early", "Early"],
  ["strengthening", "Strengthening"],
  ["strong", "Strong"],
  ["limited", "Limited"],
  ["fading", "Fading"],
  ["inactive", "Inactive"],
] as const;
const FRESHNESS_OPTIONS = [
  ["fresh", "Fresh"],
  ["early", "Early"],
  ["active", "Active"],
  ["maturing", "Maturing"],
  ["stale", "Stale"],
  ["inactive", "Inactive"],
] as const;
const BOOLEAN_ACTIVITY_OPTIONS = [
  ["true", "Active"],
  ["false", "Inactive"],
] as const;
const GOVERNMENT_CONTRACT_BOOLEAN_OPTIONS = [
  ["true", "Has Government Contracts"],
  ["false", "No Government Contracts"],
] as const;
const GOVERNMENT_CONTRACT_AMOUNT_OPTIONS = [
  ["1000000", "$1M+"],
  ["10000000", "$10M+"],
  ["50000000", "$50M+"],
  ["250000000", "$250M+"],
] as const;
const GOVERNMENT_CONTRACT_LOOKBACK_OPTIONS = [
  ["90", "90D"],
  ["180", "180D"],
  ["365", "1Y"],
  ["1095", "3Y"],
] as const;
const OPTIONS_FLOW_DIRECTION_OPTIONS = [
  ["bullish", "Bullish"],
  ["bearish", "Bearish"],
  ["mixed", "Mixed"],
] as const;
const OPTIONS_FLOW_SCORE_OPTIONS = [
  ["50", "50+"],
  ["65", "65+"],
  ["80", "80+"],
] as const;
const OPTIONS_FLOW_PREMIUM_OPTIONS = [
  ["100000", "$100K+"],
  ["500000", "$500K+"],
  ["1000000", "$1M+"],
  ["5000000", "$5M+"],
] as const;
const OPTIONS_FLOW_LOOKBACK_OPTIONS = [
  ["1", "1D"],
  ["7", "7D"],
  ["30", "30D"],
  ["90", "90D"],
] as const;
const INSTITUTIONAL_DIRECTION_OPTIONS = [
  ["bullish", "Bullish"],
  ["bearish", "Bearish"],
  ["mixed", "Mixed"],
] as const;
const INSTITUTIONAL_ACTIVITY_TYPE_OPTIONS = [
  ["accumulation", "Accumulation"],
  ["distribution", "Distribution"],
  ["new_position", "New Position"],
  ["exit", "Exit"],
  ["major_holder_move", "Major Holder Move"],
  ["cluster_move", "Cluster Move"],
] as const;
const INSTITUTIONAL_VALUE_OPTIONS = [
  ["1000000", "$1M+"],
  ["10000000", "$10M+"],
  ["50000000", "$50M+"],
  ["100000000", "$100M+"],
] as const;
const INSTITUTIONAL_OWNERSHIP_OPTIONS = [
  ["0.1", "0.1%+"],
  ["0.5", "0.5%+"],
  ["1", "1%+"],
  ["5", "5%+"],
] as const;
const INSTITUTIONAL_HOLDER_BREADTH_OPTIONS = [
  ["net_3", "Net +3"],
  ["net_10", "Net +10"],
  ["increasing_10", "10+ Increasing"],
  ["increasing_25", "25+ Increasing"],
] as const;
const INSTITUTIONAL_LOOKBACK_OPTIONS = [
  ["30d", "30D"],
  ["90d", "90D"],
  ["latest_quarter", "Latest Quarter"],
  ["1y", "1Y"],
] as const;
const MACD_STATE_OPTIONS = [
  ["bullish", "Bullish"],
  ["bearish", "Bearish"],
  ["crossover_bullish", "Crossover bullish"],
  ["crossover_bearish", "Crossover bearish"],
] as const;
const TREND_STATE_OPTIONS = [
  ["sma_above_lma", "SMA above LMA"],
  ["sma_below_lma", "SMA below LMA"],
] as const;
const DEFAULT_PAGE_SIZE = 10;
const PAGE_SIZE_OPTIONS = [5, 10, 25, 50, 100] as const;

const filterLabelClassName = "grid gap-1 text-xs font-semibold uppercase tracking-wide text-slate-400";
const sectionCardClassName = "rounded-2xl border border-slate-800 bg-slate-950/35 p-3";
const presetLinkClassName =
  "block rounded-2xl border border-slate-800 bg-slate-950/35 p-3 transition hover:border-emerald-400/30 hover:bg-slate-900/70";
const tableCellClassName = "px-3 py-2.5 align-top";
const tableMetricClassName = `${tableCellClassName} whitespace-nowrap tabular-nums text-slate-200`;
const compactBadgeClassName = "inline-flex items-center gap-1.5 rounded-full border px-2.5 py-1 text-xs font-medium";
const tinyStateBadgeClassName =
  "inline-flex shrink-0 items-center rounded-full border px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wide";
const NUMERIC_PARAM_KEYS = new Set<string>([
  "market_cap_min",
  "market_cap_max",
  "price_min",
  "price_max",
  "volume_min",
  "beta_min",
  "beta_max",
  "dividend_yield_min",
  "dividend_yield_max",
  "confirmation_score_min",
  "government_contracts_min_amount",
  "government_contracts_lookback_days",
  "options_flow_min_score",
  "options_flow_min_premium",
  "options_flow_lookback_days",
  "institutional_activity_min_value",
  "institutional_activity_min_ownership_pct",
  "institutional_activity_lookback_days",
  ...TECHNICAL_PARAM_KEYS,
  ...FUNDAMENTAL_PARAM_KEYS,
]);

const planTierLabels: Record<"free" | "premium" | "pro", string> = {
  free: "Free",
  premium: "Premium",
  pro: "Pro",
};

function normalizedPlanTier(value: string | null | undefined): "free" | "premium" | "pro" | null {
  if (value === "free" || value === "premium" || value === "pro") return value;
  return null;
}

function featureRequiredPlanLabel(planConfig: PlanConfig, featureKey: string) {
  const feature = planConfig.features.find((item) => item.feature_key === featureKey);
  return planTierLabels[normalizedPlanTier(feature?.required_tier) ?? "premium"];
}

function csvExportRequiredPlanLabel(planConfig: PlanConfig, data?: ScreenerResponse | null) {
  const tier = normalizedPlanTier(data?.access?.csv_export_required_plan);
  return tier ? planTierLabels[tier] : featureRequiredPlanLabel(planConfig, "screener_csv_export");
}
const STARTER_PRESETS = [
  {
    id: "large_caps_congress",
    label: "Large Caps + Congress",
    description: "10B+ market cap with recent Congress activity.",
    params: {
      market_cap_min: "10,000,000,000",
      congress_activity: "has_activity",
      sort: "confirmation_score",
      sort_dir: "desc",
      lookback_days: "30",
    },
  },
  {
    id: "recent_insiders",
    label: "Recent Insider Activity",
    description: "Recent insider activity ranked by confirmation.",
    params: {
      insider_activity: "has_activity",
      sort: "confirmation_score",
      sort_dir: "desc",
      lookback_days: "30",
    },
  },
  {
    id: "aligned_activity",
    label: "Congress + Insider Alignment",
    description: "Both activity streams active with 40+ confirmation.",
    params: {
      congress_activity: "has_activity",
      insider_activity: "has_activity",
      confirmation_score_min: "40",
      sort: "confirmation_score",
      sort_dir: "desc",
      lookback_days: "30",
    },
  },
  {
    id: "liquid_confirmation",
    label: "Liquid + Confirmed",
    description: "1M+ volume and strong confirmation.",
    params: {
      volume_min: "1,000,000",
      confirmation_score_min: "60",
      confirmation_band: "strong_plus",
      sort: "confirmation_score",
      sort_dir: "desc",
      lookback_days: "30",
    },
  },
  {
    id: "bullish_confirmation",
    label: "Bullish Confirmation",
    description: "Bullish names with 60+ confirmation.",
    params: {
      confirmation_direction: "bullish",
      confirmation_score_min: "60",
      confirmation_band: "strong_plus",
      sort: "confirmation_score",
      sort_dir: "desc",
      lookback_days: "30",
    },
  },
] as const;

function getParam(sp: SearchParams, key: string): string {
  const value = sp[key];
  if (Array.isArray(value)) {
    for (let idx = value.length - 1; idx >= 0; idx -= 1) {
      if (typeof value[idx] === "string") return value[idx] ?? "";
    }
    return "";
  }
  return typeof value === "string" ? value : "";
}

function getPositiveInt(raw: string, fallback: number, max: number): number {
  const parsed = Number(stripNumberFormatting(raw));
  if (!Number.isFinite(parsed) || parsed <= 0) return fallback;
  return Math.min(Math.floor(parsed), max);
}

function stripNumberFormatting(value: string | number): string {
  return String(value).replace(/,/g, "").trim();
}

function currentParams(sp: SearchParams) {
  const sortRaw = getParam(sp, "sort");
  const sort = SORTS.some(([value]) => value === sortRaw) ? sortRaw : "relevance";
  const sortDir = getParam(sp, "sort_dir") === "asc" ? "asc" : "desc";
  const page = getPositiveInt(getParam(sp, "page"), 1, 10);
  const pageSize = PAGE_SIZE_OPTIONS.includes(Number(getParam(sp, "page_size")) as (typeof PAGE_SIZE_OPTIONS)[number])
    ? Number(getParam(sp, "page_size"))
    : DEFAULT_PAGE_SIZE;
  const lookbackDays = [30, 60, 90].includes(Number(getParam(sp, "lookback_days")))
    ? Number(getParam(sp, "lookback_days"))
    : 30;
  const governmentContractsLookbackDays = [90, 180, 365, 1095].includes(Number(getParam(sp, "government_contracts_lookback_days")))
    ? Number(getParam(sp, "government_contracts_lookback_days"))
    : 365;
  const optionsFlowLookbackDays = [1, 7, 30, 90].includes(Number(getParam(sp, "options_flow_lookback_days")))
    ? Number(getParam(sp, "options_flow_lookback_days"))
    : 30;
  const institutionalLookback = ["30d", "90d", "latest_quarter", "1y"].includes(getParam(sp, "institutional_activity_lookback"))
    ? getParam(sp, "institutional_activity_lookback")
    : "latest_quarter";
  const institutionalLookbackDays = institutionalLookback === "30d" ? 30 : institutionalLookback === "1y" ? 365 : 90;
  const governmentContractsMinAmount = getParam(sp, "government_contracts_min_amount").trim() || "1000000";

  const params: Record<string, string | number> = {
    sort,
    sort_dir: sortDir,
    page,
    page_size: pageSize,
    lookback_days: lookbackDays,
    government_contracts_lookback_days: governmentContractsLookbackDays,
    government_contracts_min_amount: governmentContractsMinAmount,
    options_flow_lookback_days: optionsFlowLookbackDays,
    institutional_activity_lookback: institutionalLookback,
    institutional_activity_lookback_days: institutionalLookbackDays,
  };
  PARAM_KEYS.forEach((key) => {
    if (key in params) return;
    const value = getParam(sp, key);
    if (value.trim()) params[key] = value.trim();
  });
  return params;
}

function pageHref(params: Record<string, string | number>, overrides: Record<string, string | number | null>): string {
  const url = new URL("https://local/screener");
  Object.entries(params).forEach(([key, value]) => {
    if (key === "page") return;
    const trimmed = String(value).trim();
    if (trimmed) url.searchParams.set(key, trimmed);
  });
  Object.entries(overrides).forEach(([key, value]) => {
    if (value === null || String(value).trim() === "") {
      url.searchParams.delete(key);
      return;
    }
    url.searchParams.set(key, String(value));
  });
  if (!url.searchParams.has("page")) url.searchParams.set("page", "1");
  return `${url.pathname}${url.search}`;
}

function presetHref(
  params: Record<string, string | number>,
  overrides: Record<string, string | number>,
): string {
  const url = new URL("https://local/screener");
  url.searchParams.set("page_size", String(params.page_size ?? DEFAULT_PAGE_SIZE));
  Object.entries(overrides).forEach(([key, value]) => {
    const trimmed = NUMERIC_PARAM_KEYS.has(key) ? stripNumberFormatting(value) : String(value).trim();
    if (trimmed) url.searchParams.set(key, trimmed);
  });
  url.searchParams.set("page", "1");
  return `${url.pathname}${url.search}`;
}

function formatCompact(value?: number | null): string {
  if (typeof value !== "number" || !Number.isFinite(value)) return "--";
  return new Intl.NumberFormat("en-US", { notation: "compact", maximumFractionDigits: 1 }).format(value);
}

function formatCurrency(value?: number | null, digits = 2): string {
  if (typeof value !== "number" || !Number.isFinite(value)) return "--";
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: value >= 100 ? 0 : digits,
    maximumFractionDigits: value >= 100 ? 0 : digits,
  }).format(value);
}

function formatCurrencyCompact(value?: number | null): string {
  if (typeof value !== "number" || !Number.isFinite(value)) return "--";
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    notation: "compact",
    maximumFractionDigits: 1,
  }).format(value);
}

function formatPercent(value?: number | null, signed = false): string {
  if (typeof value !== "number" || !Number.isFinite(value)) return "--";
  const prefix = signed && value > 0 ? "+" : "";
  return `${prefix}${value.toFixed(1)}%`;
}

function formatMultiple(value?: number | null): string {
  if (typeof value !== "number" || !Number.isFinite(value)) return "--";
  return `${value.toFixed(1)}x`;
}

function formatPlainNumber(value?: number | null, digits = 1): string {
  if (typeof value !== "number" || !Number.isFinite(value)) return "--";
  return value.toFixed(digits);
}

function formatBeta(value?: number | null): string {
  if (typeof value !== "number" || !Number.isFinite(value)) return "--";
  return value.toFixed(2);
}

function lockedMetricLine(label: string) {
  return (
    <div className="space-y-1">
      <div className="h-3 w-16 rounded-full bg-slate-800/90" />
      <div className="text-[11px] leading-4 text-amber-200/90">{label}</div>
    </div>
  );
}

function confirmationBandClass(band: string): string {
  if (band === "exceptional") return "text-emerald-100";
  if (band === "strong") return "text-cyan-100";
  if (band === "moderate") return "text-amber-100";
  if (band === "weak") return "text-slate-300";
  return "text-slate-500";
}

function directionTextClass(direction?: string | null): string {
  if (direction === "bearish") return "text-rose-200";
  if (direction === "bullish") return "text-emerald-200";
  if (direction === "mixed") return "text-slate-300";
  return "text-slate-500";
}

function activityTextClass(activity: ActivityOverlay): string {
  if (!activity.present) return "text-slate-500";
  return directionTextClass(activity.direction);
}

function whyNowClass(state: string, direction: string): string {
  if (state === "strong" || state === "strengthening") {
    if (direction === "bearish") return "border-rose-300/30 bg-rose-400/10 text-rose-100";
    if (direction === "bullish") return "border-emerald-300/30 bg-emerald-400/10 text-emerald-100";
    return "border-cyan-300/25 bg-cyan-400/10 text-cyan-100";
  }
  if (state === "mixed") return "border-amber-300/30 bg-amber-400/10 text-amber-100";
  return "border-slate-800 bg-slate-950/40 text-slate-400";
}

function titleCase(value: string): string {
  return value ? `${value.slice(0, 1).toUpperCase()}${value.slice(1).replace(/_/g, " ")}` : value;
}

function formatShortDate(value?: string | null): string {
  if (!value) return "--";
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return value;
  return new Intl.DateTimeFormat("en-US", { month: "short", day: "numeric" }).format(parsed);
}

function overlayAvailabilityDefaults(): ScreenerResponse["overlay_availability"] {
  return {
    government_contracts: { enabled: true, status: "ok", filterable: true },
    options_flow: { enabled: true, status: "unavailable", filterable: false },
    institutional_activity: { enabled: true, status: "unavailable", filterable: false },
  };
}

function confirmationDirectionLabel(direction: string): string {
  if (direction === "bullish") return "BULLISH";
  if (direction === "bearish") return "BEARISH";
  if (direction === "mixed") return "CONFLICTED";
  return "NEUTRAL";
}

function confirmationMeta(status: string, direction: string): string {
  const cleaned = status.trim();
  if (!cleaned || cleaned.toLowerCase() === "inactive") return "No active confirmation";
  const withoutDirection = cleaned
    .replace(/\b(bullish|bearish|mixed|conflicted|neutral)\b\s*/gi, "")
    .replace(/\s+/g, " ")
    .trim();
  if (!withoutDirection) return "Confirmation";
  return /^[0-9]/.test(withoutDirection) ? withoutDirection : titleCase(withoutDirection);
}

function activityMeta(activity: ActivityOverlay): string {
  if (!activity.present) return "No recent flow";
  if (typeof activity.freshness_days === "number") {
    return activity.freshness_days === 0 ? "Today" : `${activity.freshness_days}d ago`;
  }
  return activity.label.replace(/^Active\s*\/\s*/i, "");
}

function whyNowStateLabel(state: string): string {
  if (state === "mixed") return "Limited";
  return titleCase(state);
}

function freshnessStateLabel(state: string): string {
  return titleCase(state);
}

function FilterSelect({
  name,
  label,
  value,
  options,
  allLabel = "Any",
  includeEmptyOption = true,
  disabled = false,
}: {
  name: string;
  label: string;
  value?: string | number;
  options: readonly string[] | readonly (readonly [string, string])[];
  allLabel?: string;
  includeEmptyOption?: boolean;
  disabled?: boolean;
}) {
  return (
    <label className={filterLabelClassName}>
      {label}
      <select
        name={name}
        defaultValue={String(value ?? "")}
        disabled={disabled}
        className={`${value ? `${selectClassName} ${activeFilterControlClassName}` : selectClassName} ${disabled ? "cursor-not-allowed" : ""}`}
      >
        {includeEmptyOption ? <option value="">{allLabel}</option> : null}
        {options.map((option) => {
          const pair = Array.isArray(option) ? option : [option, option];
          return (
            <option key={pair[0]} value={pair[0]}>
              {pair[1]}
            </option>
          );
        })}
      </select>
    </label>
  );
}

function PairedNumberInputs({
  minName,
  maxName,
  label,
  params,
  placeholderMin,
  placeholderMax,
}: {
  minName: string;
  maxName: string;
  label: string;
  params: Record<string, string | number>;
  placeholderMin?: string;
  placeholderMax?: string;
}) {
  return (
    <div className="grid gap-2">
      <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-500">{label}</p>
      <div className="grid gap-2 sm:grid-cols-2">
        <FormattedNumberInput name={minName} label="Min" value={params[minName]} placeholder={placeholderMin} labelClassName={filterLabelClassName} />
        <FormattedNumberInput name={maxName} label="Max" value={params[maxName]} placeholder={placeholderMax} labelClassName={filterLabelClassName} />
      </div>
    </div>
  );
}

export default async function ScreenerPage({
  searchParams,
}: {
  searchParams?: Promise<SearchParams>;
}) {
  const sp = (await searchParams) ?? {};
  const returnTo = buildReturnTo("/screener", sp);
  const authState = await requirePageAuthState(returnTo);
  const authToken = authState.token;
  const entitlements = authToken
    ? await withServerTimeout(getEntitlements(authToken), "screener:entitlements").catch(() => defaultEntitlements)
    : entitlementsFromTierHint(authState.entitlementHint);
  const planConfig = await withServerTimeout(getPlanConfig(), "screener:plan-config").catch(() => defaultPlanConfig);
  const params = currentParams(sp);
  const resultsTriggerKey = JSON.stringify(params);
  const sort = String(params.sort ?? "relevance");
  const sortDir = String(params.sort_dir ?? "desc");
  const page = Number(params.page ?? 1);
  const pageSize = Number(params.page_size ?? DEFAULT_PAGE_SIZE);
  const canUseScreener = hasEntitlement(entitlements, "screener");
  const canUseIntelligence = hasEntitlement(entitlements, "screener_intelligence");
  const canUseOptionsFlow = hasEntitlement(entitlements, "options_flow_filters");
  const canUseInstitutionalActivity = hasEntitlement(entitlements, "institutional_filters");
  const canUsePresets = hasEntitlement(entitlements, "screener_presets");
  const canUseMonitoring = hasEntitlement(entitlements, "screener_monitoring");
  const canExportCsv = hasEntitlement(entitlements, "screener_csv_export");
  const resultCap = limitFor(entitlements, "screener_results");
  const csvExportPlanLabel = csvExportRequiredPlanLabel(planConfig);
  const csvExportLockedReason = `CSV export is a ${csvExportPlanLabel} feature.`;
  const overlayAvailability = overlayAvailabilityDefaults();
  const optionsFlowFilterable = canUseOptionsFlow && overlayAvailability?.options_flow?.filterable === true;
  const institutionalActivityFilterable = canUseInstitutionalActivity && overlayAvailability?.institutional_activity?.filterable === true;
  const sortOptions = SORTS.filter(([value]) => canUseIntelligence || !["confirmation_score", "freshness"].includes(value));
  const rowsOptions: ReadonlyArray<readonly [string, string]> = PAGE_SIZE_OPTIONS.map((value) => [String(value), String(value)] as const).filter(
    ([value]) => Number(value) <= Math.min(resultCap, 100),
  );
  const activeColumns = activeScreenerColumns(params);
  const intelligenceFiltersOpen = hasActiveIntelligenceFilters(params);
  const technicalFiltersOpen = hasActiveTechnicalFilters(params);
  const fundamentalFiltersOpen = hasActiveFundamentalFilters(params);

  return (
    <VerifiedSessionGuard returnTo={returnTo} initiallyAuthorized={Boolean(authToken)}>
      <div className="space-y-8">
      <EntitlementHintRefresh enabled={!authToken && authState.entitlementHint != null} renderedTier={entitlements.tier} />
      <ScreenerEntitlementRefresh enabled={!authToken && !canUseScreener} />
      <ScreenerResultsAutoScroll formId="screener-filters-form" resultsId="screener-results" triggerKey={resultsTriggerKey} />
      <div className="flex flex-wrap items-end justify-between gap-4">
        <div>
          <p className="text-xs font-semibold uppercase tracking-[0.3em] text-emerald-300">Idea Screener</p>
          <h1 className="mt-2 text-3xl font-semibold text-white">Stock Screener</h1>
          <p className="mt-2 max-w-2xl text-sm text-slate-400">
            Market data filtered through Walnut activity overlays, government contracts, options flow, institutional context, confirmation, Why Now, and freshness signals.
          </p>
        </div>
        <div className="flex flex-wrap items-center gap-2 text-xs">
          <span className={compactBadgeClassName + " border-slate-800 bg-slate-950/30 text-slate-300"}>
            plan <span className="text-white">{entitlements.tier}</span>
          </span>
          <span className={compactBadgeClassName + " border-slate-800 bg-slate-950/30 text-slate-300"}>
            results <span className="text-white">{resultCap}</span>
          </span>
          <span className={compactBadgeClassName + " border-slate-800 bg-slate-950/30 text-slate-300"}>
            saved screens <span className="text-white">{limitFor(entitlements, "screener_saved_screens")}</span>
          </span>
        </div>
      </div>

      {!canUseScreener ? (
        <div className={cardClassName}>
          <UpgradePrompt
            title="Unlock the stock screener"
            body="Your current plan does not include base screener access. Upgrade to open the full discovery workflow."
          />
        </div>
      ) : null}

      <div className={`${cardClassName} space-y-4`}>
        <SavedViewsBar
          surface="screener"
          paramKeys={PARAM_KEYS}
          formId="screener-filters-form"
          dense={true}
          clearSelectionWhenPristine={true}
          allowNotifications={false}
          allowDefaultView={false}
          defaultParams={{
            sort: "relevance",
            sort_dir: "desc",
            page_size: String(DEFAULT_PAGE_SIZE),
            lookback_days: "30",
            government_contracts_lookback_days: "365",
            government_contracts_min_amount: "1000000",
            options_flow_lookback_days: "30",
            institutional_activity_lookback: "latest_quarter",
            institutional_activity_lookback_days: "90",
          }}
          rightSlot={
            <div className="flex flex-wrap items-center gap-2">
              <Link href="/backtesting?strategy=saved_screen" className={`${subtlePrimaryButtonClassName} rounded-lg px-3 text-xs`} prefetch={false}>
                Backtest this screen
              </Link>
              <ScreenerExportButton
                params={params}
                filenamePrefix="screener"
                locked={!canExportCsv}
                lockedReason={csvExportLockedReason}
                requiredPlanLabel={csvExportPlanLabel}
              />
              {canUseMonitoring ? (
                <Link href="/monitoring" className={`${ghostButtonClassName} rounded-lg px-3 py-2 text-xs`} prefetch={false}>
                  Monitoring inbox
                </Link>
              ) : (
                <ScreenerUpgradeOverlay
                  title="Saved screen monitoring"
                  body="Saved screen monitoring and inbox events are included with Premium."
                  badge={null}
                  className="inline-flex rounded-lg pr-20"
                  buttonClassName="rounded-lg border border-transparent"
                >
                  <div className={`${ghostButtonClassName} rounded-lg px-3 py-2 text-xs text-amber-100`}>
                    Monitoring · Premium
                  </div>
                </ScreenerUpgradeOverlay>
              )}
              <span className={compactBadgeClassName + " border-slate-800 bg-slate-950/30 text-slate-300"}>
                sort <span className="text-white">{sort}</span>
              </span>
              <span className={compactBadgeClassName + " border-slate-800 bg-slate-950/30 text-slate-300"}>
                overlay <span className="text-white">{params.lookback_days ?? 30}d</span>
              </span>
            </div>
          }
        />

        {!canUseMonitoring ? (
          <div className="rounded-2xl border border-amber-300/20 bg-amber-300/[0.05] px-4 py-3 text-sm text-slate-300">
            Saved screen monitoring is visible here as a Premium workflow upgrade. Free accounts can save screens, but Inbox events and background monitoring stay locked until upgrade.
          </div>
        ) : null}

        <form id="screener-filters-form" action="/screener" className="space-y-4">
          <div className={sectionCardClassName}>
            <div className="flex flex-wrap items-center justify-between gap-2">
              <div>
                <p className="text-[11px] font-semibold uppercase tracking-[0.22em] text-slate-500">Starter presets</p>
                <p className="mt-1 text-sm text-slate-400">One click seeds the existing filter state. You can edit anything after.</p>
              </div>
            </div>
            {canUsePresets ? (
              <div className="mt-3 grid gap-2 lg:grid-cols-5">
                {STARTER_PRESETS.map((preset) => (
                  <Link key={preset.id} href={presetHref(params, preset.params)} className={presetLinkClassName} prefetch={false} data-screener-scroll-link="true">
                    <div className="text-sm font-semibold text-white">{preset.label}</div>
                    <div className="mt-1 text-xs leading-4 text-slate-400">{preset.description}</div>
                  </Link>
                ))}
              </div>
            ) : (
              <ScreenerUpgradeOverlay
                title="Starter screener presets"
                body="Starter screener presets are included with Premium, alongside the full intelligence filter stack."
                className="mt-3"
              >
                <div className="grid gap-2 lg:grid-cols-5 opacity-70 blur-[1.5px]">
                  {STARTER_PRESETS.map((preset) => (
                    <div key={preset.id} className={presetLinkClassName}>
                      <div className="text-sm font-semibold text-white">{preset.label}</div>
                      <div className="mt-1 text-xs leading-4 text-slate-400">{preset.description}</div>
                    </div>
                  ))}
                </div>
              </ScreenerUpgradeOverlay>
            )}
          </div>

          <div className={sectionCardClassName}>
            <div className="flex flex-wrap items-center justify-between gap-2">
              <div>
                <p className="text-[11px] font-semibold uppercase tracking-[0.22em] text-slate-500">Core Filters</p>
                <p className="mt-1 text-sm text-slate-400">Keep the input set compact and bias the table toward liquid, investable names.</p>
              </div>
            </div>
            <div className="mt-3 grid gap-3 md:grid-cols-4 xl:grid-cols-8">
              <FormattedNumberInput name="market_cap_min" label="Mkt cap min" value={params.market_cap_min} placeholder="10,000,000,000" labelClassName={filterLabelClassName} />
              <FormattedNumberInput name="market_cap_max" label="Mkt cap max" value={params.market_cap_max} labelClassName={filterLabelClassName} />
              <FormattedNumberInput name="price_min" label="Price min" value={params.price_min} placeholder="10" labelClassName={filterLabelClassName} />
              <FormattedNumberInput name="price_max" label="Price max" value={params.price_max} labelClassName={filterLabelClassName} />
              <FormattedNumberInput name="volume_min" label="Vol min" value={params.volume_min} placeholder="1,000,000" labelClassName={filterLabelClassName} />
              <FormattedNumberInput name="beta_min" label="Beta min" value={params.beta_min} labelClassName={filterLabelClassName} />
              <FormattedNumberInput name="beta_max" label="Beta max" value={params.beta_max} labelClassName={filterLabelClassName} />
              <FormattedNumberInput name="dividend_yield_min" label="Div min" value={params.dividend_yield_min} labelClassName={filterLabelClassName} />
            </div>

            <div className="mt-3 grid gap-3 md:grid-cols-4 xl:grid-cols-8">
              <FilterSelect name="sector" label="Sector" value={params.sector} options={SECTORS} />
              <FilterSelect name="industry" label="Industry" value={params.industry} options={INDUSTRIES} />
              <FilterSelect name="country" label="Country" value={params.country} options={COUNTRIES} />
              <FilterSelect name="exchange" label="Exchange" value={params.exchange} options={EXCHANGES} />
              <FilterSelect name="lookback_days" label="Overlay" value={params.lookback_days} options={[["30", "30d"], ["60", "60d"], ["90", "90d"]]} />
              <FilterSelect name="sort" label="Sort" value={params.sort} options={sortOptions} />
              <FilterSelect name="sort_dir" label="Direction" value={params.sort_dir} options={[["desc", "High to Low"], ["asc", "Low to High"]]} allLabel="Default" />
              <FilterSelect name="page_size" label="Rows" value={params.page_size} options={rowsOptions} includeEmptyOption={false} />
            </div>
          </div>

          <CollapsibleFilterSection
            title="Intelligence Filters"
            description="Use Walnut overlays directly without changing the table-first workflow."
            defaultOpen={intelligenceFiltersOpen}
            storageKey="screener-section-intelligence"
          >
            <div className="grid gap-3 xl:grid-cols-3">
              <div className="rounded-2xl border border-slate-800 bg-slate-950/25 p-3">
                <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-500">Activity</p>
                <div className="mt-3 grid gap-3 md:grid-cols-2">
                  <FilterSelect name="congress_activity" label="Congress" value={params.congress_activity} options={ACTIVITY_FILTER_OPTIONS} />
                  <FilterSelect name="insider_activity" label="Insiders" value={params.insider_activity} options={ACTIVITY_FILTER_OPTIONS} />
                </div>
              </div>

              <div className="rounded-2xl border border-slate-800 bg-slate-950/25 p-3">
                <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-500">Government Contracts</p>
                <div className="mt-3 grid gap-3 md:grid-cols-3">
                  <FilterSelect name="government_contracts_active" label="Contracts" value={params.government_contracts_active} options={GOVERNMENT_CONTRACT_BOOLEAN_OPTIONS} />
                  <FilterSelect name="government_contracts_min_amount" label="Minimum contract value" value={params.government_contracts_min_amount} options={GOVERNMENT_CONTRACT_AMOUNT_OPTIONS} />
                  <FilterSelect name="government_contracts_lookback_days" label="Lookback" value={params.government_contracts_lookback_days} options={GOVERNMENT_CONTRACT_LOOKBACK_OPTIONS} />
                </div>
              </div>

              {canUseIntelligence ? (
                <div className="rounded-2xl border border-slate-800 bg-slate-950/25 p-3">
                  <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-500">Confirmation</p>
                  <div className="mt-3 grid gap-3 md:grid-cols-3">
                    <FilterSelect name="confirmation_score_min" label="Score" value={params.confirmation_score_min} options={CONFIRMATION_SCORE_OPTIONS} />
                    <FilterSelect name="confirmation_direction" label="Direction" value={params.confirmation_direction} options={CONFIRMATION_DIRECTION_OPTIONS} />
                    <FilterSelect name="confirmation_band" label="Band" value={params.confirmation_band} options={CONFIRMATION_BAND_OPTIONS} />
                  </div>
                </div>
              ) : (
                <ScreenerUpgradeOverlay
                  title="Confirmation filters"
                  body="Confirmation score, direction, band, Why Now, and freshness filters are included with Premium."
                  className="rounded-2xl"
                >
                  <div className="rounded-2xl border border-slate-800 bg-slate-950/25 p-3 opacity-70 blur-[1.5px]">
                    <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-500">Confirmation</p>
                    <div className="mt-3 grid gap-3 md:grid-cols-3">
                      <FilterSelect name="confirmation_score_locked" label="Score" value="" options={CONFIRMATION_SCORE_OPTIONS} />
                      <FilterSelect name="confirmation_direction_locked" label="Direction" value="" options={CONFIRMATION_DIRECTION_OPTIONS} />
                      <FilterSelect name="confirmation_band_locked" label="Band" value="" options={CONFIRMATION_BAND_OPTIONS} />
                    </div>
                  </div>
                </ScreenerUpgradeOverlay>
              )}

              {canUseIntelligence ? (
                <div className="rounded-2xl border border-slate-800 bg-slate-950/25 p-3">
                  <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-500">Timing / Why Now</p>
                  <div className="mt-3 grid gap-3 md:grid-cols-2">
                    <FilterSelect name="why_now_state" label="Why now" value={params.why_now_state} options={WHY_NOW_OPTIONS} />
                    <FilterSelect name="freshness" label="Freshness" value={params.freshness} options={FRESHNESS_OPTIONS} />
                  </div>
                </div>
              ) : (
                <ScreenerUpgradeOverlay
                  title="Timing filters"
                  body="Why Now and freshness filters are included with Premium."
                  className="rounded-2xl"
                >
                  <div className="rounded-2xl border border-slate-800 bg-slate-950/25 p-3 opacity-70 blur-[1.5px]">
                    <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-500">Timing / Why Now</p>
                    <div className="mt-3 grid gap-3 md:grid-cols-2">
                      <FilterSelect name="why_now_locked" label="Why now" value="" options={WHY_NOW_OPTIONS} />
                      <FilterSelect name="freshness_locked" label="Freshness" value="" options={FRESHNESS_OPTIONS} />
                    </div>
                  </div>
                </ScreenerUpgradeOverlay>
              )}

              <div className="rounded-2xl border border-slate-800 bg-slate-950/25 p-3">
                <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-500">Options Flow</p>
                <div className="mt-3 grid gap-3 md:grid-cols-3">
                  <FilterSelect name="options_flow_active" label="Options flow" value={params.options_flow_active} options={BOOLEAN_ACTIVITY_OPTIONS} disabled={!optionsFlowFilterable} />
                  <FilterSelect name="options_flow_direction" label="Direction" value={params.options_flow_direction} options={OPTIONS_FLOW_DIRECTION_OPTIONS} disabled={!optionsFlowFilterable} />
                  <FilterSelect name="options_flow_min_score" label="Minimum score" value={params.options_flow_min_score} options={OPTIONS_FLOW_SCORE_OPTIONS} disabled={!optionsFlowFilterable} />
                  <FilterSelect name="options_flow_min_premium" label="Minimum premium" value={params.options_flow_min_premium} options={OPTIONS_FLOW_PREMIUM_OPTIONS} disabled={!optionsFlowFilterable} />
                  <FilterSelect name="options_flow_lookback_days" label="Lookback" value={params.options_flow_lookback_days} options={OPTIONS_FLOW_LOOKBACK_OPTIONS} disabled={!optionsFlowFilterable} />
                </div>
                {!canUseOptionsFlow ? (
                  <p className="mt-3 text-xs leading-5 text-slate-500">Options flow filters require Pro.</p>
                ) : !overlayAvailability?.options_flow?.filterable ? (
                  <p className="mt-3 text-xs leading-5 text-slate-500">Options flow data is not connected yet.</p>
                ) : null}
              </div>

              <div className="rounded-2xl border border-slate-800 bg-slate-950/25 p-3">
                <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-500">Institutional Activity</p>
                <div className="mt-3 grid gap-3 md:grid-cols-3">
                  <FilterSelect name="institutional_activity_type" label="Activity" value={params.institutional_activity_type} options={INSTITUTIONAL_ACTIVITY_TYPE_OPTIONS} disabled={!institutionalActivityFilterable} />
                  <FilterSelect name="institutional_activity_direction" label="Direction" value={params.institutional_activity_direction} options={INSTITUTIONAL_DIRECTION_OPTIONS} disabled={!institutionalActivityFilterable} />
                  <FilterSelect name="institutional_activity_min_value" label="Min Reported Value" value={params.institutional_activity_min_value} options={INSTITUTIONAL_VALUE_OPTIONS} disabled={!institutionalActivityFilterable} />
                  <FilterSelect name="institutional_activity_min_ownership_pct" label="Min Ownership %" value={params.institutional_activity_min_ownership_pct} options={INSTITUTIONAL_OWNERSHIP_OPTIONS} disabled={!institutionalActivityFilterable} />
                  <FilterSelect name="institutional_activity_holder_breadth" label="Holder Breadth" value={params.institutional_activity_holder_breadth} options={INSTITUTIONAL_HOLDER_BREADTH_OPTIONS} disabled={!institutionalActivityFilterable} />
                  <FilterSelect name="institutional_activity_lookback" label="Lookback" value={params.institutional_activity_lookback} options={INSTITUTIONAL_LOOKBACK_OPTIONS} disabled={!institutionalActivityFilterable} />
                </div>
                {!canUseInstitutionalActivity ? (
                  <p className="mt-3 text-xs leading-5 text-slate-500">Institutional activity filters require Pro.</p>
                ) : !overlayAvailability?.institutional_activity?.filterable ? (
                  <p className="mt-3 text-xs leading-5 text-slate-500">Institutional Activity is unavailable for the current universe.</p>
                ) : null}
              </div>
            </div>
          </CollapsibleFilterSection>

          <CollapsibleFilterSection
            title="Technical Filters"
            description="Screen for attention, momentum, and trend without expanding the default table."
            defaultOpen={technicalFiltersOpen}
            storageKey="screener-section-technical"
          >
            {canUseIntelligence ? (
              <TechnicalFiltersContent params={params} />
            ) : (
              <ScreenerUpgradeOverlay
                title="Technical screener filters"
                body="Relative volume, price move, RSI, MACD, and trend filters are included with Premium."
                className="mt-3"
              >
                <TechnicalFiltersContent params={params} locked />
              </ScreenerUpgradeOverlay>
            )}
          </CollapsibleFilterSection>

          <CollapsibleFilterSection
            title="Fundamental Filters"
            description="High-signal valuation, quality, growth, balance sheet, and cash-flow filters."
            defaultOpen={fundamentalFiltersOpen}
            storageKey="screener-section-fundamental"
          >
            {canUseIntelligence ? (
              <FundamentalFiltersContent params={params} />
            ) : (
              <ScreenerUpgradeOverlay
                title="Fundamental screener filters"
                body="Valuation, quality, growth, balance sheet, and cash-flow filters are included with Premium."
                className="mt-3"
              >
                <FundamentalFiltersContent params={params} locked />
              </ScreenerUpgradeOverlay>
            )}
          </CollapsibleFilterSection>

          <div className="flex flex-wrap items-center justify-between gap-3 border-t border-slate-800 pt-4">
            <div className="flex flex-wrap items-center gap-2">
              <button type="submit" className={subtlePrimaryButtonClassName}>
                Run screen
              </button>
              <Link href="/screener" className={ghostButtonClassName} prefetch={false}>
                Reset
              </Link>
            </div>
            <div className="text-xs text-slate-500">
              Default ranking blends confirmation, activity, market cap, and liquidity.
            </div>
          </div>
        </form>
      </div>

      {canUseScreener ? (
        <ScreenerResultsClient
          params={params}
          page={page}
          pageSize={pageSize}
          intelligenceLocked={!canUseIntelligence}
          resultCap={resultCap}
          activeColumns={activeColumns}
        />
      ) : null}
      </div>
    </VerifiedSessionGuard>
  );
}

function TechnicalFiltersContent({ params, locked = false }: { params: Record<string, string | number>; locked?: boolean }) {
  return (
    <fieldset disabled={locked} className={`grid gap-3 lg:grid-cols-2 xl:grid-cols-3 ${locked ? "opacity-70 blur-[1.5px]" : ""}`}>
      <div className="rounded-2xl border border-slate-800 bg-slate-950/25 p-3">
        <PairedNumberInputs minName="rel_volume_min" maxName="rel_volume_max" label="Volume vs Avg" params={params} placeholderMin="1" placeholderMax="2" />
      </div>
      <div className="rounded-2xl border border-slate-800 bg-slate-950/25 p-3">
        <PairedNumberInputs minName="price_move_min" maxName="price_move_max" label="Price Move %" params={params} placeholderMin="-10" placeholderMax="10" />
      </div>
      <div className="rounded-2xl border border-slate-800 bg-slate-950/25 p-3">
        <PairedNumberInputs minName="rsi_min" maxName="rsi_max" label="RSI" params={params} placeholderMin="30" placeholderMax="70" />
      </div>
      <div className="rounded-2xl border border-slate-800 bg-slate-950/25 p-3">
        <FilterSelect name="macd_state" label="MACD" value={params.macd_state} options={MACD_STATE_OPTIONS} />
      </div>
      <div className="rounded-2xl border border-slate-800 bg-slate-950/25 p-3">
        <FilterSelect name="trend_state" label="Trend" value={params.trend_state} options={TREND_STATE_OPTIONS} />
      </div>
    </fieldset>
  );
}

function FundamentalFiltersContent({ params, locked = false }: { params: Record<string, string | number>; locked?: boolean }) {
  return (
    <fieldset disabled={locked} className={`grid gap-3 xl:grid-cols-2 ${locked ? "opacity-70 blur-[1.5px]" : ""}`}>
      <div className="rounded-2xl border border-slate-800 bg-slate-950/25 p-3">
        <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-500">Valuation</p>
        <div className="mt-3 grid gap-3 md:grid-cols-2">
          <PairedNumberInputs minName="trailing_pe_min" maxName="trailing_pe_max" label="Trailing P/E" params={params} />
          <PairedNumberInputs minName="forward_pe_min" maxName="forward_pe_max" label="Forward P/E" params={params} />
          <PairedNumberInputs minName="price_to_sales_min" maxName="price_to_sales_max" label="P/S" params={params} />
          <PairedNumberInputs minName="ev_to_ebitda_min" maxName="ev_to_ebitda_max" label="EV/EBITDA" params={params} />
        </div>
      </div>
      <div className="rounded-2xl border border-slate-800 bg-slate-950/25 p-3">
        <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-500">Profitability / Quality</p>
        <div className="mt-3 grid gap-3 md:grid-cols-2">
          <PairedNumberInputs minName="gross_margin_min" maxName="gross_margin_max" label="Gross Margin" params={params} />
          <PairedNumberInputs minName="operating_margin_min" maxName="operating_margin_max" label="Operating Margin" params={params} />
          <PairedNumberInputs minName="net_margin_min" maxName="net_margin_max" label="Net Margin" params={params} />
          <PairedNumberInputs minName="roe_min" maxName="roe_max" label="ROE" params={params} />
          <PairedNumberInputs minName="roic_min" maxName="roic_max" label="ROIC" params={params} />
        </div>
      </div>
      <div className="rounded-2xl border border-slate-800 bg-slate-950/25 p-3">
        <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-500">Growth</p>
        <div className="mt-3 grid gap-3 md:grid-cols-2">
          <PairedNumberInputs minName="revenue_growth_min" maxName="revenue_growth_max" label="Revenue Growth" params={params} />
          <PairedNumberInputs minName="eps_growth_min" maxName="eps_growth_max" label="EPS Growth" params={params} />
          <PairedNumberInputs minName="ebitda_growth_min" maxName="ebitda_growth_max" label="EBITDA Growth" params={params} />
          <PairedNumberInputs minName="fcf_growth_min" maxName="fcf_growth_max" label="FCF Growth" params={params} />
        </div>
      </div>
      <div className="rounded-2xl border border-slate-800 bg-slate-950/25 p-3">
        <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-500">Balance Sheet</p>
        <div className="mt-3 grid gap-3 md:grid-cols-2">
          <PairedNumberInputs minName="debt_to_equity_min" maxName="debt_to_equity_max" label="Debt/Equity" params={params} />
          <PairedNumberInputs minName="current_ratio_min" maxName="current_ratio_max" label="Current Ratio" params={params} />
          <PairedNumberInputs minName="net_debt_to_ebitda_min" maxName="net_debt_to_ebitda_max" label="Net Debt / EBITDA" params={params} />
        </div>
      </div>
      <div className="rounded-2xl border border-slate-800 bg-slate-950/25 p-3 xl:col-span-2">
        <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-500">Earnings / Cash Flow Quality</p>
        <div className="mt-3 grid gap-3 md:grid-cols-2 xl:grid-cols-4">
          <PairedNumberInputs minName="eps_ttm_min" maxName="eps_ttm_max" label="EPS TTM" params={params} />
          <PairedNumberInputs minName="free_cash_flow_min" maxName="free_cash_flow_max" label="FCF" params={params} />
          <PairedNumberInputs minName="fcf_margin_min" maxName="fcf_margin_max" label="FCF Margin" params={params} />
          <PairedNumberInputs minName="earnings_yield_min" maxName="earnings_yield_max" label="Earnings Yield" params={params} />
        </div>
      </div>
    </fieldset>
  );
}

function ScreenerResults({
  data,
  errorMessage,
  params,
  page,
  pageSize,
  intelligenceLocked,
  resultCap,
  activeColumns,
}: {
  data: ScreenerResponse | null;
  errorMessage: string | null;
  params: Record<string, string | number>;
  page: number;
  pageSize: number;
  intelligenceLocked: boolean;
  resultCap: number;
  activeColumns: ScreenerColumnKey[];
}) {
  const rows = data?.items ?? [];
  const totalAvailable = data?.total_available ?? 0;
  const hasNext = data?.has_next ?? false;
  const governmentContractsAvailabilityStatus = data?.overlay_availability?.government_contracts?.status ?? "ok";
  const optionsFlowLocked = data?.access?.options_flow_locked === true || data?.overlay_availability?.options_flow?.status === "pro_locked";
  const institutionalActivityLocked = data?.access?.institutional_activity_locked === true || data?.overlay_availability?.institutional_activity?.status === "pro_locked";
  const colSpan = 7 + activeColumns.length;

  return (
    <div id="screener-results" className={`${cardClassName} min-h-[34rem] scroll-mt-6 overflow-hidden p-0`}>
      <div className="flex flex-wrap items-center justify-between gap-3 border-b border-slate-800 bg-slate-950/50 px-4 py-3">
        <div>
          <h2 className="text-lg font-semibold text-white">Results</h2>
          <p className="mt-1 text-sm text-slate-400">
            {errorMessage ? "Screener data temporarily unavailable" : `${rows.length} shown from ${totalAvailable} available results`}
          </p>
        </div>
        <div className="flex flex-wrap items-center gap-2 text-xs">
          <Link
            href={pageHref(params, { page: Math.max(page - 1, 1) })}
            className={`${ghostButtonClassName} rounded-lg px-3 py-2 text-xs ${page <= 1 ? "pointer-events-none opacity-40" : ""}`}
            prefetch={false}
          >
            Prev
          </Link>
          <span className={`${compactBadgeClassName} rounded-lg border-slate-800 bg-slate-950/40 px-3 py-2 text-slate-300`}>
            Page {page}
          </span>
          <span className={`${compactBadgeClassName} rounded-lg border-slate-800 bg-slate-950/40 px-3 py-2 text-slate-300`}>
            cap {data?.result_cap ?? resultCap}
          </span>
          <Link
            href={pageHref(params, { page: page + 1 })}
            className={`${ghostButtonClassName} rounded-lg px-3 py-2 text-xs ${!hasNext ? "pointer-events-none opacity-40" : ""}`}
            prefetch={false}
          >
            Next
          </Link>
        </div>
      </div>

      <div className={resultsTableFrameClassName(rows.length)}>
        <table className="min-w-full border-collapse text-sm">
          <thead className={`${stickyResultsTableHeaderClassName} bg-slate-950 text-xs uppercase tracking-wider text-slate-400`}>
            <tr>
              <SortHeader params={params} sort="symbol" label="Symbol" />
              <th className="px-3 py-2.5 text-left">Company</th>
              <th className="px-3 py-2.5 text-left">Sector</th>
              <SortHeader params={params} sort="market_cap" label="Market cap" />
              <SortHeader params={params} sort="price" label="Price" />
              <SortHeader params={params} sort="volume" label="Volume" />
              <SortHeader params={params} sort="beta" label="Beta" />
              {activeColumns.includes("congress") ? <SortHeader params={params} sort="congress_activity" label="Congress" /> : null}
              {activeColumns.includes("insiders") ? <SortHeader params={params} sort="insider_activity" label="Insiders" /> : null}
              {activeColumns.includes("institutional") ? <th className="px-3 py-2.5 text-left">Institutional</th> : null}
              {activeColumns.includes("options_flow") ? <th className="px-3 py-2.5 text-left">Options Flow</th> : null}
              {activeColumns.includes("government_contracts") ? <th className="px-3 py-2.5 text-left">Gov Contracts</th> : null}
              {activeColumns.includes("confirmation") ? <SortHeader params={params} sort="confirmation_score" label="Confirm" locked={intelligenceLocked} /> : null}
              {activeColumns.includes("why_now") ? <th className="px-3 py-2.5 text-left">Why Now</th> : null}
              {activeColumns.includes("rel_volume") ? <th className="px-3 py-2.5 text-left">Volume vs Avg</th> : null}
              {activeColumns.includes("price_move_pct") ? <th className="px-3 py-2.5 text-left">Price Move</th> : null}
              {activeColumns.includes("rsi") ? <th className="px-3 py-2.5 text-left">RSI</th> : null}
              {activeColumns.includes("macd_state") ? <th className="px-3 py-2.5 text-left">MACD</th> : null}
              {activeColumns.includes("trend_state") ? <th className="px-3 py-2.5 text-left">Trend</th> : null}
              {activeColumns.includes("trailing_pe") ? <th className="px-3 py-2.5 text-left">Trailing P/E</th> : null}
              {activeColumns.includes("forward_pe") ? <th className="px-3 py-2.5 text-left">Forward P/E</th> : null}
              {activeColumns.includes("price_sales") ? <th className="px-3 py-2.5 text-left">P/S</th> : null}
              {activeColumns.includes("ev_ebitda") ? <th className="px-3 py-2.5 text-left">EV/EBITDA</th> : null}
              {activeColumns.includes("gross_margin") ? <th className="px-3 py-2.5 text-left">Gross Margin</th> : null}
              {activeColumns.includes("operating_margin") ? <th className="px-3 py-2.5 text-left">Operating Margin</th> : null}
              {activeColumns.includes("net_margin") ? <th className="px-3 py-2.5 text-left">Net Margin</th> : null}
              {activeColumns.includes("roe") ? <th className="px-3 py-2.5 text-left">ROE</th> : null}
              {activeColumns.includes("roic") ? <th className="px-3 py-2.5 text-left">ROIC</th> : null}
              {activeColumns.includes("revenue_growth") ? <th className="px-3 py-2.5 text-left">Revenue Growth</th> : null}
              {activeColumns.includes("eps_growth") ? <th className="px-3 py-2.5 text-left">EPS Growth</th> : null}
              {activeColumns.includes("ebitda_growth") ? <th className="px-3 py-2.5 text-left">EBITDA Growth</th> : null}
              {activeColumns.includes("fcf_growth") ? <th className="px-3 py-2.5 text-left">FCF Growth</th> : null}
              {activeColumns.includes("debt_equity") ? <th className="px-3 py-2.5 text-left">Debt/Equity</th> : null}
              {activeColumns.includes("current_ratio") ? <th className="px-3 py-2.5 text-left">Current Ratio</th> : null}
              {activeColumns.includes("net_debt_ebitda") ? <th className="px-3 py-2.5 text-left">Net Debt / EBITDA</th> : null}
              {activeColumns.includes("eps_ttm") ? <th className="px-3 py-2.5 text-left">EPS TTM</th> : null}
              {activeColumns.includes("fcf") ? <th className="px-3 py-2.5 text-left">FCF</th> : null}
              {activeColumns.includes("fcf_margin") ? <th className="px-3 py-2.5 text-left">FCF Margin</th> : null}
              {activeColumns.includes("earnings_yield") ? <th className="px-3 py-2.5 text-left">Earnings Yield</th> : null}
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-800">
            {errorMessage ? (
              <tr>
                <td className="px-4 py-12 text-center text-slate-400" colSpan={colSpan}>
                  {errorMessage}
                </td>
              </tr>
            ) : rows.length === 0 ? (
              <tr>
                <td className="px-4 py-12 text-center text-slate-400" colSpan={colSpan}>
                  No names matched this screen. Widen the market cap, liquidity, or sector filters.
                </td>
              </tr>
            ) : (
              rows.map((row) => (
                <ScreenerTableRow
                  key={row.symbol}
                  row={row}
                  optionsFlowLocked={optionsFlowLocked}
                  institutionalActivityLocked={institutionalActivityLocked}
                  governmentContractsAvailabilityStatus={governmentContractsAvailabilityStatus}
                  activeColumns={activeColumns}
                />
              ))
            )}
          </tbody>
        </table>
      </div>

      <div className="flex flex-wrap items-center justify-between gap-3 border-t border-slate-800 bg-slate-950/40 px-4 py-3 text-xs text-slate-500">
        <span>
          Page size {pageSize}. Result cap {data?.result_cap ?? resultCap}. Confirmation overlays use a {data?.lookback_days ?? params.lookback_days ?? 30}d lookback.
        </span>
        <span>
          Market data with Walnut overlays
          {data?.overlay_availability?.options_flow?.filterable === false ? " · options flow unavailable" : ""}
          {data?.overlay_availability?.institutional_activity?.filterable === false ? " · institutional unavailable" : ""}
        </span>
      </div>
    </div>
  );
}

function SortHeader({
  params,
  sort,
  label,
  locked = false,
}: {
  params: Record<string, string | number>;
  sort: string;
  label: string;
  locked?: boolean;
}) {
  const active = params.sort === sort;
  const nextDir = active && params.sort_dir === "desc" ? "asc" : "desc";
  if (locked) {
    return (
      <th className="px-3 py-2.5 text-left">
        <span className="inline-flex items-center gap-2 text-slate-400">
          {label}
          <span className="rounded-full border border-amber-300/30 bg-amber-300/10 px-2 py-0.5 text-[10px] font-semibold tracking-[0.16em] text-amber-100">
            Premium
          </span>
        </span>
      </th>
    );
  }
  return (
    <th className={`px-3 py-2.5 text-left ${active ? "bg-emerald-400/[0.05] text-emerald-100" : ""}`}>
      <Link href={pageHref(params, { sort, sort_dir: nextDir, page: 1 })} className="inline-flex items-center gap-1 hover:text-white" prefetch={false}>
        {label}
        <span className="text-[10px] font-semibold normal-case tracking-normal text-slate-600">
          {active ? (params.sort_dir === "asc" ? "asc" : "desc") : ""}
        </span>
      </Link>
    </th>
  );
}

function WhyNowHover({ row, locked = false }: { row: ScreenerRow; locked?: boolean }) {
  if (locked) {
    return (
      <div className="space-y-1">
        <span className="inline-flex items-center rounded-full border border-amber-300/30 bg-amber-300/10 px-2 py-0.5 text-[10px] font-semibold uppercase tracking-[0.16em] text-amber-100">
          Locked
        </span>
        <div className="text-[11px] leading-4 text-slate-500">Premium Why Now + freshness</div>
      </div>
    );
  }
  const stateLabel = whyNowStateLabel(row.why_now.state);
  const tooltipId = `why-now-${row.symbol}`;
  return (
    <div className="group/why relative inline-flex max-w-full items-center">
      <button
        type="button"
        aria-describedby={tooltipId}
        className={`${tinyStateBadgeClassName} ${whyNowClass(row.why_now.state, row.confirmation.direction)} cursor-help transition hover:border-white/20 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-emerald-400/30`}
      >
        {stateLabel}
      </button>
      <div
        id={tooltipId}
        role="tooltip"
        className="pointer-events-none invisible absolute right-0 top-full z-30 mt-2 w-72 rounded-xl border border-white/10 bg-slate-950/95 p-3 text-left opacity-0 shadow-2xl shadow-black/40 backdrop-blur transition group-hover/why:visible group-hover/why:opacity-100 group-focus-within/why:visible group-focus-within/why:opacity-100"
      >
        <p className="text-[11px] font-semibold uppercase tracking-wide text-slate-500">Why now</p>
        <p className="mt-1 text-sm leading-5 text-slate-100">{row.why_now.headline}</p>
        <p className="mt-2 text-xs leading-4 text-slate-500">{row.confirmation.status}</p>
        <p className="mt-1 text-xs leading-4 text-slate-500">Freshness: {row.signal_freshness.freshness_label}</p>
      </div>
    </div>
  );
}

function GovernmentContractsCell({
  row,
  availabilityStatus,
}: {
  row: ScreenerRow;
  availabilityStatus?: string;
}) {
  if (availabilityStatus === "unavailable" && row.government_contracts_status !== "ok") {
    return <span className="text-sm text-slate-500">Unavailable</span>;
  }
  if (!row.government_contracts_active) return <span className="text-sm text-slate-500">—</span>;
  const count = row.government_contracts_count ?? 0;
  return (
    <div className="min-w-[11rem]">
      <div className="text-sm font-semibold text-slate-100">
        {formatCurrencyCompact(row.government_contracts_total_amount)} · {count} contract{count === 1 ? "" : "s"}
      </div>
      <div className="mt-0.5 truncate text-[11px] leading-4 text-slate-500">
        Latest: {formatShortDate(row.government_contracts_latest_date)} · {row.government_contracts_top_agency ?? "Agency"}
      </div>
    </div>
  );
}

function GovernmentContractsMetricCell({
  row,
  availabilityStatus,
}: {
  row: ScreenerRow;
  availabilityStatus?: string;
}) {
  if (availabilityStatus === "unavailable" && row.government_contracts_status !== "ok") {
    return <span className="text-sm text-slate-500">Unavailable</span>;
  }
  if (!row.government_contracts_active) return <span className="text-sm text-slate-500">—</span>;
  const count = row.government_contracts_count ?? 0;
  return (
    <div className="min-w-[11rem]">
      <div className="text-sm font-semibold text-slate-100">
        {formatCurrencyCompact(row.government_contracts_total_amount)} · {count} contract{count === 1 ? "" : "s"}
      </div>
      <div className="mt-0.5 truncate text-[11px] leading-4 text-slate-500">
        Latest: {formatShortDate(row.government_contracts_latest_date)} · {row.government_contracts_top_agency ?? "Agency"}
      </div>
    </div>
  );
}

function OptionsFlowCell({ row, proLocked }: { row: ScreenerRow; proLocked?: boolean }) {
  if (proLocked || row.options_flow_locked || row.options_flow_status === "pro_locked") return lockedMetricLine("Pro data locked");
  if (!row.options_flow_active || row.options_flow_status === "unavailable") return <span className="text-sm text-slate-500">—</span>;
  return (
    <div className="min-w-[10rem]">
      <div className="text-sm font-semibold text-slate-100">
        {row.options_flow_score ?? "—"} · {titleCase(row.options_flow_direction ?? "neutral")}
      </div>
      <div className="mt-0.5 truncate text-[11px] leading-4 text-slate-500">
        {formatCurrencyCompact(row.options_flow_total_premium)} premium · {titleCase(row.options_flow_intensity ?? "low")}
      </div>
    </div>
  );
}

function InstitutionalActivityCell({ row, proLocked }: { row: ScreenerRow; proLocked?: boolean }) {
  if (proLocked || row.institutional_activity_locked || row.institutional_activity_status === "pro_locked") return lockedMetricLine("Pro data locked");
  if (!row.institutional_activity_active || row.institutional_activity_status !== "ok") return <span className="text-sm text-slate-500">—</span>;
  const breadth = row.institutional_activity_holder_breadth;
  const ownership = row.institutional_activity_ownership_pct;
  return (
    <div className="min-w-[10rem]">
      <div className="text-sm font-semibold text-slate-100">
        {formatCurrencyCompact(row.institutional_activity_net_activity)} reported · {titleCase(row.institutional_activity_direction ?? "neutral")}
      </div>
      <div className="mt-0.5 truncate text-[11px] leading-4 text-slate-500">
        {breadth !== null && breadth !== undefined ? `Breadth ${breadth >= 0 ? "+" : ""}${breadth}` : `${row.institutional_activity_institution_count ?? 0} holders`}
        {ownership !== null && ownership !== undefined ? ` · ${ownership.toFixed(1)}% ownership` : ""}
      </div>
    </div>
  );
}

function macdLabel(value?: string | null): string {
  if (value === "crossover_bullish") return "Bullish crossover";
  if (value === "crossover_bearish") return "Bearish crossover";
  return titleCase(value ?? "");
}

function trendLabel(value?: string | null): string {
  if (value === "sma_above_lma") return "SMA > LMA";
  if (value === "sma_below_lma") return "SMA < LMA";
  return titleCase(value ?? "");
}

function DynamicMetricCell({ value }: { value: string }) {
  return <td className={tableMetricClassName}>{value}</td>;
}

function ScreenerTableRow({
  row,
  optionsFlowLocked = false,
  institutionalActivityLocked = false,
  governmentContractsAvailabilityStatus = "ok",
  activeColumns,
}: {
  row: ScreenerRow;
  optionsFlowLocked?: boolean;
  institutionalActivityLocked?: boolean;
  governmentContractsAvailabilityStatus?: string;
  activeColumns: ScreenerColumnKey[];
}) {
  const href = tickerHref(row.symbol) ?? row.ticker_url ?? `/ticker/${encodeURIComponent(row.symbol)}`;
  const confirmationDirection = confirmationDirectionLabel(row.confirmation.direction);
  const confirmationSourceMeta = confirmationMeta(row.confirmation.status, row.confirmation.direction);
  return (
    <ClickableScreenerRow href={href} label={`Open ${row.symbol} ticker page`}>
      <td className={`${tableCellClassName} whitespace-nowrap`}>
        <div className="flex items-center gap-2">
          <AddTickerToWatchlist symbol={row.symbol} variant="compact" align="left" />
          <Link href={href} prefetch={false} className={`${tickerMonoLinkClassName} transition group-hover:text-emerald-100`}>
            {row.symbol}
          </Link>
        </div>
      </td>
      <td className={`${tableCellClassName} min-w-[14rem]`}>
        <Link
          href={href}
          prefetch={false}
          className="block max-w-[18rem] truncate font-medium text-slate-100 transition hover:text-white hover:underline group-hover:text-white"
          title={formatCompanyName(row.company_name)}
        >
          {formatCompanyName(row.company_name)}
        </Link>
        <div className="mt-0.5 text-xs leading-4 text-slate-500">
          {[row.exchange, row.country].filter(Boolean).join(" / ") || "--"}
        </div>
      </td>
      <td className={`${tableCellClassName} min-w-[12rem] text-slate-300`}>
        <div className="max-w-[12rem] truncate">{row.sector ?? "--"}</div>
        {row.industry ? <div className="mt-0.5 max-w-[12rem] truncate text-xs leading-4 text-slate-500">{row.industry}</div> : null}
      </td>
      <td className={tableMetricClassName}>{formatCompact(row.market_cap)}</td>
      <td className={tableMetricClassName}>{formatCurrency(row.price)}</td>
      <td className={tableMetricClassName}>{formatCompact(row.volume)}</td>
      <td className={tableMetricClassName}>{formatBeta(row.beta)}</td>
      {activeColumns.includes("congress") ? <td className={`${tableCellClassName} whitespace-nowrap`} title={row.congress_activity.label}>
        <div className={`text-xs font-semibold ${activityTextClass(row.congress_activity)}`}>
          {row.congress_activity.present ? "Active" : "None"}
        </div>
        <div className="mt-0.5 text-[11px] leading-4 text-slate-500">{activityMeta(row.congress_activity)}</div>
      </td> : null}
      {activeColumns.includes("insiders") ? <td className={`${tableCellClassName} whitespace-nowrap`} title={row.insider_activity.label}>
        <div className={`text-xs font-semibold ${activityTextClass(row.insider_activity)}`}>
          {row.insider_activity.present ? "Active" : "None"}
        </div>
        <div className="mt-0.5 text-[11px] leading-4 text-slate-500">{activityMeta(row.insider_activity)}</div>
      </td> : null}
      {activeColumns.includes("institutional") ? <td className={`${tableCellClassName} min-w-[10rem]`}><InstitutionalActivityCell row={row} proLocked={institutionalActivityLocked} /></td> : null}
      {activeColumns.includes("options_flow") ? <td className={`${tableCellClassName} min-w-[10rem]`}><OptionsFlowCell row={row} proLocked={optionsFlowLocked} /></td> : null}
      {activeColumns.includes("government_contracts") ? <td className={`${tableCellClassName} min-w-[11rem]`}>
        <GovernmentContractsMetricCell
          row={row}
          availabilityStatus={governmentContractsAvailabilityStatus}
        />
      </td> : null}
      {activeColumns.includes("confirmation") ? <td className={`${tableCellClassName} min-w-[8.5rem] whitespace-nowrap`} title={row.confirmation.status}>
        <div className="flex items-baseline gap-1.5">
          <span className="text-sm font-semibold tabular-nums text-slate-100">{row.confirmation.score}</span>
          <span className={`text-xs font-medium ${confirmationBandClass(row.confirmation.band)}`}>
            {titleCase(row.confirmation.band)}
          </span>
        </div>
        <div className={`mt-0.5 text-[11px] font-semibold uppercase tracking-[0.14em] ${directionTextClass(row.confirmation.direction)}`}>
          {confirmationDirection}
        </div>
        <div className="mt-0.5 text-[11px] leading-4 text-slate-500">{confirmationSourceMeta}</div>
      </td> : null}
      {activeColumns.includes("why_now") ? <td className={`${tableCellClassName} min-w-[8rem] max-w-[10rem]`}>
        <WhyNowHover row={row} locked={false} />
        <div className="mt-1 text-[11px] leading-4 text-slate-500">
          {freshnessStateLabel(row.signal_freshness.freshness_state)}
        </div>
      </td> : null}
      {activeColumns.includes("rel_volume") ? <DynamicMetricCell value={formatMultiple(row.rel_volume)} /> : null}
      {activeColumns.includes("price_move_pct") ? <DynamicMetricCell value={formatPercent(row.price_move_pct, true)} /> : null}
      {activeColumns.includes("rsi") ? <DynamicMetricCell value={formatPlainNumber(row.rsi, 0)} /> : null}
      {activeColumns.includes("macd_state") ? <DynamicMetricCell value={macdLabel(row.macd_state)} /> : null}
      {activeColumns.includes("trend_state") ? <DynamicMetricCell value={trendLabel(row.trend_state)} /> : null}
      {activeColumns.includes("trailing_pe") ? <DynamicMetricCell value={formatMultiple(row.trailing_pe)} /> : null}
      {activeColumns.includes("forward_pe") ? <DynamicMetricCell value={formatMultiple(row.forward_pe)} /> : null}
      {activeColumns.includes("price_sales") ? <DynamicMetricCell value={formatMultiple(row.price_sales)} /> : null}
      {activeColumns.includes("ev_ebitda") ? <DynamicMetricCell value={formatMultiple(row.ev_ebitda)} /> : null}
      {activeColumns.includes("gross_margin") ? <DynamicMetricCell value={formatPercent(row.gross_margin)} /> : null}
      {activeColumns.includes("operating_margin") ? <DynamicMetricCell value={formatPercent(row.operating_margin)} /> : null}
      {activeColumns.includes("net_margin") ? <DynamicMetricCell value={formatPercent(row.net_margin)} /> : null}
      {activeColumns.includes("roe") ? <DynamicMetricCell value={formatPercent(row.roe)} /> : null}
      {activeColumns.includes("roic") ? <DynamicMetricCell value={formatPercent(row.roic)} /> : null}
      {activeColumns.includes("revenue_growth") ? <DynamicMetricCell value={formatPercent(row.revenue_growth, true)} /> : null}
      {activeColumns.includes("eps_growth") ? <DynamicMetricCell value={formatPercent(row.eps_growth, true)} /> : null}
      {activeColumns.includes("ebitda_growth") ? <DynamicMetricCell value={formatPercent(row.ebitda_growth, true)} /> : null}
      {activeColumns.includes("fcf_growth") ? <DynamicMetricCell value={formatPercent(row.fcf_growth, true)} /> : null}
      {activeColumns.includes("debt_equity") ? <DynamicMetricCell value={formatMultiple(row.debt_equity)} /> : null}
      {activeColumns.includes("current_ratio") ? <DynamicMetricCell value={formatMultiple(row.current_ratio)} /> : null}
      {activeColumns.includes("net_debt_ebitda") ? <DynamicMetricCell value={formatMultiple(row.net_debt_ebitda)} /> : null}
      {activeColumns.includes("eps_ttm") ? <DynamicMetricCell value={formatCurrency(row.eps_ttm)} /> : null}
      {activeColumns.includes("fcf") ? <DynamicMetricCell value={formatCurrencyCompact(row.fcf)} /> : null}
      {activeColumns.includes("fcf_margin") ? <DynamicMetricCell value={formatPercent(row.fcf_margin)} /> : null}
      {activeColumns.includes("earnings_yield") ? <DynamicMetricCell value={formatPercent(row.earnings_yield)} /> : null}
    </ClickableScreenerRow>
  );
}
