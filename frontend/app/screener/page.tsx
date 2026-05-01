import Link from "next/link";
import { ClickableScreenerRow } from "@/components/screener/ClickableScreenerRow";
import { ScreenerExportButton } from "@/components/screener/ScreenerExportButton";
import { ScreenerUpgradeOverlay } from "@/components/screener/ScreenerUpgradeOverlay";
import { AddTickerToWatchlist } from "@/components/watchlists/AddTickerToWatchlist";
import { SavedViewsBar } from "@/components/saved-views/SavedViewsBar";
import { UpgradePrompt } from "@/components/billing/UpgradePrompt";
import { API_BASE, getEntitlements } from "@/lib/api";
import { defaultEntitlements, hasEntitlement, limitFor } from "@/lib/entitlements";
import { optionalPageAuthToken } from "@/lib/serverAuth";
import {
  cardClassName,
  ghostButtonClassName,
  inputClassName,
  selectClassName,
  subtlePrimaryButtonClassName,
  tickerMonoLinkClassName,
} from "@/lib/styles";
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
  beta?: number | null;
  country?: string | null;
  exchange?: string | null;
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
  institutional_activity_active?: boolean | null;
  institutional_activity_direction?: string | null;
  institutional_activity_net_activity?: number | null;
  institutional_activity_institution_count?: number | null;
  institutional_activity_total_value?: number | null;
  institutional_activity_latest_date?: string | null;
  institutional_activity_status?: string | null;
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
    tier: "free" | "premium";
    intelligence_locked: boolean;
    presets_locked: boolean;
    saved_screens_limit: number;
    monitoring_locked: boolean;
    csv_export_locked: boolean;
    feature_flags?: Record<string, boolean>;
  };
};

type OverlayAvailability = {
  enabled: boolean;
  status: string;
  filterable: boolean;
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
  "institutional_activity_direction",
  "institutional_activity_min_value",
  "institutional_activity_lookback_days",
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
  ["mixed", "Mixed"],
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
const INSTITUTIONAL_VALUE_OPTIONS = [
  ["1000000", "$1M+"],
  ["10000000", "$10M+"],
  ["50000000", "$50M+"],
  ["250000000", "$250M+"],
] as const;
const INSTITUTIONAL_LOOKBACK_OPTIONS = [
  ["90", "90D"],
  ["180", "180D"],
  ["365", "1Y"],
] as const;

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
  "institutional_activity_lookback_days",
]);
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

function formatInputNumber(value?: string | number): string {
  if (value === undefined || value === null || String(value).trim() === "") return "";
  const raw = String(value).trim();
  if (!/^\d+(?:,\d{3})*(?:\.\d+)?$|^\d+(?:\.\d+)?$/.test(raw)) return raw;
  const normalized = stripNumberFormatting(raw);
  const [whole, fraction] = normalized.split(".");
  const formattedWhole = new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 }).format(Number(whole));
  return fraction === undefined ? formattedWhole : `${formattedWhole}.${fraction}`;
}

function currentParams(sp: SearchParams) {
  const sortRaw = getParam(sp, "sort");
  const sort = SORTS.some(([value]) => value === sortRaw) ? sortRaw : "relevance";
  const sortDir = getParam(sp, "sort_dir") === "asc" ? "asc" : "desc";
  const page = getPositiveInt(getParam(sp, "page"), 1, 10);
  const pageSize = [25, 50, 100].includes(Number(getParam(sp, "page_size")))
    ? Number(getParam(sp, "page_size"))
    : 50;
  const lookbackDays = [30, 60, 90].includes(Number(getParam(sp, "lookback_days")))
    ? Number(getParam(sp, "lookback_days"))
    : 30;
  const governmentContractsLookbackDays = [90, 180, 365, 1095].includes(Number(getParam(sp, "government_contracts_lookback_days")))
    ? Number(getParam(sp, "government_contracts_lookback_days"))
    : 365;
  const optionsFlowLookbackDays = [1, 7, 30, 90].includes(Number(getParam(sp, "options_flow_lookback_days")))
    ? Number(getParam(sp, "options_flow_lookback_days"))
    : 30;
  const institutionalLookbackDays = [90, 180, 365].includes(Number(getParam(sp, "institutional_activity_lookback_days")))
    ? Number(getParam(sp, "institutional_activity_lookback_days"))
    : 90;
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
    institutional_activity_lookback_days: institutionalLookbackDays,
  };
  PARAM_KEYS.forEach((key) => {
    if (key in params) return;
    const value = getParam(sp, key);
    if (value.trim()) params[key] = value.trim();
  });
  return params;
}

function buildApiUrl(params: Record<string, string | number>): string {
  const url = new URL("/api/screener", API_BASE);
  Object.entries(params).forEach(([key, value]) => {
    const trimmed = NUMERIC_PARAM_KEYS.has(key) ? stripNumberFormatting(value) : String(value).trim();
    if (trimmed) url.searchParams.set(key, trimmed);
  });
  return url.toString();
}

function authHeaders(authToken?: string | null): HeadersInit | undefined {
  return authToken ? { Authorization: `Bearer ${authToken}` } : undefined;
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
  url.searchParams.set("page_size", String(params.page_size ?? 50));
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
    institutional_activity: { enabled: true, status: "not_configured", filterable: false },
  };
}

function confirmationDirectionLabel(direction: string): string {
  if (direction === "bullish") return "BULLISH";
  if (direction === "bearish") return "BEARISH";
  if (direction === "mixed") return "MIXED";
  return "NEUTRAL";
}

function confirmationMeta(status: string, direction: string): string {
  const cleaned = status.trim();
  if (!cleaned || cleaned.toLowerCase() === "inactive") return "No active confirmation";
  const withoutDirection = cleaned
    .replace(/\b(bullish|bearish|mixed|neutral)\b\s*/gi, "")
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

function FilterInput({ name, label, value, placeholder }: { name: string; label: string; value?: string | number; placeholder?: string }) {
  return (
    <label className={filterLabelClassName}>
      {label}
      <input
        name={name}
        defaultValue={formatInputNumber(value)}
        placeholder={placeholder}
        className={value ? `${inputClassName} border-emerald-500/40 bg-slate-950/40` : inputClassName}
      />
    </label>
  );
}

function FilterSelect({
  name,
  label,
  value,
  options,
  allLabel = "Any",
  disabled = false,
}: {
  name: string;
  label: string;
  value?: string | number;
  options: readonly string[] | readonly (readonly [string, string])[];
  allLabel?: string;
  disabled?: boolean;
}) {
  return (
    <label className={filterLabelClassName}>
      {label}
      <select
        name={name}
        defaultValue={String(value ?? "")}
        disabled={disabled}
        className={`${value ? `${selectClassName} border-emerald-500/40 bg-slate-950/40` : selectClassName} ${disabled ? "cursor-not-allowed opacity-60" : ""}`}
      >
        <option value="">{allLabel}</option>
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

async function loadScreenerPayload(requestUrl: string, authToken?: string | null): Promise<{ data: ScreenerResponse | null; errorMessage: string | null }> {
  try {
    const response = await fetch(requestUrl, {
      cache: "no-store",
      next: { revalidate: 0 },
      headers: authHeaders(authToken ?? undefined),
    });
    if (!response.ok) {
      const body = await response.json().catch(() => null);
      return {
        data: null,
        errorMessage:
          typeof body?.detail?.message === "string"
            ? body.detail.message
            : typeof body?.detail === "string"
              ? body.detail
              : `Screener request failed with ${response.status}.`,
      };
    }
    return {
      data: (await response.json()) as ScreenerResponse,
      errorMessage: null,
    };
  } catch (error) {
    return {
      data: null,
      errorMessage: error instanceof Error ? error.message : "Unable to load screener.",
    };
  }
}

export default async function ScreenerPage({
  searchParams,
}: {
  searchParams?: Promise<SearchParams>;
}) {
  const sp = (await searchParams) ?? {};
  const authToken = await optionalPageAuthToken();
  const entitlements = await getEntitlements(authToken ?? undefined).catch(() => defaultEntitlements);
  const params = currentParams(sp);
  const requestUrl = buildApiUrl(params);
  const sort = String(params.sort ?? "relevance");
  const sortDir = String(params.sort_dir ?? "desc");
  const page = Number(params.page ?? 1);
  const pageSize = Number(params.page_size ?? 50);
  const canUseScreener = hasEntitlement(entitlements, "screener");
  const canUseIntelligence = hasEntitlement(entitlements, "screener_intelligence");
  const canUsePresets = hasEntitlement(entitlements, "screener_presets");
  const canUseMonitoring = hasEntitlement(entitlements, "screener_monitoring");
  const canExportCsv = hasEntitlement(entitlements, "screener_csv_export");
  const resultCap = limitFor(entitlements, "screener_results");
  const screenerPayload = canUseScreener ? await loadScreenerPayload(requestUrl, authToken) : { data: null, errorMessage: null };
  const overlayAvailability = screenerPayload.data?.overlay_availability ?? overlayAvailabilityDefaults();
  const sortOptions = canUseIntelligence
    ? SORTS
    : SORTS.filter(([value]) => !["confirmation_score", "freshness", "congress_activity", "insider_activity"].includes(value));
  const rowsOptions: ReadonlyArray<readonly [string, string]> = (
    [
      ["25", "25"],
      ["50", "50"],
      ["100", "100"],
    ] as const
  ).filter(([value]) => Number(value) <= Math.min(resultCap, 100));

  return (
    <div className="space-y-8">
      <div className="flex flex-wrap items-end justify-between gap-4">
        <div>
          <p className="text-xs font-semibold uppercase tracking-[0.3em] text-emerald-300">Idea Screener</p>
          <h1 className="mt-2 text-3xl font-semibold text-white">Stock Screener</h1>
          <p className="mt-2 max-w-2xl text-sm text-slate-400">
            FMP fundamentals filtered through Capitol Ledger activity, government contracts, options flow, institutional context, confirmation, Why Now, and freshness overlays.
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
            sort,
            sort_dir: sortDir,
            page_size: String(pageSize),
            lookback_days: String(params.lookback_days ?? 30),
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
                lockedReason="CSV export is included with Premium, along with larger result caps and saved screen monitoring."
              />
              {canUseMonitoring ? (
                <Link href="/monitoring" className={`${ghostButtonClassName} rounded-lg px-3 py-2 text-xs`} prefetch={false}>
                  Monitoring inbox
                </Link>
              ) : (
                <ScreenerUpgradeOverlay
                  title="Saved screen monitoring"
                  body="Saved screen monitoring and inbox events are included with Premium."
                  className="rounded-lg"
                  buttonClassName="border border-transparent"
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
                  <Link key={preset.id} href={presetHref(params, preset.params)} className={presetLinkClassName} prefetch={false}>
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
                <p className="text-[11px] font-semibold uppercase tracking-[0.22em] text-slate-500">Base filters</p>
                <p className="mt-1 text-sm text-slate-400">Keep the input set compact and bias the table toward liquid, investable names.</p>
              </div>
            </div>
            <div className="mt-3 grid gap-3 md:grid-cols-4 xl:grid-cols-8">
              <FilterInput name="market_cap_min" label="Mkt cap min" value={params.market_cap_min} placeholder="10,000,000,000" />
              <FilterInput name="market_cap_max" label="Mkt cap max" value={params.market_cap_max} />
              <FilterInput name="price_min" label="Price min" value={params.price_min} placeholder="10" />
              <FilterInput name="price_max" label="Price max" value={params.price_max} />
              <FilterInput name="volume_min" label="Vol min" value={params.volume_min} placeholder="1,000,000" />
              <FilterInput name="beta_min" label="Beta min" value={params.beta_min} />
              <FilterInput name="beta_max" label="Beta max" value={params.beta_max} />
              <FilterInput name="dividend_yield_min" label="Div min" value={params.dividend_yield_min} />
            </div>

            <div className="mt-3 grid gap-3 md:grid-cols-4 xl:grid-cols-8">
              <FilterSelect name="sector" label="Sector" value={params.sector} options={SECTORS} />
              <FilterSelect name="industry" label="Industry" value={params.industry} options={INDUSTRIES} />
              <FilterSelect name="country" label="Country" value={params.country} options={COUNTRIES} />
              <FilterSelect name="exchange" label="Exchange" value={params.exchange} options={EXCHANGES} />
              <FilterSelect name="lookback_days" label="Overlay" value={params.lookback_days} options={[["30", "30d"], ["60", "60d"], ["90", "90d"]]} />
              <FilterSelect name="sort" label="Sort" value={params.sort} options={sortOptions} />
              <FilterSelect name="sort_dir" label="Direction" value={params.sort_dir} options={[["desc", "High to Low"], ["asc", "Low to High"]]} allLabel="Default" />
              <FilterSelect name="page_size" label="Rows" value={params.page_size} options={rowsOptions} allLabel={String(Math.min(resultCap, 50))} />
            </div>
          </div>

          <div className={sectionCardClassName}>
            <div className="flex flex-wrap items-center justify-between gap-2">
              <div>
                <p className="text-[11px] font-semibold uppercase tracking-[0.22em] text-slate-500">Intelligence filters</p>
                <p className="mt-1 text-sm text-slate-400">Use Capitol Ledger overlays directly without changing the table-first workflow.</p>
              </div>
            </div>
            {canUseIntelligence ? (
              <div className="mt-3 grid gap-3 xl:grid-cols-3">
                <div className="rounded-2xl border border-slate-800 bg-slate-950/25 p-3">
                  <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-500">Activity</p>
                  <div className="mt-3 grid gap-3 md:grid-cols-2">
                    <FilterSelect name="congress_activity" label="Congress" value={params.congress_activity} options={ACTIVITY_FILTER_OPTIONS} />
                    <FilterSelect name="insider_activity" label="Insiders" value={params.insider_activity} options={ACTIVITY_FILTER_OPTIONS} />
                  </div>
                </div>

                <div className="rounded-2xl border border-slate-800 bg-slate-950/25 p-3">
                  <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-500">Confirmation</p>
                  <div className="mt-3 grid gap-3 md:grid-cols-3">
                    <FilterSelect
                      name="confirmation_score_min"
                      label="Score"
                      value={params.confirmation_score_min}
                      options={CONFIRMATION_SCORE_OPTIONS}
                    />
                    <FilterSelect
                      name="confirmation_direction"
                      label="Direction"
                      value={params.confirmation_direction}
                      options={CONFIRMATION_DIRECTION_OPTIONS}
                    />
                    <FilterSelect
                      name="confirmation_band"
                      label="Band"
                      value={params.confirmation_band}
                      options={CONFIRMATION_BAND_OPTIONS}
                    />
                  </div>
                </div>

                <div className="rounded-2xl border border-slate-800 bg-slate-950/25 p-3">
                  <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-500">Timing / Why Now</p>
                  <div className="mt-3 grid gap-3 md:grid-cols-2">
                    <FilterSelect name="why_now_state" label="Why now" value={params.why_now_state} options={WHY_NOW_OPTIONS} />
                    <FilterSelect name="freshness" label="Freshness" value={params.freshness} options={FRESHNESS_OPTIONS} />
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

                <div className="rounded-2xl border border-slate-800 bg-slate-950/25 p-3">
                  <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-500">Options Flow</p>
                  <div className="mt-3 grid gap-3 md:grid-cols-3">
                    <FilterSelect
                      name="options_flow_active"
                      label="Options flow"
                      value={params.options_flow_active}
                      options={BOOLEAN_ACTIVITY_OPTIONS}
                      disabled={!overlayAvailability?.options_flow?.filterable}
                    />
                    <FilterSelect
                      name="options_flow_direction"
                      label="Direction"
                      value={params.options_flow_direction}
                      options={OPTIONS_FLOW_DIRECTION_OPTIONS}
                      disabled={!overlayAvailability?.options_flow?.filterable}
                    />
                    <FilterSelect
                      name="options_flow_min_score"
                      label="Minimum score"
                      value={params.options_flow_min_score}
                      options={OPTIONS_FLOW_SCORE_OPTIONS}
                      disabled={!overlayAvailability?.options_flow?.filterable}
                    />
                    <FilterSelect
                      name="options_flow_min_premium"
                      label="Minimum premium"
                      value={params.options_flow_min_premium}
                      options={OPTIONS_FLOW_PREMIUM_OPTIONS}
                      disabled={!overlayAvailability?.options_flow?.filterable}
                    />
                    <FilterSelect
                      name="options_flow_lookback_days"
                      label="Lookback"
                      value={params.options_flow_lookback_days}
                      options={OPTIONS_FLOW_LOOKBACK_OPTIONS}
                      disabled={!overlayAvailability?.options_flow?.filterable}
                    />
                  </div>
                  {!overlayAvailability?.options_flow?.filterable ? (
                    <p className="mt-3 text-xs leading-5 text-slate-500">Options flow data is not connected yet.</p>
                  ) : null}
                </div>

                <div className="rounded-2xl border border-slate-800 bg-slate-950/25 p-3">
                  <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-500">Institutional Activity</p>
                  <div className="mt-3 grid gap-3 md:grid-cols-3">
                    <FilterSelect
                      name="institutional_activity_active"
                      label="Institutional"
                      value={params.institutional_activity_active}
                      options={BOOLEAN_ACTIVITY_OPTIONS}
                      disabled={!overlayAvailability?.institutional_activity?.filterable}
                    />
                    <FilterSelect
                      name="institutional_activity_direction"
                      label="Direction"
                      value={params.institutional_activity_direction}
                      options={INSTITUTIONAL_DIRECTION_OPTIONS}
                      disabled={!overlayAvailability?.institutional_activity?.filterable}
                    />
                    <FilterSelect
                      name="institutional_activity_min_value"
                      label="Minimum value"
                      value={params.institutional_activity_min_value}
                      options={INSTITUTIONAL_VALUE_OPTIONS}
                      disabled={!overlayAvailability?.institutional_activity?.filterable}
                    />
                    <FilterSelect
                      name="institutional_activity_lookback_days"
                      label="Lookback"
                      value={params.institutional_activity_lookback_days}
                      options={INSTITUTIONAL_LOOKBACK_OPTIONS}
                      disabled={!overlayAvailability?.institutional_activity?.filterable}
                    />
                  </div>
                  {!overlayAvailability?.institutional_activity?.filterable ? (
                    <p className="mt-3 text-xs leading-5 text-slate-500">Institutional activity is staged for a future data provider.</p>
                  ) : null}
                </div>
              </div>
            ) : (
              <ScreenerUpgradeOverlay
                title="Intelligence screener filters"
                body="Congress activity, insider activity, confirmation, Why Now, and freshness filters are included with Premium."
                className="mt-3"
              >
                <div className="grid gap-3 opacity-70 blur-[1.5px] xl:grid-cols-3">
                  <div className="rounded-2xl border border-slate-800 bg-slate-950/25 p-3">
                    <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-500">Activity</p>
                    <div className="mt-3 grid gap-3 md:grid-cols-2">
                      <FilterSelect name="congress_activity_locked" label="Congress" value="" options={ACTIVITY_FILTER_OPTIONS} />
                      <FilterSelect name="insider_activity_locked" label="Insiders" value="" options={ACTIVITY_FILTER_OPTIONS} />
                    </div>
                  </div>

                  <div className="rounded-2xl border border-slate-800 bg-slate-950/25 p-3">
                    <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-500">Confirmation</p>
                    <div className="mt-3 grid gap-3 md:grid-cols-3">
                      <FilterSelect name="confirmation_score_locked" label="Score" value="" options={CONFIRMATION_SCORE_OPTIONS} />
                      <FilterSelect name="confirmation_direction_locked" label="Direction" value="" options={CONFIRMATION_DIRECTION_OPTIONS} />
                      <FilterSelect name="confirmation_band_locked" label="Band" value="" options={CONFIRMATION_BAND_OPTIONS} />
                    </div>
                  </div>

                  <div className="rounded-2xl border border-slate-800 bg-slate-950/25 p-3">
                    <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-500">Timing / Why Now</p>
                    <div className="mt-3 grid gap-3 md:grid-cols-2">
                      <FilterSelect name="why_now_locked" label="Why now" value="" options={WHY_NOW_OPTIONS} />
                      <FilterSelect name="freshness_locked" label="Freshness" value="" options={FRESHNESS_OPTIONS} />
                    </div>
                  </div>

                  <div className="rounded-2xl border border-slate-800 bg-slate-950/25 p-3">
                    <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-500">Government Contracts</p>
                    <div className="mt-3 grid gap-3 md:grid-cols-3">
                      <FilterSelect name="government_contracts_active_locked" label="Contracts" value="" options={GOVERNMENT_CONTRACT_BOOLEAN_OPTIONS} />
                      <FilterSelect name="government_contracts_min_amount_locked" label="Minimum contract value" value="" options={GOVERNMENT_CONTRACT_AMOUNT_OPTIONS} />
                      <FilterSelect name="government_contracts_lookback_locked" label="Lookback" value="" options={GOVERNMENT_CONTRACT_LOOKBACK_OPTIONS} />
                    </div>
                  </div>

                  <div className="rounded-2xl border border-slate-800 bg-slate-950/25 p-3">
                    <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-500">Options Flow</p>
                    <div className="mt-3 grid gap-3 md:grid-cols-3">
                      <FilterSelect name="options_flow_active_locked" label="Options flow" value="" options={BOOLEAN_ACTIVITY_OPTIONS} />
                      <FilterSelect name="options_flow_direction_locked" label="Direction" value="" options={OPTIONS_FLOW_DIRECTION_OPTIONS} />
                      <FilterSelect name="options_flow_score_locked" label="Minimum score" value="" options={OPTIONS_FLOW_SCORE_OPTIONS} />
                      <FilterSelect name="options_flow_premium_locked" label="Minimum premium" value="" options={OPTIONS_FLOW_PREMIUM_OPTIONS} />
                      <FilterSelect name="options_flow_lookback_locked" label="Lookback" value="" options={OPTIONS_FLOW_LOOKBACK_OPTIONS} />
                    </div>
                  </div>

                  <div className="rounded-2xl border border-slate-800 bg-slate-950/25 p-3">
                    <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-500">Institutional Activity</p>
                    <div className="mt-3 grid gap-3 md:grid-cols-3">
                      <FilterSelect name="institutional_activity_active_locked" label="Institutional" value="" options={BOOLEAN_ACTIVITY_OPTIONS} />
                      <FilterSelect name="institutional_activity_direction_locked" label="Direction" value="" options={INSTITUTIONAL_DIRECTION_OPTIONS} />
                      <FilterSelect name="institutional_activity_value_locked" label="Minimum value" value="" options={INSTITUTIONAL_VALUE_OPTIONS} />
                      <FilterSelect name="institutional_activity_lookback_locked" label="Lookback" value="" options={INSTITUTIONAL_LOOKBACK_OPTIONS} />
                    </div>
                  </div>
                </div>
              </ScreenerUpgradeOverlay>
            )}
          </div>

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
        <ScreenerResults
          data={screenerPayload.data}
          errorMessage={screenerPayload.errorMessage}
          params={params}
          page={page}
          pageSize={pageSize}
          intelligenceLocked={!canUseIntelligence}
          resultCap={resultCap}
        />
      ) : null}
    </div>
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
}: {
  data: ScreenerResponse | null;
  errorMessage: string | null;
  params: Record<string, string | number>;
  page: number;
  pageSize: number;
  intelligenceLocked: boolean;
  resultCap: number;
}) {
  const rows = data?.items ?? [];
  const totalAvailable = data?.total_available ?? 0;
  const hasNext = data?.has_next ?? false;
  const governmentContractsAvailabilityStatus = data?.overlay_availability?.government_contracts?.status ?? "ok";

  return (
    <div className={`${cardClassName} min-h-[34rem] overflow-hidden p-0`}>
      <div className="flex flex-wrap items-center justify-between gap-3 border-b border-slate-800 bg-slate-950/50 px-4 py-3">
        <div>
          <h2 className="text-lg font-semibold text-white">Results</h2>
          <p className="mt-1 text-sm text-slate-400">
            {errorMessage ? "FMP screener unavailable" : `${rows.length} shown from ${totalAvailable} available results`}
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

      <div className="overflow-x-auto overflow-y-hidden">
        <table className="min-w-full border-collapse text-sm">
          <thead className="bg-slate-950/50 text-xs uppercase tracking-wider text-slate-400">
            <tr>
              <SortHeader params={params} sort="symbol" label="Symbol" />
              <th className="px-3 py-2.5 text-left">Company</th>
              <th className="px-3 py-2.5 text-left">Sector</th>
              <SortHeader params={params} sort="market_cap" label="Market cap" />
              <SortHeader params={params} sort="price" label="Price" />
              <SortHeader params={params} sort="volume" label="Volume" />
              <SortHeader params={params} sort="beta" label="Beta" />
              <SortHeader params={params} sort="congress_activity" label="Congress" locked={intelligenceLocked} />
              <SortHeader params={params} sort="insider_activity" label="Insiders" locked={intelligenceLocked} />
              <th className="px-3 py-2.5 text-left">Institutional</th>
              <th className="px-3 py-2.5 text-left">Options Flow</th>
              <th className="px-3 py-2.5 text-left">Gov Contracts</th>
              <SortHeader params={params} sort="confirmation_score" label="Confirm" locked={intelligenceLocked} />
              <th className="px-3 py-2.5 text-left">
                <span className="inline-flex items-center gap-2">
                  Why Now
                  {intelligenceLocked ? (
                    <span className="rounded-full border border-amber-300/30 bg-amber-300/10 px-2 py-0.5 text-[10px] font-semibold tracking-[0.16em] text-amber-100">
                      Premium
                    </span>
                  ) : null}
                </span>
              </th>
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-800">
            {errorMessage ? (
              <tr>
                <td className="px-4 py-12 text-center text-slate-400" colSpan={14}>
                  {errorMessage}
                </td>
              </tr>
            ) : rows.length === 0 ? (
              <tr>
                <td className="px-4 py-12 text-center text-slate-400" colSpan={14}>
                  No names matched this screen. Widen the market cap, liquidity, or sector filters.
                </td>
              </tr>
            ) : (
              rows.map((row) => (
                <ScreenerTableRow
                  key={row.symbol}
                  row={row}
                  intelligenceLocked={intelligenceLocked}
                  governmentContractsAvailabilityStatus={governmentContractsAvailabilityStatus}
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
          Source: FMP company screener + Capitol Ledger overlays
          {data?.overlay_availability?.options_flow?.filterable === false ? " · options flow unavailable" : ""}
          {data?.overlay_availability?.institutional_activity?.filterable === false ? " · institutional not configured" : ""}
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
  intelligenceLocked,
  availabilityStatus,
}: {
  row: ScreenerRow;
  intelligenceLocked?: boolean;
  availabilityStatus?: string;
}) {
  if (intelligenceLocked) return lockedMetricLine("Locked intelligence");
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
  intelligenceLocked,
  availabilityStatus,
}: {
  row: ScreenerRow;
  intelligenceLocked?: boolean;
  availabilityStatus?: string;
}) {
  if (intelligenceLocked) return lockedMetricLine("Locked intelligence");
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

function OptionsFlowCell({ row, intelligenceLocked }: { row: ScreenerRow; intelligenceLocked?: boolean }) {
  if (intelligenceLocked) return lockedMetricLine("Locked intelligence");
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

function InstitutionalActivityCell({ row, intelligenceLocked }: { row: ScreenerRow; intelligenceLocked?: boolean }) {
  if (intelligenceLocked) return lockedMetricLine("Locked intelligence");
  if (!row.institutional_activity_active || row.institutional_activity_status !== "ok") return <span className="text-sm text-slate-500">—</span>;
  return (
    <div className="min-w-[10rem]">
      <div className="text-sm font-semibold text-slate-100">
        {formatCurrencyCompact(row.institutional_activity_net_activity)} net · {titleCase(row.institutional_activity_direction ?? "neutral")}
      </div>
      <div className="mt-0.5 truncate text-[11px] leading-4 text-slate-500">
        {row.institutional_activity_institution_count ?? 0} institution{row.institutional_activity_institution_count === 1 ? "" : "s"}
      </div>
    </div>
  );
}

function ScreenerTableRow({
  row,
  intelligenceLocked = false,
  governmentContractsAvailabilityStatus = "ok",
}: {
  row: ScreenerRow;
  intelligenceLocked?: boolean;
  governmentContractsAvailabilityStatus?: string;
}) {
  const href = tickerHref(row.symbol) ?? row.ticker_url ?? `/ticker/${encodeURIComponent(row.symbol)}`;
  const confirmationDirection = confirmationDirectionLabel(row.confirmation.direction);
  const confirmationSourceMeta = confirmationMeta(row.confirmation.status, row.confirmation.direction);
  return (
    <ClickableScreenerRow href={href} label={`Open ${row.symbol} ticker page`}>
      <td className={`${tableCellClassName} whitespace-nowrap`}>
        <div className="flex items-center gap-2">
          <Link href={href} prefetch={false} className={`${tickerMonoLinkClassName} transition group-hover:text-emerald-100`}>
            {row.symbol}
          </Link>
          <AddTickerToWatchlist symbol={row.symbol} variant="compact" align="left" />
        </div>
      </td>
      <td className={`${tableCellClassName} min-w-[14rem]`}>
        <Link
          href={href}
          prefetch={false}
          className="block max-w-[18rem] truncate font-medium text-slate-100 transition hover:text-white hover:underline group-hover:text-white"
          title={row.company_name}
        >
          {row.company_name}
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
      <td className={`${tableCellClassName} whitespace-nowrap`} title={row.congress_activity.label}>
        {intelligenceLocked ? (
          lockedMetricLine("Locked intelligence")
        ) : (
          <>
            <div className={`text-xs font-semibold ${activityTextClass(row.congress_activity)}`}>
              {row.congress_activity.present ? "Active" : "None"}
            </div>
            <div className="mt-0.5 text-[11px] leading-4 text-slate-500">{activityMeta(row.congress_activity)}</div>
          </>
        )}
      </td>
      <td className={`${tableCellClassName} whitespace-nowrap`} title={row.insider_activity.label}>
        {intelligenceLocked ? (
          lockedMetricLine("Locked intelligence")
        ) : (
          <>
            <div className={`text-xs font-semibold ${activityTextClass(row.insider_activity)}`}>
              {row.insider_activity.present ? "Active" : "None"}
            </div>
            <div className="mt-0.5 text-[11px] leading-4 text-slate-500">{activityMeta(row.insider_activity)}</div>
          </>
        )}
      </td>
      <td className={`${tableCellClassName} min-w-[10rem]`}><InstitutionalActivityCell row={row} intelligenceLocked={intelligenceLocked} /></td>
      <td className={`${tableCellClassName} min-w-[10rem]`}><OptionsFlowCell row={row} intelligenceLocked={intelligenceLocked} /></td>
      <td className={`${tableCellClassName} min-w-[11rem]`}>
        <GovernmentContractsMetricCell
          row={row}
          intelligenceLocked={intelligenceLocked}
          availabilityStatus={governmentContractsAvailabilityStatus}
        />
      </td>
      <td className={`${tableCellClassName} min-w-[8.5rem] whitespace-nowrap`} title={row.confirmation.status}>
        {intelligenceLocked ? (
          <div className="space-y-1">
            <div className="flex items-center gap-2">
              <div className="h-4 w-12 rounded-full bg-slate-800/90" />
              <span className="rounded-full border border-amber-300/30 bg-amber-300/10 px-2 py-0.5 text-[10px] font-semibold uppercase tracking-[0.16em] text-amber-100">
                Premium
              </span>
            </div>
            <div className="text-[11px] leading-4 text-slate-500">Confirmation score, band, and direction are locked.</div>
          </div>
        ) : (
          <>
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
          </>
        )}
      </td>
      <td className={`${tableCellClassName} min-w-[8rem] max-w-[10rem]`}>
        <WhyNowHover row={row} locked={intelligenceLocked} />
        <div className="mt-1 text-[11px] leading-4 text-slate-500">
          {intelligenceLocked ? "Premium freshness" : freshnessStateLabel(row.signal_freshness.freshness_state)}
        </div>
      </td>
    </ClickableScreenerRow>
  );
}
