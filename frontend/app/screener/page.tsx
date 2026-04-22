import Link from "next/link";
import { Suspense } from "react";
import { ClickableScreenerRow } from "@/components/screener/ClickableScreenerRow";
import { AddTickerToWatchlist } from "@/components/watchlists/AddTickerToWatchlist";
import { SavedViewsBar } from "@/components/saved-views/SavedViewsBar";
import { SkeletonBlock, SkeletonTable } from "@/components/ui/LoadingSkeleton";
import { API_BASE } from "@/lib/api";
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
    score: number;
    band: "inactive" | "weak" | "moderate" | "strong" | "exceptional" | string;
    direction: "bullish" | "bearish" | "neutral" | "mixed" | string;
    status: string;
  };
  why_now: {
    state: "early" | "strengthening" | "strong" | "mixed" | "fading" | "inactive" | string;
    headline: string;
  };
  ticker_url?: string;
};

type ActivityOverlay = {
  present: boolean;
  label: string;
  direction?: string | null;
  freshness_days?: number | null;
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
  filters: Record<string, string | number>;
  lookback_days: number;
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
  "sector",
  "industry",
  "country",
  "exchange",
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
  ["market_cap", "Market cap"],
  ["price", "Price"],
  ["volume", "Volume"],
  ["beta", "Beta"],
  ["congress_activity", "Congress"],
  ["insider_activity", "Insiders"],
  ["symbol", "Symbol"],
] as const;

const filterLabelClassName = "grid gap-1 text-xs font-semibold uppercase tracking-wide text-slate-400";
const segmentShellClassName = "flex flex-wrap items-center gap-2 rounded-2xl border border-slate-800 bg-slate-950/30 p-1";
const segmentLinkClassName =
  "inline-flex items-center justify-center rounded-full border border-slate-800 bg-slate-950/30 px-3 py-1 text-xs font-medium text-slate-200 transition hover:bg-slate-900/60 hover:text-white";
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
]);

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

  const params: Record<string, string | number> = {
    sort,
    sort_dir: sortDir,
    page,
    page_size: pageSize,
    lookback_days: lookbackDays,
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

function formatBeta(value?: number | null): string {
  if (typeof value !== "number" || !Number.isFinite(value)) return "--";
  return value.toFixed(2);
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
}: {
  name: string;
  label: string;
  value?: string | number;
  options: readonly string[] | readonly (readonly [string, string])[];
  allLabel?: string;
}) {
  return (
    <label className={filterLabelClassName}>
      {label}
      <select
        name={name}
        defaultValue={String(value ?? "")}
        className={value ? `${selectClassName} border-emerald-500/40 bg-slate-950/40` : selectClassName}
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

export default async function ScreenerPage({
  searchParams,
}: {
  searchParams?: Promise<SearchParams>;
}) {
  const sp = (await searchParams) ?? {};
  const params = currentParams(sp);
  const requestUrl = buildApiUrl(params);
  const sort = String(params.sort ?? "relevance");
  const sortDir = String(params.sort_dir ?? "desc");
  const page = Number(params.page ?? 1);
  const pageSize = Number(params.page_size ?? 50);

  return (
    <div className="space-y-8">
      <div className="flex flex-wrap items-end justify-between gap-4">
        <div>
          <p className="text-xs font-semibold uppercase tracking-[0.3em] text-emerald-300">Idea Screener</p>
          <h1 className="mt-2 text-3xl font-semibold text-white">Stock Screener</h1>
          <p className="mt-2 max-w-2xl text-sm text-slate-400">
            FMP fundamentals filtered through Capitol Ledger activity, confirmation, and Why Now overlays.
          </p>
        </div>
        <div className={segmentShellClassName}>
          <Link href={pageHref(params, { sector: "Technology", volume_min: "1,000,000", sort: "confirmation_score", page: 1 })} className={segmentLinkClassName} prefetch={false}>
            Liquid confirmation
          </Link>
          <Link href={pageHref(params, { market_cap_min: "10,000,000,000", sort: "congress_activity", page: 1 })} className={segmentLinkClassName} prefetch={false}>
            Large caps with activity
          </Link>
          <Link href={pageHref(params, { sort: "insider_activity", page: 1 })} className={segmentLinkClassName} prefetch={false}>
            Insider activity
          </Link>
        </div>
      </div>

      <div className={`${cardClassName} space-y-4`}>
        <form action="/screener" className="space-y-4">
          <div className="grid gap-3 md:grid-cols-4 xl:grid-cols-8">
            <FilterInput name="market_cap_min" label="Mkt cap min" value={params.market_cap_min} placeholder="10,000,000,000" />
            <FilterInput name="market_cap_max" label="Mkt cap max" value={params.market_cap_max} />
            <FilterInput name="price_min" label="Price min" value={params.price_min} placeholder="10" />
            <FilterInput name="price_max" label="Price max" value={params.price_max} />
            <FilterInput name="volume_min" label="Vol min" value={params.volume_min} placeholder="1,000,000" />
            <FilterInput name="beta_min" label="Beta min" value={params.beta_min} />
            <FilterInput name="beta_max" label="Beta max" value={params.beta_max} />
            <FilterInput name="dividend_yield_min" label="Div min" value={params.dividend_yield_min} />
          </div>

          <div className="grid gap-3 md:grid-cols-4 xl:grid-cols-8">
            <FilterSelect name="sector" label="Sector" value={params.sector} options={SECTORS} />
            <FilterSelect name="industry" label="Industry" value={params.industry} options={INDUSTRIES} />
            <FilterSelect name="country" label="Country" value={params.country} options={COUNTRIES} />
            <FilterSelect name="exchange" label="Exchange" value={params.exchange} options={EXCHANGES} />
            <FilterSelect name="lookback_days" label="Overlay" value={params.lookback_days} options={[["30", "30d"], ["60", "60d"], ["90", "90d"]]} />
            <FilterSelect name="sort" label="Sort" value={params.sort} options={SORTS} />
            <FilterSelect name="sort_dir" label="Direction" value={params.sort_dir} options={[["desc", "High to Low"], ["asc", "Low to High"]]} allLabel="Default" />
            <FilterSelect name="page_size" label="Rows" value={params.page_size} options={[["25", "25"], ["50", "50"], ["100", "100"]]} allLabel="50" />
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

        <SavedViewsBar
          surface="screener"
          paramKeys={PARAM_KEYS}
          defaultParams={{
            sort,
            sort_dir: sortDir,
            page_size: String(pageSize),
            lookback_days: String(params.lookback_days ?? 30),
          }}
          rightSlot={
            <div className="flex flex-wrap items-center gap-2">
              <span className={compactBadgeClassName + " border-slate-800 bg-slate-950/30 text-slate-300"}>
                sort <span className="text-white">{sort}</span>
              </span>
              <span className={compactBadgeClassName + " border-slate-800 bg-slate-950/30 text-slate-300"}>
                overlay <span className="text-white">{params.lookback_days ?? 30}d</span>
              </span>
            </div>
          }
        />
      </div>

      <Suspense key={requestUrl} fallback={<ScreenerResultsFallback />}>
        <ScreenerResults requestUrl={requestUrl} params={params} page={page} pageSize={pageSize} />
      </Suspense>
    </div>
  );
}

function ScreenerResultsFallback() {
  return (
    <div className={`${cardClassName} min-h-[34rem] overflow-hidden`} aria-live="polite" aria-busy="true">
      <div className="mb-4 flex items-center justify-between">
        <SkeletonBlock className="h-4 w-48" />
        <SkeletonBlock className="h-4 w-28" />
      </div>
      <SkeletonTable columns={11} rows={9} />
    </div>
  );
}

async function ScreenerResults({
  requestUrl,
  params,
  page,
  pageSize,
}: {
  requestUrl: string;
  params: Record<string, string | number>;
  page: number;
  pageSize: number;
}) {
  let data: ScreenerResponse | null = null;
  let errorMessage: string | null = null;

  try {
    const response = await fetch(requestUrl, { cache: "no-store", next: { revalidate: 0 } });
    if (!response.ok) {
      const body = await response.json().catch(() => null);
      errorMessage = typeof body?.detail === "string" ? body.detail : `Screener request failed with ${response.status}.`;
    } else {
      data = (await response.json()) as ScreenerResponse;
    }
  } catch (error) {
    errorMessage = error instanceof Error ? error.message : "Unable to load screener.";
  }

  const rows = data?.items ?? [];
  const totalAvailable = data?.total_available ?? 0;
  const hasNext = data?.has_next ?? false;

  return (
    <div className={`${cardClassName} min-h-[34rem] overflow-hidden p-0`}>
      <div className="flex flex-wrap items-center justify-between gap-3 border-b border-slate-800 bg-slate-950/50 px-4 py-3">
        <div>
          <h2 className="text-lg font-semibold text-white">Results</h2>
          <p className="mt-1 text-sm text-slate-400">
            {errorMessage ? "FMP screener unavailable" : `${rows.length} shown from ${totalAvailable} fetched candidates`}
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
          <Link
            href={pageHref(params, { page: page + 1 })}
            className={`${ghostButtonClassName} rounded-lg px-3 py-2 text-xs ${!hasNext ? "pointer-events-none opacity-40" : ""}`}
            prefetch={false}
          >
            Next
          </Link>
        </div>
      </div>

      <div className="overflow-x-auto">
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
              <SortHeader params={params} sort="congress_activity" label="Congress" />
              <SortHeader params={params} sort="insider_activity" label="Insiders" />
              <SortHeader params={params} sort="confirmation_score" label="Confirm" />
              <th className="px-3 py-2.5 text-left">Why Now</th>
              <th className="w-12 px-3 py-2.5 text-right" aria-label="Watchlist actions">
                List
              </th>
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-800">
            {errorMessage ? (
              <tr>
                <td className="px-4 py-12 text-center text-slate-400" colSpan={12}>
                  {errorMessage}
                </td>
              </tr>
            ) : rows.length === 0 ? (
              <tr>
                <td className="px-4 py-12 text-center text-slate-400" colSpan={12}>
                  No names matched this screen. Widen the market cap, liquidity, or sector filters.
                </td>
              </tr>
            ) : (
              rows.map((row) => <ScreenerTableRow key={row.symbol} row={row} />)
            )}
          </tbody>
        </table>
      </div>

      <div className="flex flex-wrap items-center justify-between gap-3 border-t border-slate-800 bg-slate-950/40 px-4 py-3 text-xs text-slate-500">
        <span>
          Page size {pageSize}. Confirmation overlays use a {data?.lookback_days ?? params.lookback_days ?? 30}d lookback.
        </span>
        <span>Source: FMP company screener + Capitol Ledger overlays</span>
      </div>
    </div>
  );
}

function SortHeader({ params, sort, label }: { params: Record<string, string | number>; sort: string; label: string }) {
  const active = params.sort === sort;
  const nextDir = active && params.sort_dir === "desc" ? "asc" : "desc";
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

function WhyNowHover({ row }: { row: ScreenerRow }) {
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
      </div>
    </div>
  );
}

function ScreenerTableRow({ row }: { row: ScreenerRow }) {
  const href = tickerHref(row.symbol) ?? row.ticker_url ?? `/ticker/${encodeURIComponent(row.symbol)}`;
  const confirmationDirection = confirmationDirectionLabel(row.confirmation.direction);
  const confirmationSourceMeta = confirmationMeta(row.confirmation.status, row.confirmation.direction);
  return (
    <ClickableScreenerRow href={href} label={`Open ${row.symbol} ticker page`}>
      <td className={`${tableCellClassName} whitespace-nowrap`}>
        <Link href={href} prefetch={false} className={`${tickerMonoLinkClassName} transition group-hover:text-emerald-100`}>
          {row.symbol}
        </Link>
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
        <div className={`text-xs font-semibold ${activityTextClass(row.congress_activity)}`}>
          {row.congress_activity.present ? "Active" : "None"}
        </div>
        <div className="mt-0.5 text-[11px] leading-4 text-slate-500">{activityMeta(row.congress_activity)}</div>
      </td>
      <td className={`${tableCellClassName} whitespace-nowrap`} title={row.insider_activity.label}>
        <div className={`text-xs font-semibold ${activityTextClass(row.insider_activity)}`}>
          {row.insider_activity.present ? "Active" : "None"}
        </div>
        <div className="mt-0.5 text-[11px] leading-4 text-slate-500">{activityMeta(row.insider_activity)}</div>
      </td>
      <td className={`${tableCellClassName} min-w-[8.5rem] whitespace-nowrap`} title={row.confirmation.status}>
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
      </td>
      <td className={`${tableCellClassName} min-w-[8rem] max-w-[10rem]`}>
        <WhyNowHover row={row} />
      </td>
      <td className={`${tableCellClassName} whitespace-nowrap text-right`} data-row-action="true">
        <AddTickerToWatchlist symbol={row.symbol} variant="compact" align="right" />
      </td>
    </ClickableScreenerRow>
  );
}
