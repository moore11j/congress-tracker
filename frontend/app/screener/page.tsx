import Link from "next/link";
import { Suspense } from "react";
import { AddTickerToWatchlist } from "@/components/watchlists/AddTickerToWatchlist";
import { SavedViewsBar } from "@/components/saved-views/SavedViewsBar";
import { SkeletonBlock, SkeletonTable } from "@/components/ui/LoadingSkeleton";
import { API_BASE } from "@/lib/api";
import { cardClassName, inputClassName, selectClassName, tickerMonoLinkClassName } from "@/lib/styles";
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
  const parsed = Number(raw);
  if (!Number.isFinite(parsed) || parsed <= 0) return fallback;
  return Math.min(Math.floor(parsed), max);
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
    const trimmed = String(value).trim();
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

function confirmationClass(band: string): string {
  if (band === "exceptional") return "border-emerald-300/40 bg-emerald-400/10 text-emerald-100";
  if (band === "strong") return "border-cyan-300/35 bg-cyan-400/10 text-cyan-100";
  if (band === "moderate") return "border-amber-300/35 bg-amber-400/10 text-amber-100";
  if (band === "weak") return "border-slate-600 bg-slate-900/50 text-slate-200";
  return "border-slate-800 bg-slate-950/40 text-slate-500";
}

function activityClass(activity: ActivityOverlay): string {
  if (!activity.present) return "border-slate-800 bg-slate-950/30 text-slate-500";
  if (activity.direction === "bearish") return "border-rose-300/30 bg-rose-400/10 text-rose-100";
  if (activity.direction === "bullish") return "border-emerald-300/30 bg-emerald-400/10 text-emerald-100";
  return "border-cyan-300/25 bg-cyan-400/10 text-cyan-100";
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

function FilterInput({ name, label, value, placeholder }: { name: string; label: string; value?: string | number; placeholder?: string }) {
  return (
    <label className="grid gap-1 text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-500">
      {label}
      <input name={name} defaultValue={value ?? ""} placeholder={placeholder} className={`${inputClassName} h-9 rounded-lg px-2.5 py-1.5`} />
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
    <label className="grid gap-1 text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-500">
      {label}
      <select name={name} defaultValue={String(value ?? "")} className={`${selectClassName} h-9 rounded-lg px-2.5 py-1.5`}>
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
    <div className="space-y-6">
      <div className="flex flex-wrap items-end justify-between gap-4">
        <div>
          <div className="text-xs tracking-[0.25em] text-emerald-300/70">IDEA SCREENER</div>
          <h1 className="mt-2 text-3xl font-semibold text-white">Stock Screener</h1>
          <p className="mt-2 max-w-3xl text-sm text-slate-300/80">
            FMP fundamentals filtered through Capitol Ledger activity, confirmation, and Why Now overlays.
          </p>
        </div>
        <div className="flex flex-wrap items-center gap-2 text-xs text-slate-400">
          <Link href={pageHref(params, { sector: "Technology", volume_min: 1000000, sort: "confirmation_score", page: 1 })} className="rounded-lg border border-slate-800 bg-slate-950/50 px-3 py-2 text-slate-200 hover:border-emerald-300/30 hover:text-emerald-100" prefetch={false}>
            Liquid confirmation
          </Link>
          <Link href={pageHref(params, { market_cap_min: 10000000000, sort: "congress_activity", page: 1 })} className="rounded-lg border border-slate-800 bg-slate-950/50 px-3 py-2 text-slate-200 hover:border-emerald-300/30 hover:text-emerald-100" prefetch={false}>
            Large caps with activity
          </Link>
          <Link href={pageHref(params, { sort: "insider_activity", page: 1 })} className="rounded-lg border border-slate-800 bg-slate-950/50 px-3 py-2 text-slate-200 hover:border-emerald-300/30 hover:text-emerald-100" prefetch={false}>
            Insider activity
          </Link>
        </div>
      </div>

      <div className={`${cardClassName} rounded-lg p-4`}>
        <form action="/screener" className="space-y-4">
          <div className="grid gap-3 md:grid-cols-4 xl:grid-cols-8">
            <FilterInput name="market_cap_min" label="Mkt cap min" value={params.market_cap_min} placeholder="10000000000" />
            <FilterInput name="market_cap_max" label="Mkt cap max" value={params.market_cap_max} />
            <FilterInput name="price_min" label="Price min" value={params.price_min} placeholder="10" />
            <FilterInput name="price_max" label="Price max" value={params.price_max} />
            <FilterInput name="volume_min" label="Vol min" value={params.volume_min} placeholder="1000000" />
            <FilterInput name="beta_min" label="Beta min" value={params.beta_min} />
            <FilterInput name="beta_max" label="Beta max" value={params.beta_max} />
            <FilterInput name="dividend_yield_min" label="Div min" value={params.dividend_yield_min} />
          </div>

          <div className="grid gap-3 md:grid-cols-4 xl:grid-cols-8">
            <FilterSelect name="sector" label="Sector" value={params.sector} options={SECTORS} />
            <FilterInput name="industry" label="Industry" value={params.industry} placeholder="Semiconductors" />
            <FilterSelect name="country" label="Country" value={params.country} options={COUNTRIES} />
            <FilterSelect name="exchange" label="Exchange" value={params.exchange} options={EXCHANGES} />
            <FilterSelect name="lookback_days" label="Overlay" value={params.lookback_days} options={[["30", "30d"], ["60", "60d"], ["90", "90d"]]} />
            <FilterSelect name="sort" label="Sort" value={params.sort} options={SORTS} />
            <FilterSelect name="sort_dir" label="Dir" value={params.sort_dir} options={[["desc", "Desc"], ["asc", "Asc"]]} allLabel="Default" />
            <FilterSelect name="page_size" label="Rows" value={params.page_size} options={[["25", "25"], ["50", "50"], ["100", "100"]]} allLabel="50" />
          </div>

          <div className="flex flex-wrap items-center justify-between gap-3 border-t border-slate-800 pt-3">
            <div className="flex flex-wrap items-center gap-2">
              <button type="submit" className="rounded-lg border border-emerald-300/40 bg-emerald-300/15 px-4 py-2 text-sm font-semibold text-emerald-100 hover:bg-emerald-300/20">
                Run screen
              </button>
              <Link href="/screener" className="rounded-lg border border-slate-800 bg-slate-950/40 px-4 py-2 text-sm font-semibold text-slate-300 hover:border-slate-700 hover:text-white" prefetch={false}>
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
              <span className="rounded-full border border-slate-800 bg-slate-950/30 px-3 py-1 text-xs text-slate-300">
                sort <span className="text-white">{sort}</span>
              </span>
              <span className="rounded-full border border-slate-800 bg-slate-950/30 px-3 py-1 text-xs text-slate-300">
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
    <div className={`${cardClassName} min-h-[34rem] overflow-hidden rounded-lg p-4`} aria-live="polite" aria-busy="true">
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
    <div className={`${cardClassName} min-h-[34rem] overflow-hidden rounded-lg p-0`}>
      <div className="flex flex-wrap items-center justify-between gap-3 border-b border-slate-800 bg-slate-950/50 px-4 py-3">
        <div>
          <h2 className="text-sm font-semibold uppercase tracking-[0.18em] text-slate-300">Results</h2>
          <p className="mt-1 text-xs text-slate-500">
            {errorMessage ? "FMP screener unavailable" : `${rows.length} shown from ${totalAvailable} fetched candidates`}
          </p>
        </div>
        <div className="flex flex-wrap items-center gap-2 text-xs">
          <Link href={pageHref(params, { page: Math.max(page - 1, 1) })} className={`rounded-lg border px-3 py-2 ${page <= 1 ? "pointer-events-none border-slate-900 text-slate-600" : "border-slate-800 text-slate-200 hover:border-slate-700"}`} prefetch={false}>
            Prev
          </Link>
          <span className="rounded-lg border border-slate-800 bg-slate-950/40 px-3 py-2 text-slate-300">
            Page {page}
          </span>
          <Link href={pageHref(params, { page: page + 1 })} className={`rounded-lg border px-3 py-2 ${!hasNext ? "pointer-events-none border-slate-900 text-slate-600" : "border-slate-800 text-slate-200 hover:border-slate-700"}`} prefetch={false}>
            Next
          </Link>
        </div>
      </div>

      <div className="overflow-x-auto">
        <table className="min-w-full border-collapse text-sm">
          <thead className="bg-slate-950/70 text-[11px] uppercase tracking-[0.14em] text-slate-500">
            <tr>
              <SortHeader params={params} sort="symbol" label="Symbol" />
              <th className="px-3 py-3 text-left">Company</th>
              <th className="px-3 py-3 text-left">Sector</th>
              <SortHeader params={params} sort="market_cap" label="Market cap" />
              <SortHeader params={params} sort="price" label="Price" />
              <SortHeader params={params} sort="volume" label="Volume" />
              <SortHeader params={params} sort="beta" label="Beta" />
              <SortHeader params={params} sort="congress_activity" label="Congress" />
              <SortHeader params={params} sort="insider_activity" label="Insiders" />
              <SortHeader params={params} sort="confirmation_score" label="Confirm" />
              <th className="px-3 py-3 text-left">Why Now</th>
              <th className="px-3 py-3 text-right">List</th>
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
    <th className={`px-3 py-3 text-left ${active ? "bg-emerald-400/[0.05] text-emerald-100" : ""}`}>
      <Link href={pageHref(params, { sort, sort_dir: nextDir, page: 1 })} className="inline-flex items-center gap-1 hover:text-white" prefetch={false}>
        {label}
        <span className="text-slate-600">{active ? (params.sort_dir === "asc" ? "up" : "down") : ""}</span>
      </Link>
    </th>
  );
}

function ScreenerTableRow({ row }: { row: ScreenerRow }) {
  const href = tickerHref(row.symbol) ?? row.ticker_url ?? `/ticker/${encodeURIComponent(row.symbol)}`;
  return (
    <tr className="group hover:bg-slate-900/25">
      <td className="whitespace-nowrap px-3 py-3 align-top">
        <Link href={href} prefetch={false} className={tickerMonoLinkClassName}>
          {row.symbol}
        </Link>
      </td>
      <td className="min-w-[14rem] px-3 py-3 align-top">
        <Link href={href} prefetch={false} className="block max-w-[18rem] truncate font-medium text-slate-100 hover:text-white hover:underline" title={row.company_name}>
          {row.company_name}
        </Link>
        <div className="mt-0.5 text-xs text-slate-500">
          {[row.exchange, row.country].filter(Boolean).join(" / ") || "--"}
        </div>
      </td>
      <td className="min-w-[12rem] px-3 py-3 align-top text-slate-300">
        <div className="max-w-[12rem] truncate">{row.sector ?? "--"}</div>
        {row.industry ? <div className="mt-0.5 max-w-[12rem] truncate text-xs text-slate-500">{row.industry}</div> : null}
      </td>
      <td className="whitespace-nowrap px-3 py-3 align-top font-mono text-slate-200">{formatCompact(row.market_cap)}</td>
      <td className="whitespace-nowrap px-3 py-3 align-top font-mono text-slate-200">{formatCurrency(row.price)}</td>
      <td className="whitespace-nowrap px-3 py-3 align-top font-mono text-slate-200">{formatCompact(row.volume)}</td>
      <td className="whitespace-nowrap px-3 py-3 align-top font-mono text-slate-200">{formatBeta(row.beta)}</td>
      <td className="whitespace-nowrap px-3 py-3 align-top">
        <span className={`inline-flex rounded-full border px-2 py-1 text-[11px] font-semibold ${activityClass(row.congress_activity)}`} title={row.congress_activity.label}>
          {row.congress_activity.present ? "Active" : "None"}
        </span>
      </td>
      <td className="whitespace-nowrap px-3 py-3 align-top">
        <span className={`inline-flex rounded-full border px-2 py-1 text-[11px] font-semibold ${activityClass(row.insider_activity)}`} title={row.insider_activity.label}>
          {row.insider_activity.present ? "Active" : "None"}
        </span>
      </td>
      <td className="whitespace-nowrap px-3 py-3 align-top">
        <span className={`inline-flex items-center gap-2 rounded-full border px-2 py-1 text-[11px] font-semibold ${confirmationClass(row.confirmation.band)}`} title={row.confirmation.status}>
          <span className="font-mono">{row.confirmation.score}</span>
          <span>{titleCase(row.confirmation.band)}</span>
        </span>
      </td>
      <td className="min-w-[22rem] max-w-[28rem] px-3 py-3 align-top">
        <div className="flex min-w-0 items-start gap-2" title={row.why_now.headline}>
          <span className={`mt-0.5 shrink-0 rounded-full border px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wide ${whyNowClass(row.why_now.state, row.confirmation.direction)}`}>
            {titleCase(row.why_now.state)}
          </span>
          <span className="line-clamp-2 text-xs leading-5 text-slate-300">{row.why_now.headline}</span>
        </div>
      </td>
      <td className="whitespace-nowrap px-3 py-3 text-right align-top">
        <AddTickerToWatchlist symbol={row.symbol} variant="compact" align="right" />
      </td>
    </tr>
  );
}
