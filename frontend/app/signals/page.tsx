import Link from "next/link";
import { Badge } from "@/components/Badge";
import { SkeletonBlock, SkeletonTable } from "@/components/ui/LoadingSkeleton";
import { chamberBadge } from "@/lib/format";
import { getInsiderDisplayName, insiderHref } from "@/lib/insider";
import { memberHref } from "@/lib/memberSlug";
import { insiderRoleBadgeTone, normalizeInsiderRoleBadge, resolveInsiderDisplayName } from "@/lib/insiderRole";
import { tickerHref } from "@/lib/ticker";
import { tickerMonoLinkClassName } from "@/lib/styles";
import { SavedViewsBar } from "@/components/saved-views/SavedViewsBar";
import { AddTickerToWatchlist } from "@/components/watchlists/AddTickerToWatchlist";
import { Suspense } from "react";
import { buildReturnTo, requirePageAuth } from "@/lib/serverAuth";

type SearchParams = Record<string, string | string[] | undefined>;

type SignalItem = {
  kind?: "congress" | "insider" | string;
  event_id: number;
  ts: string;
  symbol: string;
  who?: string;
  position?: string;
  reporting_cik?: string | null;
  reportingCik?: string | null;
  member_bioguide_id?: string;
  party?: string;
  chamber?: string;
  trade_type?: string;
  amount_min?: number;
  amount_max?: number;
  baseline_median_amount_max?: number;
  baseline_count?: number;
  unusual_multiple?: number;
  smart_score?: number;
  smart_band?: string;
  source?: string;
  confirmation_30d?: {
    congress_active_30d: boolean;
    insider_active_30d: boolean;
    congress_trade_count_30d: number;
    insider_trade_count_30d: number;
    insider_buy_count_30d: number;
    insider_sell_count_30d: number;
    cross_source_confirmed_30d: boolean;
    repeat_congress_30d: boolean;
    repeat_insider_30d: boolean;
  } | null;
  confirmation_score?: number | null;
  confirmation_band?: ConfirmationBand | null;
  confirmation_direction?: ConfirmationDirection | null;
  confirmation_status?: string | null;
  confirmation_source_count?: number | null;
  confirmation_explanation?: string | null;
  is_multi_source?: boolean | null;
  why_now?: WhyNowBundle | null;
  signal_freshness?: SignalFreshnessBundle | null;
};

type SignalsWrappedResponse = {
  items?: SignalItem[];
  debug?: any;
};

type ConfirmationBand = "inactive" | "weak" | "moderate" | "strong" | "exceptional";
type ConfirmationBandFilter = "all" | "active" | "weak" | "moderate" | "strong" | "exceptional" | "strong_plus";
type ConfirmationDirection = "bullish" | "bearish" | "neutral" | "mixed";
type ConfirmationDirectionFilter = "all" | ConfirmationDirection;
type WhyNowState = "early" | "strengthening" | "strong" | "mixed" | "fading" | "inactive";
type WhyNowBundle = {
  ticker: string;
  lookback_days: number;
  state: WhyNowState;
  headline: string;
  evidence: string[];
  caveat?: string | null;
};
type SignalFreshnessState = "fresh" | "early" | "active" | "maturing" | "stale" | "inactive";
type SignalFreshnessBundle = {
  ticker: string;
  lookback_days: number;
  freshness_score: number;
  freshness_state: SignalFreshnessState;
  freshness_label: string;
  explanation: string;
  timing: {
    freshest_source_days: number | null;
    stalest_active_source_days: number | null;
    active_source_count: number;
    overlap_window_days: number | null;
  };
};

function getParam(sp: SearchParams, key: string): string {
  const v = sp[key];
  return typeof v === "string" ? v : "";
}

function clampMode(modeRaw: string): "all" | "congress" | "insider" {
  if (modeRaw === "all" || modeRaw === "congress" || modeRaw === "insider") return modeRaw;
  return "all";
}


function isInsiderSignalKind(kind?: string): boolean {
  const normalized = (kind ?? "").trim().toLowerCase();
  return normalized === "insider" || normalized === "insider_trade";
}

function resolveSignalReportingCik(item: SignalItem): string | null {
  return item.reporting_cik ?? item.reportingCik ?? null;
}

function clampSide(sideRaw: string): "all" | "buy" | "sell" | "buy_or_sell" | "award" | "inkind" | "exempt" {
  if (
    sideRaw === "all" ||
    sideRaw === "buy" ||
    sideRaw === "sell" ||
    sideRaw === "buy_or_sell" ||
    sideRaw === "award" ||
    sideRaw === "inkind" ||
    sideRaw === "exempt"
  ) {
    return sideRaw;
  }
  return "all";
}

function clampLimit(limitRaw: string): 25 | 50 | 100 {
  const n = Number(limitRaw);
  if (n === 25 || n === 50 || n === 100) return n;
  return 50;
}

function clampSort(sortRaw: string): "multiple" | "smart" | "recent" | "amount" | "confirmation" | "freshness" {
  if (
    sortRaw === "multiple" ||
    sortRaw === "smart" ||
    sortRaw === "recent" ||
    sortRaw === "amount" ||
    sortRaw === "confirmation" ||
    sortRaw === "freshness"
  ) return sortRaw;
  return "smart";
}

function clampConfirmationBand(value: string): ConfirmationBandFilter {
  if (
    value === "active" ||
    value === "weak" ||
    value === "moderate" ||
    value === "strong" ||
    value === "exceptional" ||
    value === "strong_plus"
  ) {
    return value;
  }
  return "all";
}

function clampConfirmationDirection(value: string): ConfirmationDirectionFilter {
  if (value === "bullish" || value === "bearish" || value === "mixed" || value === "neutral") return value;
  return "all";
}

function clampMinConfirmationSources(value: string): 0 | 2 | 3 | 4 {
  const n = Number(value);
  if (n === 2 || n === 3 || n === 4) return n;
  return 0;
}

function isTrue(v: string): boolean {
  const s = v.toLowerCase();
  return s === "true" || s === "1" || s === "yes" || s === "on";
}

function buildPageHref(params: {
  mode: string;
  side: string;
  limit: number;
  debug: boolean;
  sort: string;
  confirmationBand: ConfirmationBandFilter;
  confirmationDirection: ConfirmationDirectionFilter;
  minConfirmationSources: number;
  multiSourceOnly: boolean;
}): string {
  const u = new URL("https://local/signals");
  u.searchParams.set("mode", params.mode);
  u.searchParams.set("side", params.side);
  u.searchParams.set("limit", String(params.limit));
  u.searchParams.set("sort", params.sort);
  if (params.confirmationBand !== "all") u.searchParams.set("confirmation_band", params.confirmationBand);
  if (params.confirmationDirection !== "all") u.searchParams.set("confirmation_direction", params.confirmationDirection);
  if (params.minConfirmationSources > 0) u.searchParams.set("min_confirmation_sources", String(params.minConfirmationSources));
  if (params.multiSourceOnly) u.searchParams.set("multi_source_only", "1");
  if (params.debug) u.searchParams.set("debug", "true");
  return u.pathname + u.search;
}

function buildSignalsUrl(
  apiBase: string,
  mode: string,
  side: string,
  limit: number,
  debug: boolean,
  sort: string,
  confirmationBand: ConfirmationBandFilter,
  confirmationDirection: ConfirmationDirectionFilter,
  minConfirmationSources: number,
  multiSourceOnly: boolean,
): string {
  const u = new URL("/api/signals/all", apiBase);
  u.searchParams.set("mode", mode);
  u.searchParams.set("side", side);
  u.searchParams.set("limit", String(limit));
  u.searchParams.set("sort", sort);
  if (confirmationBand !== "all") u.searchParams.set("confirmation_band", confirmationBand);
  if (confirmationDirection !== "all") u.searchParams.set("confirmation_direction", confirmationDirection);
  if (minConfirmationSources > 0) u.searchParams.set("min_confirmation_sources", String(minConfirmationSources));
  if (multiSourceOnly) u.searchParams.set("multi_source_only", "1");
  if (debug) u.searchParams.set("debug", "1");
  return u.toString();
}

function formatUSD(n?: number): string {
  if (typeof n !== "number" || !Number.isFinite(n)) return "—";
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 0,
  }).format(n);
}

function formatMultiple(n?: number): string {
  if (typeof n !== "number" || !Number.isFinite(n)) return "—";
  return `${n.toFixed(1)}×`;
}

function formatSignalDate(value?: string): string {
  if (!value) return "--";
  const trimmed = value.trim();
  if (!trimmed) return "--";
  const dateOnly = trimmed.split("T")[0]?.split(" ")[0];
  return dateOnly || trimmed;
}

function normalizeSide(tradeType?: string | null): "buy" | "sell" | "buy_or_sell" | "award" | "inkind" | "exempt" | "return" | null {
  const t = (tradeType ?? "").trim().toLowerCase();
  if (!t) return null;

  if (t === "buy_or_sell" || t === "buy-sell" || t === "buy/sell") return "buy_or_sell";
  if (t === "d-return" || t === "return") return "return";
  if (t === "purchase" || t === "buy" || t === "p-purchase" || t.includes("purchase")) return "buy";
  if (t === "sale" || t === "sell" || t === "s-sale" || t.includes("sale")) return "sell";
  if (t.startsWith("a-") || t.includes("award")) return "award";
  if (t.startsWith("f-") || t.includes("inkind")) return "inkind";
  if (t.startsWith("m-") || t.includes("exempt")) return "exempt";
  return null;
}

function sideLabel(_kind: string, tradeType?: string): { label: string; klass: string } {
  const side = normalizeSide(tradeType);

  if (side === "buy") {
    return { label: "Buy", klass: "border-emerald-500/30 text-emerald-200 bg-emerald-500/10" };
  }
  if (side === "sell") {
    return { label: "Sell", klass: "border-red-500/30 text-red-200 bg-red-500/10" };
  }
  if (side === "buy_or_sell") {
    return { label: "Buy/Sell", klass: "border-slate-700 text-slate-300 bg-slate-900/30" };
  }
  if (side === "award") {
    return { label: "Award", klass: "border-slate-700 text-slate-300 bg-slate-900/30" };
  }
  if (side === "inkind") {
    return { label: "InKind", klass: "border-slate-700 text-slate-300 bg-slate-900/30" };
  }
  if (side === "exempt") {
    return { label: "Exempt", klass: "border-slate-700 text-slate-300 bg-slate-900/30" };
  }
  if (side === "return") {
    return { label: "Return", klass: "border-slate-700 text-slate-300 bg-slate-900/30" };
  }

  const fallback = (tradeType ?? "").trim();
  return { label: fallback ? fallback.toUpperCase() : "—", klass: "border-slate-700 text-slate-300 bg-slate-900/30" };
}

function smartLabel(band?: string, score?: number): { label: string; klass: string; dotClass: string } {
  const b = (band ?? "").toLowerCase();
  if (typeof score !== "number" || !Number.isFinite(score)) {
    return { label: "—", klass: "border-slate-700 text-slate-300 bg-slate-900/30", dotClass: "bg-slate-500" };
  }
  if (b === "strong") {
    return { label: "Strong", klass: "border-emerald-500/30 text-emerald-200 bg-emerald-500/10", dotClass: "bg-emerald-400" };
  }
  if (b === "notable") {
    return { label: "Notable", klass: "border-amber-500/30 text-amber-200 bg-amber-500/10", dotClass: "bg-amber-400" };
  }
  if (b === "mild") {
    return { label: "Mild", klass: "border-orange-500/30 text-orange-200 bg-orange-500/10", dotClass: "bg-orange-400" };
  }
  return { label: "Noise", klass: "border-slate-700 text-slate-300 bg-slate-900/30", dotClass: "bg-slate-500" };
}

function confirmationBandLabel(band?: ConfirmationBand | null): string {
  if (band === "exceptional") return "Excellent";
  if (band === "strong") return "Strong";
  if (band === "moderate") return "Moderate";
  if (band === "weak") return "Weak";
  return "Inactive";
}

function titleCase(value: string): string {
  return value ? `${value.slice(0, 1).toUpperCase()}${value.slice(1)}` : value;
}

function confirmationDirectionLabel(direction?: ConfirmationDirection | null): string {
  if (direction === "bullish") return "Bullish";
  if (direction === "bearish") return "Bearish";
  if (direction === "mixed") return "Mixed";
  return "Neutral";
}

function confirmationDirectionTextClass(direction?: ConfirmationDirection | null): string {
  if (direction === "bullish") return "text-emerald-200/90";
  if (direction === "bearish") return "text-rose-200/90";
  if (direction === "mixed") return "text-amber-200/90";
  return "text-slate-400";
}

function freshnessTextClass(state?: SignalFreshnessState | null): string {
  if (state === "fresh") return "text-emerald-200";
  if (state === "early") return "text-cyan-200";
  if (state === "active") return "text-sky-200";
  if (state === "maturing") return "text-amber-200";
  if (state === "stale") return "text-rose-200";
  return "text-slate-400";
}

function confirmationDrivers(item: SignalItem): string[] {
  const seen = new Set<string>();
  const drivers: string[] = [];
  const candidates = [item.confirmation_explanation, ...(item.why_now?.evidence ?? [])];

  for (const candidate of candidates) {
    if (typeof candidate !== "string") continue;
    const trimmed = candidate.trim();
    if (!trimmed) continue;
    const key = trimmed.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    drivers.push(trimmed);
    if (drivers.length >= 2) break;
  }

  return drivers;
}

function SignalsSortLink({
  label,
  href,
  active,
}: {
  label: string;
  href?: string;
  active: boolean;
}) {
  if (!href) return <>{label}</>;
  return (
    <Link
      href={href}
      prefetch={false}
      className={`inline-flex items-center gap-1 transition hover:text-white ${active ? "text-emerald-100" : ""}`}
    >
      {label}
      <span className={`text-[10px] font-semibold normal-case tracking-normal ${active ? "text-emerald-300/80" : "text-slate-600"}`}>
        {active ? "desc" : ""}
      </span>
    </Link>
  );
}

function ConfirmHoverCell({ item }: { item: SignalItem }) {
  const tooltipId = `confirm-${item.event_id}`;
  const score = typeof item.confirmation_score === "number" && Number.isFinite(item.confirmation_score) ? item.confirmation_score : "--";
  const directionLabel = confirmationDirectionLabel(item.confirmation_direction);
  const drivers = confirmationDrivers(item);

  return (
    <div className="group/confirm relative inline-flex max-w-full items-center">
      <button
        type="button"
        aria-describedby={tooltipId}
        className="inline-flex max-w-full items-center gap-2 rounded-md px-1 py-0.5 text-left transition hover:bg-white/[0.03] focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-emerald-400/30"
      >
        <span className="font-mono text-[13px] font-semibold text-slate-100">{score}</span>
        <span aria-hidden className="text-slate-600">&middot;</span>
        <span className={`truncate text-xs font-medium ${confirmationDirectionTextClass(item.confirmation_direction)}`}>{directionLabel}</span>
      </button>
      <div
        id={tooltipId}
        role="tooltip"
        className="pointer-events-none invisible absolute right-0 top-full z-30 mt-2 w-64 rounded-xl border border-white/10 bg-slate-950/95 p-3 text-left opacity-0 shadow-2xl shadow-black/40 backdrop-blur transition delay-75 group-hover/confirm:visible group-hover/confirm:opacity-100 group-focus-within/confirm:visible group-focus-within/confirm:opacity-100"
      >
        <p className="text-[11px] font-semibold uppercase tracking-wide text-slate-500">Confirmation</p>
        <p className="mt-1 text-sm leading-5 text-slate-100">{item.confirmation_status ?? "Confirmation unavailable"}</p>
        <div className="mt-2 flex flex-wrap items-center gap-x-2 gap-y-1 text-xs text-slate-400">
          <span>{confirmationBandLabel(item.confirmation_band)}</span>
          {typeof item.confirmation_source_count === "number" ? <span>{item.confirmation_source_count} source{item.confirmation_source_count === 1 ? "" : "s"}</span> : null}
        </div>
        {drivers.length > 0 ? (
          <div className="mt-3 space-y-1.5">
            {drivers.map((driver) => (
              <p key={driver} className="text-xs leading-4 text-slate-300">
                {driver}
              </p>
            ))}
          </div>
        ) : null}
      </div>
    </div>
  );
}

function sourceBadge(item: SignalItem): { label: string; tone: Parameters<typeof Badge>[0]["tone"] } {
  const chamber = (item.chamber ?? "").toLowerCase();
  if (chamber.includes("house")) return chamberBadge("house");
  if (chamber.includes("senate")) return chamberBadge("senate");

  const source = (item.source ?? "").toLowerCase();
  if (source.includes("house")) return chamberBadge("house");
  if (source.includes("senate")) return chamberBadge("senate");
  return chamberBadge();
}

export default async function SignalsPage({
  searchParams,
}: {
  searchParams?: Promise<SearchParams>;
}) {
  const sp = (await searchParams) ?? {};
  const authToken = await requirePageAuth(buildReturnTo("/signals", sp));
  const mode = clampMode(getParam(sp, "mode"));
  const side = clampSide(getParam(sp, "side"));
  const limit = clampLimit(getParam(sp, "limit"));
  const sort = clampSort(getParam(sp, "sort"));
  const confirmationBand = clampConfirmationBand(getParam(sp, "confirmation_band"));
  const confirmationDirection = clampConfirmationDirection(getParam(sp, "confirmation_direction"));
  const minConfirmationSources = clampMinConfirmationSources(getParam(sp, "min_confirmation_sources"));
  const multiSourceOnly = isTrue(getParam(sp, "multi_source_only"));
  const debug = isTrue(getParam(sp, "debug"));
  const API_BASE = process.env.NEXT_PUBLIC_API_BASE ?? "https://congress-tracker-api.fly.dev";
  const requestUrl = buildSignalsUrl(
    API_BASE,
    mode,
    side,
    limit,
    debug,
    sort,
    confirmationBand,
    confirmationDirection,
    minConfirmationSources,
    multiSourceOnly,
  );

  const card = "rounded-2xl border border-slate-800 bg-slate-950/40 shadow-sm";
  const pill = "inline-flex items-center gap-2 rounded-full border px-3 py-1 text-xs font-medium";
  const btn =
    "inline-flex items-center justify-center rounded-full border px-3 py-1 text-xs font-medium transition hover:bg-slate-900/60";
  const btnActive = "border-emerald-500/40 text-emerald-200 bg-emerald-500/10";
  const btnIdle = "border-slate-800 text-slate-200 bg-slate-950/30";
  const activeMinConfirmationSources = multiSourceOnly && minConfirmationSources < 2 ? 2 : minConfirmationSources;
  const pageHref = (overrides: Partial<Parameters<typeof buildPageHref>[0]>) =>
    buildPageHref({
      mode,
      side,
      limit,
      debug,
      sort,
      confirmationBand,
      confirmationDirection,
      minConfirmationSources,
      multiSourceOnly,
      ...overrides,
    });

  return (
    <div className="space-y-8">
      <div>
        <div className="text-xs tracking-[0.25em] text-emerald-300/70">SIGNALS</div>
        <h1 className="mt-2 text-3xl font-semibold text-white">Unusual trade radar</h1>
        <p className="mt-2 max-w-2xl text-sm text-slate-300/80">
          Fast scanning across Congress and insider activity, with optional debug transparency.
        </p>
      </div>

      {/* Controls */}
      <div className={`mt-6 p-4 ${card}`}>
        <div className="space-y-3">
          <div className="flex flex-wrap items-center gap-x-3 gap-y-2">
            <div className="text-xs text-slate-400">Mode</div>
            <div className="inline-flex items-center gap-2 rounded-full border border-slate-800 bg-slate-950/30 p-1">
              {([
                ["all", "ALL"],
                ["congress", "CONGRESS"],
                ["insider", "INSIDER"],
              ] as const).map(([m, label]) => (
                <Link
                  key={m}
                  href={pageHref({ mode: m })}
                  prefetch={false}
                  className={`${btn} ${mode === m ? btnActive : btnIdle}`}
                >
                  {label}
                </Link>
              ))}
            </div>

            <div className="text-xs text-slate-400">Side</div>
            <div className="inline-flex items-center gap-2 rounded-full border border-slate-800 bg-slate-950/30 p-1">
              {([
                ["all", "All"],
                ["buy", "Buy"],
                ["sell", "Sell"],
                ["buy_or_sell", "Buy/Sell"],
                ["award", "Award"],
                ["inkind", "InKind"],
                ["exempt", "Exempt"],
              ] as const).map(([s, label]) => (
                <Link
                  key={s}
                  href={pageHref({ side: s })}
                  prefetch={false}
                  className={`${btn} ${side === s ? btnActive : btnIdle}`}
                >
                  {label}
                </Link>
              ))}
            </div>

            <div className="ml-2 text-xs text-slate-400">Sort</div>
            <div className="inline-flex items-center gap-2 rounded-full border border-slate-800 bg-slate-950/30 p-1">
              {([
                ["multiple", "MULTIPLE"],
                ["smart", "SMART"],
                ["confirmation", "CONFIRM"],
                ["freshness", "FRESH"],
                ["recent", "RECENT"],
                ["amount", "AMOUNT"],
              ] as const).map(([s, label]) => (
                <Link
                  key={s}
                  href={pageHref({ sort: s })}
                  prefetch={false}
                  className={`${btn} ${sort === s ? btnActive : btnIdle}`}
                >
                  {label}
                </Link>
              ))}
            </div>
          </div>

          <div className="flex flex-wrap items-center gap-x-3 gap-y-2">
            <div className="text-xs text-slate-400">Confirm</div>
            <div className="inline-flex items-center gap-2 rounded-full border border-slate-800 bg-slate-950/30 p-1">
              {([
                ["all", "All"],
                ["strong_plus", "Strong+"],
                ["exceptional", "Exceptional"],
                ["moderate", "Moderate"],
              ] as const).map(([value, label]) => (
                <Link
                  key={value}
                  href={pageHref({ confirmationBand: value })}
                  prefetch={false}
                  className={`${btn} ${confirmationBand === value ? btnActive : btnIdle}`}
                >
                  {label}
                </Link>
              ))}
            </div>

            <div className="text-xs text-slate-400">Direction</div>
            <div className="inline-flex items-center gap-2 rounded-full border border-slate-800 bg-slate-950/30 p-1">
              {([
                ["all", "All"],
                ["bullish", "Bull"],
                ["bearish", "Bear"],
                ["mixed", "Mixed"],
              ] as const).map(([value, label]) => (
                <Link
                  key={value}
                  href={pageHref({ confirmationDirection: value })}
                  prefetch={false}
                  className={`${btn} ${confirmationDirection === value ? btnActive : btnIdle}`}
                >
                  {label}
                </Link>
              ))}
            </div>

            <div className="text-xs text-slate-400">Sources</div>
            <div className="inline-flex items-center gap-2 rounded-full border border-slate-800 bg-slate-950/30 p-1">
              {([
                [0, "Any"],
                [2, "2+"],
                [3, "3+"],
              ] as const).map(([value, label]) => (
                <Link
                  key={value}
                  href={pageHref({ minConfirmationSources: value, multiSourceOnly: value >= 2 })}
                  prefetch={false}
                  className={`${btn} ${activeMinConfirmationSources === value ? btnActive : btnIdle}`}
                >
                  {label}
                </Link>
              ))}
            </div>

            <div className="ml-2 text-xs text-slate-400">Limit</div>
            <div className="inline-flex items-center gap-2">
              {[25, 50, 100].map((l) => (
                <Link
                  key={l}
                  href={pageHref({ limit: l })}
                  prefetch={false}
                  className={`${btn} ${limit === l ? btnActive : btnIdle}`}
                >
                  {l}
                </Link>
              ))}
            </div>
          </div>
        </div>
        <SavedViewsBar
          surface="signals"
          defaultParams={{
            mode,
            side,
            limit: String(limit),
            sort,
            confirmation_band: confirmationBand,
            confirmation_direction: confirmationDirection,
            min_confirmation_sources: String(activeMinConfirmationSources),
            multi_source_only: multiSourceOnly ? "1" : "",
          }}
          paramKeys={["mode", "side", "limit", "sort", "debug", "symbol", "confirmation_band", "confirmation_direction", "min_confirmation_sources", "multi_source_only"]}
          rightSlot={
            <>
              <span className={`${pill} border-slate-800 text-slate-300 bg-slate-950/30`}>
                mode <span className="text-white">{mode}</span>
              </span>
              <span className={`${pill} border-slate-800 text-slate-300 bg-slate-950/30`}>
                side <span className="text-white">{side}</span>
              </span>
              <span className={`${pill} border-slate-800 text-slate-300 bg-slate-950/30`}>
                sort <span className="text-white">{sort}</span>
              </span>
              {confirmationBand !== "all" || confirmationDirection !== "all" || activeMinConfirmationSources > 0 ? (
                <span className={`${pill} border-cyan-400/25 text-cyan-100 bg-cyan-400/10`}>
                  confirm <span className="text-white">{confirmationBand !== "all" ? confirmationBand : confirmationDirection !== "all" ? confirmationDirection : `${activeMinConfirmationSources}+ src`}</span>
                </span>
              ) : null}
            </>
          }
        />
      </div>

      {/* Table */}
      <div className="mt-6">
        <div className="mb-3">
          <h2 className="text-xl font-semibold text-white">Signals table</h2>
          <p className="text-sm text-slate-400">Abnormal trades vs per-symbol historical median.</p>
        </div>
        <Suspense key={requestUrl} fallback={<SignalsResultsFallback card={card} />}>
          <SignalsResultsSection
            requestUrl={requestUrl}
            authToken={authToken}
            card={card}
            pill={pill}
            activeSort={sort}
            confirmationSortHref={pageHref({ sort: "confirmation" })}
            freshnessSortHref={pageHref({ sort: "freshness" })}
          />
        </Suspense>
      </div>
    </div>
  );
}

function SignalsResultsFallback({ card }: { card: string }) {
  return (
    <div className={`${card} min-h-[32rem] overflow-hidden p-4`} aria-live="polite" aria-busy="true">
      <div className="mb-4 flex items-center justify-between">
        <SkeletonBlock className="h-4 w-36" />
        <SkeletonBlock className="h-4 w-28" />
      </div>
      <SkeletonTable columns={11} rows={8} />
    </div>
  );
}

async function SignalsResultsSection({
  requestUrl,
  authToken,
  card,
  pill,
  activeSort,
  confirmationSortHref,
  freshnessSortHref,
}: {
  requestUrl: string;
  authToken: string;
  card: string;
  pill: string;
  activeSort: string;
  confirmationSortHref: string;
  freshnessSortHref: string;
}) {
  let errorMessage: string | null = null;
  let items: SignalItem[] = [];
  try {
    const res = await fetch(requestUrl, { cache: "no-store", headers: { Authorization: `Bearer ${authToken}` } });
    if (!res.ok) {
      errorMessage = `Request failed with ${res.status}`;
    } else {
      const json: unknown = await res.json();
      if (Array.isArray(json)) {
        items = json as SignalItem[];
      } else {
        const obj = json as SignalsWrappedResponse;
        items = Array.isArray(obj.items) ? obj.items : [];
      }
    }
  } catch (e) {
    errorMessage = e instanceof Error ? e.message : "Unable to load signals.";
  }

  return (
    <div className={`${card} min-h-[32rem] overflow-hidden`}>
      <div className="w-full">
        <table className="w-full table-fixed border-collapse text-sm">
          <colgroup>
            <col className="w-[7.5rem]" />
            <col className="w-[6.5rem]" />
            <col />
            <col className="w-[5.5rem]" />
            <col className="w-[7rem]" />
            <col className="w-[7rem]" />
            <col className="w-[5.5rem]" />
            <col className="w-[8rem]" />
            <col className="w-[6rem]" />
            <col className="w-[8rem]" />
            <col className="w-[5.5rem]" />
          </colgroup>
          <thead className="bg-slate-950/50 text-xs uppercase tracking-wider text-slate-400">
            <tr>
              <th className="px-3 py-3 text-left">Time</th>
              <th className="px-3 py-3 text-left">Ticker</th>
              <th className="px-3 py-3 text-left">Member</th>
              <th className="px-3 py-3 text-left">Side</th>
              <th className="px-3 py-3 text-left">Amount</th>
              <th className="px-3 py-3 text-left">Baseline</th>
              <th className="px-3 py-3 text-left">Multiple</th>
              <th className="px-3 py-3 text-left">Smart</th>
              <th className="px-3 py-3 text-left">Source</th>
              <th className={`px-3 py-3 text-left ${activeSort === "confirmation" ? "text-emerald-100" : ""}`}>
                <SignalsSortLink label="Confirm" href={confirmationSortHref} active={activeSort === "confirmation"} />
              </th>
              <th className={`px-3 py-3 text-left ${activeSort === "freshness" ? "text-emerald-100" : ""}`}>
                <SignalsSortLink label="Fresh" href={freshnessSortHref} active={activeSort === "freshness"} />
              </th>
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-800">
            {items.length === 0 ? (
              <tr>
                <td className="px-4 py-10 text-center text-slate-400" colSpan={11}>
                  {errorMessage ? "Unable to load signals." : "No unusual signals returned."}
                </td>
              </tr>
            ) : (
              items.map((it) => {
                const side = sideLabel(it.kind ?? "", it.trade_type);
                const smart = smartLabel(it.smart_band, it.smart_score);
                const freshness = it.signal_freshness;
                const source = sourceBadge(it);
                const isInsider = isInsiderSignalKind(it.kind);
                const rawPos = it.position ?? null;
                const roleCode = normalizeInsiderRoleBadge(rawPos);
                const roleTone = insiderRoleBadgeTone(roleCode);
                const insiderName = getInsiderDisplayName(resolveInsiderDisplayName(it.who, rawPos));
                const insiderProfileHref = insiderHref(insiderName, resolveSignalReportingCik(it));
                return (
                  <tr key={it.event_id} className="hover:bg-slate-900/20">
                    <td className="px-3 py-3 text-slate-300">
                      <span className="font-mono text-[12px]" title={it.ts}>{formatSignalDate(it.ts)}</span>
                    </td>
                    <td className="px-3 py-3">
                      <div className="flex items-center gap-2">
                        {it.symbol ? <AddTickerToWatchlist symbol={it.symbol} variant="compact" align="left" /> : null}
                        {tickerHref(it.symbol) ? (
                          <Link href={tickerHref(it.symbol)!} prefetch={false} className={`truncate ${tickerMonoLinkClassName}`}>{it.symbol}</Link>
                        ) : (
                          <span className="truncate font-mono text-slate-300">{it.symbol}</span>
                        )}
                      </div>
                    </td>
                    <td className="px-3 py-3 text-slate-200">
                      {isInsider ? (
                        <div className="flex min-w-0 items-center gap-2">
                          <span title={rawPos ?? undefined}><Badge tone={roleTone}>{roleCode}</Badge></span>
                          {insiderProfileHref ? (
                            <Link href={insiderProfileHref} prefetch={false} className="min-w-0 truncate text-slate-100 hover:underline">{insiderName ?? "—"}</Link>
                          ) : (
                            <span className="min-w-0 truncate text-slate-100">{insiderName ?? "—"}</span>
                          )}
                        </div>
                      ) : (
                        <div className="flex min-w-0 items-center gap-2 overflow-hidden">
                          <span className="inline-flex shrink-0 align-middle"><Badge tone={source.tone} className="px-2 py-0.5 text-[10px]">{source.label}</Badge></span>
                          {it.member_bioguide_id ? (
                            <Link href={memberHref({ name: it.who, memberId: it.member_bioguide_id })} prefetch={false} className="truncate hover:underline">{it.who ?? "—"}</Link>
                          ) : (
                            it.who ?? "—"
                          )}
                        </div>
                      )}
                    </td>
                    <td className="px-3 py-3"><span className={`${pill} ${side.klass}`}>{side.label}</span></td>
                    <td className="px-3 py-3 text-slate-200" title={`${formatUSD(it.amount_min)} – ${formatUSD(it.amount_max)}`}>{formatUSD(it.amount_max)}</td>
                    <td className="px-3 py-3 text-slate-200">{formatUSD(it.baseline_median_amount_max)}</td>
                    <td className="px-3 py-3 text-slate-200">{formatMultiple(it.unusual_multiple)}</td>
                    <td className="px-3 py-3">
                      <span className={`${pill} max-w-full ${smart.klass}`}>
                        <span className={`h-2 w-2 rounded-full ${smart.dotClass}`} />
                        <span className="font-mono">{typeof it.smart_score === "number" && Number.isFinite(it.smart_score) ? it.smart_score : "—"}</span>
                        <span className="opacity-80">{smart.label}</span>
                      </span>
                    </td>
                    <td className="px-3 py-3">
                      {isInsider ? (
                        <Badge tone="insider_default" className="px-2 py-0.5 text-[10px]">INSIDER</Badge>
                      ) : (
                        <Badge tone={source.tone} className="px-2 py-0.5 text-[10px]">{source.label}</Badge>
                      )}
                    </td>
                    <td className="px-3 py-3">
                      <div className="w-full min-w-0">
                        <ConfirmHoverCell item={it} />
                      </div>
                    </td>
                    <td className="px-3 py-3">
                      <div
                        className="w-full min-w-0"
                        title={freshness ? `${freshness.freshness_label} - ${freshness.explanation}` : "Freshness unavailable"}
                      >
                        <span className={`whitespace-nowrap text-xs font-medium ${freshnessTextClass(freshness?.freshness_state)}`}>
                          {titleCase(freshness?.freshness_state ?? "inactive")}
                        </span>
                      </div>
                    </td>
                  </tr>
                );
              })
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
