import Link from "next/link";
import { Badge } from "@/components/Badge";
import { VerifiedSessionGuard } from "@/components/auth/VerifiedSessionGuard";
import { SignalsResultsClient } from "@/components/signals/SignalsResultsClient";
import { SkeletonBlock, SkeletonTable } from "@/components/ui/LoadingSkeleton";
import { chamberBadge } from "@/lib/format";
import { getEntitlements, getSignalsAll, type SignalMode, type SignalSort } from "@/lib/api";
import { defaultEntitlements, entitlementsFromTierHint, hasEntitlement } from "@/lib/entitlements";
import { getInsiderDisplayName, insiderHref } from "@/lib/insider";
import { memberHref } from "@/lib/memberSlug";
import { insiderRoleBadgeTone, normalizeInsiderRoleBadge, resolveInsiderDisplayName } from "@/lib/insiderRole";
import { tickerHref } from "@/lib/ticker";
import { tickerMonoLinkClassName } from "@/lib/styles";
import { AddTickerToWatchlist } from "@/components/watchlists/AddTickerToWatchlist";
import { SIGNALS_COLUMN_DEFINITIONS, SignalColumnHeaderTooltip } from "@/components/signals/SignalColumnHeaderTooltip";
import { SignalsFiltersClient } from "@/components/signals/SignalsFiltersClient";
import { Suspense } from "react";
import {
  mobileResultsScrollFrameClassName,
  signalsResultsScrollFrameClassName,
  stickyResultsTableHeaderClassName,
} from "@/components/ui/resultsTableFrame";
import { buildReturnTo, requirePageAuthState } from "@/lib/serverAuth";

type SearchParams = Record<string, string | string[] | undefined>;

const SIGNALS_SYSTEM_DEFAULT_PARAMS: Record<string, string> = {
  mode: "all",
  side: "all",
  limit: "50",
  sort: "smart",
  confirmation_band: "all",
  confirmation_direction: "all",
  min_confirmation_sources: "0",
  multi_source_only: "",
};

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

function clampMode(modeRaw: string): SignalMode {
  if (modeRaw === "all" || modeRaw === "congress" || modeRaw === "insider" || modeRaw === "institutional") return modeRaw;
  return "all";
}


function isInsiderSignalKind(kind?: string): boolean {
  const normalized = (kind ?? "").trim().toLowerCase();
  return normalized === "insider" || normalized === "insider_trade";
}

function isInstitutionalSignalKind(kind?: string): boolean {
  return (kind ?? "").trim().toLowerCase() === "institutional";
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

function clampInstitutionalLookbackDays(value: string, mode: SignalMode): number | undefined {
  const n = Number(value);
  if (Number.isFinite(n) && n >= 1) return Math.min(365, Math.trunc(n));
  return mode === "institutional" ? 365 : undefined;
}

function isTrue(v: string): boolean {
  const s = v.toLowerCase();
  return s === "true" || s === "1" || s === "yes" || s === "on";
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
  institutionalLookbackDays?: number,
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
  if (institutionalLookbackDays !== undefined) u.searchParams.set("institutional_lookback_days", String(institutionalLookbackDays));
  if (debug) u.searchParams.set("debug", "1");
  return u.toString();
}

function backtestingHrefFromItems(items: SignalItem[]): string | null {
  const symbols: string[] = [];
  for (const item of items) {
    const ticker = (item.symbol ?? "").trim().toUpperCase();
    if (!ticker || symbols.includes(ticker)) continue;
    symbols.push(ticker);
    if (symbols.length >= 25) break;
  }
  if (symbols.length === 0) return null;
  return `/backtesting?tickers=${encodeURIComponent(symbols.join(","))}`;
}

function isInstitutionalProRequiredMessage(mode: SignalMode, message: string | null): boolean {
  return mode === "institutional" && Boolean(message && /requires Pro|Premium access required/i.test(message));
}

function InstitutionalSignalsUpgradeCta({ upgradeUrl }: { upgradeUrl: string }) {
  return (
    <Link
      href={upgradeUrl}
      prefetch={false}
      className="inline-flex min-h-9 items-center justify-center rounded-md border border-emerald-300/30 bg-emerald-300/10 px-4 text-sm font-semibold text-emerald-100 transition hover:border-emerald-200/50 hover:bg-emerald-300/15 hover:text-white"
    >
      Upgrade to Pro
    </Link>
  );
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

function sideLabel(kind: string, tradeType?: string): { label: string; klass: string } {
  if (isInstitutionalSignalKind(kind)) {
    const label = tradeType?.trim() || "13F Filing";
    const lower = label.toLowerCase();
    const klass = lower.includes("reduction") || lower.includes("exit")
      ? "border-rose-400/30 bg-rose-400/15 text-rose-100"
      : lower.includes("increase") || lower.includes("new")
        ? "border-emerald-400/30 bg-emerald-400/15 text-emerald-100"
        : "border-white/10 bg-white/5 text-slate-200";
    return { label, klass };
  }
  const side = normalizeSide(tradeType);

  if (side === "buy") {
    return { label: "Buy", klass: "border-emerald-400/30 bg-emerald-400/15 text-emerald-100" };
  }
  if (side === "sell") {
    return { label: "Sell", klass: "border-rose-400/30 bg-rose-400/15 text-rose-100" };
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
  if (b === "exceptional" || score >= 85) {
    return { label: "Exceptional", klass: "border-emerald-300/35 bg-emerald-300/10 text-emerald-100", dotClass: "bg-emerald-300" };
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
  active,
  title,
}: {
  label: string;
  active: boolean;
  title?: string;
}) {
  return (
    <span
      title={title}
      aria-label={title}
      className={`inline-flex items-center gap-1 ${active ? "text-emerald-100" : ""}`}
    >
      {label}
      <span className={`text-[10px] font-semibold normal-case tracking-normal ${active ? "text-emerald-300/80" : "text-slate-600"}`}>
        {active ? "desc" : ""}
      </span>
    </span>
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
  if (isInstitutionalSignalKind(item.kind)) return { label: "13F", tone: "neutral" };
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
  const returnTo = buildReturnTo("/signals", sp);
  const authState = await requirePageAuthState(returnTo);
  const authToken = authState.token;
  const entitlements = authToken
    ? await getEntitlements(authToken).catch(() => defaultEntitlements)
    : entitlementsFromTierHint(authState.entitlementHint);
  const mode = clampMode(getParam(sp, "mode"));
  const side = clampSide(getParam(sp, "side"));
  const limit = clampLimit(getParam(sp, "limit"));
  const sort = clampSort(getParam(sp, "sort"));
  const confirmationBand = clampConfirmationBand(getParam(sp, "confirmation_band"));
  const confirmationDirection = clampConfirmationDirection(getParam(sp, "confirmation_direction"));
  const minConfirmationSources = clampMinConfirmationSources(getParam(sp, "min_confirmation_sources"));
  const multiSourceOnly = isTrue(getParam(sp, "multi_source_only"));
  const institutionalLookbackDays = clampInstitutionalLookbackDays(getParam(sp, "institutional_lookback_days"), mode);
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
    institutionalLookbackDays,
  );

  const card = "min-w-0 max-w-full rounded-2xl border border-slate-800 bg-slate-950/40 shadow-sm";
  const pill = "inline-flex items-center gap-2 rounded-full border px-3 py-1 text-xs font-medium";
  const activeMinConfirmationSources = multiSourceOnly && minConfirmationSources < 2 ? 2 : minConfirmationSources;

  return (
    <VerifiedSessionGuard returnTo={returnTo} initiallyAuthorized={Boolean(authToken)}>
      <div className="min-w-0 max-w-full space-y-8 overflow-x-hidden">
      <div>
        <div className="text-xs tracking-[0.25em] text-emerald-300/70">SIGNALS</div>
        <h1 className="mt-2 text-3xl font-semibold text-white">Unusual trade radar</h1>
        <p className="mt-2 max-w-2xl text-sm text-slate-300/80">
          Signals rank high-conviction disclosure activity across Congress trades, insider activity, Institutional Activity, confirmation, freshness, and Why Now context.
        </p>
      </div>

      <SignalsFiltersClient
        mode={mode}
        side={side}
        limit={limit}
        debug={debug}
        sort={sort}
        confirmationBand={confirmationBand}
        confirmationDirection={confirmationDirection}
        minConfirmationSources={activeMinConfirmationSources}
        multiSourceOnly={multiSourceOnly}
        card={card}
        pill={pill}
        defaultParams={SIGNALS_SYSTEM_DEFAULT_PARAMS}
      />

      {/* Table */}
      <div className="mt-6 min-w-0 max-w-full overflow-x-hidden">
        <div className="mb-3">
          <h2 className="text-xl font-semibold text-white">Signals table</h2>
          <p className="text-sm text-slate-400">{mode === "institutional" ? "Material 13F filing activity by filing date." : "Abnormal trades vs per-symbol historical median."}</p>
        </div>
        <Suspense key={requestUrl} fallback={<SignalsResultsFallback card={card} />}>
          <SignalsResultsSection
            mode={mode}
            side={side}
            limit={limit}
            debug={debug}
            sort={sort}
            confirmationBand={confirmationBand}
            confirmationDirection={confirmationDirection}
            minConfirmationSources={activeMinConfirmationSources}
            multiSourceOnly={multiSourceOnly}
            institutionalLookbackDays={institutionalLookbackDays}
            authToken={authToken}
            card={card}
            pill={pill}
            activeSort={sort}
            canBacktest={hasEntitlement(entitlements, "backtesting")}
            upgradeUrl={entitlements.upgrade_url || "/pricing"}
          />
        </Suspense>
      </div>
      </div>
    </VerifiedSessionGuard>
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
  mode,
  side,
  limit,
  debug,
  sort,
  confirmationBand,
  confirmationDirection,
  minConfirmationSources,
  multiSourceOnly,
  institutionalLookbackDays,
  authToken,
  card,
  pill,
  activeSort,
  canBacktest,
  upgradeUrl,
}: {
  mode: SignalMode;
  side: string;
  limit: number;
  debug: boolean;
  sort: SignalSort;
  confirmationBand: ConfirmationBandFilter;
  confirmationDirection: ConfirmationDirectionFilter;
  minConfirmationSources: number;
  multiSourceOnly: boolean;
  institutionalLookbackDays?: number;
  authToken: string;
  card: string;
  pill: string;
  activeSort: string;
  canBacktest: boolean;
  upgradeUrl: string;
}) {
  let errorMessage: string | null = null;
  let items: SignalItem[] = [];
  if (!authToken) {
    return (
      <SignalsResultsClient
        mode={mode}
        side={side}
        limit={limit}
        debug={debug}
        sort={sort}
        confirmationBand={confirmationBand}
        confirmationDirection={confirmationDirection}
        minConfirmationSources={minConfirmationSources}
        multiSourceOnly={multiSourceOnly}
        institutionalLookbackDays={institutionalLookbackDays}
        card={card}
        pill={pill}
        activeSort={activeSort}
        canBacktest={canBacktest}
        upgradeUrl={upgradeUrl}
      />
    );
  } else try {
    const data = await getSignalsAll({
      mode,
      side,
      sort,
      limit,
      debug,
      confirmation_band: confirmationBand,
      confirmation_direction: confirmationDirection,
      min_confirmation_sources: minConfirmationSources,
      multi_source_only: multiSourceOnly,
      institutional_lookback_days: institutionalLookbackDays,
      authToken,
    });
    items = data.items as SignalItem[];
  } catch (e) {
    errorMessage = e instanceof Error ? e.message : "Unable to load signals.";
  }
  const backtestingHref = backtestingHrefFromItems(items);
  const isInstitutionalMode = mode === "institutional";
  const showInstitutionalUpgradeCta = isInstitutionalProRequiredMessage(mode, errorMessage);
  const headerLabels = isInstitutionalMode
    ? {
        time: "Filing Date",
        actor: "Institution",
        side: "Action",
        amount: "Reported Value",
        baseline: "Prior Q Value",
        multiple: "Delta %",
        score: "Institutional Score",
      }
    : {
        time: "Time",
        actor: "Member",
        side: "Side",
        amount: "Amount",
        baseline: "Base",
        multiple: "Mult",
        score: "Score",
      };

  return (
    <div className={`${card} min-h-[32rem] overflow-hidden`}>
      <div className="flex min-w-0 max-w-full flex-col items-stretch gap-3 border-b border-slate-800 px-4 py-3 text-sm md:flex-row md:items-center md:justify-between">
        <p className="min-w-0 text-slate-400">
          {items.length > 0 ? `${items.length} visible signals` : errorMessage ? "Signals unavailable" : "No visible signals"}
        </p>
        {isInstitutionalMode ? (
          <span className="inline-flex w-full items-center justify-center rounded-full border border-white/10 px-3 py-1 text-center text-xs font-semibold text-slate-500 md:w-auto">Institutional backtests coming soon</span>
        ) : canBacktest ? (
          backtestingHref ? (
            <Link
              href={backtestingHref}
              prefetch={false}
              className="inline-flex w-full items-center justify-center rounded-full border border-emerald-300/30 bg-emerald-300/10 px-3 py-1 text-center text-xs font-semibold text-emerald-100 transition hover:border-emerald-200/40 hover:text-white md:w-auto"
            >
              Backtest these signals
            </Link>
          ) : (
            <span className="inline-flex w-full items-center justify-center rounded-full border border-white/10 px-3 py-1 text-center text-xs font-semibold text-slate-500 md:w-auto">No tickers to backtest</span>
          )
        ) : (
          <Link
            href={upgradeUrl}
            prefetch={false}
            className="inline-flex w-full items-center justify-center rounded-full border border-white/10 px-3 py-1 text-center text-xs font-semibold text-slate-300 transition hover:border-white/20 hover:text-white md:w-auto"
          >
            Backtest Signals with Premium.
          </Link>
        )}
      </div>
      <div className={`${mobileResultsScrollFrameClassName} md:hidden`}>
        {items.length === 0 ? (
          <div className="px-4 py-10 text-center text-sm text-slate-400">
            <div>{errorMessage || "No unusual signals returned."}</div>
            {showInstitutionalUpgradeCta ? <div className="mt-4"><InstitutionalSignalsUpgradeCta upgradeUrl={upgradeUrl} /></div> : null}
          </div>
        ) : (
          <div className="divide-y divide-slate-800">
            {items.map((it) => {
              const side = sideLabel(it.kind ?? "", it.trade_type);
              const smart = smartLabel(it.smart_band, it.smart_score);
              const freshness = it.signal_freshness;
              const source = sourceBadge(it);
              const isInsider = isInsiderSignalKind(it.kind);
              const isInstitutional = isInstitutionalSignalKind(it.kind);
              const rawPos = it.position ?? null;
              const roleCode = normalizeInsiderRoleBadge(rawPos);
              const roleTone = insiderRoleBadgeTone(roleCode);
              const insiderName = getInsiderDisplayName(resolveInsiderDisplayName(it.who, rawPos));
              const insiderProfileHref = insiderHref(insiderName, resolveSignalReportingCik(it));
              const confirmationScore = typeof it.confirmation_score === "number" && Number.isFinite(it.confirmation_score) ? it.confirmation_score : "--";
              const confirmationDirection = confirmationDirectionLabel(it.confirmation_direction);

              return (
                <article key={it.event_id} className="px-4 py-4">
                  <div className="grid grid-cols-[minmax(0,1fr)_auto] gap-3">
                    <div className="min-w-0">
                      <div className="flex min-w-0 flex-wrap items-center gap-2">
                        {it.symbol ? <AddTickerToWatchlist symbol={it.symbol} variant="compact" align="left" /> : null}
                        {tickerHref(it.symbol) ? (
                          <Link href={tickerHref(it.symbol)!} prefetch={false} className={`min-w-0 truncate text-sm ${tickerMonoLinkClassName}`}>
                            {it.symbol}
                          </Link>
                        ) : (
                          <span className="min-w-0 truncate font-mono text-sm font-semibold text-slate-300">{it.symbol}</span>
                        )}
                        <span className={`${pill} shrink-0 px-2.5 py-0.5 text-[11px] leading-none ${side.klass}`}>{side.label}</span>
                      </div>
                    </div>
                    <div className="shrink-0 text-right">
                      <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">{isInstitutional ? "Reported Value" : "Amount"}</div>
                      <div className="font-mono text-sm font-semibold text-slate-100" title={`${formatUSD(it.amount_min)} - ${formatUSD(it.amount_max)}`}>
                        {formatUSD(it.amount_max)}
                      </div>
                    </div>
                  </div>

                  <div className="mt-3 grid grid-cols-[auto_minmax(0,1fr)] items-center gap-3 text-xs text-slate-400">
                    <span className="font-mono text-[12px] text-slate-300" title={it.ts}>{formatSignalDate(it.ts)}</span>
                    <div className="min-w-0">
                      {isInsider ? (
                        <div className="flex min-w-0 items-center gap-2">
                          <span className="inline-flex shrink-0" title={rawPos ?? undefined}><Badge tone={roleTone} className="px-2 py-0.5 text-[10px]">{roleCode}</Badge></span>
                          {insiderProfileHref ? (
                            <Link href={insiderProfileHref} prefetch={false} className="min-w-0 truncate text-slate-100 hover:underline">{insiderName ?? "--"}</Link>
                          ) : (
                            <span className="min-w-0 truncate text-slate-100">{insiderName ?? "--"}</span>
                          )}
                        </div>
                      ) : isInstitutional ? (
                        <div className="flex min-w-0 items-center gap-2">
                          <span className="inline-flex shrink-0"><Badge tone="neutral" className="px-2 py-0.5 text-[10px]">13F</Badge></span>
                          <span className="min-w-0 truncate text-slate-100">{it.who ?? "Institutional holders"}</span>
                        </div>
                      ) : (
                        <div className="flex min-w-0 items-center gap-2">
                          <span className="inline-flex shrink-0"><Badge tone={source.tone} className="px-2 py-0.5 text-[10px]">{source.label}</Badge></span>
                          {it.member_bioguide_id ? (
                            <Link href={memberHref({ name: it.who, memberId: it.member_bioguide_id })} prefetch={false} className="min-w-0 truncate text-slate-100 hover:underline">{it.who ?? "--"}</Link>
                          ) : (
                            <span className="min-w-0 truncate text-slate-100">{it.who ?? "--"}</span>
                          )}
                        </div>
                      )}
                    </div>
                  </div>

                  <div className="mt-3 grid grid-cols-2 gap-x-3 gap-y-3 border-t border-slate-800/70 pt-3 text-xs">
                    <div className="min-w-0">
                      <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-500" title={isInstitutional ? "Prior Q Value" : "Baseline"}>{isInstitutional ? "Prior Q Value" : "Base"}</div>
                      <div className="truncate font-mono text-slate-200">{formatUSD(it.baseline_median_amount_max)}</div>
                    </div>
                    <div className="min-w-0">
                      <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-500" title={isInstitutional ? "Delta %" : "Multiple"}>{isInstitutional ? "Delta %" : "Mult"}</div>
                      <div className="truncate font-mono text-slate-200">{isInstitutional ? `${(((it.unusual_multiple ?? 1) - 1) * 100).toFixed(1)}%` : formatMultiple(it.unusual_multiple)}</div>
                    </div>
                    <div className="min-w-0">
                      <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-500" title={isInstitutional ? "Institutional Score" : "Conviction"}>{isInstitutional ? "Institutional Score" : "Score"}</div>
                      <span className={`${pill} mt-1 min-w-0 max-w-full justify-center gap-1.5 px-2.5 py-1 text-[11px] leading-none ${smart.klass}`}>
                        <span className={`h-2 w-2 shrink-0 rounded-full ${smart.dotClass}`} />
                        <span className="font-mono">{typeof it.smart_score === "number" && Number.isFinite(it.smart_score) ? it.smart_score : "--"}</span>
                        <span className="min-w-0 truncate opacity-80">{smart.label}</span>
                      </span>
                    </div>
                    <div className="min-w-0">
                      <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">Source</div>
                      <div className="mt-1">
                        {isInsider ? (
                          <Badge tone="insider_default" className="px-2 py-0.5 text-[10px]">INSIDER</Badge>
                        ) : isInstitutional ? (
                          <Badge tone="neutral" className="px-2 py-0.5 text-[10px]">13F</Badge>
                        ) : (
                          <Badge tone={source.tone} className="px-2 py-0.5 text-[10px]">{source.label}</Badge>
                        )}
                      </div>
                    </div>
                    <div className="min-w-0">
                      <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-500" title="Confirmation">Conf.</div>
                      <div className="mt-1 flex min-w-0 items-center gap-1.5">
                        <span className="font-mono font-semibold text-slate-100">{confirmationScore}</span>
                        <span className="text-slate-600">&middot;</span>
                        <span className={`min-w-0 truncate font-medium ${confirmationDirectionTextClass(it.confirmation_direction)}`}>{confirmationDirection}</span>
                      </div>
                    </div>
                    <div className="min-w-0">
                      <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">Fresh</div>
                      <div
                        className="mt-1 min-w-0"
                        title={freshness ? `${freshness.freshness_label} - ${freshness.explanation}` : "Freshness unavailable"}
                      >
                        <span className={`block truncate text-xs font-medium ${freshnessTextClass(freshness?.freshness_state)}`}>
                          {titleCase(freshness?.freshness_state ?? "inactive")}
                        </span>
                      </div>
                    </div>
                  </div>
                </article>
              );
            })}
          </div>
        )}
      </div>
      <div className={`${signalsResultsScrollFrameClassName} hidden min-w-0 md:block`}>
        <table className="w-full min-w-[65rem] table-fixed border-collapse text-sm">
          <colgroup>
            <col className="w-[5rem]" />
            <col className="w-[5.25rem]" />
            <col className="w-[9.5rem]" />
            <col className="w-[4.5rem]" />
            <col className="w-[5.75rem]" />
            <col className="w-[5.25rem]" />
            <col className="w-[5rem]" />
            <col className="w-[9.25rem]" />
            <col className="w-[4.75rem]" />
            <col className="w-[5.75rem]" />
            <col className="w-[5rem]" />
          </colgroup>
          <thead className={`${stickyResultsTableHeaderClassName} whitespace-nowrap bg-slate-950 text-xs uppercase tracking-wider text-slate-400`}>
            <tr>
              <th className="px-2 py-3 text-left xl:px-3">{headerLabels.time}</th>
              <th className="px-2 py-3 text-left xl:px-3">Ticker</th>
              <th className="px-2 py-3 text-left xl:px-3">{headerLabels.actor}</th>
              <th className="px-2 py-3 text-left xl:px-3">{headerLabels.side}</th>
              <th className="px-2 py-3 text-left xl:px-3">{headerLabels.amount}</th>
              <th className="px-2 py-3 text-left xl:px-3">
                <SignalColumnHeaderTooltip id="signals-header-baseline" label={<span title={headerLabels.baseline}>{headerLabels.baseline}</span>} description={isInstitutionalMode ? "The prior quarter reported value when available." : SIGNALS_COLUMN_DEFINITIONS.baseline} />
              </th>
              <th className="px-2 py-3 text-left xl:px-3">
                <SignalColumnHeaderTooltip id="signals-header-multiple" label={<span title={headerLabels.multiple}>{headerLabels.multiple}</span>} description={isInstitutionalMode ? "Reported quarter-over-quarter value change percentage when available." : SIGNALS_COLUMN_DEFINITIONS.multiple} />
              </th>
              <th className="px-2 py-3 text-left xl:px-3">
                <SignalColumnHeaderTooltip id="signals-header-conviction" label={<span title={headerLabels.score}>{headerLabels.score}</span>} description={isInstitutionalMode ? "A materiality score for reported 13F institutional activity." : SIGNALS_COLUMN_DEFINITIONS.conviction} />
              </th>
              <th className="px-2 py-3 text-left xl:px-3">
                <SignalColumnHeaderTooltip id="signals-header-source" label="Source" description={SIGNALS_COLUMN_DEFINITIONS.source} align="right" />
              </th>
              <th className={`px-2 py-3 text-left xl:px-3 ${activeSort === "confirmation" ? "text-emerald-100" : ""}`}>
                <SignalColumnHeaderTooltip
                  id="signals-header-confirmation"
                  label={<SignalsSortLink label="Conf." active={activeSort === "confirmation"} title="Confirmation" />}
                  description={SIGNALS_COLUMN_DEFINITIONS.confirmation}
                  align="right"
                />
              </th>
              <th className={`px-2 py-3 text-left xl:px-3 ${activeSort === "freshness" ? "text-emerald-100" : ""}`}>
                <SignalColumnHeaderTooltip
                  id="signals-header-freshness"
                  label={<SignalsSortLink label="Fresh" active={activeSort === "freshness"} title="Freshness" />}
                  description={SIGNALS_COLUMN_DEFINITIONS.freshness}
                  align="right"
                />
              </th>
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-800">
            {items.length === 0 ? (
              <tr>
                <td className="px-4 py-10 text-center text-slate-400" colSpan={11}>
                  <div className="flex flex-col items-center gap-3">
                    <span>{errorMessage || "No unusual signals returned."}</span>
                    {showInstitutionalUpgradeCta ? <InstitutionalSignalsUpgradeCta upgradeUrl={upgradeUrl} /> : null}
                  </div>
                </td>
              </tr>
            ) : (
              items.map((it) => {
                const side = sideLabel(it.kind ?? "", it.trade_type);
                const smart = smartLabel(it.smart_band, it.smart_score);
                const freshness = it.signal_freshness;
                const source = sourceBadge(it);
                const isInsider = isInsiderSignalKind(it.kind);
                const isInstitutional = isInstitutionalSignalKind(it.kind);
                const rawPos = it.position ?? null;
                const roleCode = normalizeInsiderRoleBadge(rawPos);
                const roleTone = insiderRoleBadgeTone(roleCode);
                const insiderName = getInsiderDisplayName(resolveInsiderDisplayName(it.who, rawPos));
                const insiderProfileHref = insiderHref(insiderName, resolveSignalReportingCik(it));
                return (
                  <tr key={it.event_id} className="hover:bg-slate-900/20">
                    <td className="px-2 py-3 text-slate-300 xl:px-3">
                      <span className="font-mono text-[12px]" title={it.ts}>{formatSignalDate(it.ts)}</span>
                    </td>
                    <td className="px-2 py-3 xl:px-3">
                      <div className="flex min-w-0 items-center gap-1.5 xl:gap-2">
                        {it.symbol ? <AddTickerToWatchlist symbol={it.symbol} variant="compact" align="left" /> : null}
                        {tickerHref(it.symbol) ? (
                          <Link href={tickerHref(it.symbol)!} prefetch={false} className={`min-w-0 truncate ${tickerMonoLinkClassName}`}>{it.symbol}</Link>
                        ) : (
                          <span className="min-w-0 truncate font-mono text-slate-300">{it.symbol}</span>
                        )}
                      </div>
                    </td>
                    <td className="px-2 py-3 text-slate-200 xl:px-3">
                      {isInsider ? (
                        <div className="flex min-w-0 items-center gap-2">
                          <span title={rawPos ?? undefined}><Badge tone={roleTone}>{roleCode}</Badge></span>
                          {insiderProfileHref ? (
                            <Link href={insiderProfileHref} prefetch={false} className="min-w-0 truncate text-slate-100 hover:underline">{insiderName ?? "—"}</Link>
                          ) : (
                            <span className="min-w-0 truncate text-slate-100">{insiderName ?? "—"}</span>
                          )}
                        </div>
                      ) : isInstitutional ? (
                        <div className="flex min-w-0 items-center gap-2 overflow-hidden">
                          <span className="inline-flex shrink-0 align-middle"><Badge tone="neutral" className="px-2 py-0.5 text-[10px]">13F</Badge></span>
                          <span className="truncate">{it.who ?? "Institutional holders"}</span>
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
                    <td className="px-2 py-3 xl:px-3"><span className={`${pill} max-w-full px-2.5 ${side.klass}`}>{side.label}</span></td>
                    <td className="px-2 py-3 text-slate-200 xl:px-3" title={`${formatUSD(it.amount_min)} – ${formatUSD(it.amount_max)}`}>{formatUSD(it.amount_max)}</td>
                    <td className="px-2 py-3 text-slate-200 xl:px-3">{formatUSD(it.baseline_median_amount_max)}</td>
                    <td className="px-2 py-3 text-slate-200 xl:px-3">{isInstitutional ? `${(((it.unusual_multiple ?? 1) - 1) * 100).toFixed(1)}%` : formatMultiple(it.unusual_multiple)}</td>
                    <td className="px-2 py-3 xl:px-3">
                      <span className={`${pill} min-w-[7.75rem] max-w-full justify-center gap-1.5 px-2 text-[11px] leading-none ${smart.klass}`}>
                        <span className={`h-2 w-2 rounded-full ${smart.dotClass}`} />
                        <span className="font-mono">{typeof it.smart_score === "number" && Number.isFinite(it.smart_score) ? it.smart_score : "—"}</span>
                        <span className="min-w-0 truncate opacity-80">{smart.label}</span>
                      </span>
                    </td>
                    <td className="px-2 py-3 xl:px-3">
                      {isInsider ? (
                        <Badge tone="insider_default" className="px-2 py-0.5 text-[10px]">INSIDER</Badge>
                      ) : isInstitutional ? (
                        <Badge tone="neutral" className="px-2 py-0.5 text-[10px]">13F</Badge>
                      ) : (
                        <Badge tone={source.tone} className="px-2 py-0.5 text-[10px]">{source.label}</Badge>
                      )}
                    </td>
                    <td className="px-2 py-3 xl:px-3">
                      <div className="w-full min-w-0">
                        <ConfirmHoverCell item={it} />
                      </div>
                    </td>
                    <td className="px-2 py-3 xl:px-3">
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
