import Link from "next/link";
import type { ReactNode } from "react";
import { Suspense } from "react";
import { Badge } from "@/components/Badge";
import { getEvents, getSignalsAll, getTickerChartBundle, getTickerProfile, type TickerChartBundle } from "@/lib/api";
import { PremiumTickerChart, PremiumTickerChartSkeleton } from "@/components/ticker/PremiumTickerChart";
import { TickerContextCard } from "@/components/ticker/TickerContextCard";
import { AddTickerToWatchlist } from "@/components/watchlists/AddTickerToWatchlist";
import { SkeletonBlock } from "@/components/ui/LoadingSkeleton";
import {
  cardClassName,
  compactInteractiveSurfaceClassName,
  compactInteractiveTitleClassName,
  ghostButtonClassName,
  pillClassName,
} from "@/lib/styles";
import {
  chamberBadge,
  formatCurrency,
  formatCurrencyRange,
  formatDateShort,
  formatTransactionLabel,
  partyBadge,
  transactionTone,
} from "@/lib/format";
import { memberHref } from "@/lib/memberSlug";
import { tickerHref } from "@/lib/ticker";
import { getInsiderDisplayName, insiderHref } from "@/lib/insider";
import { insiderRoleBadgeTone, resolveInsiderRoleBadge } from "@/lib/insiderRole";
import { SmartSignalPill } from "@/components/ui/SmartSignalPill";
import { resolveSmartSignalValue } from "@/lib/smartSignal";
import {
  resolveInsiderDisplayPrice,
} from "@/lib/insiderTradeDisplay";
import { resolveInsiderActivityDisplay } from "@/lib/tradeDisplay";
import { optionalPageAuthToken } from "@/lib/serverAuth";

type Props = {
  params: Promise<{ symbol: string }>;
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
};

type Lookback = "30" | "90" | "180" | "365";
type SourceFilter = "all" | "congress" | "insider" | "signals";
type SideFilter = "all" | "buy" | "sell";
type ParticipantStats = {
  name: string;
  trades: number;
  buys: number;
  sells: number;
  netFlow: number;
  href?: string;
  reportingCik?: string;
};

type ConfirmationSummary = {
  congress_active_30d: boolean;
  insider_active_30d: boolean;
  congress_trade_count_30d: number;
  insider_trade_count_30d: number;
  insider_buy_count_30d: number;
  insider_sell_count_30d: number;
  cross_source_confirmed_30d: boolean;
  repeat_congress_30d: boolean;
  repeat_insider_30d: boolean;
};
type ConfirmationScoreBundle = NonNullable<Awaited<ReturnType<typeof getTickerProfile>>["confirmation_score_bundle"]>;
type SignalFreshnessBundle = NonNullable<Awaited<ReturnType<typeof getTickerProfile>>["signal_freshness"]>;
type OptionsFlowSummary = NonNullable<Awaited<ReturnType<typeof getTickerProfile>>["options_flow_summary"]>;
type TechnicalIndicators = NonNullable<Awaited<ReturnType<typeof getTickerProfile>>["technical_indicators"]>;
type ConfirmationSourceKey = keyof ConfirmationScoreBundle["sources"];

type TickerActivityData = {
  events: Awaited<ReturnType<typeof getEvents>>["items"];
  signals: Awaited<ReturnType<typeof getSignalsAll>>["items"];
  signalsUnavailableMessage: string | null;
  congressEvents: Awaited<ReturnType<typeof getEvents>>["items"];
  insiderEvents: Awaited<ReturnType<typeof getEvents>>["items"];
  congressBuys: number;
  congressSells: number;
  insiderBuys: number;
  insiderSells: number;
  netFlow: number;
  topSignal: (Awaited<ReturnType<typeof getSignalsAll>>["items"])[number] | undefined;
  congressParticipantCount: number;
  insiderParticipantCount: number;
  topCongressParticipants: ParticipantStats[];
  topInsiderParticipants: ParticipantStats[];
};

function one(sp: Record<string, string | string[] | undefined>, key: string): string {
  const value = sp[key];
  return typeof value === "string" ? value : "";
}

function clampLookback(v: string): Lookback {
  return v === "30" || v === "90" || v === "180" || v === "365" ? v : "365";
}

function clampSource(v: string): SourceFilter {
  return v === "congress" || v === "insider" || v === "signals" || v === "all" ? v : "all";
}

function clampSide(v: string): SideFilter {
  return v === "buy" || v === "sell" || v === "all" ? v : "all";
}

function normalizeTradeSide(value?: string | null): "buy" | "sell" | null {
  const t = (value ?? "").trim().toLowerCase();
  if (!t) return null;
  if (t.includes("buy") || t.includes("purchase") || t.startsWith("p-")) return "buy";
  if (t.includes("sell") || t.includes("sale") || t.startsWith("s-")) return "sell";
  return null;
}

function toDateKey(value?: string | null): string | null {
  const raw = (value ?? "").trim();
  if (!raw) return null;
  const day = raw.slice(0, 10);
  return /^\d{4}-\d{2}-\d{2}$/.test(day) ? day : null;
}

function asTrimmedString(value: unknown): string | null {
  if (typeof value !== "string") return null;
  const cleaned = value.trim();
  return cleaned ? cleaned : null;
}

function canonicalize(value: unknown): string {
  return typeof value === "string" ? value.trim().toLowerCase() : "";
}

function normalizedAmountLabel(min?: number | null, max?: number | null): string {
  const minValue = Number.isFinite(min) ? Number(min) : null;
  const maxValue = Number.isFinite(max) ? Number(max) : null;
  return `${minValue ?? ""}-${maxValue ?? ""}`;
}

function tickerHeaderMetadata(ticker: Awaited<ReturnType<typeof getTickerProfile>>["ticker"]): string[] {
  return [ticker.sector, ticker.industry, ticker.country, ticker.exchange]
    .map((value) => (typeof value === "string" ? value.trim() : ""))
    .filter((value): value is string => Boolean(value));
}

function payloadDateKey(payload: any): string {
  const raw = payload?.raw && typeof payload.raw === "object" ? payload.raw : null;
  return (
    toDateKey(asTrimmedString(payload?.transaction_date)) ??
    toDateKey(asTrimmedString(payload?.trade_date)) ??
    toDateKey(asTrimmedString(raw?.transactionDate)) ??
    toDateKey(asTrimmedString(raw?.tradeDate)) ??
    ""
  );
}

function dedupeByKey<T>(items: T[], keyFor: (item: T) => string): T[] {
  const seen = new Set<string>();
  return items.filter((item) => {
    const key = keyFor(item);
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

function stableEventIdentity(event: { event_type?: string | null; source?: string | null; payload?: any }): string | null {
  const payload = event.payload && typeof event.payload === "object" ? event.payload : null;
  const raw = payload?.raw && typeof payload.raw === "object" ? payload.raw : null;
  const transaction = payload?.transaction && typeof payload.transaction === "object" ? payload.transaction : null;

  const stableId =
    asTrimmedString(payload?.event_id) ??
    asTrimmedString(payload?.external_id) ??
    asTrimmedString(payload?.transaction_id) ??
    asTrimmedString(payload?.transactionId) ??
    asTrimmedString(transaction?.id) ??
    asTrimmedString(payload?.filing_id) ??
    asTrimmedString(payload?.filingId) ??
    asTrimmedString(payload?.disclosure_id) ??
    asTrimmedString(payload?.disclosureId) ??
    asTrimmedString(raw?.id) ??
    asTrimmedString(raw?.transaction_id) ??
    asTrimmedString(raw?.transactionId) ??
    asTrimmedString(raw?.filing_id) ??
    asTrimmedString(raw?.filingId) ??
    asTrimmedString(raw?.disclosure_id) ??
    asTrimmedString(raw?.disclosureId);

  if (!stableId) return null;
  return [canonicalize(event.event_type), canonicalize(event.source), canonicalize(stableId)].join("|");
}

function readNumeric(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string") {
    const cleaned = value.replace(/[$,]/g, "").trim();
    if (!cleaned) return null;
    const parsed = Number(cleaned);
    if (Number.isFinite(parsed)) return parsed;
  }
  return null;
}

function resolveInsiderName(event: { member_name?: string | null; payload?: any }): string {
  const payload = event.payload;
  const raw = payload?.raw && typeof payload.raw === "object" ? payload.raw : null;
  const insider = payload?.insider && typeof payload.insider === "object" ? payload.insider : null;

  return (
    getInsiderDisplayName(
      asTrimmedString(payload?.insider_name),
      asTrimmedString(insider?.name),
      asTrimmedString(raw?.reportingName),
      asTrimmedString(raw?.reportingOwnerName),
      asTrimmedString(raw?.ownerName),
      asTrimmedString(raw?.insiderName),
      asTrimmedString(event.member_name),
    ) ?? "Unknown Insider"
  );
}



function resolveInsiderReportingCik(event: { payload?: any }): string | null {
  const payload = event.payload;
  const raw = payload?.raw && typeof payload.raw === "object" ? payload.raw : null;
  return (
    asTrimmedString(payload?.reporting_cik) ??
    asTrimmedString(payload?.reportingCik) ??
    asTrimmedString(raw?.reportingCik) ??
    asTrimmedString(raw?.reportingCIK) ??
    null
  );
}

function resolveInsiderRole(event: { payload?: any }): string | null {
  const payload = event.payload && typeof event.payload === "object" ? event.payload : null;
  const raw = payload?.raw && typeof payload.raw === "object" ? payload.raw : null;
  const insider = payload?.insider && typeof payload.insider === "object" ? payload.insider : null;

  return (
    asTrimmedString(payload?.role) ??
    asTrimmedString(payload?.typeOfOwner) ??
    asTrimmedString(payload?.position) ??
    asTrimmedString(payload?.officer_title) ??
    asTrimmedString(payload?.officerTitle) ??
    asTrimmedString(insider?.role) ??
    asTrimmedString(insider?.position) ??
    asTrimmedString(raw?.typeOfOwner) ??
    asTrimmedString(raw?.officerTitle) ??
    asTrimmedString(raw?.insiderRole) ??
    asTrimmedString(raw?.position) ??
    null
  );
}

function resolveCongressPayloadMember(event: { payload?: any }) {
  const payload = event.payload && typeof event.payload === "object" ? event.payload : null;
  return payload?.member && typeof payload.member === "object" ? payload.member : null;
}

function resolveCongressChamber(event: { chamber?: string | null; payload?: any }): string | null {
  const payload = event.payload && typeof event.payload === "object" ? event.payload : null;
  const memberPayload = resolveCongressPayloadMember(event);
  return (
    asTrimmedString(memberPayload?.chamber) ??
    asTrimmedString(payload?.chamber) ??
    asTrimmedString(payload?.raw?.chamber) ??
    asTrimmedString(event.chamber) ??
    null
  );
}

function resolveCongressParty(event: { party?: string | null; payload?: any }): string | null {
  const payload = event.payload && typeof event.payload === "object" ? event.payload : null;
  const memberPayload = resolveCongressPayloadMember(event);
  return (
    asTrimmedString(memberPayload?.party) ??
    asTrimmedString(payload?.party) ??
    asTrimmedString(payload?.raw?.party) ??
    asTrimmedString(event.party) ??
    null
  );
}

function resolveCongressState(event: { state?: string | null; payload?: any }): string | null {
  const payload = event.payload && typeof event.payload === "object" ? event.payload : null;
  const memberPayload = resolveCongressPayloadMember(event);
  return (
    asTrimmedString(memberPayload?.state) ??
    asTrimmedString(payload?.state) ??
    asTrimmedString(payload?.raw?.state) ??
    asTrimmedString(event.state) ??
    null
  );
}

function formatCongressIdentity(event: { member_name?: string | null; party?: string | null; payload?: any }): string {
  const memberName = event.member_name?.trim() || "An unknown Congress member";
  const payload = event.payload && typeof event.payload === "object" ? event.payload : null;
  const memberPayload = payload?.member && typeof payload.member === "object" ? payload.member : null;
  const party = partyBadge(
    asTrimmedString(memberPayload?.party) ??
    asTrimmedString(payload?.party) ??
    event.party ??
    null,
  ).label;
  const state =
    asTrimmedString(memberPayload?.state) ??
    asTrimmedString(payload?.state) ??
    asTrimmedString(payload?.raw?.state) ??
    null;
  const suffix = [party !== "Ã¢â‚¬â€" ? party : null, state ? state.toUpperCase() : null].filter(Boolean).join("-");
  return suffix ? `${memberName} (${suffix})` : memberName;
}

function formatInsiderIdentity(event: { member_name?: string | null; payload?: any }): string {
  const insiderName = resolveInsiderName(event);
  const role = resolveInsiderRole(event);
  return role ? `${insiderName} (${role})` : insiderName;
}

function resolveCongressTradeDate(event: { ts?: string | null; payload?: any }): string | null {
  const payload = event.payload && typeof event.payload === "object" ? event.payload : null;
  return (
    asTrimmedString(payload?.trade_date) ??
    asTrimmedString(payload?.transaction_date) ??
    asTrimmedString(payload?.raw?.tradeDate) ??
    asTrimmedString(payload?.raw?.transactionDate) ??
    asTrimmedString(event.ts) ??
    null
  );
}

function resolveCongressReportDate(event: { ts?: string | null; payload?: any }): string | null {
  const payload = event.payload && typeof event.payload === "object" ? event.payload : null;
  return (
    asTrimmedString(payload?.report_date) ??
    asTrimmedString(payload?.filing_date) ??
    asTrimmedString(payload?.raw?.reportDate) ??
    asTrimmedString(payload?.raw?.filingDate) ??
    asTrimmedString(event.ts) ??
    null
  );
}

function resolveCongressTradePrice(event: { estimated_price?: number | null; payload?: any }): number | null {
  const payload = event.payload && typeof event.payload === "object" ? event.payload : null;
  return (
    readNumeric(event.estimated_price) ??
    readNumeric(payload?.estimated_price) ??
    readNumeric(payload?.price) ??
    readNumeric(payload?.raw?.price) ??
    readNumeric(payload?.raw?.estimatedPrice) ??
    null
  );
}

function resolveInsiderTradeDate(event: { ts?: string | null; payload?: any }): string | null {
  const payload = event.payload && typeof event.payload === "object" ? event.payload : null;
  return (
    asTrimmedString(payload?.transaction_date) ??
    asTrimmedString(payload?.trade_date) ??
    asTrimmedString(payload?.raw?.transactionDate) ??
    asTrimmedString(payload?.raw?.tradeDate) ??
    asTrimmedString(event.ts) ??
    null
  );
}

function resolveInsiderFilingDate(event: { ts?: string | null; payload?: any }): string | null {
  const payload = event.payload && typeof event.payload === "object" ? event.payload : null;
  return (
    asTrimmedString(payload?.filing_date) ??
    asTrimmedString(payload?.raw?.filingDate) ??
    asTrimmedString(event.ts) ??
    null
  );
}

function resolveInsiderTradePrice(event: { estimated_price?: number | null; payload?: any }): number | null {
  return resolveInsiderDisplayPrice(event);
}

function latestEvent<T extends { ts?: string | null }>(events: T[]): T | null {
  if (events.length === 0) return null;
  return [...events].sort((a, b) => {
    const aTime = Date.parse(a.ts ?? "");
    const bTime = Date.parse(b.ts ?? "");
    if (!Number.isNaN(aTime) && !Number.isNaN(bTime)) return bTime - aTime;
    if (!Number.isNaN(aTime)) return -1;
    if (!Number.isNaN(bTime)) return 1;
    return 0;
  })[0] ?? null;
}

function buildCrossSourceSummary({
  confirmation,
  congressEvents,
  insiderEvents,
}: {
  confirmation: ConfirmationSummary | null;
  congressEvents: Awaited<ReturnType<typeof getEvents>>["items"];
  insiderEvents: Awaited<ReturnType<typeof getEvents>>["items"];
}): string {
  const congressEvent = latestEvent(congressEvents);
  const insiderEvent = latestEvent(insiderEvents);

  if (!congressEvent && !insiderEvent) {
    return confirmation?.cross_source_confirmed_30d
      ? "Congress and insider activity are both flagged in the last 30 days, but detailed trade records are not available in the current filter."
      : "No matching Congress or insider trade details are available in the current filter.";
  }

  const parts: string[] = [];

  if (congressEvent) {
    const memberName = formatCongressIdentity(congressEvent);
    const side = formatTransactionLabel(congressEvent.trade_type).toLowerCase();
    const price = resolveCongressTradePrice(congressEvent);
    const tradeDate = formatDateShort(resolveCongressTradeDate(congressEvent));
    const reportDate = formatDateShort(resolveCongressReportDate(congressEvent));
    parts.push(
      `${memberName} ${side} at ${price !== null ? formatCurrency(price) : "an undisclosed price"} on ${tradeDate} (reported ${reportDate}).`,
    );
  }

  if (insiderEvent) {
    const insiderName = formatInsiderIdentity(insiderEvent);
    const side = formatTransactionLabel(insiderEvent.trade_type).toLowerCase();
    const price = resolveInsiderTradePrice(insiderEvent);
    const tradeDate = formatDateShort(resolveInsiderTradeDate(insiderEvent));
    const filingDate = formatDateShort(resolveInsiderFilingDate(insiderEvent));
    parts.push(
      `${insiderName} ${side} at ${price !== null ? formatCurrency(price) : "an undisclosed price"} on ${tradeDate} (filed ${filingDate}).`,
    );
  }

  return parts.join(" ");
}

function formatCompactUsd(value: number): string {
  const abs = Math.abs(value);
  if (abs >= 1_000_000_000) return `${(value / 1_000_000_000).toFixed(2)}B`;
  if (abs >= 1_000_000) return `${(value / 1_000_000).toFixed(2)}M`;
  if (abs >= 1_000) return `${(value / 1_000).toFixed(1)}K`;
  return value.toFixed(0);
}

function formatPnl(value: number): string {
  const marker = value > 0 ? "+" : value < 0 ? "-" : "";
  return `${marker} ${Math.abs(value).toFixed(1)}%`;
}

function pnlClass(value: number): string {
  if (value > 0) return "text-emerald-300";
  if (value < 0) return "text-rose-300";
  return "text-slate-300";
}

function biasLabel(buys: number, sells: number): { label: string; tone: "pos" | "neg" | "neutral" } {
  if (buys === 0 && sells === 0) return { label: "No side data", tone: "neutral" };
  if (buys > sells) return { label: "Buy-leaning", tone: "pos" };
  if (sells > buys) return { label: "Sell-leaning", tone: "neg" };
  return { label: "Balanced", tone: "neutral" };
}

function signalTone(band?: string): "pos" | "neutral" | "neg" {
  const value = (band ?? "").toLowerCase();
  if (value === "strong" || value === "notable") return "pos";
  if (value === "mild") return "neutral";
  return "neg";
}

function signalAccessMessage(error: unknown): string {
  const message = error instanceof Error ? error.message : String(error ?? "");
  if (message.includes("HTTP 401")) {
    return "Create a free account or log in to unlock premium ticker signals.";
  }
  if (message.includes("HTTP 403")) {
    return "Ticker signals are included with Premium.";
  }
  return "Ticker signals are temporarily unavailable.";
}

function insiderBiasLabel(confirmation: ConfirmationSummary | null): { label: string; tone: "pos" | "neg" | "neutral" } {
  if (!confirmation || !confirmation.insider_active_30d) return { label: "No insider side signal", tone: "neutral" };
  if (confirmation.insider_buy_count_30d > confirmation.insider_sell_count_30d) return { label: "Insider buy-skewed", tone: "pos" };
  if (confirmation.insider_sell_count_30d > confirmation.insider_buy_count_30d) return { label: "Insider sell-skewed", tone: "neg" };
  return { label: "Insider mixed", tone: "neutral" };
}

function inactiveConfirmationBundle(ticker: string): ConfirmationScoreBundle {
  return {
    ticker,
    lookback_days: 30,
    score: 0,
    band: "inactive",
    direction: "neutral",
    status: "Inactive",
    explanation: "Congress, insider, smart signal, and price confirmation sources are inactive for this lookback.",
    sources: {
      congress: { present: false, direction: "neutral", strength: 0, quality: 0, freshness_days: null, label: "Inactive" },
      insiders: { present: false, direction: "neutral", strength: 0, quality: 0, freshness_days: null, label: "Inactive" },
      signals: { present: false, direction: "neutral", strength: 0, quality: 0, freshness_days: null, label: "No current smart signal" },
      price_volume: { present: false, direction: "neutral", strength: 0, quality: 0, freshness_days: null, label: "No price confirmation" },
      options_flow: { present: false, direction: "neutral", strength: 0, quality: 0, freshness_days: null, label: "Options flow not confirming" },
    },
    drivers: ["Congress inactive", "Insiders inactive", "No current smart signal"],
  };
}

function TickerOverviewPanel({
  confirmationBundle,
  freshnessBundle,
  alignedSources,
  intelligenceBullets,
}: {
  confirmationBundle: ConfirmationScoreBundle;
  freshnessBundle: SignalFreshnessBundle;
  alignedSources: ConfirmationSourceKey[];
  intelligenceBullets: string[];
}) {
  const lookbackDays = confirmationBundle.lookback_days;
  const mutedLine = overviewMutedLine(confirmationBundle);

  return (
    <div>
      <div className="flex items-start justify-between gap-4">
        <div>
          <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-500">Overview</p>
          <p className="mt-2 text-[11px] uppercase tracking-[0.12em] text-slate-600">{lookbackDays}D confirmation</p>
        </div>
        <span className="text-[11px] uppercase tracking-[0.12em] text-slate-500">{overviewTimestamp(freshnessBundle)}</span>
      </div>

      <div className="mt-7">
        <p className="max-w-3xl text-2xl font-semibold leading-tight text-white md:text-3xl">
          {overviewHeadline(confirmationBundle)}
        </p>
        <p className="mt-3 text-sm text-slate-300">{overviewSubheadline(alignedSources)}</p>
        <p className={`mt-4 text-base font-semibold ${sourceStateClass(confirmationBundle.direction)}`}>{overviewScoreLine(confirmationBundle)}</p>
      </div>

      <div className="mt-7 grid gap-3 text-sm text-slate-300">
        {intelligenceBullets.map((bullet) => (
          <div key={bullet} className="flex gap-3">
            <span className={`mt-2 h-1.5 w-1.5 shrink-0 rounded-full ${confirmationBundle.direction === "bearish" ? "bg-rose-300" : confirmationBundle.direction === "bullish" ? "bg-emerald-300" : "bg-slate-500"}`} />
            <p className="leading-relaxed">{bullet}</p>
          </div>
        ))}
      </div>

      <div className="mt-6 rounded-2xl border border-white/10 bg-slate-950/45 px-4 py-3">
        <div className="flex items-center justify-between gap-3">
          <p className="text-sm font-semibold text-slate-200">{setupTimingLabel(freshnessBundle)} / {Math.round(freshnessBundle.freshness_score)}/100</p>
          <p className="text-[11px] uppercase tracking-[0.12em] text-slate-500">Freshness</p>
        </div>
        <p className="mt-1 text-[11px] leading-relaxed text-slate-500">{timingDetailLine(freshnessBundle)}</p>
      </div>

      {mutedLine ? <p className="mt-6 text-sm text-slate-500">{mutedLine}</p> : null}
      <p className="mt-4 border-t border-white/10 pt-4 text-xs leading-relaxed text-slate-500">{overviewCaveat(confirmationBundle)}</p>
    </div>
  );
}

function inactiveTechnicalIndicators(): TechnicalIndicators {
  return {
    source: "daily_close_history",
    asof: null,
    price_points: 0,
    rsi: {
      status: "unavailable",
      signal: "unavailable",
      message: "RSI temporarily unavailable",
      reason: "provider_error",
      value: null,
      period: 14,
    },
    macd: {
      status: "unavailable",
      signal: "unavailable",
      message: "MACD temporarily unavailable",
      reason: "provider_error",
      value: null,
    },
    ema_trend: {
      status: "unavailable",
      signal: "unavailable",
      message: "EMA trend temporarily unavailable",
      reason: "provider_error",
      value: null,
    },
  };
}

function inactiveOptionsFlowSummary(ticker: string): OptionsFlowSummary {
  return {
    ticker,
    lookback_days: 30,
    state: "unavailable",
    label: "Options flow unavailable",
    is_active: false,
    confidence: "low",
    freshness_days: null,
    summary: "Options flow unavailable.",
    signals: ["Options flow unavailable"],
    metrics: {
      put_call_premium_ratio: null,
      net_premium_skew: 0,
      recent_contract_volume: 0,
      observed_contracts: 0,
      freshness_days: null,
    },
    can_confirm: false,
    provider: "options",
  };
}

function inactiveSignalFreshnessBundle(ticker: string): SignalFreshnessBundle {
  return {
    ticker,
    lookback_days: 30,
    freshness_score: 0,
    freshness_state: "inactive",
    freshness_label: "No active setup",
    explanation: "No active directional confirmation sources are present in this lookback.",
    timing: {
      freshest_source_days: null,
      stalest_active_source_days: null,
      active_source_count: 0,
      overlap_window_days: null,
    },
  };
}

function normalizeConfirmationBundle(bundle: ConfirmationScoreBundle | null | undefined, ticker: string): ConfirmationScoreBundle {
  const fallback = inactiveConfirmationBundle(ticker);
  if (!bundle) return fallback;
  return {
    ...fallback,
    ...bundle,
    ticker: bundle.ticker || ticker,
    sources: {
      congress: { ...fallback.sources.congress, ...(bundle.sources?.congress ?? {}) },
      insiders: { ...fallback.sources.insiders, ...(bundle.sources?.insiders ?? {}) },
      signals: { ...fallback.sources.signals, ...(bundle.sources?.signals ?? {}) },
      price_volume: { ...fallback.sources.price_volume, ...(bundle.sources?.price_volume ?? {}) },
      options_flow: { ...fallback.sources.options_flow, ...(bundle.sources?.options_flow ?? {}) },
    },
    drivers: Array.isArray(bundle.drivers) && bundle.drivers.length > 0 ? bundle.drivers.slice(0, 4) : fallback.drivers,
  };
}

function normalizeOptionsFlowSummary(bundle: OptionsFlowSummary | null | undefined, ticker: string): OptionsFlowSummary {
  const fallback = inactiveOptionsFlowSummary(ticker);
  if (!bundle) return fallback;
  return {
    ...fallback,
    ...bundle,
    ticker: bundle.ticker || ticker,
    signals: Array.isArray(bundle.signals) && bundle.signals.length > 0 ? bundle.signals.slice(0, 4) : fallback.signals,
    metrics: {
      ...fallback.metrics,
      ...(bundle.metrics ?? {}),
    },
  };
}

function normalizeSignalFreshness(bundle: SignalFreshnessBundle | null | undefined, ticker: string): SignalFreshnessBundle {
  const fallback = inactiveSignalFreshnessBundle(ticker);
  if (!bundle) return fallback;
  return {
    ...fallback,
    ...bundle,
    ticker: bundle.ticker || ticker,
    timing: {
      ...fallback.timing,
      ...(bundle.timing ?? {}),
    },
  };
}

function normalizeTechnicalIndicators(bundle: TechnicalIndicators | null | undefined): TechnicalIndicators {
  const fallback = inactiveTechnicalIndicators();
  if (!bundle) return fallback;
  return {
    ...fallback,
    ...bundle,
    rsi: { ...fallback.rsi, ...(bundle.rsi ?? {}) },
    macd: { ...fallback.macd, ...(bundle.macd ?? {}) },
    ema_trend: { ...fallback.ema_trend, ...(bundle.ema_trend ?? {}) },
  };
}

const confirmationSourceLabels: Record<ConfirmationSourceKey, string> = {
  congress: "Congress",
  insiders: "Insiders",
  signals: "Signals",
  price_volume: "Price / Volume",
  options_flow: "Options Flow",
};

const confirmationSourceOrder: ConfirmationSourceKey[] = ["congress", "insiders", "signals", "price_volume", "options_flow"];

function sourceStateClass(direction: ConfirmationScoreBundle["direction"] | "inactive"): string {
  if (direction === "bullish") return "text-emerald-300";
  if (direction === "bearish") return "text-rose-300";
  if (direction === "mixed") return "text-amber-300";
  return "text-slate-400";
}

function technicalToneClass(tone: "bullish" | "bearish" | "mixed" | "inactive" | "unavailable"): string {
  if (tone === "bullish") return "text-emerald-300";
  if (tone === "bearish") return "text-rose-300";
  if (tone === "mixed") return "text-amber-300";
  if (tone === "unavailable") return "text-slate-500";
  return "text-slate-400";
}

function sourceStateLabel(source: ConfirmationScoreBundle["sources"][ConfirmationSourceKey]): string {
  return source.present ? source.direction.toUpperCase() : "INACTIVE";
}

function sourceFreshnessLabel(source: ConfirmationScoreBundle["sources"][ConfirmationSourceKey]): string {
  if (!source.present) return "Inactive";
  if (source.freshness_days === null || source.freshness_days === undefined) return "Freshness unavailable";
  if (source.freshness_days === 0) return "Seen today";
  if (source.freshness_days === 1) return "Seen 1d ago";
  return `Seen ${source.freshness_days}d ago`;
}

function formatConfirmationSourceList(keys: ConfirmationSourceKey[]): string {
  if (keys.length === 0) return "No active sources";
  return keys.map((key) => confirmationSourceLabels[key]).join(" + ");
}

function alignedConfirmationSources(bundle: ConfirmationScoreBundle): ConfirmationSourceKey[] {
  if (bundle.direction === "neutral" || bundle.direction === "mixed") {
    return confirmationSourceOrder.filter((key) => bundle.sources[key].present);
  }
  return confirmationSourceOrder.filter((key) => {
    const source = bundle.sources[key];
    return source.present && source.direction === bundle.direction;
  });
}

function inactiveOrUnalignedSourceLine(bundle: ConfirmationScoreBundle, alignedSources: ConfirmationSourceKey[]): string {
  const aligned = new Set(alignedSources);
  const parts = confirmationSourceOrder
    .filter((key) => !aligned.has(key))
    .map((key) => {
      const source = bundle.sources[key];
      if (!source.present) return `${confirmationSourceLabels[key]} inactive`;
      return `${confirmationSourceLabels[key]} ${source.direction}`;
    });
  return parts.length > 0 ? parts.join(" Â· ") : "All tracked sources aligned";
}

function setupTimingLabel(freshness: SignalFreshnessBundle): string {
  if (freshness.timing.active_source_count <= 0) return "Timing inactive";
  if (freshness.freshness_state === "stale") return "Stale setup";
  if (freshness.freshness_state === "maturing") return "Maturing setup";
  return "Fresh setup";
}

function timingDetailLine(freshness: SignalFreshnessBundle): string {
  const timing = freshness.timing;
  const freshest = timing.freshest_source_days === null ? "--" : `${timing.freshest_source_days}d`;
  const oldest = timing.stalest_active_source_days === null ? "--" : `${timing.stalest_active_source_days}d`;
  const overlap = timing.overlap_window_days === null ? "--" : `${timing.overlap_window_days}d`;
  return `${freshest} freshest Â· ${oldest} oldest Â· ${overlap} overlap`;
}

function overviewTimestamp(freshness: SignalFreshnessBundle): string {
  const freshest = freshness.timing.freshest_source_days;
  if (freshest === null || freshest === undefined) return "Updated --";
  if (freshest === 0) return "Updated today";
  return `Updated ${freshest}d ago`;
}

function overviewHeadline(bundle: ConfirmationScoreBundle): string {
  if (bundle.direction === "bearish") return "Bearish confirmation";
  if (bundle.direction === "bullish") return "Bullish confirmation";
  if (bundle.direction === "mixed") return "Mixed confirmation";
  return "No active confirmation";
}

function overviewSubheadline(alignedSources: ConfirmationSourceKey[]): string {
  if (alignedSources.length <= 0) return "No active sources aligned.";
  return `${alignedSources.length} active source${alignedSources.length === 1 ? "" : "s"} aligned.`;
}

function capitalizeWord(value: string): string {
  if (!value) return value;
  return `${value.slice(0, 1).toUpperCase()}${value.slice(1)}`;
}

function overviewScoreLine(bundle: ConfirmationScoreBundle): string {
  if (bundle.band === "inactive" && bundle.direction === "neutral") {
    return `${Math.round(bundle.score)} / 100 Â· Inactive`;
  }
  return `${Math.round(bundle.score)} / 100 Â· ${capitalizeWord(bundle.band)} ${bundle.direction}`;
}

function overviewBullets({
  confirmationBundle,
  alignedSources,
}: {
  confirmationBundle: ConfirmationScoreBundle;
  alignedSources: ConfirmationSourceKey[];
}): string[] {
  const bullets = new Set<string>();
  const activeLabels = Array.from(new Set(alignedSources.map((key) => confirmationSourceLabels[key])));
  if (activeLabels.length > 0) bullets.add(`Active sources: ${activeLabels.join(" Â· ")}`);
  if (confirmationBundle.sources.insiders.present) {
    if (confirmationBundle.sources.insiders.direction === "bearish") bullets.add("Insider activity: active / sell-skewed");
    else if (confirmationBundle.sources.insiders.direction === "bullish") bullets.add("Insider activity: active / buy-skewed");
    else bullets.add("Insider activity: active / balanced");
  }
  if (confirmationBundle.sources.signals.present) {
    if (confirmationBundle.sources.signals.direction === "bearish") bullets.add("Signals: confirmed bearish");
    else if (confirmationBundle.sources.signals.direction === "bullish") bullets.add("Signals: confirmed bullish");
    else bullets.add("Signals: mixed");
  }
  if (confirmationBundle.sources.congress.present) {
    if (confirmationBundle.sources.congress.direction === "bearish") bullets.add("Congress activity: active / sell-skewed");
    else if (confirmationBundle.sources.congress.direction === "bullish") bullets.add("Congress activity: active / buy-skewed");
    else bullets.add("Congress activity: active / mixed");
  }
  return Array.from(bullets).slice(0, 3);
}

function overviewMutedLine(bundle: ConfirmationScoreBundle): string | null {
  if (!bundle.sources.price_volume.present && !bundle.sources.options_flow.present) {
    return "Price / volume and options flow are inactive.";
  }
  return null;
}

function overviewCaveat(bundle: ConfirmationScoreBundle): string {
  if (!bundle.sources.signals.present) return "No smart signal is reinforcing this move.";
  return "Smart signal activity is reinforcing this move.";
}

function priceVolumeSummary(
  source: ConfirmationScoreBundle["sources"]["price_volume"],
  technicalIndicators: TechnicalIndicators,
): { state: string; summary: string; diagnostics: string[]; tone: "bullish" | "bearish" | "mixed" | "inactive" | "unavailable" } {
  const diagnostics = [
    technicalIndicators.rsi.message,
    technicalIndicators.macd.message,
    technicalIndicators.ema_trend.message,
  ];
  const indicatorsUnavailable = diagnostics.every((item) => item.toLowerCase().includes("unavailable"));
  if (!source.present && indicatorsUnavailable) {
    const insufficientHistory = [technicalIndicators.rsi, technicalIndicators.macd, technicalIndicators.ema_trend].some(
      (item) => item.reason === "insufficient_price_history",
    );
    return {
      state: "UNAVAILABLE",
      summary: insufficientHistory ? "Limited price history for technical indicators" : "Technical indicators temporarily unavailable",
      diagnostics,
      tone: "unavailable",
    };
  }
  if (!source.present) {
    return {
      state: "INACTIVE",
      summary: "No active tape confirmation",
      diagnostics,
      tone: "inactive",
    };
  }
  if (source.direction === "bearish") {
    return {
      state: "BEARISH",
      summary: "Bearish days with elevated volume",
      diagnostics,
      tone: "bearish",
    };
  }
  if (source.direction === "bullish") {
    return {
      state: "BULLISH",
      summary: "Bullish days with elevated volume",
      diagnostics,
      tone: "bullish",
    };
  }
  return {
    state: "MIXED",
    summary: "Tape confirmation is mixed",
    diagnostics,
    tone: "mixed",
  };
}

function sourceCardToneClass(source: ConfirmationScoreBundle["sources"][ConfirmationSourceKey]): string {
  return source.present ? sourceStateClass(source.direction) : "text-slate-500";
}

function sourceCardBorderClass(source: ConfirmationScoreBundle["sources"][ConfirmationSourceKey]): string {
  if (!source.present) return "border-white/10 bg-white/[0.025]";
  if (source.direction === "bearish") return "border-rose-400/20 bg-rose-400/[0.045]";
  if (source.direction === "bullish") return "border-emerald-400/20 bg-emerald-400/[0.045]";
  return "border-amber-400/20 bg-amber-400/[0.04]";
}

function optionsFlowToneClass(state: OptionsFlowSummary["state"]): string {
  if (state === "bullish") return "text-emerald-300";
  if (state === "bearish") return "text-rose-300";
  if (state === "mixed") return "text-amber-300";
  return "text-slate-500";
}

function optionsFlowBorderClass(summary: OptionsFlowSummary): string {
  if (summary.state === "bullish") return "border-emerald-400/20 bg-emerald-400/[0.045]";
  if (summary.state === "bearish") return "border-rose-400/20 bg-rose-400/[0.045]";
  if (summary.state === "mixed") return "border-amber-400/20 bg-amber-400/[0.04]";
  return "border-white/10 bg-white/[0.025]";
}

function optionsFlowDiagnostics(summary: OptionsFlowSummary): string[] {
  if (Array.isArray(summary.signals) && summary.signals.length > 0) return summary.signals.slice(0, 4);
  if (summary.state === "inactive") return ["No notable recent options flow"];
  if (summary.state === "unavailable") return ["Options flow unavailable"];
  return [summary.summary || "Options flow is active"];
}

function insiderSourceBody(buys: number, sells: number, source: ConfirmationScoreBundle["sources"]["insiders"]): string {
  if (!source.present) return "No recent activity";
  if (sells > buys) return "Active / sell-skewed";
  if (buys > sells) return "Active / buy-skewed";
  return "Active / balanced";
}

function insiderSourceSupport(buys: number, sells: number, lookbackDays: number): string {
  if (sells > buys) return `${sells - buys} net sells Â· ${lookbackDays}D`;
  if (buys > sells) return `${buys - sells} net buys Â· ${lookbackDays}D`;
  return `${buys + sells} trades Â· ${lookbackDays}D`;
}

function sourceCardBody(key: "congress" | "signals", source: ConfirmationScoreBundle["sources"][ConfirmationSourceKey], topSignal: TickerActivityData["topSignal"]): string {
  if (!source.present) return key === "congress" ? "No recent trades" : "No recent activity";
  if (key === "signals") return topSignal ? "Smart signal active" : "Signal source active";
  return source.direction === "bearish" ? "Active / sell-skewed" : source.direction === "bullish" ? "Active / buy-skewed" : "Active / mixed";
}

type IntelligenceIconKind =
  | "congress"
  | "insider-buy"
  | "insider-sell"
  | "signals"
  | "price-volume"
  | "flow"
  | "people";

function IntelligenceIcon({ kind, className = "h-4 w-4" }: { kind: IntelligenceIconKind; className?: string }) {
  if (kind === "congress") {
    return (
      <svg viewBox="0 0 24 24" aria-hidden="true" className={className} fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round">
        <path d="M4 9h16" />
        <path d="M5 19h14" />
        <path d="M7 9v10" />
        <path d="M12 9v10" />
        <path d="M17 9v10" />
        <path d="M3 21h18" />
        <path d="M12 3 4 7h16l-8-4Z" />
      </svg>
    );
  }
  if (kind === "signals") {
    return (
      <svg viewBox="0 0 24 24" aria-hidden="true" className={className} fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round">
        <path d="M4 12h2.5l2-5 3.5 10 3-7 2 2H20" />
        <path d="M4 19h16" opacity="0.45" />
      </svg>
    );
  }
  if (kind === "price-volume") {
    return (
      <svg viewBox="0 0 24 24" aria-hidden="true" className={className} fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round">
        <path d="M4 17 9 12l3 3 7-8" />
        <path d="M5 21V9" opacity="0.45" />
        <path d="M11 21v-5" opacity="0.45" />
        <path d="M17 21V7" opacity="0.45" />
      </svg>
    );
  }
  if (kind === "flow") {
    return (
      <svg viewBox="0 0 24 24" aria-hidden="true" className={className} fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round">
        <path d="M7 7h9.5a3.5 3.5 0 0 1 0 7H8" />
        <path d="m11 4-4 3 4 3" />
        <path d="M17 17H7.5a3.5 3.5 0 0 1 0-7H16" opacity="0.45" />
      </svg>
    );
  }
  if (kind === "people") {
    return (
      <svg viewBox="0 0 24 24" aria-hidden="true" className={className} fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round">
        <path d="M16 19v-1.5a3.5 3.5 0 0 0-3.5-3.5h-5A3.5 3.5 0 0 0 4 17.5V19" />
        <path d="M10 10a3 3 0 1 0 0-6 3 3 0 0 0 0 6Z" />
        <path d="M20 19v-1a3 3 0 0 0-2.2-2.9" opacity="0.55" />
        <path d="M16 4.4a3 3 0 0 1 0 5.8" opacity="0.55" />
      </svg>
    );
  }
  const isSell = kind === "insider-sell";
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true" className={className} fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round">
      <path d="M12 12a4 4 0 1 0 0-8 4 4 0 0 0 0 8Z" />
      <path d="M5 20a7 7 0 0 1 14 0" />
      <path d={isSell ? "M18 8v7" : "M18 15V8"} />
      <path d={isSell ? "m15 12 3 3 3-3" : "m15 11 3-3 3 3"} />
    </svg>
  );
}

function SourceEvidenceCard({
  title,
  icon,
  source,
  body,
  support,
}: {
  title: string;
  icon: IntelligenceIconKind;
  source: ConfirmationScoreBundle["sources"][ConfirmationSourceKey];
  body: string;
  support: string;
}) {
  return (
    <div className={`rounded-xl border px-3 py-3 ${sourceCardBorderClass(source)}`}>
      <div className="flex items-center justify-between gap-2">
        <div className="flex min-w-0 items-center gap-2">
          <span className={`shrink-0 ${sourceCardToneClass(source)}`}>
            <IntelligenceIcon kind={icon} />
          </span>
          <p className="whitespace-nowrap text-[10px] font-semibold uppercase tracking-[0.1em] text-slate-400">{title}</p>
        </div>
        <p className={`shrink-0 text-[10px] font-semibold uppercase tracking-[0.08em] ${sourceCardToneClass(source)}`}>{sourceStateLabel(source)}</p>
      </div>
      <p className="mt-3 text-sm font-semibold text-slate-100">{body}</p>
      <p className="mt-1 text-xs text-slate-500">{support}</p>
    </div>
  );
}

function OptionsFlowCard({ summary }: { summary: OptionsFlowSummary }) {
  const diagnostics = optionsFlowDiagnostics(summary);
  return (
    <div className={`rounded-xl border px-3 py-3 ${optionsFlowBorderClass(summary)}`}>
      <div className="flex items-center justify-between gap-2">
        <div className="flex min-w-0 items-center gap-2">
          <span className={`shrink-0 ${optionsFlowToneClass(summary.state)}`}>
            <IntelligenceIcon kind="flow" />
          </span>
          <p className="whitespace-nowrap text-[10px] font-semibold uppercase tracking-[0.1em] text-slate-400">Options Flow</p>
        </div>
        <p className={`shrink-0 text-[10px] font-semibold uppercase tracking-[0.08em] ${optionsFlowToneClass(summary.state)}`}>
          {summary.state.toUpperCase()}
        </p>
      </div>
      <p className="mt-3 text-sm font-semibold text-slate-100">{summary.summary}</p>
      <div className="mt-3 grid gap-1.5">
        {diagnostics.map((diagnostic) => (
          <p key={diagnostic} className="text-xs text-slate-400">{diagnostic}</p>
        ))}
      </div>
    </div>
  );
}

function PriceVolumeSection({
  state,
  summary,
  diagnostics,
  tone,
}: {
  state: string;
  summary: string;
  diagnostics: string[];
  tone: "bullish" | "bearish" | "mixed" | "inactive" | "unavailable";
}) {
  return (
    <section className={cardClassName}>
      <div className="mb-4 flex items-center justify-between gap-3">
        <h2 className="text-lg font-semibold text-white">Price / Volume</h2>
        <p className={`text-xs font-semibold uppercase tracking-[0.14em] ${technicalToneClass(tone)}`}>{state}</p>
      </div>
      <p className="text-sm font-semibold text-slate-100">{summary}</p>
      <div className="mt-4 grid gap-2">
        {diagnostics.map((diagnostic) => (
          <p key={diagnostic} className="text-sm text-slate-400">{diagnostic}</p>
        ))}
      </div>
    </section>
  );
}

function OptionsFlowSection({ summary }: { summary: OptionsFlowSummary }) {
  const diagnostics = optionsFlowDiagnostics(summary);
  return (
    <section className={cardClassName}>
      <div className="mb-4 flex items-center justify-between gap-3">
        <h2 className="text-lg font-semibold text-white">Options Flow</h2>
        <p className={`text-xs font-semibold uppercase tracking-[0.14em] ${optionsFlowToneClass(summary.state)}`}>
          {summary.state.toUpperCase()}
        </p>
      </div>
      <p className="text-sm font-semibold text-slate-100">{summary.summary}</p>
      <div className="mt-4 grid gap-2">
        {diagnostics.map((diagnostic) => (
          <p key={diagnostic} className="text-sm text-slate-400">{diagnostic}</p>
        ))}
      </div>
    </section>
  );
}

function MetricTile({
  label,
  value,
  toneClass,
  icon,
}: {
  label: string;
  value: ReactNode;
  toneClass: string;
  icon: IntelligenceIconKind;
}) {
  return (
    <div className={`${cardClassName} p-3.5`}>
      <div className="flex items-start justify-between gap-3">
        <p className="text-[11px] uppercase tracking-[0.14em] text-slate-400">{label}</p>
        <span className={`shrink-0 ${toneClass}`}>
          <IntelligenceIcon kind={icon} className="h-4 w-4" />
        </span>
      </div>
      <div className={`mt-2 text-right text-2xl font-semibold tabular-nums ${toneClass}`}>{value}</div>
    </div>
  );
}

function hrefWithFilters(symbol: string, lookback: Lookback, source: SourceFilter, side: SideFilter): string {
  const q = new URLSearchParams();
  q.set("lookback", lookback);
  q.set("source", source);
  q.set("side", side);
  const base = tickerHref(symbol) ?? `/ticker/${encodeURIComponent(symbol)}`;
  return `${base}?${q.toString()}`;
}

function lookbackStartDateKey(days: number): string {
  const date = new Date();
  date.setUTCDate(date.getUTCDate() - Math.max(days - 1, 0));
  return date.toISOString().slice(0, 10);
}

function InlineEmptyState({ message }: { message: string }) {
  return (
    <div className="rounded-2xl border border-dashed border-white/15 bg-white/[0.02] px-4 py-3">
      <p className="text-sm text-slate-400">{message}</p>
    </div>
  );
}

function ActivityCard({ children }: { children: ReactNode }) {
  return (
    <div className="rounded-2xl border border-white/10 bg-white/5 px-3 py-2.5 sm:px-4">
      {children}
    </div>
  );
}

function ActivityCardGrid({
  identity,
  sideBadge,
  dateLabel,
  price,
  priceSubtext,
  tradeValue,
  pnl,
  pnlClassName,
  signal,
}: {
  identity: ReactNode;
  sideBadge: ReactNode;
  dateLabel: ReactNode;
  price: ReactNode;
  priceSubtext?: ReactNode;
  tradeValue: ReactNode;
  pnl: ReactNode;
  pnlClassName?: string;
  signal: ReactNode;
}) {
  const metricLabelClassName = "text-[10px] font-semibold uppercase tracking-[0.14em] text-slate-500";
  const metricValueClassName = "truncate text-sm font-semibold tabular-nums";

  return (
    <div className="grid min-w-0 gap-x-3 gap-y-2 sm:grid-cols-[minmax(150px,1.45fr)_minmax(76px,.7fr)_minmax(104px,.9fr)_minmax(64px,.6fr)_minmax(84px,auto)] sm:items-center lg:grid-cols-[minmax(170px,1.65fr)_minmax(84px,.72fr)_minmax(120px,.95fr)_minmax(72px,.62fr)_minmax(92px,auto)]">
      <div className="min-w-0 sm:col-start-1 sm:row-start-1">{identity}</div>
      <div className={`${metricLabelClassName} hidden sm:block sm:col-start-2 sm:row-start-1`}>Price</div>
      <div className={`${metricLabelClassName} hidden sm:block sm:col-start-3 sm:row-start-1`}>Trade value</div>
      <div className={`${metricLabelClassName} hidden sm:block sm:col-start-4 sm:row-start-1`}>PnL</div>
      <div className="flex min-w-0 items-center justify-start sm:col-start-5 sm:row-start-1 sm:justify-end">{sideBadge}</div>

      <div className="text-xs text-slate-400 sm:col-start-1 sm:row-start-2">{dateLabel}</div>
      <div className="min-w-0 sm:col-start-2 sm:row-start-2">
        <div className={`${metricLabelClassName} sm:hidden`}>Price</div>
        <div className={`${metricValueClassName} text-white`}>{price}</div>
        {priceSubtext ? <div className="mt-0.5 truncate text-[11px] tabular-nums text-slate-500">{priceSubtext}</div> : null}
      </div>
      <div className="min-w-0 sm:col-start-3 sm:row-start-2">
        <div className={`${metricLabelClassName} sm:hidden`}>Trade value</div>
        <div className={`${metricValueClassName} text-white`}>{tradeValue}</div>
      </div>
      <div className="min-w-0 sm:col-start-4 sm:row-start-2">
        <div className={`${metricLabelClassName} sm:hidden`}>PnL</div>
        <div className={`${metricValueClassName} ${pnlClassName ?? "text-slate-400"}`}>{pnl}</div>
      </div>
      <div className="flex min-w-0 items-center justify-start sm:col-start-5 sm:row-start-2 sm:justify-end">{signal}</div>
    </div>
  );
}

function DeferredTickerSummarySkeleton() {
  return (
    <div className="space-y-6">
      <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4 2xl:grid-cols-7">
        {Array.from({ length: 7 }).map((_, idx) => (
          <div key={idx} className={`${cardClassName} p-4`}>
            <SkeletonBlock className="h-3 w-28" />
            <SkeletonBlock className="mt-3 h-7 w-20" />
          </div>
        ))}
      </div>
      <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
        <div className={`${cardClassName} p-4 md:col-span-2 xl:col-span-3`}>
          <SkeletonBlock className="h-3 w-28" />
          <div className="mt-3 flex gap-2">
            {Array.from({ length: 4 }).map((_, idx) => <SkeletonBlock key={idx} className="h-8 w-20 rounded-lg" />)}
          </div>
        </div>
        <div className={`${cardClassName} p-4`}>
          <SkeletonBlock className="h-3 w-32" />
          <SkeletonBlock className="mt-3 h-7 w-16" />
        </div>
      </div>
      <section className={`${cardClassName} p-4`}>
        <SkeletonBlock className="h-3 w-40" />
        <SkeletonBlock className="mt-3 h-64 w-full" />
      </section>
    </div>
  );
}

async function DeferredTickerChart({ chartBundlePromise }: { chartBundlePromise: Promise<TickerChartBundle | null> }) {
  const chartBundle = await chartBundlePromise;
  return <PremiumTickerChart bundle={chartBundle} />;
}

async function resolveTickerActivityData({
  eventsPromise,
  signalsPromise,
  signalsUnavailableMessage,
  lookbackStartKey,
  side,
}: {
  eventsPromise?: ReturnType<typeof getEvents>;
  signalsPromise?: ReturnType<typeof getSignalsAll>;
  signalsUnavailableMessage?: string | null;
  lookbackStartKey: string;
  side: SideFilter;
}): Promise<TickerActivityData> {
  const [eventsRes, signalsResult] = await Promise.all([
    eventsPromise ?? Promise.resolve({ items: [] }),
    signalsPromise
      ? signalsPromise
          .then((response) => ({ response, unavailableMessage: null as string | null }))
          .catch((error) => ({
            response: { items: [] as Awaited<ReturnType<typeof getSignalsAll>>["items"] },
            unavailableMessage: signalAccessMessage(error),
          }))
      : Promise.resolve({
          response: { items: [] as Awaited<ReturnType<typeof getSignalsAll>>["items"] },
          unavailableMessage: signalsUnavailableMessage ?? null,
        }),
  ]);
  const signalsRes = signalsResult.response;

  const events = dedupeByKey(eventsRes.items ?? [], (event) => {
    const stableIdentity = stableEventIdentity(event);
    if (stableIdentity) return `stable|${stableIdentity}`;

    const actor =
      canonicalize(event.member_bioguide_id) ||
      canonicalize(event.member_name) ||
      canonicalize(resolveInsiderName(event));
    const sideValue = normalizeTradeSide(event.trade_type) ?? canonicalize(event.trade_type);

    return [
      canonicalize(event.event_type),
      canonicalize(event.source),
      canonicalize(event.symbol ?? event.ticker),
      actor,
      sideValue,
      toDateKey(event.ts) ?? "",
      payloadDateKey(event.payload),
      normalizedAmountLabel(event.amount_min, event.amount_max),
    ].join("|");
  });

  const signals = dedupeByKey(signalsRes.items ?? [], (signal) => [
    canonicalize(signal.kind),
    canonicalize(signal.symbol),
    canonicalize(signal.who),
    canonicalize(signal.member_bioguide_id),
    normalizeTradeSide(signal.trade_type) ?? canonicalize(signal.trade_type),
    toDateKey(signal.ts) ?? "",
    normalizedAmountLabel(signal.amount_min, signal.amount_max),
    canonicalize(signal.smart_band),
    String(signal.smart_score ?? ""),
    String(signal.unusual_multiple ?? ""),
  ].join("|")).filter((signal) => {
    const key = toDateKey(signal.ts);
    return Boolean(key && key >= lookbackStartKey);
  });

  const filteredEvents = side === "all"
    ? events
    : events.filter((event) => normalizeTradeSide(event.trade_type) === side);

  const congressEvents = filteredEvents.filter((event) => event.event_type === "congress_trade");
  const insiderEvents = filteredEvents.filter((event) => event.event_type === "insider_trade");
  const congressBuys = congressEvents.filter((event) => normalizeTradeSide(event.trade_type) === "buy").length;
  const congressSells = congressEvents.filter((event) => normalizeTradeSide(event.trade_type) === "sell").length;
  const insiderBuys = insiderEvents.filter((event) => normalizeTradeSide(event.trade_type) === "buy").length;
  const insiderSells = insiderEvents.filter((event) => normalizeTradeSide(event.trade_type) === "sell").length;

  const netFlow = filteredEvents.reduce((acc, event) => {
    const sideValue = normalizeTradeSide(event.trade_type);
    const amount = Number(event.amount_max ?? event.amount_min ?? 0);
    if (!Number.isFinite(amount) || amount <= 0 || !sideValue) return acc;
    if (sideValue === "buy") return acc + amount;
    return acc - amount;
  }, 0);

  const topSignal = [...signals].sort((a, b) => (b.smart_score ?? 0) - (a.smart_score ?? 0))[0];
  const congressParticipantMap = new Map<string, ParticipantStats>();
  const insiderParticipantMap = new Map<string, ParticipantStats>();

  for (const event of congressEvents) {
    const who = (event.member_name ?? "Unknown Member").trim();
    const sideValue = normalizeTradeSide(event.trade_type);
    const amount = Number(event.amount_max ?? event.amount_min ?? 0);
    const existing = congressParticipantMap.get(who) ?? { name: who, trades: 0, buys: 0, sells: 0, netFlow: 0 };
    existing.trades += 1;
    if (sideValue === "buy") existing.buys += 1;
    if (sideValue === "sell") existing.sells += 1;
    if (Number.isFinite(amount) && amount > 0) {
      existing.netFlow += sideValue === "sell" ? -amount : sideValue === "buy" ? amount : 0;
    }
    const safeHref = memberHref({ name: event.member_name ?? undefined, memberId: event.member_bioguide_id ?? undefined });
    if (safeHref && safeHref !== "/member/UNKNOWN" && !existing.href) existing.href = safeHref;
    congressParticipantMap.set(who, existing);
  }

  for (const event of insiderEvents) {
    const who = resolveInsiderName(event);
    const reportingCik = resolveInsiderReportingCik(event);
    const participantKey = reportingCik ? `cik:${reportingCik}` : `name:${who.toLowerCase()}`;
    const sideValue = normalizeTradeSide(event.trade_type);
    const amount = Number(event.amount_max ?? event.amount_min ?? 0);
    const existing = insiderParticipantMap.get(participantKey) ?? { name: who, trades: 0, buys: 0, sells: 0, netFlow: 0 };
    existing.trades += 1;
    if (sideValue === "buy") existing.buys += 1;
    if (sideValue === "sell") existing.sells += 1;
    if (Number.isFinite(amount) && amount > 0) {
      existing.netFlow += sideValue === "sell" ? -amount : sideValue === "buy" ? amount : 0;
    }
    if (reportingCik && !existing.reportingCik) existing.reportingCik = reportingCik;
    insiderParticipantMap.set(participantKey, existing);
  }

  const topCongressParticipants = [...congressParticipantMap.values()].sort((a, b) => b.trades - a.trades).slice(0, 5);
  const topInsiderParticipants = [...insiderParticipantMap.values()].sort((a, b) => b.trades - a.trades).slice(0, 5);

  return {
    events,
    signals,
    signalsUnavailableMessage: signalsResult.unavailableMessage,
    congressEvents,
    insiderEvents,
    congressBuys,
    congressSells,
    insiderBuys,
    insiderSells,
    netFlow,
    topSignal,
    congressParticipantCount: congressParticipantMap.size,
    insiderParticipantCount: insiderParticipantMap.size,
    topCongressParticipants,
    topInsiderParticipants,
  };
}

async function DeferredTickerContent({
  activityPromise,
  normalizedSymbol,
  lookback,
  source,
  side,
  topMembers,
  confirmationScoreBundle,
  optionsFlowSummary,
  signalFreshness,
  technicalIndicators,
  chartBundlePromise,
}: {
  activityPromise: Promise<TickerActivityData>;
  normalizedSymbol: string;
  lookback: Lookback;
  source: SourceFilter;
  side: SideFilter;
  topMembers: NonNullable<Awaited<ReturnType<typeof getTickerProfile>>["top_members"]>;
  confirmationScoreBundle: ConfirmationScoreBundle | null | undefined;
  optionsFlowSummary: OptionsFlowSummary | null | undefined;
  signalFreshness: SignalFreshnessBundle | null | undefined;
  technicalIndicators: TechnicalIndicators | null | undefined;
  chartBundlePromise: Promise<TickerChartBundle | null>;
}) {
  const {
    signals,
    signalsUnavailableMessage,
    congressEvents,
    insiderEvents,
    congressBuys,
    congressSells,
    insiderBuys,
    insiderSells,
    netFlow,
    topSignal,
    congressParticipantCount,
    insiderParticipantCount,
    topCongressParticipants,
    topInsiderParticipants,
  } = await activityPromise;
  const confirmationBundle = normalizeConfirmationBundle(confirmationScoreBundle, normalizedSymbol);
  const optionsFlow = normalizeOptionsFlowSummary(optionsFlowSummary, normalizedSymbol);
  const freshnessBundle = normalizeSignalFreshness(signalFreshness, normalizedSymbol);
  const normalizedTechnicals = normalizeTechnicalIndicators(technicalIndicators);
  const showCongress = source === "all" || source === "congress";
  const showInsider = source === "all" || source === "insider";
  const showSignals = source === "all" || source === "signals";
  const activityPnlByEventId = new Map<number, number | null>(
    [...congressEvents, ...insiderEvents].map((event) => [event.id, readNumeric(event.pnl_pct)]),
  );
  const activityEventById = new Map<number, (typeof congressEvents)[number] | (typeof insiderEvents)[number]>(
    [...congressEvents, ...insiderEvents].map((event) => [event.id, event]),
  );
  const tickerReturnTo = tickerHref(normalizedSymbol) ?? `/ticker/${normalizedSymbol}`;
  const signalGateHref = signalsUnavailableMessage?.includes("Premium")
    ? "/pricing"
    : `/login?return_to=${encodeURIComponent(tickerReturnTo)}`;
  const signalGateLabel = signalsUnavailableMessage?.includes("Premium") ? "View Premium" : "Login or register";
  const alignedSources = alignedConfirmationSources(confirmationBundle);
  const priceVolume = priceVolumeSummary(confirmationBundle.sources.price_volume, normalizedTechnicals);
  const intelligenceBullets = overviewBullets({ confirmationBundle, alignedSources });
  const lookbackDays = confirmationBundle.lookback_days;

  return (
    <>
      <section className="grid grid-cols-1 items-start gap-4 xl:grid-cols-12">
        <div className="xl:col-span-7">
          <TickerContextCard
            key={normalizedSymbol}
            symbol={normalizedSymbol}
            overview={
              <TickerOverviewPanel
                confirmationBundle={confirmationBundle}
                freshnessBundle={freshnessBundle}
                alignedSources={alignedSources}
                intelligenceBullets={intelligenceBullets}
              />
            }
          />
        </div>

        <div className="space-y-4 xl:col-span-5">
          <PriceVolumeSection
            state={priceVolume.state}
            summary={priceVolume.summary}
            diagnostics={priceVolume.diagnostics}
            tone={priceVolume.tone}
          />

          {showInsider ? (
            <section className={cardClassName}>
              <div className="mb-4 flex items-center justify-between">
                <div>
                  <h2 className="text-lg font-semibold text-white">Insider activity</h2>
                  <p className="mt-1 text-xs text-slate-500">
                    Displayed quotes are USD. Current foreign prices use spot FX where applicable; historical foreign filing prices use trade-date FX and ADR ratios when normalized.
                  </p>
                </div>
                <span className="text-xs text-slate-400">{insiderEvents.length} events</span>
              </div>
              <div className="space-y-3">
                {insiderEvents.length === 0 ? (
                  <p className="text-sm text-slate-400">No insider trades in the selected window.</p>
                ) : (
                  insiderEvents.slice(0, 20).map((event) => {
                    const display = resolveInsiderActivityDisplay(event as Record<string, unknown>);
                    const insiderProfileHref = insiderHref(display.insiderName, display.reportingCik ?? resolveInsiderReportingCik(event));
                    const insiderRoleRaw = display.role ?? resolveInsiderRole(event);
                    const insiderRoleBadge = resolveInsiderRoleBadge(insiderRoleRaw);
                    const insiderRoleTone = insiderRoleBadgeTone(insiderRoleBadge);

                    return (
                    <ActivityCard key={event.id}>
                      <ActivityCardGrid
                        identity={
                          <div className="flex flex-wrap items-center gap-2">
                            {insiderProfileHref ? (
                              <Link href={insiderProfileHref} prefetch={false} className="text-sm font-semibold text-emerald-200">
                                {display.insiderName}
                              </Link>
                            ) : (
                              <span className="text-sm font-semibold text-slate-100">{display.insiderName}</span>
                            )}
                            <Badge tone={insiderRoleTone} className="px-2 py-0.5 text-[10px]">{insiderRoleBadge}</Badge>
                          </div>
                        }
                        sideBadge={<Badge tone={transactionTone(event.trade_type)}>{formatTransactionLabel(event.trade_type)}</Badge>}
                        dateLabel={<>Reported {formatDateShort(display.filingDate ?? resolveInsiderFilingDate(event))}</>}
                        price={display.price !== null ? formatCurrency(display.price) : "-"}
                        priceSubtext={display.reportedLabel}
                        tradeValue={display.tradeValue !== null ? formatCurrency(display.tradeValue) : formatCurrencyRange(event.amount_min ?? null, event.amount_max ?? null)}
                        pnl={display.pnl !== null ? formatPnl(display.pnl) : "-"}
                        pnlClassName={display.pnl !== null ? pnlClass(display.pnl) : "text-slate-400"}
                        signal={<SmartSignalPill score={display.signal.score} band={display.signal.band} size="compact" />}
                      />
                    </ActivityCard>
                    );
                  })
                )}
              </div>
            </section>
          ) : null}

          {showCongress ? (
            <section className={cardClassName}>
              <div className="mb-4 flex items-center justify-between">
                <h2 className="text-lg font-semibold text-white">Congress activity</h2>
                <span className="text-xs text-slate-400">{congressEvents.length} events</span>
              </div>
              <div className="space-y-3">
                {congressEvents.length === 0 ? (
                  <p className="text-sm text-slate-400">No Congress trades in the selected window.</p>
                ) : (
                  congressEvents.slice(0, 20).map((event) => {
                    const memberName = event.member_name ?? "Unknown";
                    const memberLink = event.member_bioguide_id
                      ? memberHref({ name: memberName, memberId: event.member_bioguide_id })
                      : null;
                    const chamber = chamberBadge(resolveCongressChamber(event));
                    const party = partyBadge(resolveCongressParty(event));
                    const state = resolveCongressState(event)?.toUpperCase() || "—";
                    const signal = resolveSmartSignalValue(event as Record<string, unknown>);
                    const displayPrice = resolveCongressTradePrice(event);
                    const pnl = readNumeric(event.pnl_pct);

                    return (
                      <ActivityCard key={event.id}>
                        <ActivityCardGrid
                          identity={
                            <div className="flex flex-wrap items-center gap-2">
                              {memberLink ? (
                                <Link href={memberLink} prefetch={false} className="text-sm font-semibold text-emerald-200">
                                  {memberName}
                                </Link>
                              ) : (
                                <span className="text-sm font-semibold text-slate-100">{memberName}</span>
                              )}
                              <Badge tone={chamber.tone} className="px-2 py-0.5 text-[10px]">{chamber.label}</Badge>
                              <Badge tone={party.tone} className="px-2 py-0.5 text-[10px]">{party.label}</Badge>
                              <Badge tone="neutral" className="px-2 py-0.5 text-[10px]">{state}</Badge>
                            </div>
                          }
                          sideBadge={<Badge tone={transactionTone(event.trade_type)}>{formatTransactionLabel(event.trade_type)}</Badge>}
                          dateLabel={<>Filed {formatDateShort(resolveCongressReportDate(event))}</>}
                          price={displayPrice !== null ? formatCurrency(displayPrice) : "-"}
                          tradeValue={formatCurrencyRange(event.amount_min ?? null, event.amount_max ?? null)}
                          pnl={pnl !== null ? formatPnl(pnl) : "-"}
                          pnlClassName={pnl !== null ? pnlClass(pnl) : "text-slate-400"}
                          signal={
                            <SmartSignalPill score={signal.score} band={signal.band} size="compact" />
                          }
                        />
                      </ActivityCard>
                    );
                  })
                )}
              </div>
            </section>
          ) : null}

          {showSignals ? (
            <section className={cardClassName}>
              <div className="mb-4 flex items-center justify-between">
                <h2 className="text-lg font-semibold text-white">Signal activity</h2>
                <span className="text-xs text-slate-400">
                  {signalsUnavailableMessage ? "locked" : `${signals.length} signals`}
                </span>
              </div>
              <div className="space-y-3">
                {signalsUnavailableMessage ? (
                  <div className="rounded-lg border border-white/10 bg-white/[0.03] p-4">
                    <p className="text-sm font-semibold text-white">Signals are gated for this view.</p>
                    <p className="mt-1 text-sm text-slate-400">{signalsUnavailableMessage}</p>
                    <Link
                      href={signalGateHref}
                      prefetch={false}
                      className="mt-3 inline-flex rounded-lg border border-emerald-300/40 bg-emerald-300/10 px-3 py-1.5 text-sm font-semibold text-emerald-100 transition hover:bg-emerald-300/15"
                    >
                      {signalGateLabel}
                    </Link>
                  </div>
                ) : signals.length === 0 ? (
                  <p className="text-sm text-slate-400">No smart signals for this symbol in current filters.</p>
                ) : (
                  signals.slice(0, 20).map((signal) => {
                    const isInsiderSignal = signal.kind === "insider";
                    const insiderProfileHref = insiderHref(getInsiderDisplayName(signal.who), signal.reporting_cik ?? null);
                    const sourceEvent = activityEventById.get(signal.event_id) ?? null;
                    const displayPrice =
                      sourceEvent && isInsiderSignal
                        ? resolveInsiderActivityDisplay(sourceEvent as Record<string, unknown>).price
                        : sourceEvent
                          ? resolveCongressTradePrice(sourceEvent)
                          : null;
                    const pnl = activityPnlByEventId.get(signal.event_id) ?? null;

                    return (
                    <ActivityCard key={`${signal.kind}-${signal.event_id}-${signal.ts}`}>
                      <ActivityCardGrid
                        identity={
                          <div className="flex flex-wrap items-center gap-2">
                            {isInsiderSignal && insiderProfileHref ? (
                              <Link href={insiderProfileHref} prefetch={false} className="text-sm font-semibold text-emerald-200">
                                {getInsiderDisplayName(signal.who) ?? "Unknown"}
                              </Link>
                            ) : (
                              <span className="text-sm font-semibold text-slate-100">{getInsiderDisplayName(signal.who) ?? "Unknown"}</span>
                            )}
                            <Badge tone={signal.kind === "insider" ? "ind" : "house"}>{signal.kind ?? "signal"}</Badge>
                            <Badge tone={signalTone(signal.smart_band)}>{signal.smart_band ?? "signal"}</Badge>
                          </div>
                        }
                        sideBadge={<Badge tone={transactionTone(signal.trade_type)}>{formatTransactionLabel(signal.trade_type)}</Badge>}
                        dateLabel={formatDateShort(signal.ts)}
                        price={displayPrice !== null ? formatCurrency(displayPrice) : "-"}
                        tradeValue={formatCurrencyRange(signal.amount_min ?? null, signal.amount_max ?? null)}
                        pnl={pnl !== null ? formatPnl(pnl) : "-"}
                        pnlClassName={pnl !== null ? pnlClass(pnl) : "text-slate-400"}
                        signal={<SmartSignalPill score={signal.smart_score ?? null} band={signal.smart_band ?? null} size="compact" />}
                      />
                    </ActivityCard>
                    );
                  })
                )}
              </div>
            </section>
          ) : null}

          <OptionsFlowSection summary={optionsFlow} />
        </div>
      </section>
      <div className="grid gap-3 md:grid-cols-3">
        <div className={`${cardClassName} p-4`}>
          <div className="flex items-center justify-between gap-3">
            <p className="text-xs uppercase tracking-widest text-slate-400">Activity view</p>
            <p className="text-xs text-slate-500">All / Congress / Insiders / Signals</p>
          </div>
          <div className="mt-3 flex flex-wrap rounded-xl border border-white/10 bg-slate-950/80 p-1">
            {([
              ["all", "All"],
              ["congress", "Congress"],
              ["insider", "Insiders"],
              ["signals", "Signals"],
            ] as const).map(([value, label]) => (
              <Link
                key={value}
                href={hrefWithFilters(normalizedSymbol, lookback, value, side)}
                prefetch={false}
                className={`rounded-lg px-3 py-1.5 text-xs font-semibold ${
                  source === value
                    ? "bg-emerald-400/15 text-emerald-200"
                    : "text-slate-300 hover:bg-white/5"
                }`}
              >
                {label}
              </Link>
            ))}
          </div>
        </div>
        <div className={`${cardClassName} p-4`}>
          <p className="mb-2 text-xs uppercase tracking-widest text-slate-400">Lookback</p>
          <div className="flex flex-wrap gap-2">
            {(["30", "90", "180", "365"] as const).map((value) => (
              <Link
                key={value}
                href={hrefWithFilters(normalizedSymbol, value, source, side)}
                prefetch={false}
                className={`rounded-full border px-3 py-1 text-xs font-semibold ${
                  lookback === value
                    ? "border-emerald-400/40 bg-emerald-400/10 text-emerald-200"
                    : "border-white/10 bg-slate-900/60 text-slate-300"
                }`}
              >
                {value}D
              </Link>
            ))}
          </div>
        </div>
        <div className={`${cardClassName} p-4`}>
          <p className="mb-2 text-xs uppercase tracking-widest text-slate-400">Trade side</p>
          <div className="flex flex-wrap gap-2">
            {(["all", "buy", "sell"] as const).map((value) => (
              <Link
                key={value}
                href={hrefWithFilters(normalizedSymbol, lookback, source, value)}
                prefetch={false}
                className={`rounded-full border px-3 py-1 text-xs font-semibold uppercase ${
                  side === value
                    ? "border-emerald-400/40 bg-emerald-400/10 text-emerald-200"
                    : "border-white/10 bg-slate-900/60 text-slate-300"
                }`}
              >
                {value}
              </Link>
            ))}
          </div>
        </div>
      </div>

      <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-8">
        <MetricTile label="Congress buys" value={congressBuys} toneClass="text-emerald-300" icon="congress" />
        <MetricTile label="Congress sells" value={congressSells} toneClass="text-rose-300" icon="congress" />
        <MetricTile label="Insider buys" value={insiderBuys} toneClass="text-emerald-300" icon="insider-buy" />
        <MetricTile label="Insider sells" value={insiderSells} toneClass="text-rose-300" icon="insider-sell" />
        <MetricTile
          label="Net disclosed flow"
          value={`${netFlow >= 0 ? "+" : "-"}$${formatCompactUsd(Math.abs(netFlow))}`}
          toneClass={netFlow >= 0 ? "text-emerald-300" : "text-rose-300"}
          icon="flow"
        />
        <MetricTile label="Unique Congress traders" value={congressParticipantCount} toneClass="text-white" icon="people" />
        <MetricTile label="Unique insiders" value={insiderParticipantCount} toneClass="text-white" icon="people" />
        <MetricTile
          label="Latest smart signal"
          value={topSignal ? topSignal.smart_score ?? "-" : "None"}
          toneClass={topSignal ? "text-white" : "text-slate-400"}
          icon="signals"
        />
      </div>

      <Suspense fallback={<PremiumTickerChartSkeleton />}>
        <DeferredTickerChart chartBundlePromise={chartBundlePromise} />
      </Suspense>

      <div className="space-y-5">
          <section className={cardClassName}>
            <h2 className="text-lg font-semibold text-white">Top Congress traders</h2>
            <div className="mt-4 space-y-2.5">
              {topCongressParticipants.length === 0 ? (
                <InlineEmptyState message="No Congress participants in current window." />
              ) : (
                topCongressParticipants.map((participant) => {
                  const match = topMembers.find((member) => member.name === participant.name);
                  const resolvedHref = participant.href ?? (match ? memberHref({ name: match.name, memberId: match.bioguide_id }) : undefined);
                  const bias = biasLabel(participant.buys, participant.sells);
                  const chamber = chamberBadge(match?.chamber);
                  const party = partyBadge(match?.party);
                  const state = match?.state?.trim().toUpperCase() || "â€”";
                  const rowClassName = `${compactInteractiveSurfaceClassName} block px-3 py-2.5 text-sm`;

                  const content = (
                    <>
                      <div className="flex items-start justify-between gap-3">
                        <div className="min-w-0">
                          <span className={`block truncate text-sm font-semibold ${compactInteractiveTitleClassName}`}>{participant.name}</span>
                          <div className="mt-1.5 flex flex-wrap items-center gap-1.5">
                            <Badge tone={chamber.tone} className="px-2 py-0.5 text-[10px]">{chamber.label}</Badge>
                            <Badge tone={party.tone} className="px-2 py-0.5 text-[10px]">{party.label}</Badge>
                            <Badge tone="neutral" className="px-2 py-0.5 text-[10px]">{state}</Badge>
                          </div>
                        </div>
                        <div className="text-right">
                          <span className="text-sm font-semibold tabular-nums text-slate-200">{participant.trades}</span>
                          <p className="text-[11px] text-slate-500">Trades</p>
                        </div>
                      </div>
                      <div className="mt-2 flex items-center justify-between gap-3 text-xs text-slate-400">
                        <Badge tone={bias.tone} className="px-2 py-0.5 text-[10px]">{bias.label}</Badge>
                        <span className={`font-semibold tabular-nums ${participant.netFlow >= 0 ? "text-emerald-300" : "text-rose-300"}`}>
                          {participant.netFlow >= 0 ? "+" : "-"}${formatCompactUsd(Math.abs(participant.netFlow))}
                        </span>
                      </div>
                    </>
                  );

                  if (resolvedHref) {
                    return (
                      <Link key={participant.name} href={resolvedHref} prefetch={false} className={rowClassName}>
                        {content}
                      </Link>
                    );
                  }

                  return (
                    <div key={participant.name} className={rowClassName}>
                      {content}
                    </div>
                  );
                })
              )}
            </div>
          </section>

          <section className={cardClassName}>
            <h2 className="text-lg font-semibold text-white">Top insiders</h2>
            <div className="mt-4 space-y-2.5">
              {topInsiderParticipants.length === 0 ? (
                <InlineEmptyState message="No insiders in current window." />
              ) : (
                topInsiderParticipants.map((participant) => {
                  const bias = biasLabel(participant.buys, participant.sells);
                  const href = insiderHref(participant.name, participant.reportingCik);
                  const content = (
                    <>
                      <div className="flex items-start justify-between gap-3">
                        <span className={`truncate font-semibold ${compactInteractiveTitleClassName}`}>{participant.name}</span>
                        <div className="text-right">
                          <span className="text-sm font-semibold tabular-nums text-slate-200">{participant.trades}</span>
                          <p className="text-[11px] text-slate-500">Trades</p>
                        </div>
                      </div>
                      <div className="mt-2 flex items-center justify-between gap-3 text-xs text-slate-400">
                        <Badge tone={bias.tone} className="px-2 py-0.5 text-[10px]">{bias.label}</Badge>
                        <span className={`font-semibold tabular-nums ${participant.netFlow >= 0 ? "text-emerald-300" : "text-rose-300"}`}>
                          {participant.netFlow >= 0 ? "+" : "-"}${formatCompactUsd(Math.abs(participant.netFlow))}
                        </span>
                      </div>
                    </>
                  );

                  if (href) {
                    return (
                      <Link
                        key={participant.name}
                        href={href}
                        prefetch={false}
                        className={`${compactInteractiveSurfaceClassName} block w-full px-3 py-2.5 text-sm`}
                      >
                        {content}
                      </Link>
                    );
                  }

                  return (
                    <div
                      key={participant.name}
                      className={`${compactInteractiveSurfaceClassName} block w-full px-3 py-2.5 text-sm`}
                    >
                      {content}
                    </div>
                  );
                })
              )}
            </div>
          </section>

          <section className={cardClassName}>
            <h2 className="text-lg font-semibold text-white">Historical Congress participants</h2>
            <div className="mt-4 space-y-2.5">
              {topMembers.length === 0 ? (
                <InlineEmptyState message="No historical member profile data." />
              ) : (
                topMembers.slice(0, 5).map((member) => {
                  const chamber = chamberBadge(member.chamber);
                  const party = partyBadge(member.party);
                  const state = member.state?.trim().toUpperCase() || "â€”";
                  return (
                    <Link
                      key={member.member_id}
                      href={memberHref({ name: member.name, memberId: member.bioguide_id })}
                      prefetch={false}
                      className={`${compactInteractiveSurfaceClassName} flex items-center justify-between gap-3 px-3 py-2.5 text-sm`}
                    >
                      <div className="min-w-0">
                        <div className={`truncate text-sm font-semibold ${compactInteractiveTitleClassName}`}>{member.name}</div>
                        <div className="mt-1.5 flex flex-wrap items-center gap-1.5">
                          <Badge tone={chamber.tone} className="px-2 py-0.5 text-[10px]">{chamber.label}</Badge>
                          <Badge tone={party.tone} className="px-2 py-0.5 text-[10px]">{party.label}</Badge>
                          <Badge tone="neutral" className="px-2 py-0.5 text-[10px]">{state}</Badge>
                        </div>
                      </div>
                      <div className="text-right">
                        <span className="text-sm font-semibold tabular-nums text-slate-200">{member.trade_count}</span>
                        <p className="text-[11px] text-slate-500">Trades</p>
                      </div>
                    </Link>
                  );
                })
              )}
            </div>
          </section>
        </div>
    </>
  );
}

export default async function TickerPage({ params, searchParams }: Props) {
  const { symbol } = await params;
  const sp = (await searchParams) ?? {};
  const lookback = clampLookback(one(sp, "lookback"));
  const source = clampSource(one(sp, "source"));
  const side = clampSide(one(sp, "side"));
  const normalizedSymbol = symbol.trim().toUpperCase();
  const lookbackDays = Number(lookback);
  const authToken = await optionalPageAuthToken();

  const profilePromise = getTickerProfile(normalizedSymbol);
  const chartBundlePromise = getTickerChartBundle(normalizedSymbol, lookbackDays).catch((error) => {
    console.error("[ticker-chart] bundle unavailable", error);
    return null;
  });
  const eventsPromise = getEvents({
    symbol: normalizedSymbol,
    recent_days: lookbackDays,
    limit: 100,
    enrich_prices: 1,
    ...(source === "congress" ? { event_type: "congress_trade" } : {}),
    ...(source === "insider" ? { event_type: "insider_trade" } : {}),
  });
  const shouldLoadSignals = source === "all" || source === "signals";
  const signalsPromise =
    shouldLoadSignals && authToken
      ? getSignalsAll({
          mode: "all",
          side,
          sort: "smart",
          limit: 100,
          symbol: normalizedSymbol,
          authToken,
        })
      : undefined;

  const profile = await profilePromise;
  const headerMetadata = tickerHeaderMetadata(profile.ticker);
  const activityPromise = resolveTickerActivityData({
    eventsPromise,
    signalsPromise,
    signalsUnavailableMessage: shouldLoadSignals && !authToken ? "Create a free account or log in to unlock premium ticker signals." : null,
    lookbackStartKey: lookbackStartDateKey(lookbackDays),
    side,
  });

  return (
    <div className="space-y-6">
      <div className="flex flex-wrap items-center justify-between gap-4">
        <div className="min-w-0">
          <p className="text-xs font-semibold uppercase tracking-[0.3em] text-emerald-300">Ticker intelligence</p>
          <h1 className="text-3xl font-semibold text-white">
            {profile.ticker.symbol}
            <span className="text-slate-400"> Â· {profile.ticker.name ?? profile.ticker.symbol}</span>
          </h1>
          <div className="mt-2 flex flex-wrap items-center gap-x-3 gap-y-1">
            <span className={pillClassName}>{profile.ticker.asset_class ?? "Equity"}</span>
            {headerMetadata.length ? (
              <p className="min-w-0 text-[11px] font-medium tracking-[0.02em] text-slate-400 sm:max-w-[44rem] sm:truncate">
                {headerMetadata.join(" Â· ")}
              </p>
            ) : null}
          </div>
        </div>
        <div className="flex flex-wrap items-center justify-end gap-2">
          <AddTickerToWatchlist symbol={normalizedSymbol} />
          <Link href="/?mode=all" className={ghostButtonClassName}>Back to feed</Link>
        </div>
      </div>
      <Suspense fallback={<DeferredTickerSummarySkeleton />}>
        <DeferredTickerContent
          activityPromise={activityPromise}
          normalizedSymbol={normalizedSymbol}
          lookback={lookback}
          source={source}
          side={side}
          topMembers={profile.top_members ?? []}
          confirmationScoreBundle={profile.confirmation_score_bundle}
          optionsFlowSummary={profile.options_flow_summary}
          signalFreshness={profile.signal_freshness}
          technicalIndicators={profile.technical_indicators}
          chartBundlePromise={chartBundlePromise}
        />
      </Suspense>
    </div>
  );
}

