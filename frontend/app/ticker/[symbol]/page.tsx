import Link from "next/link";
import type { ReactNode } from "react";
import { Suspense } from "react";
import { Badge, type BadgeTone } from "@/components/Badge";
import { getEvents, getSignalsAll, getTickerChartBundle, getTickerProfile, type TickerChartBundle } from "@/lib/api";
import { PremiumTickerChart, PremiumTickerChartSkeleton } from "@/components/ticker/PremiumTickerChart";
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

type IntelligenceNarrative = {
  summary: string;
  badges: Array<{ label: string; tone: BadgeTone }>;
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
  const suffix = [party !== "â€”" ? party : null, state ? state.toUpperCase() : null].filter(Boolean).join("-");
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

function flowPosture(value: number): { label: string; tone: "pos" | "neg" | "neutral" } {
  const absValue = Math.abs(value);
  if (absValue < 100_000) return { label: "Muted", tone: "neutral" };
  if (value > 0) return { label: "Positive", tone: "pos" };
  return { label: "Negative", tone: "neg" };
}

function signalPosture(topSignal: TickerActivityData["topSignal"]): { label: string; tone: "pos" | "neutral" } {
  const band = (topSignal?.smart_band ?? "").toLowerCase();
  if (band === "strong") return { label: "Strong", tone: "pos" };
  if (band === "notable") return { label: "Notable", tone: "pos" };
  return { label: "None", tone: "neutral" };
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
    },
    drivers: ["Congress inactive", "Insiders inactive", "No current smart signal"],
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
    },
    drivers: Array.isArray(bundle.drivers) && bundle.drivers.length > 0 ? bundle.drivers.slice(0, 4) : fallback.drivers,
  };
}

function titleCase(value: string): string {
  return value ? `${value.slice(0, 1).toUpperCase()}${value.slice(1)}` : value;
}

function confirmationTone(direction: ConfirmationScoreBundle["direction"]): BadgeTone {
  if (direction === "bullish") return "pos";
  if (direction === "bearish") return "neg";
  return "neutral";
}

const confirmationSourceLabels: Record<ConfirmationSourceKey, string> = {
  congress: "Congress",
  insiders: "Insiders",
  signals: "Signals",
  price_volume: "Price / volume",
};

const confirmationSourceOrder: ConfirmationSourceKey[] = ["congress", "insiders", "signals", "price_volume"];

function sourceFreshnessLabel(days: number | null): string {
  if (days === null || days === undefined) return "No recent read";
  if (days === 0) return "Updated today";
  if (days === 1) return "1D fresh";
  return `${days}D fresh`;
}

function sourceScoreLabel(source: ConfirmationScoreBundle["sources"][ConfirmationSourceKey]): string {
  if (!source.present) return "Inactive";
  return `${Math.round(source.strength)} strength / ${Math.round(source.quality)} quality`;
}

function buildTickerIntelligenceNarrative({
  confirmationBundle,
  topSignal,
  netFlow,
}: {
  confirmationBundle: ConfirmationScoreBundle;
  topSignal: TickerActivityData["topSignal"];
  netFlow: number;
}): IntelligenceNarrative {
  const recentSignal = signalPosture(topSignal);
  const flow = flowPosture(netFlow);
  const signalSummary = topSignal
    ? `Latest smart signal is ${recentSignal.label.toLowerCase()} (${Math.round(topSignal.smart_score ?? 0)}).`
    : "No notable smart signal is currently active.";
  const summary = [
    confirmationBundle.explanation,
    `Disclosed flow posture is ${flow.label.toLowerCase()}.`,
    signalSummary,
  ].join(" ");

  return {
    summary,
    badges: [
      { label: `Score ${confirmationBundle.score}`, tone: confirmationTone(confirmationBundle.direction) },
      { label: titleCase(confirmationBundle.band), tone: confirmationTone(confirmationBundle.direction) },
      { label: `Flow ${flow.label}`, tone: flow.tone },
    ],
  };
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
  chartBundlePromise,
}: {
  activityPromise: Promise<TickerActivityData>;
  normalizedSymbol: string;
  lookback: Lookback;
  source: SourceFilter;
  side: SideFilter;
  topMembers: NonNullable<Awaited<ReturnType<typeof getTickerProfile>>["top_members"]>;
  confirmationScoreBundle: ConfirmationScoreBundle | null | undefined;
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
  const showCongress = source === "all" || source === "congress";
  const showInsider = source === "all" || source === "insider";
  const showSignals = source === "all" || source === "signals";
  const activityPnlByEventId = new Map<number, number | null>(
    [...congressEvents, ...insiderEvents].map((event) => [event.id, readNumeric(event.pnl_pct)]),
  );
  const activityEventById = new Map<number, (typeof congressEvents)[number] | (typeof insiderEvents)[number]>(
    [...congressEvents, ...insiderEvents].map((event) => [event.id, event]),
  );
  const intelligenceNarrative = buildTickerIntelligenceNarrative({
    confirmationBundle,
    topSignal,
    netFlow,
  });
  const tickerReturnTo = tickerHref(normalizedSymbol) ?? `/ticker/${normalizedSymbol}`;
  const signalGateHref = signalsUnavailableMessage?.includes("Premium")
    ? "/pricing"
    : `/login?return_to=${encodeURIComponent(tickerReturnTo)}`;
  const signalGateLabel = signalsUnavailableMessage?.includes("Premium") ? "View Premium" : "Login or register";

  return (
    <>
      <section className={`${cardClassName} p-4`}>
        <div className="flex items-center justify-between gap-3">
          <h2 className="text-sm font-semibold uppercase tracking-[0.16em] text-slate-300">Ticker intelligence</h2>
          <span className="text-[11px] uppercase tracking-[0.12em] text-slate-500">Current posture</span>
        </div>
        <p className="mt-3 text-sm leading-relaxed text-slate-200">{intelligenceNarrative.summary}</p>
        <div className="mt-3 flex flex-wrap gap-2">
          {intelligenceNarrative.badges.map((badge) => (
            <Badge key={badge.label} tone={badge.tone} className="px-2.5 py-1 text-[11px]">
              {badge.label}
            </Badge>
          ))}
        </div>
        <div className="mt-4 border-t border-white/10 pt-4">
          <div className="flex flex-wrap items-start justify-between gap-4">
            <div>
              <p className="text-xs uppercase tracking-widest text-slate-400">{confirmationBundle.lookback_days}D confirmation</p>
              <div className="mt-1 flex flex-wrap items-center gap-2">
                <p className="text-3xl font-semibold text-white tabular-nums">{Math.round(confirmationBundle.score)}</p>
                <p className="text-xs uppercase tracking-widest text-slate-500">/ 100</p>
                <Badge tone={confirmationTone(confirmationBundle.direction)} className="px-2.5 py-1 text-[11px]">
                  {titleCase(confirmationBundle.band)}
                </Badge>
                <Badge tone={confirmationTone(confirmationBundle.direction)} className="px-2.5 py-1 text-[11px]">
                  {titleCase(confirmationBundle.direction)}
                </Badge>
              </div>
            </div>
            <div className="max-w-xl text-right">
              <p className="text-sm font-semibold text-slate-100">{confirmationBundle.status}</p>
              <p className="mt-1 text-xs leading-relaxed text-slate-400">{confirmationBundle.explanation}</p>
            </div>
          </div>
          <div className="mt-4 grid gap-2 md:grid-cols-2 xl:grid-cols-4">
            {confirmationSourceOrder.map((key) => {
              const sourceSummary = confirmationBundle.sources[key];
              return (
                <div key={key} className="rounded-lg border border-white/10 bg-white/[0.03] px-3 py-2">
                  <div className="flex items-center justify-between gap-2">
                    <p className="text-[11px] font-semibold uppercase tracking-[0.14em] text-slate-500">{confirmationSourceLabels[key]}</p>
                    <Badge tone={sourceSummary.present ? confirmationTone(sourceSummary.direction) : "neutral"} className="px-2 py-0.5 text-[10px]">
                      {sourceSummary.present ? titleCase(sourceSummary.direction) : "Inactive"}
                    </Badge>
                  </div>
                  <p className="mt-2 truncate text-xs font-semibold text-slate-200">{sourceSummary.label}</p>
                  <div className="mt-1 flex items-center justify-between gap-2 text-[11px] text-slate-500">
                    <span>{sourceScoreLabel(sourceSummary)}</span>
                    <span>{sourceFreshnessLabel(sourceSummary.freshness_days)}</span>
                  </div>
                </div>
              );
            })}
          </div>
          <div className="mt-3 flex flex-wrap gap-2 text-xs">
            {confirmationBundle.drivers.slice(0, 4).map((driver) => (
              <Badge key={driver} tone="neutral" className="px-2.5 py-1 text-[11px] normal-case tracking-normal">
                {driver}
              </Badge>
            ))}
          </div>
        </div>
      </section>

      <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4 2xl:grid-cols-8">
        <div className={`${cardClassName} p-4`}>
          <p className="text-xs uppercase tracking-widest text-slate-400">Congress buys</p>
          <p className="mt-2 text-right text-2xl font-semibold text-emerald-300 tabular-nums">{congressBuys}</p>
        </div>
        <div className={`${cardClassName} p-4`}>
          <p className="text-xs uppercase tracking-widest text-slate-400">Congress sells</p>
          <p className="mt-2 text-right text-2xl font-semibold text-rose-300 tabular-nums">{congressSells}</p>
        </div>
        <div className={`${cardClassName} p-4`}>
          <p className="text-xs uppercase tracking-widest text-slate-400">Insider buys</p>
          <p className="mt-2 text-right text-2xl font-semibold text-emerald-300 tabular-nums">{insiderBuys}</p>
        </div>
        <div className={`${cardClassName} p-4`}>
          <p className="text-xs uppercase tracking-widest text-slate-400">Insider sells</p>
          <p className="mt-2 text-right text-2xl font-semibold text-rose-300 tabular-nums">{insiderSells}</p>
        </div>
        <div className={`${cardClassName} p-4`}>
          <p className="text-xs uppercase tracking-widest text-slate-400">Net disclosed flow</p>
          <p className={`mt-2 text-right text-2xl font-semibold tabular-nums ${netFlow >= 0 ? "text-emerald-300" : "text-rose-300"}`}>
            {netFlow >= 0 ? "+" : "-"}${formatCompactUsd(Math.abs(netFlow))}
          </p>
        </div>
        <div className={`${cardClassName} p-4`}>
          <p className="text-xs uppercase tracking-widest text-slate-400">Unique Congress traders</p>
          <p className="mt-2 text-right text-2xl font-semibold text-white tabular-nums">{congressParticipantCount}</p>
        </div>
        <div className={`${cardClassName} p-4`}>
          <p className="text-xs uppercase tracking-widest text-slate-400">Unique insiders</p>
          <p className="mt-2 text-right text-2xl font-semibold text-white tabular-nums">{insiderParticipantCount}</p>
        </div>
        <div className={`${cardClassName} p-4`}>
          <p className="text-xs uppercase tracking-widest text-slate-400">Latest smart signal</p>
          {topSignal ? (
            <div className="mt-3 flex items-center justify-end">
              <div className="flex items-center gap-2">
                <Badge tone={signalTone(topSignal.smart_band)}>{topSignal.smart_band ?? "signal"}</Badge>
                <p className="text-xl font-semibold text-white tabular-nums">{topSignal.smart_score ?? "-"}</p>
              </div>
            </div>
          ) : (
            <p className="mt-3 text-sm text-slate-400">No current signal.</p>
          )}
        </div>
      </div>

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

      <Suspense fallback={<PremiumTickerChartSkeleton />}>
        <DeferredTickerChart chartBundlePromise={chartBundlePromise} />
      </Suspense>

      <div className="grid gap-6 xl:grid-cols-[2fr_1fr]">
        <div className="space-y-6">
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
        </div>

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
                  const state = match?.state?.trim().toUpperCase() || "—";
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
                  const state = member.state?.trim().toUpperCase() || "—";
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
        <div>
          <p className="text-xs font-semibold uppercase tracking-[0.3em] text-emerald-300">Ticker intelligence</p>
          <h1 className="text-3xl font-semibold text-white">
            {profile.ticker.symbol}
            <span className="text-slate-400"> · {profile.ticker.name ?? profile.ticker.symbol}</span>
          </h1>
          <div className="mt-2 flex flex-wrap gap-2 text-xs text-slate-400">
            <span className={pillClassName}>{profile.ticker.asset_class ?? "Equity"}</span>
            {profile.ticker.sector ? <span className={pillClassName}>{profile.ticker.sector}</span> : null}
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
          chartBundlePromise={chartBundlePromise}
        />
      </Suspense>
    </div>
  );
}
