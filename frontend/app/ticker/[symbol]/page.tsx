import Link from "next/link";
import { Suspense } from "react";
import { Badge } from "@/components/Badge";
import { getEvents, getSignalsAll, getTickerPriceHistory, getTickerProfile } from "@/lib/api";
import { TickerActivityChart } from "@/components/ticker/TickerActivityChart";
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
  formatCurrencyRange,
  formatDateShort,
  formatTransactionLabel,
  partyBadge,
  transactionTone,
} from "@/lib/format";
import { memberHref } from "@/lib/memberSlug";
import { tickerHref } from "@/lib/ticker";
import { getInsiderDisplayName, insiderHref } from "@/lib/insider";

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

type TickerActivityData = {
  events: Awaited<ReturnType<typeof getEvents>>["items"];
  signals: Awaited<ReturnType<typeof getSignalsAll>>["items"];
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
  chartMarkers: Array<{
    id: string;
    kind: "congress" | "insider" | "signals";
    date: string;
    label: string;
    actor: string;
    action: string;
    amountMin?: number | null;
    amountMax?: number | null;
  }>;
  confirmation: ConfirmationSummary | null;
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

function formatCompactUsd(value: number): string {
  const abs = Math.abs(value);
  if (abs >= 1_000_000_000) return `${(value / 1_000_000_000).toFixed(2)}B`;
  if (abs >= 1_000_000) return `${(value / 1_000_000).toFixed(2)}M`;
  if (abs >= 1_000) return `${(value / 1_000).toFixed(1)}K`;
  return value.toFixed(0);
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

function insiderBiasLabel(confirmation: ConfirmationSummary | null): { label: string; tone: "pos" | "neg" | "neutral" } {
  if (!confirmation || !confirmation.insider_active_30d) return { label: "No insider side signal", tone: "neutral" };
  if (confirmation.insider_buy_count_30d > confirmation.insider_sell_count_30d) return { label: "Insider buy-skewed", tone: "pos" };
  if (confirmation.insider_sell_count_30d > confirmation.insider_buy_count_30d) return { label: "Insider sell-skewed", tone: "neg" };
  return { label: "Insider mixed", tone: "neutral" };
}

function hrefWithFilters(symbol: string, lookback: Lookback, source: SourceFilter, side: SideFilter): string {
  const q = new URLSearchParams();
  q.set("lookback", lookback);
  q.set("source", source);
  q.set("side", side);
  const base = tickerHref(symbol) ?? `/ticker/${encodeURIComponent(symbol)}`;
  return `${base}?${q.toString()}`;
}

function InlineEmptyState({ message }: { message: string }) {
  return (
    <div className="rounded-2xl border border-dashed border-white/15 bg-white/[0.02] px-4 py-3">
      <p className="text-sm text-slate-400">{message}</p>
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

async function resolveTickerActivityData({
  eventsPromise,
  signalsPromise,
  lookbackStartKey,
  side,
}: {
  eventsPromise: ReturnType<typeof getEvents>;
  signalsPromise: ReturnType<typeof getSignalsAll>;
  lookbackStartKey: string;
  side: SideFilter;
}): Promise<TickerActivityData> {
  const [eventsRes, signalsRes] = await Promise.all([eventsPromise, signalsPromise]);

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
  const chartMarkers = [
    ...congressEvents.map((event) => ({ id: `congress-${event.id}`, kind: "congress" as const, date: toDateKey(event.ts), label: "Congress", actor: event.member_name ?? "Unknown Member", action: formatTransactionLabel(event.trade_type), amountMin: event.amount_min, amountMax: event.amount_max })),
    ...insiderEvents.map((event) => ({ id: `insider-${event.id}`, kind: "insider" as const, date: toDateKey(event.ts), label: "Insider", actor: resolveInsiderName(event), action: formatTransactionLabel(event.trade_type), amountMin: event.amount_min, amountMax: event.amount_max })),
    ...signals.map((signal) => ({ id: `signal-${signal.event_id}-${signal.ts}`, kind: "signals" as const, date: toDateKey(signal.ts), label: "Signal", actor: getInsiderDisplayName(signal.who) ?? signal.symbol, action: signal.smart_band ? `${signal.smart_band} signal` : "signal", amountMin: signal.amount_min, amountMax: signal.amount_max })),
  ].reduce<TickerActivityData["chartMarkers"]>((acc, marker) => {
    if (!marker.date) return acc;
    acc.push({ ...marker, date: marker.date });
    return acc;
  }, []);
  const confirmationFromEvents = events.find((event) => (event as any).confirmation_30d)?.confirmation_30d as ConfirmationSummary | undefined;
  const confirmationFromSignals = signals.find((signal) => signal.confirmation_30d)?.confirmation_30d as ConfirmationSummary | undefined;
  const confirmation = confirmationFromEvents ?? confirmationFromSignals ?? null;

  return {
    events,
    signals,
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
    chartMarkers,
    confirmation,
  };
}

async function DeferredTickerContent({
  activityPromise,
  normalizedSymbol,
  lookback,
  source,
  side,
  topMembers,
  pricePoints,
  benchmarkPoints,
}: {
  activityPromise: Promise<TickerActivityData>;
  normalizedSymbol: string;
  lookback: Lookback;
  source: SourceFilter;
  side: SideFilter;
  topMembers: NonNullable<Awaited<ReturnType<typeof getTickerProfile>>["top_members"]>;
  pricePoints: Awaited<ReturnType<typeof getTickerPriceHistory>>["points"];
  benchmarkPoints: Awaited<ReturnType<typeof getTickerPriceHistory>>["points"];
}) {
  const {
    signals,
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
    chartMarkers,
    confirmation,
  } = await activityPromise;
  const showCongress = source === "all" || source === "congress";
  const showInsider = source === "all" || source === "insider";
  const showSignals = source === "all" || source === "signals";
  const insiderBias = insiderBiasLabel(confirmation);

  return (
    <>
      <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4 2xl:grid-cols-7">
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
      </div>

      <div className="grid gap-3 md:grid-cols-[minmax(0,2.3fr)_minmax(0,1fr)]">
        <div className={`${cardClassName} p-4`}>
          <div className="flex flex-wrap items-center justify-between gap-3">
            <p className="text-xs uppercase tracking-widest text-slate-400">Cross-source confirmation (30D)</p>
            {confirmation?.cross_source_confirmed_30d ? (
              <Badge tone="neutral" className="border-cyan-400/25 bg-cyan-400/10 text-cyan-100">Congress + Insider active</Badge>
            ) : (
              <Badge tone="neutral">Single-source / inactive</Badge>
            )}
          </div>
          <div className="mt-3 flex flex-wrap gap-2 text-xs">
            <Badge tone={confirmation?.congress_active_30d ? "house" : "neutral"}>
              Congress {confirmation?.congress_active_30d ? "active" : "inactive"} · {confirmation?.congress_trade_count_30d ?? 0}
            </Badge>
            <Badge tone={confirmation?.insider_active_30d ? "ind" : "neutral"}>
              Insider {confirmation?.insider_active_30d ? "active" : "inactive"} · {confirmation?.insider_trade_count_30d ?? 0}
            </Badge>
            <Badge tone={insiderBias.tone}>{insiderBias.label}</Badge>
          </div>
        </div>
        <div className={`${cardClassName} p-4`}>
          <p className="text-xs uppercase tracking-widest text-slate-400">Latest smart signal</p>
          {topSignal ? (
            <div className="mt-3 flex items-center justify-end">
              <div className="flex items-center gap-2">
                <Badge tone={signalTone(topSignal.smart_band)}>{topSignal.smart_band ?? "signal"}</Badge>
                <p className="text-xl font-semibold text-white tabular-nums">{topSignal.smart_score ?? "—"}</p>
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

      <TickerActivityChart points={pricePoints} benchmarkPoints={benchmarkPoints} markers={chartMarkers} symbol={normalizedSymbol} />

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

                    return (
                    <div key={event.id} className="rounded-2xl border border-white/10 bg-white/5 p-4">
                      <div className="flex flex-wrap items-center justify-between gap-3">
                        <div className="flex items-center gap-2">
                          {memberLink ? (
                            <Link href={memberLink} className="text-sm font-semibold text-emerald-200">
                              {memberName}
                            </Link>
                          ) : (
                            <span className="text-sm font-semibold text-slate-100">{memberName}</span>
                          )}
                          <Badge tone="house">Congress</Badge>
                        </div>
                        <Badge tone={transactionTone(event.trade_type)}>{formatTransactionLabel(event.trade_type)}</Badge>
                      </div>
                      <div className="mt-2 text-xs text-slate-400">Filed {formatDateShort(event.ts)}</div>
                      <div className="mt-2 text-right text-sm font-semibold text-white tabular-nums">
                        {formatCurrencyRange(event.amount_min ?? null, event.amount_max ?? null)}
                      </div>
                    </div>
                    );
                  })
                )}
              </div>
            </section>
          ) : null}

          {showInsider ? (
            <section className={cardClassName}>
              <div className="mb-4 flex items-center justify-between">
                <h2 className="text-lg font-semibold text-white">Insider activity</h2>
                <span className="text-xs text-slate-400">{insiderEvents.length} events</span>
              </div>
              <div className="space-y-3">
                {insiderEvents.length === 0 ? (
                  <p className="text-sm text-slate-400">No insider trades in the selected window.</p>
                ) : (
                  insiderEvents.slice(0, 20).map((event) => {
                    const insiderProfileHref = insiderHref(resolveInsiderName(event), resolveInsiderReportingCik(event));

                    return (
                    <div key={event.id} className="rounded-2xl border border-white/10 bg-white/5 p-4">
                      <div className="flex flex-wrap items-center justify-between gap-3">
                        <div className="flex items-center gap-2">
                          {insiderProfileHref ? (
                            <Link href={insiderProfileHref} className="text-sm font-semibold text-emerald-200">
                              {resolveInsiderName(event)}
                            </Link>
                          ) : (
                            <p className="text-sm font-semibold text-slate-100">{resolveInsiderName(event)}</p>
                          )}
                          <Badge tone="ind">Insider</Badge>
                        </div>
                        <Badge tone={transactionTone(event.trade_type)}>{formatTransactionLabel(event.trade_type)}</Badge>
                      </div>
                      <div className="mt-2 text-xs text-slate-400">Reported {formatDateShort(event.ts)}</div>
                      <div className="mt-2 text-right text-sm font-semibold text-white tabular-nums">
                        {formatCurrencyRange(event.amount_min ?? null, event.amount_max ?? null)}
                      </div>
                    </div>
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
                <span className="text-xs text-slate-400">{signals.length} signals</span>
              </div>
              <div className="space-y-3">
                {signals.length === 0 ? (
                  <p className="text-sm text-slate-400">No smart signals for this symbol in current filters.</p>
                ) : (
                  signals.slice(0, 20).map((signal) => {
                    const isInsiderSignal = signal.kind === "insider";
                    const insiderProfileHref = insiderHref(getInsiderDisplayName(signal.who), signal.reporting_cik ?? null);

                    return (
                    <div key={`${signal.kind}-${signal.event_id}-${signal.ts}`} className="rounded-2xl border border-white/10 bg-white/5 p-4">
                      <div className="flex flex-wrap items-center justify-between gap-3">
                        <div className="flex items-center gap-2">
                          {isInsiderSignal && insiderProfileHref ? (
                            <Link href={insiderProfileHref} className="text-sm font-semibold text-emerald-200">
                              {getInsiderDisplayName(signal.who) ?? "Unknown"}
                            </Link>
                          ) : (
                            <p className="text-sm font-semibold text-slate-100">{getInsiderDisplayName(signal.who) ?? "Unknown"}</p>
                          )}
                          <Badge tone={signal.kind === "insider" ? "ind" : "house"}>{signal.kind ?? "signal"}</Badge>
                        </div>
                        <div className="flex items-center gap-2">
                          <Badge tone={transactionTone(signal.trade_type)}>{formatTransactionLabel(signal.trade_type)}</Badge>
                          <Badge tone={signalTone(signal.smart_band)}>{signal.smart_band ?? "signal"}</Badge>
                        </div>
                      </div>
                      <div className="mt-2 flex items-center justify-between text-xs text-slate-400">
                        <span>{formatDateShort(signal.ts)}</span>
                        <span className="font-semibold text-slate-200">Smart {signal.smart_score ?? "—"}</span>
                      </div>
                      <div className="mt-2 text-right text-sm font-semibold text-white tabular-nums">
                        {formatCurrencyRange(signal.amount_min ?? null, signal.amount_max ?? null)}
                      </div>
                    </div>
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
                      <Link key={participant.name} href={resolvedHref} className={rowClassName}>
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

  const profilePromise = getTickerProfile(normalizedSymbol);
  const priceHistoryPromise = getTickerPriceHistory(normalizedSymbol, Number(lookback));
  const benchmarkHistoryPromise = getTickerPriceHistory("SPY", Number(lookback)).catch(() => null);
  const eventsPromise = getEvents({ symbol: normalizedSymbol, recent_days: Number(lookback), limit: 100, include_total: "1" });
  const signalsPromise = getSignalsAll({
    mode: source === "congress" || source === "insider" ? source : "all",
    side,
    preset: "balanced",
    sort: "smart",
    limit: 100,
    symbol: normalizedSymbol,
  });

  const [profile, priceHistoryRes, benchmarkHistoryRes] = await Promise.all([profilePromise, priceHistoryPromise, benchmarkHistoryPromise]);
  const activityPromise = resolveTickerActivityData({
    eventsPromise,
    signalsPromise,
    lookbackStartKey: priceHistoryRes.start_date,
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
        <Link href="/?mode=all" className={ghostButtonClassName}>Back to feed</Link>
      </div>
      <Suspense fallback={<DeferredTickerSummarySkeleton />}>
        <DeferredTickerContent
          activityPromise={activityPromise}
          normalizedSymbol={normalizedSymbol}
          lookback={lookback}
          source={source}
          side={side}
          topMembers={profile.top_members ?? []}
          pricePoints={priceHistoryRes.points ?? []}
          benchmarkPoints={benchmarkHistoryRes?.points ?? []}
        />
      </Suspense>
    </div>
  );
}
