import Link from "next/link";
import { Badge } from "@/components/Badge";
import { getEvents, getSignalsAll, getTickerProfile } from "@/lib/api";
import {
  cardClassName,
  ghostButtonClassName,
  pillClassName,
} from "@/lib/styles";
import {
  formatCurrencyRange,
  formatDateShort,
  formatMemberSubtitle,
  formatTransactionLabel,
  transactionTone,
} from "@/lib/format";
import { memberHref } from "@/lib/memberSlug";
import { tickerHref } from "@/lib/ticker";

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

function asTrimmedString(value: unknown): string | null {
  if (typeof value !== "string") return null;
  const cleaned = value.trim();
  return cleaned ? cleaned : null;
}

function resolveInsiderName(event: { member_name?: string | null; payload?: any }): string {
  const payload = event.payload;
  const raw = payload?.raw && typeof payload.raw === "object" ? payload.raw : null;
  const insider = payload?.insider && typeof payload.insider === "object" ? payload.insider : null;

  return (
    asTrimmedString(payload?.insider_name) ??
    asTrimmedString(insider?.name) ??
    asTrimmedString(raw?.reportingName) ??
    asTrimmedString(raw?.reportingOwnerName) ??
    asTrimmedString(raw?.ownerName) ??
    asTrimmedString(raw?.insiderName) ??
    asTrimmedString(event.member_name) ??
    "Unknown Insider"
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

function hrefWithFilters(symbol: string, lookback: Lookback, source: SourceFilter, side: SideFilter): string {
  const q = new URLSearchParams();
  q.set("lookback", lookback);
  q.set("source", source);
  q.set("side", side);
  const base = tickerHref(symbol) ?? `/ticker/${encodeURIComponent(symbol)}`;
  return `${base}?${q.toString()}`;
}

export default async function TickerPage({ params, searchParams }: Props) {
  const { symbol } = await params;
  const sp = (await searchParams) ?? {};

  const lookback = clampLookback(one(sp, "lookback"));
  const source = clampSource(one(sp, "source"));
  const side = clampSide(one(sp, "side"));

  const normalizedSymbol = symbol.trim().toUpperCase();

  const [profile, eventsRes, signalsRes] = await Promise.all([
    getTickerProfile(normalizedSymbol),
    getEvents({
      symbol: normalizedSymbol,
      recent_days: Number(lookback),
      limit: 100,
      include_total: "1",
    }),
    getSignalsAll({
      mode: source === "congress" || source === "insider" ? source : "all",
      side,
      preset: "balanced",
      sort: "smart",
      limit: 100,
      symbol: normalizedSymbol,
    }),
  ]);

  const events = eventsRes.items ?? [];
  const signals = signalsRes.items ?? [];

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
    if (safeHref && safeHref !== "/member/UNKNOWN" && !existing.href) {
      existing.href = safeHref;
    }
    congressParticipantMap.set(who, existing);
  }

  for (const event of insiderEvents) {
    const who = resolveInsiderName(event);
    const sideValue = normalizeTradeSide(event.trade_type);
    const amount = Number(event.amount_max ?? event.amount_min ?? 0);
    const existing = insiderParticipantMap.get(who) ?? { name: who, trades: 0, buys: 0, sells: 0, netFlow: 0 };
    existing.trades += 1;
    if (sideValue === "buy") existing.buys += 1;
    if (sideValue === "sell") existing.sells += 1;
    if (Number.isFinite(amount) && amount > 0) {
      existing.netFlow += sideValue === "sell" ? -amount : sideValue === "buy" ? amount : 0;
    }
    insiderParticipantMap.set(who, existing);
  }

  const topCongressParticipants = [...congressParticipantMap.values()]
    .sort((a, b) => b.trades - a.trades)
    .slice(0, 5);
  const topInsiderParticipants = [...insiderParticipantMap.values()]
    .sort((a, b) => b.trades - a.trades)
    .slice(0, 5);

  const showCongress = source === "all" || source === "congress";
  const showInsider = source === "all" || source === "insider";
  const showSignals = source === "all" || source === "signals";
  const topMembers = profile.top_members ?? [];

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
        <Link href="/?mode=all" className={ghostButtonClassName}>
          Back to feed
        </Link>
      </div>

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
          <p className="mt-2 text-right text-2xl font-semibold text-white tabular-nums">{congressParticipantMap.size}</p>
        </div>
        <div className={`${cardClassName} p-4`}>
          <p className="text-xs uppercase tracking-widest text-slate-400">Unique insiders</p>
          <p className="mt-2 text-right text-2xl font-semibold text-white tabular-nums">{insiderParticipantMap.size}</p>
        </div>
      </div>

      <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
        <div className={`${cardClassName} p-4 md:col-span-2 xl:col-span-3`}>
          <div className="flex items-center justify-between gap-3">
            <p className="text-xs uppercase tracking-widest text-slate-400">Activity view</p>
            <p className="text-xs text-slate-500">All / Congress / Insiders / Signals</p>
          </div>
          <div className="mt-3 inline-flex rounded-xl border border-white/10 bg-slate-950/80 p-1">
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
          <p className="text-xs uppercase tracking-widest text-slate-400">Latest smart signal</p>
          {topSignal ? (
            <div className="mt-2 flex items-center justify-between gap-3">
              <Badge tone={signalTone(topSignal.smart_band)}>{topSignal.smart_band ?? "signal"}</Badge>
              <p className="text-xl font-semibold text-white tabular-nums">{topSignal.smart_score ?? "—"}</p>
            </div>
          ) : (
            <p className="mt-2 text-sm text-slate-400">No current signal.</p>
          )}
        </div>
      </div>

      <div className={`${cardClassName} p-4`}>
        <div className="grid gap-4 lg:grid-cols-2">
          <div>
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
          <div>
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
      </div>

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
                  congressEvents.slice(0, 20).map((event) => (
                    <div key={event.id} className="rounded-2xl border border-white/10 bg-white/5 p-4">
                      <div className="flex flex-wrap items-center justify-between gap-3">
                        <Link href={memberHref({ name: event.member_name ?? "Unknown", memberId: event.member_bioguide_id })} className="text-sm font-semibold text-emerald-200">
                          {event.member_name ?? "Unknown"}
                        </Link>
                        <div className="flex items-center gap-2">
                          <Badge tone="house">Congress</Badge>
                          <Badge tone={transactionTone(event.trade_type)}>{formatTransactionLabel(event.trade_type)}</Badge>
                        </div>
                      </div>
                      <div className="mt-2 text-xs text-slate-400">Filed {formatDateShort(event.ts)}</div>
                      <div className="mt-2 text-right text-sm font-semibold text-white tabular-nums">
                        {formatCurrencyRange(event.amount_min ?? null, event.amount_max ?? null)}
                      </div>
                    </div>
                  ))
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
                  insiderEvents.slice(0, 20).map((event) => (
                    <div key={event.id} className="rounded-2xl border border-white/10 bg-white/5 p-4">
                      <div className="flex flex-wrap items-center justify-between gap-3">
                        <p className="text-sm font-semibold text-slate-100">
                          {resolveInsiderName(event)}
                        </p>
                        <div className="flex items-center gap-2">
                          <Badge tone="ind">Insider</Badge>
                          <Badge tone={transactionTone(event.trade_type)}>{formatTransactionLabel(event.trade_type)}</Badge>
                        </div>
                      </div>
                      <div className="mt-2 text-xs text-slate-400">Reported {formatDateShort(event.ts)}</div>
                      <div className="mt-2 text-right text-sm font-semibold text-white tabular-nums">
                        {formatCurrencyRange(event.amount_min ?? null, event.amount_max ?? null)}
                      </div>
                    </div>
                  ))
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
                  signals.slice(0, 20).map((signal) => (
                    <div key={`${signal.kind}-${signal.event_id}`} className="rounded-2xl border border-white/10 bg-white/5 p-4">
                      <div className="flex flex-wrap items-center justify-between gap-3">
                        <p className="text-sm font-semibold text-slate-100">{signal.who ?? "Unknown"}</p>
                        <div className="flex items-center gap-2">
                          <Badge tone={signal.kind === "insider" ? "ind" : "house"}>{signal.kind ?? "signal"}</Badge>
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
                  ))
                )}
              </div>
            </section>
          ) : null}
        </div>

        <div className="space-y-6">
          <section className={cardClassName}>
            <h2 className="text-lg font-semibold text-white">Top Congress traders</h2>
            <div className="mt-4 space-y-2">
              {topCongressParticipants.length === 0 ? (
                <p className="text-sm text-slate-400">No Congress participants in current window.</p>
              ) : (
                topCongressParticipants.map((participant) => {
                  const match = topMembers.find((member) => member.name === participant.name);
                  const resolvedHref = participant.href ?? (match ? memberHref({ name: match.name, memberId: match.bioguide_id }) : undefined);
                  const bias = biasLabel(participant.buys, participant.sells);
                  const rowClassName = "rounded-xl border border-white/10 bg-white/5 px-3 py-2 text-sm text-slate-200";

                  const content = (
                    <>
                      <div className="flex items-start justify-between gap-3">
                        <span className="truncate font-semibold text-slate-100">{participant.name}</span>
                        <span className="tabular-nums text-slate-300">{participant.trades}</span>
                      </div>
                      <div className="mt-1 flex items-center justify-between gap-3 text-xs text-slate-400">
                        <Badge tone={bias.tone}>{bias.label}</Badge>
                        <span className={`tabular-nums ${participant.netFlow >= 0 ? "text-emerald-300" : "text-rose-300"}`}>
                          {participant.netFlow >= 0 ? "+" : "-"}${formatCompactUsd(Math.abs(participant.netFlow))}
                        </span>
                      </div>
                    </>
                  );

                  if (resolvedHref) {
                    return (
                      <Link key={participant.name} href={resolvedHref} className={`${rowClassName} block`}>
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
            <div className="mt-4 space-y-2">
              {topInsiderParticipants.length === 0 ? (
                <p className="text-sm text-slate-400">No insiders in current window.</p>
              ) : (
                topInsiderParticipants.map((participant) => {
                  const bias = biasLabel(participant.buys, participant.sells);
                  return (
                  <div
                    key={participant.name}
                    className="rounded-xl border border-white/10 bg-white/5 px-3 py-2 text-sm text-slate-200"
                  >
                    <div className="flex items-start justify-between gap-3">
                      <span className="truncate font-semibold text-slate-100">{participant.name}</span>
                      <span className="tabular-nums text-slate-300">{participant.trades}</span>
                    </div>
                    <div className="mt-1 flex items-center justify-between gap-3 text-xs text-slate-400">
                      <Badge tone={bias.tone}>{bias.label}</Badge>
                      <span className={`tabular-nums ${participant.netFlow >= 0 ? "text-emerald-300" : "text-rose-300"}`}>
                        {participant.netFlow >= 0 ? "+" : "-"}${formatCompactUsd(Math.abs(participant.netFlow))}
                      </span>
                    </div>
                  </div>
                  );
                })
              )}
            </div>
          </section>

          <section className={cardClassName}>
            <h2 className="text-lg font-semibold text-white">Historical Congress participants</h2>
            <div className="mt-3 space-y-2">
              {topMembers.length === 0 ? (
                <p className="text-sm text-slate-400">No historical member profile data.</p>
              ) : (
                topMembers.slice(0, 5).map((member) => (
                  <Link
                    key={member.member_id}
                    href={memberHref({ name: member.name, memberId: member.bioguide_id })}
                    className="flex items-center justify-between rounded-xl border border-white/10 bg-white/5 px-3 py-2 text-sm text-slate-200"
                  >
                    <div>
                      <div className="text-sm font-semibold text-slate-100">{member.name}</div>
                      <div className="text-xs text-slate-400">{formatMemberSubtitle(member)}</div>
                    </div>
                    <span className="tabular-nums text-slate-400">{member.trade_count}</span>
                  </Link>
                ))
              )}
            </div>
          </section>
        </div>
      </div>
    </div>
  );
}
