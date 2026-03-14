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

type Props = {
  params: Promise<{ symbol: string }>;
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
};

type Lookback = "30" | "90" | "180" | "365";
type SourceFilter = "all" | "congress" | "insider" | "signals";
type SideFilter = "all" | "buy" | "sell";

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

function formatCompactUsd(value: number): string {
  const abs = Math.abs(value);
  if (abs >= 1_000_000_000) return `${(value / 1_000_000_000).toFixed(2)}B`;
  if (abs >= 1_000_000) return `${(value / 1_000_000).toFixed(2)}M`;
  if (abs >= 1_000) return `${(value / 1_000).toFixed(1)}K`;
  return value.toFixed(0);
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
  return `/ticker/${symbol}?${q.toString()}`;
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

  const netFlow = filteredEvents.reduce((acc, event) => {
    const sideValue = normalizeTradeSide(event.trade_type);
    const amount = Number(event.amount_max ?? event.amount_min ?? 0);
    if (!Number.isFinite(amount) || amount <= 0 || !sideValue) return acc;
    if (sideValue === "buy") return acc + amount;
    return acc - amount;
  }, 0);

  const topSignal = [...signals].sort((a, b) => (b.smart_score ?? 0) - (a.smart_score ?? 0))[0];

  const congressParticipantMap = new Map<string, number>();
  const insiderParticipantMap = new Map<string, number>();
  const congressParticipantHrefMap = new Map<string, string>();

  for (const event of congressEvents) {
    const who = (event.member_name ?? "Unknown Member").trim();
    congressParticipantMap.set(who, (congressParticipantMap.get(who) ?? 0) + 1);

    const safeHref = memberHref({ name: event.member_name ?? undefined, memberId: event.member_bioguide_id ?? undefined });
    if (safeHref && safeHref !== "/member/UNKNOWN" && !congressParticipantHrefMap.has(who)) {
      congressParticipantHrefMap.set(who, safeHref);
    }
  }
  for (const event of insiderEvents) {
    const who =
      (typeof event.payload?.insider_name === "string" && event.payload.insider_name.trim()) ||
      event.member_name ||
      "Unknown Insider";
    insiderParticipantMap.set(who, (insiderParticipantMap.get(who) ?? 0) + 1);
  }

  const topCongressParticipants = [...congressParticipantMap.entries()]
    .sort((a, b) => b[1] - a[1])
    .slice(0, 5);
  const topInsiderParticipants = [...insiderParticipantMap.entries()]
    .sort((a, b) => b[1] - a[1])
    .slice(0, 5);

  const showCongress = source === "all" || source === "congress";
  const showInsider = source === "all" || source === "insider";
  const showSignals = source === "all" || source === "signals";

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

      <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
        <div className={`${cardClassName} p-4`}>
          <p className="text-xs uppercase tracking-widest text-slate-400">Congress trades</p>
          <p className="mt-2 text-2xl font-semibold text-white tabular-nums">{congressEvents.length}</p>
        </div>
        <div className={`${cardClassName} p-4`}>
          <p className="text-xs uppercase tracking-widest text-slate-400">Insider trades</p>
          <p className="mt-2 text-2xl font-semibold text-white tabular-nums">{insiderEvents.length}</p>
        </div>
        <div className={`${cardClassName} p-4`}>
          <p className="text-xs uppercase tracking-widest text-slate-400">Net disclosed flow</p>
          <p className={`mt-2 text-2xl font-semibold tabular-nums ${netFlow >= 0 ? "text-emerald-300" : "text-rose-300"}`}>
            {netFlow >= 0 ? "+" : "-"}${formatCompactUsd(Math.abs(netFlow))}
          </p>
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
        <div className="grid gap-4 lg:grid-cols-3">
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
            <p className="mb-2 text-xs uppercase tracking-widest text-slate-400">Source</p>
            <div className="flex flex-wrap gap-2">
              {(["all", "congress", "insider", "signals"] as const).map((value) => (
                <Link
                  key={value}
                  href={hrefWithFilters(normalizedSymbol, lookback, value, side)}
                  className={`rounded-full border px-3 py-1 text-xs font-semibold uppercase ${
                    source === value
                      ? "border-emerald-400/40 bg-emerald-400/10 text-emerald-200"
                      : "border-white/10 bg-slate-900/60 text-slate-300"
                  }`}
                >
                  {value}
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
                          {(typeof event.payload?.insider_name === "string" && event.payload.insider_name) || event.member_name || "Unknown insider"}
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
            <h2 className="text-lg font-semibold text-white">Notable Congress participants</h2>
            <div className="mt-4 space-y-2">
              {topCongressParticipants.length === 0 ? (
                <p className="text-sm text-slate-400">No participants in current window.</p>
              ) : (
                topCongressParticipants.map(([name, count]) => {
                  const match = profile.top_members.find((member) => member.name === name);
                  const resolvedHref = match
                    ? memberHref({ name: match.name, memberId: match.bioguide_id })
                    : congressParticipantHrefMap.get(name);
                  const rowClassName = "flex items-center justify-between rounded-xl border border-white/10 bg-white/5 px-3 py-2 text-sm text-slate-200";

                  if (resolvedHref) {
                    return (
                      <Link key={name} href={resolvedHref} className={rowClassName}>
                        <span className="truncate">{name}</span>
                        <span className="tabular-nums text-slate-400">{count}</span>
                      </Link>
                    );
                  }

                  return (
                    <div key={name} className={rowClassName}>
                      <span className="truncate">{name}</span>
                      <span className="tabular-nums text-slate-400">{count}</span>
                    </div>
                  );
                })
              )}
            </div>
          </section>

          <section className={cardClassName}>
            <h2 className="text-lg font-semibold text-white">Notable insiders</h2>
            <div className="mt-4 space-y-2">
              {topInsiderParticipants.length === 0 ? (
                <p className="text-sm text-slate-400">No insiders in current window.</p>
              ) : (
                topInsiderParticipants.map(([name, count]) => (
                  <div
                    key={name}
                    className="flex items-center justify-between rounded-xl border border-white/10 bg-white/5 px-3 py-2 text-sm text-slate-200"
                  >
                    <span className="truncate">{name}</span>
                    <span className="tabular-nums text-slate-400">{count}</span>
                  </div>
                ))
              )}
            </div>
          </section>

          <section className={cardClassName}>
            <h2 className="text-lg font-semibold text-white">Legacy top members</h2>
            <div className="mt-3 space-y-2">
              {profile.top_members.length === 0 ? (
                <p className="text-sm text-slate-400">No historical member profile data.</p>
              ) : (
                profile.top_members.slice(0, 5).map((member) => (
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
