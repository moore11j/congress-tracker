import Link from "next/link";
import { FeedCard } from "@/components/feed/FeedCard";
import { WatchlistTickerManager } from "@/components/watchlists/WatchlistTickerManager";
import { getWatchlist, getWatchlistEvents, getWatchlistSignals, type EventItem, type SignalItem } from "@/lib/api";
import type { FeedItem } from "@/lib/types";
import { cardClassName, ghostButtonClassName, primaryButtonClassName, selectClassName } from "@/lib/styles";

type ActivityMode = "all" | "congress" | "insider" | "signals";

const modeOptions: { value: ActivityMode; label: string }[] = [
  { value: "all", label: "All" },
  { value: "congress", label: "Congress" },
  { value: "insider", label: "Insiders" },
  { value: "signals", label: "Signals" },
];

function getParam(sp: Record<string, string | string[] | undefined>, key: string) {
  const value = sp[key];
  return typeof value === "string" ? value : "";
}

function parseMode(value: string): ActivityMode {
  return value === "congress" || value === "insider" || value === "signals" ? value : "all";
}

function recentDaysToSince(value: string): string | undefined {
  const days = Number(value);
  if (!Number.isFinite(days) || days < 1) return undefined;
  return new Date(Date.now() - days * 24 * 60 * 60 * 1000).toISOString();
}

function payloadText(payload: any, keys: string[]): string | null {
  for (const key of keys) {
    const value = payload?.[key] ?? payload?.raw?.[key];
    if (typeof value === "string" && value.trim()) return value.trim();
  }
  return null;
}

function eventToFeedItem(event: EventItem): FeedItem {
  const payload = event.payload ?? {};
  const isInsider = event.event_type === "insider_trade";
  const symbol = event.symbol ?? event.ticker ?? payloadText(payload, ["symbol", "ticker"]);
  const insiderName = payloadText(payload, ["insider_name", "insiderName"]) ?? event.member_name ?? "Unknown insider";
  const securityName =
    payloadText(payload, ["company_name", "companyName", "security_name", "securityName"]) ??
    symbol ??
    "Unknown";

  return {
    id: event.id,
    kind: event.event_type as FeedItem["kind"],
    member: {
      bioguide_id: event.member_bioguide_id ?? "",
      name: isInsider ? insiderName : event.member_name ?? "Unknown",
      chamber: event.chamber ?? "",
      party: event.party ?? null,
      state: null,
    },
    security: {
      symbol,
      name: securityName,
      asset_class: payloadText(payload, ["asset_class", "securityName"]) ?? "stock",
      sector: payloadText(payload, ["sector"]),
    },
    transaction_type: event.trade_type ?? "",
    owner_type: payloadText(payload, ["owner_type", "ownership"]) ?? (isInsider ? "insider" : ""),
    trade_date: payloadText(payload, ["transaction_date", "transactionDate", "trade_date", "tradeDate"]),
    report_date: payloadText(payload, ["filing_date", "filingDate", "report_date", "reportDate"]) ?? event.ts,
    amount_range_min: event.amount_min ?? null,
    amount_range_max: event.amount_max ?? null,
    estimated_price: event.estimated_price ?? null,
    current_price: event.current_price ?? null,
    display_price: event.display_price ?? null,
    reported_price: event.reported_price ?? null,
    reported_price_currency: event.reported_price_currency ?? null,
    pnl_pct: event.pnl_pct ?? null,
    smart_score: event.smart_score ?? null,
    smart_band: event.smart_band ?? null,
    member_net_30d: event.member_net_30d ?? null,
    symbol_net_30d: event.symbol_net_30d ?? null,
    confirmation_30d: event.confirmation_30d ?? null,
    insider: isInsider
      ? {
          name: insiderName,
          ownership: payloadText(payload, ["owner_type", "ownership"]),
          filing_date: payloadText(payload, ["filing_date", "filingDate"]),
          transaction_date: payloadText(payload, ["transaction_date", "transactionDate"]),
          price: typeof payload.price === "number" ? payload.price : null,
          display_price: typeof payload.display_price === "number" ? payload.display_price : null,
          reported_price: typeof payload.reported_price === "number" ? payload.reported_price : null,
          reported_price_currency: payloadText(payload, ["reported_price_currency", "reportedPriceCurrency"]),
          role: payloadText(payload, ["role", "position", "officerTitle", "typeOfOwner"]),
          reporting_cik: payloadText(payload, ["reporting_cik", "reportingCik"]),
        }
      : undefined,
  };
}

function signalToFeedItem(signal: SignalItem): FeedItem {
  const isInsider = signal.kind === "insider";
  const eventType = isInsider ? "insider_trade" : "congress_trade";
  const name = signal.who ?? (isInsider ? "Unknown insider" : "Unknown member");

  return {
    id: signal.event_id,
    kind: eventType,
    member: {
      bioguide_id: signal.member_bioguide_id ?? "",
      name,
      chamber: signal.chamber ?? "",
      party: signal.party ?? null,
      state: null,
    },
    security: {
      symbol: signal.symbol ?? null,
      name: signal.symbol ?? "Unknown",
      asset_class: "stock",
      sector: null,
    },
    transaction_type: signal.trade_type ?? "",
    owner_type: isInsider ? "insider" : "",
    trade_date: signal.ts,
    report_date: signal.ts,
    amount_range_min: signal.amount_min ?? null,
    amount_range_max: signal.amount_max ?? null,
    smart_score: signal.smart_score ?? null,
    smart_band: signal.smart_band ?? null,
    confirmation_30d: signal.confirmation_30d ?? null,
    insider: isInsider
      ? {
          name,
          role: signal.position ?? null,
          reporting_cik: signal.reporting_cik ?? null,
        }
      : undefined,
  };
}

function tabHref(watchlistId: number, mode: ActivityMode, recentDays: string, limit: string) {
  const params = new URLSearchParams();
  if (mode !== "all") params.set("mode", mode);
  if (recentDays) params.set("recent_days", recentDays);
  if (limit) params.set("limit", limit);
  const qs = params.toString();
  return `/watchlists/${watchlistId}${qs ? `?${qs}` : ""}`;
}

type Props = {
  params: Promise<{ id: string }>;
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
};

export default async function WatchlistDetailPage({ params, searchParams }: Props) {
  const { id } = await params;
  const watchlistId = Number(id);
  const sp = (await searchParams) ?? {};

  const mode = parseMode(getParam(sp, "mode"));
  const recentDays = getParam(sp, "recent_days") || "30";
  const cursor = getParam(sp, "cursor");
  const offset = Number(getParam(sp, "offset") || "0");
  const limit = getParam(sp, "limit") || "25";
  const numericLimit = Math.min(Math.max(Number(limit) || 25, 1), 100);

  const watchlist = await getWatchlist(watchlistId);
  const activity =
    mode === "signals"
      ? await getWatchlistSignals(watchlistId, {
          mode: "all",
          sort: "smart",
          limit: numericLimit,
          offset: Number.isFinite(offset) ? offset : 0,
        })
      : await getWatchlistEvents(watchlistId, {
          mode,
          since: recentDaysToSince(recentDays),
          cursor: cursor || undefined,
          limit: numericLimit,
        });

  const items =
    mode === "signals"
      ? (activity.items as SignalItem[]).map(signalToFeedItem)
      : (activity.items as EventItem[]).map(eventToFeedItem);

  const nextParams = new URLSearchParams();
  if (mode !== "all") nextParams.set("mode", mode);
  if (recentDays) nextParams.set("recent_days", recentDays);
  nextParams.set("limit", String(numericLimit));
  if (mode === "signals") {
    nextParams.set("offset", String((Number.isFinite(offset) ? offset : 0) + numericLimit));
  } else if ("next_cursor" in activity && activity.next_cursor) {
    nextParams.set("cursor", activity.next_cursor);
  }

  const canLoadMore = mode === "signals" ? items.length === numericLimit : Boolean("next_cursor" in activity && activity.next_cursor);

  return (
    <div className="space-y-6">
      <div className="flex flex-wrap items-center justify-between gap-4">
        <div>
          <p className="text-xs font-semibold uppercase tracking-[0.3em] text-emerald-300">Watchlist</p>
          <h1 className="text-3xl font-semibold text-white">{watchlist.name ?? `Watchlist #${watchlist.watchlist_id}`}</h1>
          <p className="text-sm text-slate-400">Monitor filings, insider trades, and unusual signals across saved tickers.</p>
        </div>
        <Link href="/watchlists" className={ghostButtonClassName}>
          Back to watchlists
        </Link>
      </div>

      <div className="grid gap-6 lg:grid-cols-[0.9fr_1.6fr]">
        <WatchlistTickerManager watchlistId={watchlist.watchlist_id} tickers={watchlist.tickers} />

        <section className={cardClassName}>
          <div className="flex flex-wrap items-start justify-between gap-4">
            <div>
              <h2 className="text-lg font-semibold text-white">Recent activity</h2>
              <p className="text-sm text-slate-400">
                {watchlist.tickers.length
                  ? `${items.length} items across ${watchlist.tickers.length} saved tickers.`
                  : "Add tickers to turn this into a monitoring feed."}
              </p>
            </div>
            <form method="get" className="flex flex-wrap items-end gap-3">
              <input type="hidden" name="mode" value={mode === "all" ? "" : mode} />
              <label className="grid gap-1 text-xs font-semibold uppercase tracking-wide text-slate-400">
                Window
                <select name="recent_days" defaultValue={recentDays} className={`${selectClassName} min-w-[140px] rounded-lg py-1.5`}>
                  <option value="7">Last 7 days</option>
                  <option value="30">Last 30 days</option>
                  <option value="90">Last 90 days</option>
                  <option value="180">Last 180 days</option>
                </select>
              </label>
              <label className="grid gap-1 text-xs font-semibold uppercase tracking-wide text-slate-400">
                Rows
                <select name="limit" defaultValue={String(numericLimit)} className={`${selectClassName} min-w-[96px] rounded-lg py-1.5`}>
                  <option value="25">25</option>
                  <option value="50">50</option>
                  <option value="100">100</option>
                </select>
              </label>
              <button type="submit" className={`${primaryButtonClassName} rounded-lg py-1.5`}>
                Apply
              </button>
            </form>
          </div>

          <div className="mt-5 flex flex-wrap gap-2">
            {modeOptions.map((option) => {
              const active = option.value === mode;
              return (
                <Link
                  key={option.value}
                  href={tabHref(watchlistId, option.value, recentDays, String(numericLimit))}
                  className={`rounded-lg border px-3 py-1.5 text-sm font-semibold transition ${
                    active
                      ? "border-emerald-300/40 bg-emerald-300/15 text-emerald-100"
                      : "border-white/10 text-slate-300 hover:border-white/20 hover:text-white"
                  }`}
                >
                  {option.label}
                </Link>
              );
            })}
          </div>

          <div className="mt-5 space-y-4">
            {items.length === 0 ? (
              <div className="rounded-lg border border-dashed border-white/15 bg-white/[0.03] p-6">
                <h3 className="font-semibold text-white">No recent activity yet</h3>
                <p className="mt-1 text-sm text-slate-400">
                  Add liquid tickers or widen the window to catch congressional filings, insider Form 4s, and unusual activity.
                </p>
              </div>
            ) : (
              items.map((item) => <FeedCard key={`${item.kind}-${item.id}`} item={item} density="compact" />)
            )}
          </div>

          <div className="mt-5 flex flex-wrap items-center justify-between gap-3">
            <span className="text-xs text-slate-500">
              Activity is filtered to symbols saved in this watchlist via the unified events and signals APIs.
            </span>
            {canLoadMore ? (
              <Link href={`/watchlists/${watchlistId}?${nextParams.toString()}`} className={primaryButtonClassName}>
                Load more
              </Link>
            ) : (
              <span className="text-sm text-slate-500">No more results.</span>
            )}
          </div>
        </section>
      </div>
    </div>
  );
}
