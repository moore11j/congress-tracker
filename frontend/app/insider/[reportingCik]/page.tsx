import Link from "next/link";
import {
  getInsiderSummary,
  getInsiderTopTickers,
  getInsiderTrades,
} from "@/lib/api";
import { Badge, type BadgeTone } from "@/components/Badge";
import {
  cardClassName,
  ghostButtonClassName,
  compactInteractiveSurfaceClassName,
} from "@/lib/styles";
import { formatDateShort, formatTransactionLabel, transactionTone } from "@/lib/format";
import { getInsiderDisplayName } from "@/lib/insider";
import { tickerHref } from "@/lib/ticker";
import { TickerPill } from "@/components/ui/TickerPill";

type Props = {
  params: Promise<{ reportingCik: string }>;
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
};

type Lookback = "30" | "90" | "365";

function one(sp: Record<string, string | string[] | undefined>, key: string): string {
  const value = sp[key];
  return typeof value === "string" ? value : "";
}

function clampLookback(v: string): Lookback {
  return v === "30" || v === "90" || v === "365" ? v : "90";
}

function formatCompactUsd(value: number): string {
  const abs = Math.abs(value);
  if (abs >= 1_000_000_000) return `${(value / 1_000_000_000).toFixed(2)}B`;
  if (abs >= 1_000_000) return `${(value / 1_000_000).toFixed(2)}M`;
  if (abs >= 1_000) return `${(value / 1_000).toFixed(1)}K`;
  return value.toFixed(0);
}

function formatMoney(value: number): string {
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 0,
  }).format(value);
}

function parseNum(value: unknown): number | null {
  if (typeof value === "number") return Number.isFinite(value) ? value : null;
  if (typeof value === "string") {
    const n = Number(value.replace(/[$,% ,]/g, "").trim());
    return Number.isFinite(n) ? n : null;
  }
  return null;
}

function formatPnl(pnl: number): string {
  const arrow = pnl > 0 ? "▲" : pnl < 0 ? "▼" : "•";
  return `${arrow} ${Math.abs(pnl).toFixed(1)}%`;
}

function pnlClass(pnl: number): string {
  if (pnl > 0) return "text-emerald-300";
  if (pnl < 0) return "text-rose-300";
  return "text-slate-300";
}

function signalFromTrade(trade: Record<string, unknown>, tradeType: string | null): { label: string; tone: BadgeTone } {
  const smartScore = parseNum(trade.smart_score);
  const smartBand = typeof trade.smart_band === "string" ? trade.smart_band.toLowerCase() : null;
  if (smartScore !== null) {
    if (smartBand === "strong") return { label: `Smart ${Math.round(smartScore)}`, tone: "pos" };
    if (smartBand === "notable" || smartBand === "mild") return { label: `Smart ${Math.round(smartScore)}`, tone: "neutral" };
    return { label: `Smart ${Math.round(smartScore)}`, tone: "neg" };
  }

  return {
    label: formatTransactionLabel(tradeType ?? "") ?? "—",
    tone: transactionTone(tradeType ?? ""),
  };
}

function hrefWithLookback(reportingCik: string, lookback: Lookback): string {
  return `/insider/${encodeURIComponent(reportingCik)}?lookback=${lookback}`;
}

export default async function InsiderPage({ params, searchParams }: Props) {
  const { reportingCik } = await params;
  const sp = (await searchParams) ?? {};
  const lookback = clampLookback(one(sp, "lookback"));

  const [summary, topTickers, trades] = await Promise.all([
    getInsiderSummary(reportingCik, Number(lookback)),
    getInsiderTopTickers(reportingCik, Number(lookback), 10),
    getInsiderTrades(reportingCik, Number(lookback), 50),
  ]);

  const insiderName = getInsiderDisplayName(summary.insider_name) ?? "Unknown Insider";
  const roleText = summary.primary_role ?? "Role unavailable";

  return (
    <div className="space-y-6">
      <div className="flex flex-wrap items-center justify-between gap-4">
        <div>
          <p className="text-xs font-semibold uppercase tracking-[0.3em] text-emerald-300">Insider research</p>
          <h1 className="text-3xl font-semibold text-white">{insiderName}</h1>
          <div className="mt-2 flex flex-wrap gap-2 text-xs text-slate-400">
            <span className="rounded-full border border-white/10 bg-slate-900/60 px-2.5 py-1">{summary.primary_company_name ?? "Company unavailable"}</span>
            <Badge tone="neutral">{roleText}</Badge>
          </div>
        </div>
        <Link href="/" className={ghostButtonClassName}>Back to feed</Link>
      </div>

      <div className={`${cardClassName} p-4`}>
        <p className="mb-2 text-xs uppercase tracking-widest text-slate-400">Lookback</p>
        <div className="flex flex-wrap gap-2">
          {(["30", "90", "365"] as const).map((value) => (
            <Link
              key={value}
              href={hrefWithLookback(reportingCik, value)}
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

      <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-5">
        <div className={`${cardClassName} p-4`}><p className="text-xs uppercase tracking-widest text-slate-400">Total trades</p><p className="mt-2 text-right text-2xl font-semibold text-white tabular-nums">{summary.total_trades}</p></div>
        <div className={`${cardClassName} p-4`}><p className="text-xs uppercase tracking-widest text-slate-400">Buys</p><p className="mt-2 text-right text-2xl font-semibold text-emerald-300 tabular-nums">{summary.buy_count}</p></div>
        <div className={`${cardClassName} p-4`}><p className="text-xs uppercase tracking-widest text-slate-400">Sells</p><p className="mt-2 text-right text-2xl font-semibold text-rose-300 tabular-nums">{summary.sell_count}</p></div>
        <div className={`${cardClassName} p-4`}><p className="text-xs uppercase tracking-widest text-slate-400">Unique tickers</p><p className="mt-2 text-right text-2xl font-semibold text-white tabular-nums">{summary.unique_tickers}</p></div>
        <div className={`${cardClassName} p-4`}><p className="text-xs uppercase tracking-widest text-slate-400">Net flow</p><p className={`mt-2 text-right text-2xl font-semibold tabular-nums ${summary.net_flow >= 0 ? "text-emerald-300" : "text-rose-300"}`}>{summary.net_flow >= 0 ? "+" : "-"}${formatCompactUsd(Math.abs(summary.net_flow))}</p></div>
      </div>

      <div className="grid gap-6 xl:grid-cols-[1fr_2fr]">
        <section className={cardClassName}>
          <h2 className="text-lg font-semibold text-white">Top tickers</h2>
          <div className="mt-4 space-y-2.5">
            {topTickers.items.length === 0 ? (
              <p className="text-sm text-slate-400">No ticker concentration in selected window.</p>
            ) : (
              topTickers.items.map((row) => {
                const href = tickerHref(row.symbol);
                const body = (
                  <div className={`${compactInteractiveSurfaceClassName} rounded-xl px-3 py-2.5 text-sm`}>
                    <div className="flex items-center justify-between gap-3">
                      <TickerPill symbol={row.symbol} className="inline-flex shrink-0" />
                      <div className="text-right">
                        <span className="text-sm font-semibold tabular-nums text-slate-200">{row.trades} trades</span>
                      </div>
                    </div>
                    <div className="mt-2 flex items-center justify-between text-xs text-slate-400">
                      <span>Buys {row.buy_count} · Sells {row.sell_count}</span>
                      <span className={`font-semibold tabular-nums ${row.net_flow >= 0 ? "text-emerald-300" : "text-rose-300"}`}>
                        {row.net_flow >= 0 ? "+" : "-"}${formatCompactUsd(Math.abs(row.net_flow))}
                      </span>
                    </div>
                  </div>
                );
                if (href) {
                  return <Link key={row.symbol} href={href}>{body}</Link>;
                }
                return <div key={row.symbol}>{body}</div>;
              })
            )}
          </div>
        </section>

        <section className={cardClassName}>
          <div className="mb-4 flex items-center justify-between">
            <h2 className="text-lg font-semibold text-white">Recent trades</h2>
            <span className="text-xs text-slate-400">{trades.items.length} events</span>
          </div>
          <div className="space-y-3">
            {trades.items.length === 0 ? (
              <p className="text-sm text-slate-400">No insider trades in the selected window.</p>
            ) : (
              trades.items.map((trade) => {
                const tradeRecord = trade as Record<string, unknown>;
                const signal = signalFromTrade(tradeRecord, trade.trade_type ?? null);
                const tradeValue =
                  typeof trade.amount_max === "number"
                    ? trade.amount_max
                    : typeof trade.amount_min === "number"
                      ? trade.amount_min
                      : null;
                const pnl = parseNum(tradeRecord.pnl_pct ?? tradeRecord.pnl);

                return (
                  <div
                    key={trade.external_id ?? `${trade.event_id}`}
                    className="relative overflow-hidden rounded-3xl border border-white/5 bg-slate-900/70 p-5 shadow-card"
                  >
                    <div className="flex w-full min-w-0 flex-col gap-4 pr-2 md:grid md:min-w-0 md:items-center md:gap-y-3 lg:gap-y-0 lg:gap-x-5 lg:grid-cols-[minmax(180px,1fr)_minmax(120px,.7fr)_minmax(100px,.65fr)_minmax(120px,.75fr)_100px_130px]">
                      <div className="min-w-0 flex items-center gap-3">
                        {trade.symbol ? (
                          <TickerPill symbol={trade.symbol} href={tickerHref(trade.symbol) ?? undefined} className="inline-flex shrink-0" />
                        ) : (
                          <TickerPill symbol="—" />
                        )}
                        <div className="min-w-0">
                          <div className="min-w-0 overflow-hidden truncate font-semibold text-white">
                            {trade.company_name ?? "—"}
                          </div>
                        </div>
                      </div>

                      <div className="min-w-0 text-xs leading-5 text-slate-400 text-center md:text-left md:whitespace-nowrap">
                        <div>
                          Trade date:{" "}
                          <span className="text-slate-200">
                            {trade.transaction_date ? formatDateShort(trade.transaction_date) : "—"}
                          </span>
                        </div>
                      </div>

                      <div className="min-w-0 text-xs leading-5 text-slate-400 text-center md:text-left md:whitespace-nowrap">
                        <div>
                          Price:{" "}
                          <span className="text-slate-200 tabular-nums">
                            {typeof trade.price === "number" ? formatMoney(trade.price) : "—"}
                          </span>
                        </div>
                      </div>

                      <div className="min-w-0 whitespace-nowrap text-right tabular-nums">
                        <div className="text-base font-semibold text-white">
                          {tradeValue !== null ? formatMoney(tradeValue) : "—"}
                        </div>
                        <div className="mt-1 text-xs text-slate-400">Trade value</div>
                      </div>

                      <div className="text-right">
                        <div className={`text-sm font-semibold tabular-nums ${pnl !== null ? pnlClass(pnl) : "text-slate-400"}`}>{pnl !== null ? formatPnl(pnl) : "—"}</div>
                        <div className="mt-1 text-xs text-slate-400">PnL</div>
                      </div>

                      <div className="flex justify-end">
                        <Badge tone={signal.tone}>{signal.label}</Badge>
                      </div>
                    </div>
                  </div>
                );
              })
            )}
          </div>
        </section>
      </div>
    </div>
  );
}
