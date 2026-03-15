import Link from "next/link";
import {
  getInsiderSummary,
  getInsiderTopTickers,
  getInsiderTrades,
} from "@/lib/api";
import { Badge } from "@/components/Badge";
import {
  cardClassName,
  ghostButtonClassName,
  compactInteractiveSurfaceClassName,
  compactInteractiveTitleClassName,
} from "@/lib/styles";
import { formatDateShort, formatTransactionLabel, transactionTone } from "@/lib/format";
import { getInsiderDisplayName } from "@/lib/insider";
import { tickerHref } from "@/lib/ticker";

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
            <span className="rounded-full border border-white/10 bg-slate-900/60 px-2.5 py-1">CIK {summary.reporting_cik}</span>
            {summary.primary_company_name ? (
              <span className="rounded-full border border-white/10 bg-slate-900/60 px-2.5 py-1">{summary.primary_company_name}</span>
            ) : null}
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
                  <>
                    <div className="flex items-start justify-between gap-3">
                      <div className="min-w-0">
                        <span className={`block truncate text-sm font-semibold ${compactInteractiveTitleClassName}`}>{row.symbol}</span>
                        <p className="truncate text-xs text-slate-500">{row.company_name ?? "—"}</p>
                      </div>
                      <div className="text-right">
                        <span className="text-sm font-semibold tabular-nums text-slate-200">{row.trades}</span>
                        <p className="text-[11px] text-slate-500">Trades</p>
                      </div>
                    </div>
                    <div className="mt-2 flex items-center justify-between text-xs text-slate-400">
                      <span>Buys {row.buy_count} · Sells {row.sell_count}</span>
                      <span className={`font-semibold tabular-nums ${row.net_flow >= 0 ? "text-emerald-300" : "text-rose-300"}`}>
                        {row.net_flow >= 0 ? "+" : "-"}${formatCompactUsd(Math.abs(row.net_flow))}
                      </span>
                    </div>
                  </>
                );
                if (href) {
                  return <Link key={row.symbol} href={href} className={`${compactInteractiveSurfaceClassName} px-3 py-2.5 text-sm`}>{body}</Link>;
                }
                return <div key={row.symbol} className={`${compactInteractiveSurfaceClassName} px-3 py-2.5 text-sm`}>{body}</div>;
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
              trades.items.map((trade) => (
                <div key={trade.external_id ?? `${trade.event_id}`} className="rounded-2xl border border-white/10 bg-white/5 p-4">
                  <div className="flex flex-wrap items-center justify-between gap-3">
                    <div className="flex items-center gap-2">
                      {trade.symbol ? <Link href={tickerHref(trade.symbol) ?? "#"} className="text-sm font-semibold text-emerald-200">{trade.symbol}</Link> : <span className="text-sm font-semibold text-slate-100">—</span>}
                      <Badge tone="ind">Insider</Badge>
                    </div>
                    <Badge tone={transactionTone(trade.trade_type ?? "")}>{formatTransactionLabel(trade.trade_type ?? "")}</Badge>
                  </div>
                  <div className="mt-2 text-xs text-slate-400">Filed {formatDateShort(trade.filing_date ?? "")}</div>
                  <div className="mt-2 flex flex-wrap items-center justify-between gap-3 text-xs text-slate-400">
                    <span>{trade.company_name ?? "—"}</span>
                    <span>{trade.transaction_date ? `Traded ${formatDateShort(trade.transaction_date)}` : "Transaction date —"}</span>
                  </div>
                  <div className="mt-2 text-right text-sm font-semibold text-white tabular-nums">
                    {typeof trade.amount_min === "number" || typeof trade.amount_max === "number"
                      ? `$${formatCompactUsd(Number(trade.amount_max ?? trade.amount_min ?? 0))}`
                      : "—"}
                  </div>
                </div>
              ))
            )}
          </div>
        </section>
      </div>
    </div>
  );
}
