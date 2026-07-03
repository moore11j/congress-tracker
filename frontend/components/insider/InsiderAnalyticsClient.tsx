"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";
import {
  getInsiderAlphaSummary,
  getInsiderStockChart,
  getInsiderTopTickers,
  getInsiderTrades,
  type InsiderAlphaSummary,
  type InsiderTopTicker,
} from "@/lib/api";
import { Badge } from "@/components/Badge";
import { TickerPill } from "@/components/ui/TickerPill";
import { PerformanceChart } from "@/components/member/PerformanceChart";
import { PremiumTickerChart, PremiumTickerChartSkeleton } from "@/components/ticker/PremiumTickerChart";
import { TickerActivityPaginationFooter } from "@/components/ticker/TickerActivityPaginationFooter";
import { AddTickerToWatchlist } from "@/components/watchlists/AddTickerToWatchlist";
import { SmartSignalPill } from "@/components/ui/SmartSignalPill";
import { SkeletonBlock } from "@/components/ui/LoadingSkeleton";
import { cardClassName, compactInteractiveSurfaceClassName, tickerLinkClassName } from "@/lib/styles";
import { formatDateShort, formatTransactionLabel, transactionTone } from "@/lib/format";
import { tickerHref } from "@/lib/ticker";
import { insiderSlug } from "@/lib/insider";
import { resolveInsiderActivityDisplay } from "@/lib/tradeDisplay";
import { gainLossLabel, gainLossTooltip } from "@/lib/gainLossCopy";

type Lookback = "30" | "90" | "180" | "365" | "1095";
type ChartMetric = "return" | "alpha";
type ChartMode = "performance" | "stock";

const RECENT_TRADES_PAGE_SIZE = 20;
const UNAVAILABLE_COPY = "Analytics temporarily unavailable. Try again shortly.";

type InsiderTradesData = Awaited<ReturnType<typeof getInsiderTrades>>;
type InsiderStockChartData = Awaited<ReturnType<typeof getInsiderStockChart>>;

function fallbackInsiderAlphaSummary(reportingCik: string, lookbackDays: number): InsiderAlphaSummary {
  return {
    reporting_cik: reportingCik,
    lookback_days: lookbackDays,
    benchmark_symbol: null,
    trades_analyzed: 0,
    avg_return_pct: null,
    avg_alpha_pct: null,
    win_rate: null,
    avg_holding_days: null,
    best_trades: [],
    worst_trades: [],
    member_series: [],
    benchmark_series: [],
    performance_series: [],
  };
}

function fallbackInsiderTrades(reportingCik: string, lookbackDays: number, page: number): InsiderTradesData {
  return {
    reporting_cik: reportingCik,
    lookback_days: lookbackDays,
    total: 0,
    page,
    limit: RECENT_TRADES_PAGE_SIZE,
    has_next: false,
    items: [],
  };
}

function formatMoney(value: number): string {
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 0,
  }).format(value);
}

function pct(n: number | null | undefined) {
  if (n == null || !Number.isFinite(n)) return "—";
  return `${n.toFixed(1)}%`;
}

function pct0(n: number | null | undefined) {
  if (n == null || !Number.isFinite(n)) return "—";
  return `${Math.round(n * 100)}%`;
}

function numberOrDash(n: number | null | undefined) {
  if (n == null || !Number.isFinite(n)) return "—";
  return `${Math.round(n)}`;
}

function asDate(v: string | null | undefined) {
  if (!v) return "—";
  const d = new Date(v);
  if (!Number.isFinite(d.getTime())) return v;
  return d.toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" });
}

function tone(n: number | null | undefined) {
  if (n == null || !Number.isFinite(n)) return "text-white/85";
  if (n > 0) return "text-emerald-300";
  if (n < 0) return "text-rose-300";
  return "text-white/70";
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

function pnlSourceBadgeLabel(source: string | null | undefined): string | null {
  if (source === "normalized_filing") return "NORMALIZED";
  if (source === "filing") return "FILING";
  if (source === "eod") return "EOD";
  if (source === "trade_outcome") return "OUTCOME";
  return null;
}

function hrefWithParams(
  name: string | null,
  reportingCik: string,
  lookback: Lookback,
  chartMetric: ChartMetric,
  issuer?: string,
  chartMode: ChartMode = "performance",
  chartSymbol?: string,
): string {
  const query = new URLSearchParams();
  query.set("lookback", lookback);
  query.set("chart", chartMode);
  if (chartMetric !== "return") query.set("am", chartMetric);
  if (issuer) query.set("issuer", issuer);
  if (chartMode === "stock" && chartSymbol) query.set("symbol", chartSymbol);
  const slug = insiderSlug(name, reportingCik) ?? reportingCik;
  return `/insider/${encodeURIComponent(slug)}?${query.toString()}`;
}

function AnalyticsStatsSkeleton() {
  return (
    <div className="mt-4 grid gap-3 sm:grid-cols-2 xl:grid-cols-5">
      {Array.from({ length: 5 }).map((_, idx) => (
        <div key={idx} className="rounded-2xl border border-white/10 bg-white/[0.03] px-4 py-3">
          <SkeletonBlock className="h-3 w-24" />
          <SkeletonBlock className="mt-3 h-7 w-20" />
        </div>
      ))}
    </div>
  );
}

function TopTickersPanel({
  items,
  loading,
  unavailable,
}: {
  items: InsiderTopTicker[];
  loading: boolean;
  unavailable: boolean;
}) {
  return (
    <div className={`${cardClassName} w-full`}>
      <h2 className="text-lg font-semibold text-white">Top tickers</h2>
      <div className="mt-4 space-y-2">
        {loading ? (
          Array.from({ length: 5 }).map((_, idx) => (
            <div key={idx} className="rounded-2xl border border-white/10 bg-white/[0.03] px-3 py-2">
              <SkeletonBlock className="h-4 w-full" />
            </div>
          ))
        ) : unavailable ? (
          <p className="text-sm text-slate-400">{UNAVAILABLE_COPY}</p>
        ) : items.length === 0 ? (
          <p className="text-sm text-slate-400">No ticker concentration yet.</p>
        ) : (
          items.map((ticker) => (
            <div
              key={ticker.symbol}
              className={`${compactInteractiveSurfaceClassName} flex items-center justify-between gap-4 whitespace-nowrap px-3 py-2 text-sm`}
            >
              <div className="flex items-center gap-2">
                <TickerPill symbol={ticker.symbol} href={tickerHref(ticker.symbol)} />
              </div>
              <span className="whitespace-nowrap text-xs text-white/50 tabular-nums">{ticker.trades} trades</span>
            </div>
          ))
        )}
      </div>
    </div>
  );
}

export function InsiderAnalyticsClient({
  reportingCik,
  insiderName,
  lookback,
  lookbackDays,
  chartMetric,
  chartMode,
  issuer,
  stockSymbol,
  recentTradesPage,
}: {
  reportingCik: string;
  insiderName: string;
  lookback: Lookback;
  lookbackDays: number;
  chartMetric: ChartMetric;
  chartMode: ChartMode;
  issuer?: string;
  stockSymbol?: string;
  recentTradesPage: number;
}) {
  const [alphaSummary, setAlphaSummary] = useState<InsiderAlphaSummary>(() =>
    fallbackInsiderAlphaSummary(reportingCik, lookbackDays),
  );
  const [trades, setTrades] = useState<InsiderTradesData>(() =>
    fallbackInsiderTrades(reportingCik, lookbackDays, recentTradesPage),
  );
  const [topTickers, setTopTickers] = useState<InsiderTopTicker[]>([]);
  const [stockChart, setStockChart] = useState<InsiderStockChartData | null>(null);
  const [loading, setLoading] = useState(true);
  const [stockChartLoading, setStockChartLoading] = useState(chartMode === "stock");
  const [alphaUnavailable, setAlphaUnavailable] = useState(false);
  const [tradesUnavailable, setTradesUnavailable] = useState(false);
  const [topTickersUnavailable, setTopTickersUnavailable] = useState(false);
  const [stockChartUnavailable, setStockChartUnavailable] = useState(false);

  useEffect(() => {
    const controller = new AbortController();
    let cancelled = false;
    setLoading(true);
    setAlphaUnavailable(false);
    setTradesUnavailable(false);
    setTopTickersUnavailable(false);

    Promise.all([
      getInsiderAlphaSummary(reportingCik, {
        lookback_days: lookbackDays,
        issuer,
        source: "InsiderAlphaSummary",
        signal: controller.signal,
      })
        .then((data) => {
          if (!cancelled) setAlphaSummary(data);
        })
        .catch(() => {
          if (!cancelled) setAlphaUnavailable(true);
        }),
      getInsiderTrades(reportingCik, lookbackDays, RECENT_TRADES_PAGE_SIZE, issuer, {
        page: recentTradesPage,
        source: "InsiderTrades",
        signal: controller.signal,
      })
        .then((data) => {
          if (!cancelled) setTrades(data);
        })
        .catch(() => {
          if (!cancelled) setTradesUnavailable(true);
        }),
      getInsiderTopTickers(reportingCik, lookbackDays, 10, issuer, {
        source: "InsiderTopTickers",
        signal: controller.signal,
      })
        .then((data) => {
          if (!cancelled) setTopTickers(data.items ?? []);
        })
        .catch(() => {
          if (!cancelled) setTopTickersUnavailable(true);
        }),
    ]).finally(() => {
      if (!cancelled) setLoading(false);
    });

    return () => {
      cancelled = true;
      controller.abort();
    };
  }, [issuer, lookbackDays, recentTradesPage, reportingCik]);

  useEffect(() => {
    if (chartMode !== "stock") return;
    const controller = new AbortController();
    let cancelled = false;
    setStockChartLoading(true);
    setStockChartUnavailable(false);
    getInsiderStockChart(reportingCik, {
      lookback_days: lookbackDays,
      symbol: stockSymbol,
      source: "InsiderStockChart",
      signal: controller.signal,
    })
      .then((data) => {
        if (!cancelled) setStockChart(data);
      })
      .catch(() => {
        if (!cancelled) setStockChartUnavailable(true);
      })
      .finally(() => {
        if (!cancelled) setStockChartLoading(false);
      });
    return () => {
      cancelled = true;
      controller.abort();
    };
  }, [chartMode, lookbackDays, reportingCik, stockSymbol]);

  const analyticsStats = [
    { label: "Trades Analyzed", value: numberOrDash(alphaSummary.trades_analyzed), valueClass: "text-white" },
    { label: "Avg Trade Return", value: pct(alphaSummary.avg_return_pct), valueClass: tone(alphaSummary.avg_return_pct) },
    { label: "Avg Trade Alpha", value: pct(alphaSummary.avg_alpha_pct), valueClass: tone(alphaSummary.avg_alpha_pct) },
    { label: "Win Rate", value: pct0(alphaSummary.win_rate), valueClass: tone(alphaSummary.win_rate == null ? null : (alphaSummary.win_rate - 0.5) * 100) },
    { label: "Avg Holding Days", value: numberOrDash(alphaSummary.avg_holding_days), valueClass: "text-white/90" },
  ];
  const memberSeries = alphaSummary.member_series ?? alphaSummary.performance_series ?? [];
  const benchmarkSeries = alphaSummary.benchmark_series ?? [];
  const chartHasEnoughTrades = memberSeries.filter((point) => {
    const value = chartMetric === "alpha" ? point.cumulative_alpha_pct : point.cumulative_return_pct;
    return typeof value === "number" && Number.isFinite(value);
  }).length >= 2;
  const issuerOptions = useMemo(
    () => Array.from(new Set(trades.items.map((trade) => trade.symbol).filter((symbol): symbol is string => Boolean(symbol)))),
    [trades.items],
  );
  const recentTradesLimit = typeof trades.limit === "number" && trades.limit > 0 ? trades.limit : RECENT_TRADES_PAGE_SIZE;
  const recentTradesPageValue = typeof trades.page === "number" && trades.page >= 0 ? trades.page : recentTradesPage;
  const recentTradesTotal = typeof trades.total === "number" && trades.total >= 0 ? trades.total : trades.items.length;
  const recentTradesHasNext =
    typeof trades.has_next === "boolean"
      ? trades.has_next
      : recentTradesPageValue * recentTradesLimit + trades.items.length < recentTradesTotal;

  return (
    <>
      <section className={cardClassName}>
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div>
            <h2 className="text-lg font-semibold text-white">Insider Alpha Analytics</h2>
            <p className="mt-1 text-sm text-white/45">Average trade metrics summarize scored disclosures individually. Backtests simulate portfolio allocation over time.</p>
          </div>
        </div>

        {loading ? <AnalyticsStatsSkeleton /> : (
          <div className="mt-4 grid gap-3 sm:grid-cols-2 xl:grid-cols-5">
            {analyticsStats.map((stat) => (
              <div key={stat.label} className="rounded-2xl border border-white/10 bg-white/[0.03] px-4 py-3">
                <p className="text-[11px] uppercase tracking-[0.18em] text-white/45">{stat.label}</p>
                <p className={`mt-2 text-xl font-semibold tabular-nums ${stat.valueClass}`}>{stat.value}</p>
              </div>
            ))}
          </div>
        )}

        {alphaUnavailable ? (
          <p className="mt-4 rounded-xl border border-amber-300/25 bg-amber-400/10 px-3 py-2 text-sm text-amber-100">
            {UNAVAILABLE_COPY}
          </p>
        ) : null}

        {alphaSummary.trades_analyzed === 0 && trades.items.length > 0 ? (
          <p className="mt-4 rounded-xl border border-amber-300/25 bg-amber-400/10 px-3 py-2 text-sm text-amber-100">
            No market trades analyzed in this window. Showing recent insider activity below.
          </p>
        ) : null}

        <div className="mt-4 rounded-2xl border border-white/10 bg-white/[0.03] p-4">
          <div className="flex flex-wrap items-start justify-between gap-3">
            <div>
              <h3 className="text-sm font-semibold uppercase tracking-[0.14em] text-white/70">Insider Performance</h3>
              <p className="mt-1 text-[11px] text-white/40">
                {chartMode === "stock"
                  ? "Company stock with this insider's own disclosed trades."
                  : "Equal-weight scored trade outcomes, not portfolio CAGR."}
              </p>
            </div>
            <div className="flex flex-wrap items-center justify-end gap-2 text-xs">
              <div className="inline-flex rounded-full border border-white/10 bg-slate-950/50 p-1">
                <Link
                  href={hrefWithParams(insiderName, reportingCik, lookback, chartMetric, issuer, "performance")}
                  prefetch={false}
                  className={`rounded-full px-3 py-1 font-semibold transition ${
                    chartMode === "performance" ? "bg-white/[0.08] text-white" : "text-white/55 hover:text-white/80"
                  }`}
                >
                  Performance Curve
                </Link>
                <Link
                  href={hrefWithParams(insiderName, reportingCik, lookback, chartMetric, issuer, "stock", stockSymbol)}
                  prefetch={false}
                  className={`rounded-full px-3 py-1 font-semibold transition ${
                    chartMode === "stock" ? "bg-white/[0.08] text-white" : "text-white/55 hover:text-white/80"
                  }`}
                >
                  Company Stock
                </Link>
              </div>
              {issuerOptions.length > 1 ? (
                <div className="flex flex-wrap items-center gap-1">
                  {issuerOptions.slice(0, 5).map((symbol) => (
                    <Link
                      key={symbol}
                      href={hrefWithParams(insiderName, reportingCik, lookback, chartMetric, symbol, chartMode, symbol)}
                      prefetch={false}
                      className={`rounded-full border px-2.5 py-1 ${
                        issuer === symbol
                          ? "border-emerald-400/40 bg-emerald-400/10 text-emerald-200"
                          : "border-white/10 text-white/55 hover:text-white/80"
                      }`}
                    >
                      {symbol}
                    </Link>
                  ))}
                </div>
              ) : null}
              {chartMode === "performance" ? (
                <>
                  <Link
                    href={hrefWithParams(insiderName, reportingCik, lookback, "return", issuer, chartMode, stockSymbol)}
                    prefetch={false}
                    className={`rounded-full border px-2.5 py-1 ${
                      chartMetric === "return"
                        ? "border-white/30 bg-white/[0.07] text-white"
                        : "border-white/10 text-white/55 hover:text-white/80"
                    }`}
                  >
                    Return
                  </Link>
                  <Link
                    href={hrefWithParams(insiderName, reportingCik, lookback, "alpha", issuer, chartMode, stockSymbol)}
                    prefetch={false}
                    className={`rounded-full border px-2.5 py-1 ${
                      chartMetric === "alpha"
                        ? "border-white/30 bg-white/[0.07] text-white"
                        : "border-white/10 text-white/55 hover:text-white/80"
                    }`}
                  >
                    Alpha
                  </Link>
                </>
              ) : null}
            </div>
          </div>

          {chartMode === "stock" ? (
            <div className="mt-4">
              {stockChartLoading ? (
                <PremiumTickerChartSkeleton />
              ) : stockChartUnavailable ? (
                <p className="rounded-xl border border-amber-300/25 bg-amber-400/10 px-3 py-2 text-sm text-amber-100">
                  {UNAVAILABLE_COPY}
                </p>
              ) : (
                <PremiumTickerChart
                  bundle={stockChart}
                  eyebrow="Company stock"
                  title={stockChart?.symbol ? `${stockChart.symbol} Stock Chart` : "Company Stock Chart"}
                  subtitle="Showing this insider's disclosed buys and sells only."
                  allowedMarkerKinds={["insider"]}
                  showMarkerControls={false}
                  emptyTitle="No company stock chart is available for this insider yet."
                  emptyMessage="The chart will appear once this insider has a valid issuer symbol and daily price history."
                />
              )}
            </div>
          ) : loading ? (
            <SkeletonBlock className="mt-4 h-64 w-full" />
          ) : !chartHasEnoughTrades ? (
            <p className="mt-3 text-sm text-slate-400">Not enough scored trades to render a performance chart.</p>
          ) : (
            <PerformanceChart
              memberSeries={memberSeries}
              benchmarkSeries={benchmarkSeries}
              metric={chartMetric}
              benchmarkLabel="S&P 500"
              subjectLabel="Insider"
            />
          )}
        </div>

        <div className="mt-4 grid gap-4 lg:grid-cols-2">
          {[
            { title: "Best Trades", rows: alphaSummary.best_trades ?? [] },
            { title: "Worst Trades", rows: alphaSummary.worst_trades ?? [] },
          ].map((panel) => (
            <div key={panel.title} className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
              <h3 className="text-sm font-semibold uppercase tracking-[0.14em] text-white/70">{panel.title}</h3>
              {loading ? (
                <div className="mt-3 space-y-2">
                  <SkeletonBlock className="h-12 w-full" />
                  <SkeletonBlock className="h-12 w-full" />
                </div>
              ) : panel.rows.length === 0 ? (
                <p className="mt-3 text-sm text-slate-400">No scored trades for this lookback window.</p>
              ) : (
                <div className="mt-3 space-y-2">
                  {panel.rows.map((trade) => (
                    <div
                      key={`${panel.title}-${trade.event_id}-${trade.symbol}`}
                      className="grid grid-cols-[1fr_auto_auto] items-center gap-3 rounded-xl border border-white/10 px-3 py-2"
                    >
                      <div className="min-w-0">
                        {tickerHref(trade.symbol) ? (
                          <Link href={tickerHref(trade.symbol)!} prefetch={false} className={`${tickerLinkClassName} truncate`}>
                            {trade.symbol}
                          </Link>
                        ) : (
                          <p className="truncate text-sm font-medium text-white">{trade.symbol}</p>
                        )}
                        <p className="truncate text-xs text-white/45">{asDate(trade.asof_date)}{trade.trade_type ? ` · ${trade.trade_type}` : ""}</p>
                      </div>
                      <div className="text-right">
                        <p className={`text-sm font-semibold tabular-nums ${tone(trade.return_pct)}`}>{pct(trade.return_pct)}</p>
                        <p className="text-[11px] text-white/40">Return</p>
                      </div>
                      <div className="text-right">
                        <p className={`text-sm font-semibold tabular-nums ${tone(trade.alpha_pct)}`}>{pct(trade.alpha_pct)}</p>
                        <p className="text-[11px] text-white/40">Alpha</p>
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </div>
          ))}
        </div>
      </section>

      <div className="grid items-start gap-6 lg:grid-cols-[minmax(260px,0.85fr)_minmax(0,2.15fr)]">
        <div className="w-full min-w-0">
          <TopTickersPanel items={topTickers} loading={loading} unavailable={topTickersUnavailable} />
        </div>

        <section id="recent-trades" className={`${cardClassName} w-full min-w-0 scroll-mt-6`}>
          <h2 className="text-lg font-semibold text-white">Recent trades</h2>
          <p className="mt-1 text-xs text-slate-500">
            Displayed quotes are USD. Current foreign prices use spot FX where applicable; historical foreign filing prices use trade-date FX and ADR ratios when normalized. Original reported prices remain shown below.
          </p>
          <div data-activity-scroll-region className="mt-4 space-y-3">
            {loading ? (
              Array.from({ length: 4 }).map((_, idx) => <SkeletonBlock key={idx} className="h-24 w-full rounded-3xl" />)
            ) : tradesUnavailable ? (
              <p className="text-sm text-slate-400">{UNAVAILABLE_COPY}</p>
            ) : recentTradesTotal === 0 ? (
              <p className="text-sm text-slate-400">No recent activity found.</p>
            ) : trades.items.length === 0 ? (
              <p className="text-sm text-slate-400">No trades on this page.</p>
            ) : (
              trades.items.map((trade) => {
                const tradeRecord = trade as Record<string, unknown>;
                const display = resolveInsiderActivityDisplay(tradeRecord);
                const tradeType = display.tradeType ?? "";
                const sideLabel = formatTransactionLabel(tradeType) ?? "Trade";
                const sideTone = transactionTone(tradeType);
                const pnlSourceLabel = pnlSourceBadgeLabel(display.pnlSource);

                return (
                  <div
                    key={trade.external_id ?? `${trade.event_id}`}
                    className="relative overflow-hidden rounded-3xl border border-white/5 bg-slate-900/70 p-5 shadow-card"
                  >
                    <div className="grid min-w-0 gap-4 lg:grid-cols-[minmax(280px,1fr)_minmax(110px,.6fr)_minmax(90px,.5fr)_minmax(50px,.55fr)_minmax(100px,.65fr)_minmax(100px,.55fr)_minmax(120px,.5fr)] lg:items-center">
                      <div className="min-w-0">
                        <div className="flex min-w-0 items-center gap-2">
                          {trade.symbol ? (
                            <AddTickerToWatchlist symbol={display.displaySymbol} variant="compact" align="left" />
                          ) : null}
                          {trade.symbol ? (
                            <TickerPill symbol={display.displaySymbol} href={tickerHref(trade.symbol) ?? undefined} className="inline-flex shrink-0" />
                          ) : (
                            <TickerPill symbol="—" />
                          )}
                          <div className="min-w-0">
                            <p className="truncate font-semibold text-white">{display.companyName}</p>
                          </div>
                        </div>
                      </div>

                      <div className="text-xs leading-5 text-slate-400">
                        <div>Trade date</div>
                        <div className="mt-1 text-sm text-slate-200">{display.transactionDate ? formatDateShort(display.transactionDate) : "—"}</div>
                      </div>

                      <div className="text-xs leading-5 text-slate-400">
                        <div>Side</div>
                        <div className="mt-1">
                          <Badge tone={sideTone}>{sideLabel}</Badge>
                        </div>
                      </div>

                      <div className="text-xs leading-5 text-slate-400">
                        <div>Price</div>
                        <div className="mt-1 text-sm font-semibold tabular-nums text-slate-100">{display.price !== null ? formatMoney(display.price) : "—"}</div>
                        {display.reportedLabel ? (
                          <div className="mt-0.5 text-[11px] tabular-nums text-slate-500">{display.reportedLabel}</div>
                        ) : null}
                      </div>

                      <div className="text-right text-xs text-slate-400">
                        <div>Trade value</div>
                        <div className="mt-1 text-base font-semibold tabular-nums text-white">{display.tradeValue !== null ? formatMoney(display.tradeValue) : "—"}</div>
                      </div>

                      <div className="text-right text-xs text-slate-400">
                        <div className="cursor-help whitespace-nowrap" title={gainLossTooltip} aria-label={`${gainLossLabel}: ${gainLossTooltip}`}>
                          {gainLossLabel}
                        </div>
                        <div className={`mt-1 text-sm font-semibold tabular-nums ${display.pnl !== null ? pnlClass(display.pnl) : "text-slate-400"}`}>{display.pnl !== null ? formatPnl(display.pnl) : "—"}</div>
                        {pnlSourceLabel ? (
                          <div className="mt-1">
                            <span className="inline-flex items-center rounded-md border border-slate-700 bg-slate-900/30 px-1.5 py-0.5 text-[10px] font-semibold text-slate-300">
                              {pnlSourceLabel}
                            </span>
                          </div>
                        ) : null}
                      </div>

                      <div className="text-right text-xs text-slate-400">
                        <div>Signal</div>
                        <div className="mt-1 flex justify-end">
                          {display.hasSignal ? (
                            <SmartSignalPill score={display.signal.score} band={display.signal.band} size="compact" className="ml-auto" />
                          ) : (
                            <span className="text-[11px] text-slate-500">No signal</span>
                          )}
                        </div>
                      </div>
                    </div>
                  </div>
                );
              })
            )}
          </div>
          {!tradesUnavailable && recentTradesTotal > recentTradesLimit ? (
            <div className="mt-4">
              <TickerActivityPaginationFooter
                sectionId="recent-trades"
                pageParam="recent_trades_page"
                page={recentTradesPageValue}
                limit={recentTradesLimit}
                total={recentTradesTotal}
                itemCount={trades.items.length}
                hasNext={recentTradesHasNext}
              />
            </div>
          ) : null}
        </section>
      </div>
    </>
  );
}
