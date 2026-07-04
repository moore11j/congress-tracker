"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";
import {
  getMemberAlphaSummary,
  getMemberPortfolioPerformance,
  getMemberTrades,
  type MemberAlphaSummary,
  type MemberPortfolioPerformance,
  type MemberTradesResponse,
} from "@/lib/api";
import { Badge } from "@/components/Badge";
import { TickerPill } from "@/components/ui/TickerPill";
import { PerformanceChart } from "@/components/member/PerformanceChart";
import { SkeletonBlock } from "@/components/ui/LoadingSkeleton";
import { SmartSignalPill } from "@/components/ui/SmartSignalPill";
import { cardClassName, compactInteractiveSurfaceClassName } from "@/lib/styles";
import { formatDateShort, formatTransactionLabel, transactionTone } from "@/lib/format";
import { tickerHref } from "@/lib/ticker";
import { resolveSmartSignalValue } from "@/lib/smartSignal";
import {
  PORTFOLIO_MODE,
  normalizeMemberPortfolioChartData,
  normalizeMemberPortfolioEventMarkers,
} from "@/lib/portfolioPerformance.mjs";

const REFRESHING_COPY = "Refreshing analytics from disclosed activity.";

function pct(n: number | null | undefined) {
  if (n == null || !Number.isFinite(n)) return "—";
  return `${n.toFixed(1)}%`;
}

function pct0(n: number | null | undefined) {
  if (n == null || !Number.isFinite(n)) return "—";
  return `${Math.round(n * 100)}%`;
}

function decimal(n: number | null | undefined, digits = 2) {
  if (n == null || !Number.isFinite(n)) return "—";
  return n.toFixed(digits);
}

function numberOrDash(n: number | null | undefined) {
  if (n == null || !Number.isFinite(n)) return "—";
  return `${Math.round(n)}`;
}

function tone(n: number | null | undefined) {
  if (n == null || !Number.isFinite(n)) return "text-white/85";
  if (n > 0) return "text-emerald-300";
  if (n < 0) return "text-rose-300";
  return "text-white/70";
}

function compactUSD(n: number) {
  const abs = Math.abs(n);
  if (abs >= 1e9) return `${(n / 1e9).toFixed(1)}B`;
  if (abs >= 1e6) return `${(n / 1e6).toFixed(1)}M`;
  if (abs >= 1e3) return `${(n / 1e3).toFixed(1)}K`;
  return `${Math.round(n)}`;
}

function tradeDirection(tradeType: string): "buy" | "sell" | null {
  const normalized = tradeType.trim().toLowerCase();
  if (!normalized) return null;
  if (normalized === "s" || normalized === "s-sale") return "sell";
  if (normalized === "p" || normalized === "p-purchase") return "buy";
  if (["sale", "sell", "disposition", "dispose"].some((token) => normalized.includes(token))) return "sell";
  if (["buy", "purchase", "acquire", "acquisition"].some((token) => normalized.includes(token))) return "buy";
  return null;
}

function latestActivePositions(points: Array<{ active_positions?: number | null }> | null | undefined) {
  const latest = points?.at(-1)?.active_positions;
  return typeof latest === "number" && Number.isFinite(latest) ? latest : null;
}

function distinctActiveTickerPositions(
  positions: Array<{ status?: string | null; symbol?: string | null }> | null | undefined,
) {
  if (!Array.isArray(positions)) return null;
  const symbols = new Set<string>();
  positions.forEach((position) => {
    if (String(position?.status ?? "").toLowerCase() !== "open") return;
    const symbol = String(position?.symbol ?? "").trim().toUpperCase();
    if (symbol) symbols.add(symbol);
  });
  return symbols.size;
}

function alphaFallback(memberId: string, lookbackDays: number): MemberAlphaSummary {
  return {
    member_id: memberId,
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

function tradesFallback(memberId: string, lookbackDays: number): MemberTradesResponse {
  return {
    member_id: memberId,
    lookback_days: lookbackDays,
    limit: 100,
    items: [],
  };
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

function PortfolioSkeleton() {
  return (
    <section className={`${cardClassName} p-4 sm:p-6`}>
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <SkeletonBlock className="h-5 w-56" />
          <SkeletonBlock className="mt-2 h-3 w-80 max-w-full" />
        </div>
        <SkeletonBlock className="h-8 w-20 rounded-full" />
      </div>
      <SkeletonBlock className="mt-4 h-64 w-full" />
    </section>
  );
}

function MemberPortfolioPanel({
  portfolio,
  unavailable,
  loading,
  selectedLookbackDays,
  lookbackLinks,
  simulatedTradesCount,
}: {
  portfolio: MemberPortfolioPerformance | null;
  unavailable: boolean;
  loading: boolean;
  selectedLookbackDays: number;
  lookbackLinks: Array<{ label: string; value: number; href: string }>;
  simulatedTradesCount: number | null;
}) {
  if (loading) return <PortfolioSkeleton />;
  const summary = portfolio?.summary ?? null;
  const { memberSeries: portfolioSeries, benchmarkSeries } = normalizeMemberPortfolioChartData(portfolio);
  const portfolioEvents = normalizeMemberPortfolioEventMarkers(portfolio);
  const hasPersistedRun = portfolio?.persisted_only === true && portfolio.status === "ok" && summary != null;
  const hasChartData = portfolioSeries.length >= 2 && benchmarkSeries.length >= 2;
  const positionsCount = summary?.positions_count ?? 0;
  const activePositionsCount = latestActivePositions(portfolio?.points) ?? latestActivePositions(portfolioSeries);
  const activeTickerPositionsCount = distinctActiveTickerPositions(portfolio?.positions);
  const curveQualityStatus = portfolio?.curve_quality_status ?? "good";
  const showNoActiveHoldings = hasPersistedRun && portfolio?.no_active_holdings === true;
  const showLimitedPriceHistory = hasPersistedRun && positionsCount > 0 && (curveQualityStatus === "warning" || curveQualityStatus === "poor");
  const metrics = summary ? [
    { label: "Total Return", value: pct(summary.total_return_pct), tone: tone(summary.total_return_pct) },
    { label: "CAGR", value: pct(summary.cagr_pct), tone: tone(summary.cagr_pct) },
    { label: "Alpha", value: pct(summary.alpha_pct), tone: tone(summary.alpha_pct) },
    { label: "S&P Return", value: pct(summary.benchmark_return_pct), tone: tone(summary.benchmark_return_pct) },
    { label: "Max Drawdown", value: pct(summary.max_drawdown_pct), tone: tone(summary.max_drawdown_pct == null ? null : -Math.abs(summary.max_drawdown_pct)) },
    { label: "Sharpe", value: decimal(summary.sharpe_ratio, 2), tone: "text-white/90" },
    { label: "Win Rate", value: pct(summary.win_rate_pct), tone: "text-white/90" },
    { label: "Simulated Trades", value: numberOrDash(simulatedTradesCount), tone: "text-white/90" },
    { label: "Active Tickers", value: numberOrDash(activeTickerPositionsCount), tone: "text-white/90" },
  ] : [];

  return (
    <section className={`${cardClassName} p-4 sm:p-6`}>
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h2 className="text-lg font-semibold text-white">Portfolio Performance</h2>
          <p className="mt-1 text-xs uppercase tracking-[0.2em] text-emerald-300">Disclosure-lag realistic portfolio</p>
          <p className="mt-2 max-w-3xl text-sm text-white/45">
            Trades are simulated after public disclosure, not transaction date. Open positions are carried forward through the selected window.
          </p>
        </div>
        <div className="flex flex-wrap items-center gap-2 sm:justify-end">
          {lookbackLinks.map((option) => (
            <Link
              key={option.value}
              href={option.href}
              prefetch={false}
              className={`relative rounded-full border px-3 py-1.5 text-xs transition-colors ${
                option.value === selectedLookbackDays
                  ? "border-emerald-300/50 bg-emerald-300/10 font-medium text-emerald-100"
                  : "border-white/10 bg-slate-950/30 text-white/60 hover:border-emerald-300/30 hover:text-white/85"
              }`}
            >
              {option.label}
            </Link>
          ))}
        </div>
      </div>

      {unavailable ? (
        <p className="mt-4 rounded-2xl border border-amber-300/15 bg-amber-300/[0.06] px-4 py-3 text-sm text-amber-100/85">
          {REFRESHING_COPY}
        </p>
      ) : !hasPersistedRun ? (
        <p className="mt-4 rounded-2xl border border-white/10 bg-white/[0.03] px-4 py-3 text-sm text-slate-400">
          Portfolio simulation is not available for this lookback yet.
        </p>
      ) : (
        <>
          <div className="mt-4 grid gap-3 sm:grid-cols-2 xl:grid-cols-5">
            {metrics.map((metric) => (
              <div key={metric.label} className="rounded-2xl border border-white/10 bg-white/[0.03] px-4 py-3">
                <p className="text-[11px] uppercase tracking-[0.18em] text-white/45">{metric.label}</p>
                <p className={`mt-2 text-xl font-semibold tabular-nums ${metric.tone}`}>{metric.value}</p>
              </div>
            ))}
          </div>
          {activePositionsCount != null ? (
            <p className="mt-3 text-xs text-slate-400">Active position rows at end: {numberOrDash(activePositionsCount)}</p>
          ) : null}
          {showNoActiveHoldings ? (
            <p className="mt-4 rounded-2xl border border-white/10 bg-white/[0.03] px-4 py-3 text-sm text-slate-400">
              No simulated holdings were active in this window.
            </p>
          ) : showLimitedPriceHistory ? (
            <p className="mt-4 rounded-2xl border border-amber-300/15 bg-amber-300/[0.06] px-4 py-3 text-sm text-amber-100/80">
              Some holdings have limited price history, so parts of the simulated curve may use stale or incomplete pricing.
            </p>
          ) : null}
          {hasChartData ? (
            <PerformanceChart
              memberSeries={portfolioSeries}
              benchmarkSeries={benchmarkSeries}
              metric="return"
              benchmarkLabel="S&P 500"
              subjectLabel="Portfolio"
              chartLabel="Portfolio Return"
              events={portfolioEvents}
            />
          ) : (
            <p className="mt-4 rounded-2xl border border-white/10 bg-white/[0.03] px-4 py-3 text-sm text-slate-400">
              Portfolio simulation is not available for this lookback yet.
            </p>
          )}
        </>
      )}
    </section>
  );
}

export function MemberAnalyticsClient({
  memberId,
  memberName,
  lookbackDays,
  portfolioLookbackDays,
  portfolioLookbackLinks,
  initialTopTickers,
  initialAlphaSummary,
  initialTrades,
}: {
  memberId: string;
  memberName: string;
  lookbackDays: number;
  portfolioLookbackDays: number;
  portfolioLookbackLinks: Array<{ label: string; value: number; href: string }>;
  initialTopTickers: Array<{ symbol: string; trades: number }>;
  initialAlphaSummary?: MemberAlphaSummary;
  initialTrades?: MemberTradesResponse;
}) {
  const [alphaSummary, setAlphaSummary] = useState<MemberAlphaSummary>(() => initialAlphaSummary ?? alphaFallback(memberId, lookbackDays));
  const [portfolioTradeCountSummary, setPortfolioTradeCountSummary] = useState<MemberAlphaSummary | null>(null);
  const [portfolio, setPortfolio] = useState<MemberPortfolioPerformance | null>(null);
  const [trades, setTrades] = useState<MemberTradesResponse>(() => initialTrades ?? tradesFallback(memberId, lookbackDays));
  const [loading, setLoading] = useState(true);
  const [alphaUnavailable, setAlphaUnavailable] = useState(false);
  const [portfolioUnavailable, setPortfolioUnavailable] = useState(false);
  const [tradesUnavailable, setTradesUnavailable] = useState(false);

  useEffect(() => {
    const controller = new AbortController();
    let cancelled = false;
    setLoading(true);
    setAlphaUnavailable(false);
    setPortfolioUnavailable(false);
    setTradesUnavailable(false);

    const alphaRequest = getMemberAlphaSummary(memberId, {
      lookback_days: lookbackDays,
      source: "MemberAnalytics",
      signal: controller.signal,
    })
      .then((data) => {
        if (!cancelled) setAlphaSummary(data);
      })
      .catch(() => {
        if (!cancelled) setAlphaUnavailable(true);
      });

    const tradesRequest = getMemberTrades(memberId, {
      lookback_days: lookbackDays,
      limit: 100,
      source: "MemberAnalytics",
      signal: controller.signal,
    })
      .then((data) => {
        if (!cancelled) setTrades(data);
      })
      .catch(() => {
        if (!cancelled) setTradesUnavailable(true);
      });

    const portfolioRequest = getMemberPortfolioPerformance(memberId, {
      lookback_days: portfolioLookbackDays,
      mode: PORTFOLIO_MODE,
      source: "MemberAnalytics",
      signal: controller.signal,
    })
      .then((data) => {
        if (!cancelled) setPortfolio(data);
      })
      .catch(() => {
        if (!cancelled) setPortfolioUnavailable(true);
      });

    const portfolioTradeCountRequest =
      portfolioLookbackDays === lookbackDays
        ? Promise.resolve(null)
        : getMemberAlphaSummary(memberId, {
            lookback_days: portfolioLookbackDays,
            source: "MemberAnalytics",
            signal: controller.signal,
          }).catch(() => null);

    portfolioTradeCountRequest.then((data) => {
      if (!cancelled) setPortfolioTradeCountSummary(data);
    });

    Promise.allSettled([alphaRequest, tradesRequest, portfolioRequest, portfolioTradeCountRequest]).finally(() => {
      if (!cancelled) setLoading(false);
    });

    return () => {
      cancelled = true;
      controller.abort();
    };
  }, [lookbackDays, memberId, portfolioLookbackDays]);

  const analyticsStats = [
    { label: "Trades Analyzed", value: String(alphaSummary.trades_analyzed ?? 0), valueClass: "text-white" },
    { label: "Avg Trade Return", value: pct(alphaSummary.avg_return_pct), valueClass: tone(alphaSummary.avg_return_pct) },
    { label: "Avg Trade Alpha", value: pct(alphaSummary.avg_alpha_pct), valueClass: tone(alphaSummary.avg_alpha_pct) },
    { label: "Win Rate", value: pct0(alphaSummary.win_rate), valueClass: "text-white/90" },
    { label: "Avg Holding Days", value: numberOrDash(alphaSummary.avg_holding_days), valueClass: "text-white/90" },
  ];
  const hasAlphaMetrics =
    (alphaSummary.trades_analyzed ?? 0) > 0 ||
    alphaSummary.avg_return_pct != null ||
    alphaSummary.avg_alpha_pct != null ||
    alphaSummary.win_rate != null;
  const simulatedTradesCount =
    portfolioLookbackDays === lookbackDays
      ? alphaSummary.trades_analyzed
      : portfolioTradeCountSummary?.trades_analyzed ?? null;
  const net = useMemo(() => {
    let value = 0;
    const cutoff = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000);
    for (const trade of trades.items) {
      const tradeDate = new Date(trade.trade_date ?? "");
      if (!Number.isFinite(tradeDate.getTime()) || tradeDate < cutoff) continue;
      const amountMin = trade.amount_range_min;
      const amountMax = trade.amount_range_max;
      const amount = amountMin != null && amountMax != null ? (amountMin + amountMax) / 2 : (amountMax ?? amountMin);
      if (amount == null || !Number.isFinite(amount)) continue;
      const direction = tradeDirection(trade.transaction_type ?? "");
      if (direction === "buy") value += amount;
      if (direction === "sell") value -= amount;
    }
    return value;
  }, [trades.items]);

  const topTickers = useMemo(() => {
    const counts = new Map<string, number>();
    trades.items.forEach((trade) => {
      const symbol = String(trade.symbol ?? "").trim().toUpperCase();
      if (!symbol) return;
      counts.set(symbol, (counts.get(symbol) ?? 0) + 1);
    });
    if (counts.size === 0) return initialTopTickers;
    return Array.from(counts.entries())
      .sort((a, b) => b[1] - a[1])
      .slice(0, 10)
      .map(([symbol, count]) => ({ symbol, trades: count }));
  }, [initialTopTickers, trades.items]);

  return (
    <>
      <MemberPortfolioPanel
        portfolio={portfolio}
        unavailable={portfolioUnavailable}
        loading={loading}
        selectedLookbackDays={portfolioLookbackDays}
        lookbackLinks={portfolioLookbackLinks}
        simulatedTradesCount={simulatedTradesCount}
      />

      <section className={`${cardClassName} p-4 sm:p-6`}>
        <div className="flex flex-wrap items-start justify-between gap-3">
          <div>
            <h2 className="text-lg font-semibold text-white">Trade Outcome Analytics</h2>
            <p className="mt-1 text-xs uppercase tracking-[0.2em] text-white/45">
              Benchmark: S&P 500 · Net flow 30D {loading ? "loading" : net < 0 ? `-$${compactUSD(Math.abs(net))}` : `$${compactUSD(net)}`}
            </p>
            <p className="mt-2 max-w-2xl text-sm text-white/45">
              Compact metrics from individually scored disclosures.
            </p>
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

        <div className="mt-2 flex flex-wrap items-center gap-x-4 gap-y-1 text-xs text-white/45">
          <span>Scored trades: {alphaSummary.trades_analyzed ?? 0}</span>
          {alphaUnavailable && !hasAlphaMetrics && <span>{REFRESHING_COPY}</span>}
        </div>
      </section>

      <div className="grid items-start gap-6 lg:grid-cols-[max-content_1fr]">
        <div className="w-fit">
          <div className={`${cardClassName} w-fit max-w-[240px]`}>
            <h2 className="text-lg font-semibold text-white">Top tickers</h2>
            <div className="mt-4 space-y-2">
              {loading && initialTopTickers.length === 0 ? (
                Array.from({ length: 5 }).map((_, idx) => <SkeletonBlock key={idx} className="h-9 w-44" />)
              ) : tradesUnavailable && topTickers.length === 0 ? (
                <p className="text-sm text-slate-400">{REFRESHING_COPY}</p>
              ) : topTickers.length === 0 ? (
                <p className="text-sm text-slate-400">No ticker concentration yet.</p>
              ) : (
                topTickers.map((ticker) => (
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
        </div>

        <div className={`${cardClassName} w-full min-w-0`}>
          <h2 className="text-lg font-semibold text-white">Recent trades</h2>
          <div className="mt-4 space-y-2">
            {loading ? (
              Array.from({ length: 6 }).map((_, idx) => <SkeletonBlock key={idx} className="h-24 w-full rounded-3xl" />)
            ) : tradesUnavailable && trades.items.length === 0 ? (
              <p className="rounded-2xl border border-white/10 bg-white/[0.03] px-4 py-3 text-sm text-slate-400">
                Recent activity is refreshing from disclosed trades.
              </p>
            ) : trades.items.length === 0 ? (
              <p className="text-sm text-slate-400">No recent trades for this member.</p>
            ) : (
              trades.items.map((trade) => {
                const signal = resolveSmartSignalValue(trade as Record<string, unknown>);
                const sideLabel = formatTransactionLabel(trade.transaction_type ?? "") ?? "Trade";
                const sideTone = transactionTone(trade.transaction_type ?? "");
                return (
                  <div key={trade.event_id ?? trade.id} className="rounded-3xl border border-white/5 bg-slate-900/70 p-4 shadow-card">
                    <div className="grid gap-3 xl:grid-cols-[minmax(180px,1fr)_105px_105px_100px_120px_105px_105px_90px] xl:items-center">
                      <div className="min-w-0">
                        <div className="flex items-center gap-2">
                          <TickerPill symbol={trade.symbol ?? "—"} href={trade.symbol ? tickerHref(trade.symbol) ?? undefined : undefined} />
                          <div className="min-w-0">
                            <p className="truncate text-sm font-semibold text-white">{trade.security_name ?? trade.symbol ?? "Security"}</p>
                            <p className="truncate text-xs text-white/45">{memberName}</p>
                          </div>
                        </div>
                      </div>
                      <div className="text-xs text-slate-400">
                        <div>Trade date</div>
                        <div className="mt-1 text-sm text-slate-200">{trade.trade_date ? formatDateShort(trade.trade_date) : "—"}</div>
                      </div>
                      <div className="text-xs text-slate-400">
                        <div>Filed</div>
                        <div className="mt-1 text-sm text-slate-200">{trade.report_date ? formatDateShort(trade.report_date) : "—"}</div>
                      </div>
                      <div className="text-xs text-slate-400">
                        <div>Side</div>
                        <div className="mt-1"><Badge tone={sideTone}>{sideLabel}</Badge></div>
                      </div>
                      <div className="text-xs text-slate-400">
                        <div>Trade value</div>
                        <div className="mt-1 text-sm font-semibold text-white">
                          {trade.estimated_trade_value != null ? `$${compactUSD(trade.estimated_trade_value)}` : "—"}
                        </div>
                        <div className="mt-0.5 text-[11px] text-slate-500">
                          {trade.amount_range_min != null || trade.amount_range_max != null
                            ? `$${compactUSD(trade.amount_range_min ?? 0)} – $${compactUSD(trade.amount_range_max ?? trade.amount_range_min ?? 0)}`
                            : "Range unavailable"}
                        </div>
                      </div>
                      <div className="text-xs text-slate-400">
                        <div>Price</div>
                        <div className="mt-1 text-sm font-semibold text-white">
                          {trade.estimated_price != null ? `$${trade.estimated_price.toFixed(2)}` : "—"}
                        </div>
                        <div className="mt-0.5 text-[11px] text-slate-500">
                          Current {trade.current_price != null ? `$${trade.current_price.toFixed(2)}` : "unavailable"}
                        </div>
                      </div>
                      <div className="text-xs text-slate-400">
                        <div>Gain/Loss</div>
                        <div className={`mt-1 text-sm font-semibold ${tone(trade.pnl_pct)}`}>
                          {pct(trade.pnl_pct)}
                        </div>
                        {trade.pnl_source ? <div className="mt-0.5 text-[11px] text-slate-500">{trade.pnl_source}</div> : null}
                      </div>
                      <div className="text-xs text-slate-400">
                        <div>Signal</div>
                        <div className="mt-1">
                          {signal.score != null && signal.band ? (
                            <SmartSignalPill score={signal.score} band={signal.band} size="compact" />
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
        </div>
      </div>
    </>
  );
}
