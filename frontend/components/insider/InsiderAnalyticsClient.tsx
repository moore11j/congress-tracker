"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";
import {
  getInsiderAlphaSummary,
  getInsiderSummary,
  getInsiderStockChart,
  getInsiderTrades,
  type InsiderAlphaSummary,
  type InsiderSummary,
} from "@/lib/api";
import { Badge } from "@/components/Badge";
import { TickerPill } from "@/components/ui/TickerPill";
import { PremiumTickerChart, PremiumTickerChartSkeleton } from "@/components/ticker/PremiumTickerChart";
import { TickerActivityPaginationFooter } from "@/components/ticker/TickerActivityPaginationFooter";
import { AddTickerToWatchlist } from "@/components/watchlists/AddTickerToWatchlist";
import { SmartSignalPill } from "@/components/ui/SmartSignalPill";
import { SkeletonBlock } from "@/components/ui/LoadingSkeleton";
import { tickerLinkClassName } from "@/lib/styles";
import { formatDateShort, formatTransactionLabel, transactionTone } from "@/lib/format";
import { tickerHref } from "@/lib/ticker";
import { resolveInsiderActivityDisplay } from "@/lib/tradeDisplay";
import { gainLossLabel, gainLossTooltip } from "@/lib/gainLossCopy";

type Lookback = "30" | "90" | "180" | "365" | "1095";
type PerformanceLookback = "7" | "30" | "90" | "180" | "365";

const RECENT_TRADES_PAGE_SIZE = 20;
const ACTIVITY_TREND_LOOKBACK_DAYS = 365;
const ACTIVITY_TREND_LOOKBACK_LABEL = "1Y";
const TREND_TRADES_LIMIT = 240;
const REFRESHING_COPY = "Refreshing the latest analytics from disclosed activity.";
const CARD = "overflow-hidden rounded-lg border border-white/10 bg-[#0a1726]/95 shadow-[0_14px_34px_rgba(0,0,0,0.22)]";
const PANEL = "rounded-lg border border-white/8 bg-white/[0.025]";
const PERFORMANCE_LOOKBACK_OPTIONS = [
  { label: "7D", value: "7" },
  { label: "30D", value: "30" },
  { label: "90D", value: "90" },
  { label: "180D", value: "180" },
  { label: "1Y", value: "365" },
] as const satisfies readonly { label: string; value: PerformanceLookback }[];
const DEFAULT_PERFORMANCE_LOOKBACK: PerformanceLookback = "365";

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

function compactMoney(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return "—";
  const sign = value < 0 ? "-" : "";
  const abs = Math.abs(value);
  if (abs >= 1e9) return `${sign}$${(abs / 1e9).toFixed(1)}B`;
  if (abs >= 1e6) return `${sign}$${(abs / 1e6).toFixed(1)}M`;
  if (abs >= 1e3) return `${sign}$${(abs / 1e3).toFixed(0)}K`;
  return `${sign}$${Math.round(abs)}`;
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
  const prefix = pnl > 0 ? "+" : pnl < 0 ? "-" : "";
  return `${prefix}${Math.abs(pnl).toFixed(1)}%`;
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

function tradeDirection(value?: string | null): "buy" | "sell" | null {
  const normalized = (value ?? "").toLowerCase();
  if (normalized === "p" || normalized.includes("buy") || normalized.includes("purchase") || normalized.includes("acquire")) return "buy";
  if (normalized === "s" || normalized.includes("sale") || normalized.includes("sell") || normalized.includes("dispose")) return "sell";
  return null;
}

function tradeValue(trade: InsiderTradesData["items"][number]) {
  if (trade.trade_value != null) return trade.trade_value;
  if (trade.tradeValue != null) return trade.tradeValue;
  if (trade.amount_min != null && trade.amount_max != null) return (trade.amount_min + trade.amount_max) / 2;
  return trade.amount_max ?? trade.amount_min ?? null;
}

function priceRange(trade: InsiderTradesData["items"][number]) {
  const price = trade.display_price ?? trade.displayPrice ?? trade.price;
  if (price != null) return `$${price.toFixed(2)}`;
  return "—";
}

function compactNumber(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return "â€”";
  return new Intl.NumberFormat("en-US", {
    notation: "compact",
    maximumFractionDigits: Math.abs(value) >= 1_000_000 ? 1 : 0,
  }).format(value);
}

function sharesOwnedFollowing(trade: InsiderTradesData["items"][number]): number | null {
  const value = trade.shares_owned_following ?? trade.sharesOwnedFollowing;
  return typeof value === "number" && Number.isFinite(value) && value >= 0 ? value : null;
}

function isDirectOwnership(trade: InsiderTradesData["items"][number]): boolean {
  const value = (trade.direct_or_indirect ?? trade.directOrIndirectOwnership ?? "").trim().toLowerCase();
  return value === "d" || value === "direct";
}

function summarizeInsiderTrades(items: InsiderTradesData["items"]) {
  const sorted = [...items].sort((left, right) => {
    const a = Date.parse(left.filing_date ?? left.transaction_date ?? left.trade_date ?? "");
    const b = Date.parse(right.filing_date ?? right.transaction_date ?? right.trade_date ?? "");
    return (Number.isFinite(b) ? b : 0) - (Number.isFinite(a) ? a : 0);
  });
  const months = new Map<string, { label: string; buy: number; sell: number }>();
  let sharesBought = 0;
  let sharesSold = 0;
  let saleValue = 0;
  let saleCount = 0;
  let buyValue = 0;
  let buyCount = 0;

  sorted.forEach((trade) => {
    const direction = tradeDirection(trade.trade_type ?? trade.tradeType);
    const shares = trade.shares ?? 0;
    const value = tradeValue(trade) ?? 0;
    if (direction === "buy") {
      sharesBought += shares;
      buyValue += value;
      buyCount += 1;
    }
    if (direction === "sell") {
      sharesSold += shares;
      saleValue += value;
      saleCount += 1;
    }
    const rawDate = trade.filing_date ?? trade.transaction_date ?? trade.trade_date;
    const date = rawDate ? new Date(rawDate) : null;
    if (date && Number.isFinite(date.getTime())) {
      const key = `${date.getUTCFullYear()}-${String(date.getUTCMonth() + 1).padStart(2, "0")}`;
      const label = date.toLocaleDateString("en-US", { month: "short", year: "2-digit", timeZone: "UTC" });
      const bucket = months.get(key) ?? { label, buy: 0, sell: 0 };
      if (direction === "buy") bucket.buy += Math.max(1, Math.round(shares || 1));
      if (direction === "sell") bucket.sell += Math.max(1, Math.round(shares || 1));
      months.set(key, bucket);
    }
  });

  return {
    sorted,
    sharesBought,
    sharesSold,
    saleValue,
    saleCount,
    buyValue,
    buyCount,
    avgSale: saleCount > 0 ? saleValue / saleCount : null,
    avgBuy: buyCount > 0 ? buyValue / buyCount : null,
    buckets: Array.from(months.entries()).sort((a, b) => a[0].localeCompare(b[0])).slice(-12).map((entry) => entry[1]),
  };
}

function directOwnershipValue(trades: InsiderTradesData["items"], stockChart: InsiderStockChartData | null): string {
  const latestDirect = trades.find((trade) => isDirectOwnership(trade) && sharesOwnedFollowing(trade) != null);
  const directShares = latestDirect ? sharesOwnedFollowing(latestDirect) : null;
  if (directShares == null) return "â€”";

  const marketCap = stockChart?.quote?.market_cap;
  const currentPrice = stockChart?.quote?.current_price ?? stockChart?.quote?.latest_close;
  const impliedSharesOutstanding =
    typeof marketCap === "number" && Number.isFinite(marketCap) && typeof currentPrice === "number" && Number.isFinite(currentPrice) && currentPrice > 0
      ? marketCap / currentPrice
      : null;

  if (impliedSharesOutstanding && impliedSharesOutstanding > 0) {
    return pct((directShares / impliedSharesOutstanding) * 100);
  }
  return `${compactNumber(directShares)} sh`;
}

function WatchIcon({ name, toneClass }: { name: "purchase" | "frequency" | "plan" | "ownership" | "performance"; toneClass: string }) {
  const paths = {
    purchase: "M6 10.5V3.5m0 0L3.5 6m2.5-2.5L8.5 6M3 10.5h6",
    frequency: "M2.5 7a3.5 3.5 0 0 1 5.8-2.6L9.5 5.5M9.5 3v2.5H7M9.5 7a3.5 3.5 0 0 1-5.8 2.6L2.5 8.5M2.5 11V8.5H5",
    plan: "M3.5 2.5h5l1 1v6h-7v-6l1-1Zm1 2h4m-4 2h4m-4 2h2",
    ownership: "M6 2.5 9.5 4v3.2c0 2-1.4 3.5-3.5 4.3-2.1-.8-3.5-2.3-3.5-4.3V4L6 2.5Zm-1.5 4 1 1 2-2",
    performance: "M2.5 9.5 5 7l1.5 1.5 3-4M7.5 4.5h2v2",
  } as const;
  return (
    <span className={`grid h-7 w-7 shrink-0 place-items-center rounded-md border ${toneClass}`}>
      <svg viewBox="0 0 12 12" aria-hidden="true" className="h-4 w-4" fill="none">
        <path d={paths[name]} stroke="currentColor" strokeLinecap="round" strokeLinejoin="round" strokeWidth="1.15" />
      </svg>
    </span>
  );
}

function SectionTitle({ title, detail }: { title: string; detail?: string }) {
  return (
    <div className="flex items-center justify-between gap-3">
      <h2 className="text-[11px] font-semibold uppercase leading-none tracking-[0.14em] text-slate-200">{title}</h2>
      {detail ? <span className="text-[10px] leading-none text-slate-500">{detail}</span> : null}
    </div>
  );
}

function MetricGrid({ metrics }: { metrics: Array<{ label: string; value: string; sub?: string; valueClass?: string }> }) {
  return (
    <div className="mt-2.5 grid grid-cols-2 gap-px overflow-hidden rounded-md border border-white/8 bg-white/8 md:grid-cols-5">
      {metrics.map((metric) => (
        <div key={metric.label} className="bg-[#081321] px-2.5 py-2">
          <p className="text-[9px] font-medium uppercase leading-none tracking-[0.12em] text-slate-500">{metric.label}</p>
          <p className={`mt-1.5 text-base font-semibold leading-none tabular-nums ${metric.valueClass ?? "text-white"}`}>{metric.value}</p>
          {metric.sub ? <p className="mt-1 text-[10px] leading-tight text-slate-500">{metric.sub}</p> : null}
        </div>
      ))}
    </div>
  );
}

function MiniBars({ buckets }: { buckets: Array<{ label: string; buy: number; sell: number }> }) {
  const max = Math.max(1, ...buckets.map((bucket) => Math.max(bucket.buy, bucket.sell)));
  const width = 360;
  const height = 150;
  const zero = 76;
  const gap = 8;
  const barWidth = Math.max(4, (width - 48 - gap * buckets.length) / Math.max(1, buckets.length * 2));

  return (
    <div className="mt-2 h-28 w-full">
      <svg viewBox={`0 0 ${width} ${height}`} className="h-full w-full overflow-visible">
        <line x1="28" x2={width - 8} y1={zero} y2={zero} stroke="rgba(148,163,184,0.25)" />
        {[0, 1, 2].map((tick) => (
          <line key={tick} x1="28" x2={width - 8} y1={28 + tick * 48} y2={28 + tick * 48} stroke="rgba(148,163,184,0.08)" />
        ))}
        {buckets.map((bucket, index) => {
          const x = 34 + index * (barWidth * 2 + gap);
          const buyHeight = Math.max(2, (bucket.buy / max) * 58);
          const sellHeight = Math.max(2, (bucket.sell / max) * 58);
          return (
            <g key={bucket.label}>
              <rect x={x} y={zero - buyHeight} width={barWidth} height={buyHeight} rx="1.5" fill="#34d399" />
              <rect x={x + barWidth + 2} y={zero} width={barWidth} height={sellHeight} rx="1.5" fill="#fb7185" />
              {index % 2 === 0 ? <text x={x} y={height - 8} fill="#64748b" fontSize="9">{bucket.label}</text> : null}
            </g>
          );
        })}
      </svg>
    </div>
  );
}

function AnalyticsStatsSkeleton() {
  return (
    <div className="mt-3 grid grid-cols-2 gap-2 md:grid-cols-5">
      {Array.from({ length: 5 }).map((_, idx) => (
        <div key={idx} className={`${PANEL} px-3 py-3`}>
          <SkeletonBlock className="h-3 w-24" />
          <SkeletonBlock className="mt-3 h-6 w-20" />
        </div>
      ))}
    </div>
  );
}

export function InsiderAnalyticsClient({
  reportingCik,
  lookbackDays,
  issuer,
  stockSymbol,
  recentTradesPage,
  summary,
  initialAlphaSummary,
  initialTrades,
}: {
  reportingCik: string;
  lookback: Lookback;
  lookbackDays: number;
  issuer?: string;
  stockSymbol?: string;
  recentTradesPage: number;
  summary: InsiderSummary;
  initialAlphaSummary?: InsiderAlphaSummary;
  initialTrades?: InsiderTradesData;
}) {
  const [performanceLookback, setPerformanceLookback] = useState<PerformanceLookback>(DEFAULT_PERFORMANCE_LOOKBACK);
  const performanceLookbackDays = Number(performanceLookback);
  const [alphaSummary, setAlphaSummary] = useState<InsiderAlphaSummary>(() =>
    initialAlphaSummary?.lookback_days === performanceLookbackDays
      ? initialAlphaSummary
      : fallbackInsiderAlphaSummary(reportingCik, performanceLookbackDays),
  );
  const [trades, setTrades] = useState<InsiderTradesData>(() =>
    initialTrades ?? fallbackInsiderTrades(reportingCik, lookbackDays, recentTradesPage),
  );
  const [trendTrades, setTrendTrades] = useState<InsiderTradesData>(() =>
    initialTrades?.lookback_days === ACTIVITY_TREND_LOOKBACK_DAYS
      ? initialTrades
      : fallbackInsiderTrades(reportingCik, ACTIVITY_TREND_LOOKBACK_DAYS, 0),
  );
  const [liveSummary, setLiveSummary] = useState<InsiderSummary>(summary);
  const [stockChart, setStockChart] = useState<InsiderStockChartData | null>(null);
  const hasInitialAnalytics = Boolean(initialAlphaSummary || initialTrades);
  const [loading, setLoading] = useState(!hasInitialAnalytics);
  const [performanceLoading, setPerformanceLoading] = useState(initialAlphaSummary?.lookback_days !== performanceLookbackDays);
  const [stockChartLoading, setStockChartLoading] = useState(true);
  const [alphaUnavailable, setAlphaUnavailable] = useState(false);
  const [tradesUnavailable, setTradesUnavailable] = useState(false);
  const [stockChartUnavailable, setStockChartUnavailable] = useState(false);

  useEffect(() => {
    const controller = new AbortController();
    let cancelled = false;
    setLiveSummary(summary);
    getInsiderSummary(reportingCik, lookbackDays, issuer, {
      signal: controller.signal,
      source: "InsiderAnalyticsSummaryClient",
    })
      .then((data) => {
        if (!cancelled) setLiveSummary(data);
      })
      .catch(() => undefined);
    return () => {
      cancelled = true;
      controller.abort();
    };
  }, [issuer, lookbackDays, reportingCik, summary]);

  useEffect(() => {
    const controller = new AbortController();
    let cancelled = false;
    setLoading(!initialTrades || initialTrades.lookback_days !== lookbackDays || initialTrades.page !== recentTradesPage);
    setTradesUnavailable(false);

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
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });

    return () => {
      cancelled = true;
      controller.abort();
    };
  }, [initialTrades, issuer, lookbackDays, recentTradesPage, reportingCik]);

  useEffect(() => {
    const controller = new AbortController();
    let cancelled = false;
    setPerformanceLoading(!initialAlphaSummary || initialAlphaSummary.lookback_days !== performanceLookbackDays);
    setAlphaUnavailable(false);

    getInsiderAlphaSummary(reportingCik, {
      lookback_days: performanceLookbackDays,
      issuer,
      source: "InsiderAlphaSummary",
      signal: controller.signal,
    })
      .then((data) => {
        if (!cancelled) setAlphaSummary(data);
      })
      .catch(() => {
        if (!cancelled) setAlphaUnavailable(true);
      })
      .finally(() => {
        if (!cancelled) setPerformanceLoading(false);
      });

    return () => {
      cancelled = true;
      controller.abort();
    };
  }, [initialAlphaSummary, issuer, performanceLookbackDays, reportingCik]);

  useEffect(() => {
    const controller = new AbortController();
    let cancelled = false;
    getInsiderTrades(reportingCik, ACTIVITY_TREND_LOOKBACK_DAYS, TREND_TRADES_LIMIT, issuer, {
      page: 0,
      source: "InsiderActivityTrend",
      signal: controller.signal,
    })
      .then((data) => {
        if (!cancelled) setTrendTrades(data);
      })
      .catch(() => undefined);

    return () => {
      cancelled = true;
      controller.abort();
    };
  }, [issuer, reportingCik]);

  useEffect(() => {
    const controller = new AbortController();
    let cancelled = false;
    setStockChartLoading(true);
    setStockChartUnavailable(false);
    getInsiderStockChart(reportingCik, {
      lookback_days: ACTIVITY_TREND_LOOKBACK_DAYS,
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
  }, [reportingCik, stockSymbol]);

  const recentTradesLimit = typeof trades.limit === "number" && trades.limit > 0 ? trades.limit : RECENT_TRADES_PAGE_SIZE;
  const recentTradesPageValue = typeof trades.page === "number" && trades.page >= 0 ? trades.page : recentTradesPage;
  const recentTradesTotal = typeof trades.total === "number" && trades.total >= 0 ? trades.total : trades.items.length;
  const recentTradesHasNext =
    typeof trades.has_next === "boolean"
      ? trades.has_next
      : recentTradesPageValue * recentTradesLimit + trades.items.length < recentTradesTotal;

  const derived = useMemo(() => summarizeInsiderTrades(trades.items), [trades.items]);
  const trendDerived = useMemo(() => summarizeInsiderTrades(trendTrades.items), [trendTrades.items]);

  const summaryMetrics = [
    { label: "Filings", value: numberOrDash(liveSummary.total_trades), sub: `Rank window: ${lookbackDays}D` },
    {
      label: "Buy / Sell Ratio",
      value: `${liveSummary.buy_count} / ${liveSummary.sell_count}`,
      sub: liveSummary.net_flow >= 0 ? "Net buyer" : "Net seller",
      valueClass: liveSummary.net_flow >= 0 ? "text-emerald-300" : "text-rose-300",
    },
    { label: "Shares Sold", value: numberOrDash(liveSummary.sell_count), sub: "From visible filings" },
    { label: "Est. Value Range", value: `${compactMoney(liveSummary.gross_buy_value)} - ${compactMoney(liveSummary.gross_sell_value)}`, sub: "Visible filings" },
    { label: "Avg. Trade Size", value: compactMoney((liveSummary.gross_buy_value + liveSummary.gross_sell_value) / Math.max(1, liveSummary.total_trades)), sub: "Visible filings" },
  ];
  const changeRows = derived.sorted.slice(0, 5).map((trade) => {
    const direction = tradeDirection(trade.trade_type ?? trade.tradeType);
    const side = formatTransactionLabel(trade.trade_type ?? trade.tradeType) ?? "Trade";
    return {
      key: trade.external_id ?? `${trade.event_id}`,
      tone: direction === "sell" ? "border-rose-400/20 bg-rose-400/10 text-rose-300" : direction === "buy" ? "border-emerald-400/20 bg-emerald-400/10 text-emerald-300" : "border-slate-400/20 bg-slate-400/10 text-slate-300",
      title: `${side} disclosed`,
      body: `${trade.symbol ?? "Issuer"} ${trade.shares != null ? `${numberOrDash(trade.shares)} shares` : compactMoney(tradeValue(trade))}`,
      date: formatDateShort(trade.filing_date),
    };
  });
  const patternRows = [
    ["Total Buys", numberOrDash(derived.buyCount)],
    ["Total Sells", numberOrDash(derived.saleCount)],
    ["Net Shares", numberOrDash(derived.sharesBought - derived.sharesSold)],
    ["Avg. Sale Size", compactMoney(derived.avgSale)],
    ["Avg. Buy Size", compactMoney(derived.avgBuy)],
    ["Most Recent Filing", asDate(liveSummary.latest_filing_date)],
    ["Direct Ownership", directOwnershipValue(derived.sorted, stockChart)],
  ];
  const performanceRows = [
    { label: "Avg Return", value: pct(alphaSummary.avg_return_pct), tone: tone(alphaSummary.avg_return_pct) },
    { label: "Avg Alpha", value: pct(alphaSummary.avg_alpha_pct), tone: tone(alphaSummary.avg_alpha_pct) },
    { label: "Win Rate", value: pct0(alphaSummary.win_rate), tone: tone(alphaSummary.win_rate == null ? null : (alphaSummary.win_rate - 0.5) * 100) },
    { label: "Holding Days", value: numberOrDash(alphaSummary.avg_holding_days), tone: "text-white/85" },
    { label: "Scored", value: numberOrDash(alphaSummary.trades_analyzed), tone: "text-white/85" },
  ];
  const watchRows = [
    {
      label: liveSummary.buy_count > 0 ? "Any open-market purchases" : "No open-market purchases in this window",
      icon: "purchase" as const,
      tone: "border-emerald-400/20 bg-emerald-400/10 text-emerald-300",
    },
    {
      label: "Change in transaction frequency",
      icon: "frequency" as const,
      tone: "border-rose-400/20 bg-rose-400/10 text-rose-300",
    },
    {
      label: liveSummary.latest_filing_date ? `Next Form 4 after ${asDate(liveSummary.latest_filing_date)}` : "Next Form 4 filing",
      icon: "plan" as const,
      tone: "border-emerald-400/20 bg-emerald-400/10 text-emerald-300",
    },
    {
      label: liveSummary.primary_symbol ? `${liveSummary.primary_symbol} ownership changes` : "Issuer ownership changes",
      icon: "ownership" as const,
      tone: "border-emerald-400/20 bg-emerald-400/10 text-emerald-300",
    },
    {
      label: alphaSummary.avg_alpha_pct != null ? `Post-transaction alpha: ${pct(alphaSummary.avg_alpha_pct)}` : "Performance after future transactions",
      icon: "performance" as const,
      tone: "border-violet-400/20 bg-violet-400/10 text-violet-300",
    },
  ];
  const performanceLookbackLabel = PERFORMANCE_LOOKBACK_OPTIONS.find((option) => option.value === performanceLookback)?.label ?? `${performanceLookbackDays}D`;

  return (
    <div className="space-y-3">
      <div className="grid items-stretch gap-3 xl:grid-cols-[minmax(0,1.45fr)_minmax(380px,0.8fr)] xl:[&>section]:h-[152px]">
        <section className={`${CARD} p-3`}>
          <SectionTitle title="Insider Activity Summary" detail={`${lookbackDays}D`} />
          <p className="mt-2 truncate text-xs text-slate-500">Recent activity is summarized from public Form 4 filings and scored outcomes.</p>
          {loading ? <AnalyticsStatsSkeleton /> : <MetricGrid metrics={summaryMetrics} />}
          {alphaSummary.trades_analyzed === 0 && trades.items.length > 0 ? (
            <p className="mt-2 rounded-md border border-amber-300/25 bg-amber-400/10 px-3 py-1.5 text-[11px] leading-tight text-amber-100">
              No market trades analyzed in this window. Showing recent insider activity below.
            </p>
          ) : null}
        </section>

        <section className={`${CARD} p-3`}>
          <SectionTitle title="Activity Trend" detail={ACTIVITY_TREND_LOOKBACK_LABEL} />
          <div className="mt-3 flex flex-wrap justify-between gap-3">
            <div className="flex gap-4 text-[11px] text-slate-500">
              <span><span className="mr-1 inline-block h-2 w-2 rounded-full bg-emerald-400" />Buys</span>
              <span><span className="mr-1 inline-block h-2 w-2 rounded-full bg-rose-400" />Sells</span>
            </div>
          </div>
          <MiniBars buckets={trendDerived.buckets} />
        </section>
      </div>

      <div className="grid items-stretch gap-3 xl:grid-cols-[minmax(280px,0.75fr)_minmax(280px,0.75fr)_minmax(320px,0.9fr)] xl:[&>section]:h-[210px]">
        <section className={`${CARD} p-3`}>
          <SectionTitle title="What Changed" detail="View all" />
          <div className="mt-2 space-y-1.5">
            {loading ? (
              Array.from({ length: 4 }).map((_, idx) => <SkeletonBlock key={idx} className="h-10 w-full" />)
            ) : changeRows.length === 0 ? (
              <p className="text-sm text-slate-500">No recent changes found.</p>
            ) : changeRows.map((row) => (
              <div key={row.key} className="grid grid-cols-[20px_1fr_auto] gap-2">
                <span className={`grid h-5 w-5 place-items-center rounded-md border text-[10px] ${row.tone}`}>•</span>
                <div className="min-w-0">
                  <p className="truncate text-xs font-medium leading-tight text-slate-100">{row.title}</p>
                  <p className="truncate text-[10px] leading-tight text-slate-500">{row.body}</p>
                </div>
                <span className="text-[10px] leading-tight text-slate-500">{row.date}</span>
              </div>
            ))}
          </div>
        </section>

        <section id="insider-ownership" className={`${CARD} p-3 scroll-mt-6`}>
          <SectionTitle title="Transaction Pattern" detail="LTM" />
          <div className="mt-4 divide-y divide-white/8">
            {patternRows.map(([label, value]) => (
              <div key={label} className="flex items-center justify-between py-1.5 text-xs">
                <span className="text-slate-500">{label}</span>
                <span className="font-medium text-slate-200 tabular-nums">{value}</span>
              </div>
            ))}
          </div>
          <Link href="#recent-filings" className="mt-3 inline-flex text-xs font-medium text-sky-300 hover:text-sky-200">View full pattern</Link>
        </section>

        <section id="insider-performance" className={`${CARD} p-3 scroll-mt-6`}>
          <SectionTitle title="Performance After Sales" detail={performanceLookbackLabel} />
          <div className="mt-3 flex flex-wrap gap-1.5 text-xs">
            {PERFORMANCE_LOOKBACK_OPTIONS.map((option) => (
              <button
                key={option.value}
                type="button"
                onClick={() => setPerformanceLookback(option.value)}
                className={`rounded-md border px-2.5 py-1 font-semibold ${
                  performanceLookback === option.value
                    ? "border-sky-400/50 bg-sky-400/10 text-sky-100"
                    : "border-white/10 bg-slate-900/60 text-slate-300 hover:border-white/20 hover:text-white"
                }`}
                aria-pressed={performanceLookback === option.value}
              >
                {option.label}
              </button>
            ))}
          </div>
          <div className="mt-4 grid grid-cols-2 gap-3 sm:grid-cols-5 xl:grid-cols-1 2xl:grid-cols-5">
            {performanceLoading ? (
              Array.from({ length: 5 }).map((_, idx) => <SkeletonBlock key={idx} className="h-10 w-full" />)
            ) : (
              performanceRows.map((row) => (
                <div key={row.label}>
                  <p className={`text-base font-semibold leading-none tabular-nums ${row.tone}`}>{row.value}</p>
                  <p className="mt-1 text-[11px] uppercase tracking-[0.12em] text-slate-500">{row.label}</p>
                </div>
              ))
            )}
          </div>
          <p className="mt-3 text-xs text-slate-500">Performance measured from transaction date when outcome data is available.</p>
          {alphaUnavailable ? (
            <p className="mt-2 rounded-md border border-amber-300/25 bg-amber-400/10 px-3 py-1.5 text-[11px] leading-tight text-amber-100">
              {REFRESHING_COPY}
            </p>
          ) : null}
          <Link href="/pricing" className="mt-2 inline-flex text-xs font-medium text-sky-300 hover:text-sky-200">How we calculate</Link>
        </section>
      </div>

      <section className={`${CARD} p-3`}>
        <SectionTitle title="What to Watch Next" />
        <div className="mt-2.5 grid gap-px overflow-hidden rounded-md border border-white/8 bg-white/8 md:grid-cols-5">
          {watchRows.map((row) => (
            <div key={row.label} className="flex items-center gap-2 bg-[#081321] px-2.5 py-2">
              <WatchIcon name={row.icon} toneClass={row.tone} />
              <p className="text-xs leading-tight text-slate-300">{row.label}</p>
            </div>
          ))}
        </div>
      </section>

      <section id="recent-filings" className={`${CARD} w-full min-w-0 p-3 scroll-mt-6`}>
        <SectionTitle title="Recent Form 4 Filings" detail="View all" />
        <p className="mt-1 text-xs text-slate-500">
          Displayed quotes are USD. Current foreign prices use spot FX where applicable; historical foreign filing prices use trade-date FX and ADR ratios when normalized.
        </p>
        <div data-activity-scroll-region className="mt-3 overflow-x-auto">
          {loading ? (
            <div className="space-y-2">
              {Array.from({ length: 6 }).map((_, idx) => <SkeletonBlock key={idx} className="h-9 w-full" />)}
            </div>
          ) : tradesUnavailable ? (
            <p className="text-sm text-slate-400">Recent activity is refreshing from disclosed trades.</p>
          ) : recentTradesTotal === 0 ? (
            <p className="text-sm text-slate-400">No recent activity found.</p>
          ) : trades.items.length === 0 ? (
            <p className="text-sm text-slate-400">No trades on this page.</p>
          ) : (
            <table className="w-full min-w-[780px] text-left text-sm">
              <thead className="text-[10px] uppercase tracking-[0.14em] text-slate-500">
                <tr>
                  <th className="pb-3 font-medium">Filing date</th>
                  <th className="pb-3 font-medium">Transaction date</th>
                  <th className="pb-3 font-medium">Type</th>
                  <th className="pb-3 font-medium">Shares</th>
                  <th className="pb-3 font-medium">Price range</th>
                  <th className="pb-3 font-medium">Est. value</th>
                  <th className="pb-3 font-medium">Signal</th>
                  <th className="pb-3 font-medium">{gainLossLabel}</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-white/8">
                {trades.items.map((trade) => {
                  const tradeRecord = trade as Record<string, unknown>;
                  const display = resolveInsiderActivityDisplay(tradeRecord);
                  const tradeType = display.tradeType ?? "";
                  const sideLabel = formatTransactionLabel(tradeType) ?? "Trade";
                  const sideTone = transactionTone(tradeType);
                  const pnlSourceLabel = pnlSourceBadgeLabel(display.pnlSource);
                  return (
                    <tr key={trade.external_id ?? `${trade.event_id}`}>
                      <td className="py-2.5 text-slate-300">{trade.filing_date ? formatDateShort(trade.filing_date) : "—"}</td>
                      <td className="py-2.5 text-slate-300">{display.transactionDate ? formatDateShort(display.transactionDate) : "—"}</td>
                      <td className="py-2.5"><Badge tone={sideTone}>{sideLabel}</Badge></td>
                      <td className="py-2.5 text-slate-300 tabular-nums">{trade.shares != null ? numberOrDash(trade.shares) : "—"}</td>
                      <td className="py-2.5 text-slate-300 tabular-nums">
                        {priceRange(trade)}
                        {display.reportedLabel ? <div className="mt-0.5 text-[11px] text-slate-500">{display.reportedLabel}</div> : null}
                      </td>
                      <td className="py-2.5 text-slate-300 tabular-nums">{display.tradeValue !== null ? formatMoney(display.tradeValue) : compactMoney(tradeValue(trade))}</td>
                      <td className="py-2.5">
                        <div className="flex items-center gap-2">
                          {trade.symbol ? <AddTickerToWatchlist symbol={display.displaySymbol} variant="compact" align="left" /> : null}
                          {trade.symbol ? (
                            <TickerPill symbol={display.displaySymbol} href={tickerHref(trade.symbol) ?? undefined} className="inline-flex shrink-0" />
                          ) : (
                            <TickerPill symbol="—" />
                          )}
                          {display.hasSignal ? (
                            <SmartSignalPill score={display.signal.score} band={display.signal.band} size="compact" />
                          ) : (
                            <span className="text-[11px] text-slate-500">No signal</span>
                          )}
                        </div>
                      </td>
                      <td className="py-2.5 text-right text-xs text-slate-400">
                        <div className="cursor-help whitespace-nowrap" title={gainLossTooltip} aria-label={`${gainLossLabel}: ${gainLossTooltip}`}>
                          {display.pnl !== null ? (
                            <span className={`text-sm font-semibold tabular-nums ${pnlClass(display.pnl)}`}>{formatPnl(display.pnl)}</span>
                          ) : (
                            <span>—</span>
                          )}
                        </div>
                        {pnlSourceLabel ? (
                          <div className="mt-1">
                            <span className="inline-flex items-center rounded-md border border-slate-700 bg-slate-900/30 px-1.5 py-0.5 text-[10px] font-semibold text-slate-300">
                              {pnlSourceLabel}
                            </span>
                          </div>
                        ) : null}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          )}
        </div>
        {!tradesUnavailable && recentTradesTotal > recentTradesLimit ? (
          <div id="recent-trades" className="mt-4">
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
        <div className="hidden">
          {alphaSummary.best_trades.map((trade) => (
            <Link key={`best-${trade.event_id}-${trade.symbol}`} href={tickerHref(trade.symbol) ?? "#"} prefetch={false} className={`${tickerLinkClassName} truncate`}>
              {trade.symbol} {asDate(trade.asof_date)}
            </Link>
          ))}
        </div>
      </section>

      <section className={`${CARD} p-3`}>
        <SectionTitle title="Company Stock Chart" detail="Activity Trend" />
        <div className="mt-4 rounded-lg border border-white/10 bg-white/[0.03] p-4">
          {stockChartLoading ? (
            <PremiumTickerChartSkeleton />
          ) : stockChartUnavailable ? (
            <p className="rounded-lg border border-amber-300/25 bg-amber-400/10 px-3 py-2 text-sm text-amber-100">
              {REFRESHING_COPY}
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
      </section>
    </div>
  );
}
