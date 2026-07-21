"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";
import {
  getEntitlements,
  getMemberAlphaSummary,
  getMemberPortfolioPerformance,
  getMemberTrades,
  type MemberAlphaSummary,
  type MemberPortfolioPerformance,
  type MemberTradesResponse,
} from "@/lib/api";
import type { MemberTrade } from "@/lib/types";
import { UpgradePrompt } from "@/components/billing/UpgradePrompt";
import { Badge } from "@/components/Badge";
import { TickerPill } from "@/components/ui/TickerPill";
import { PerformanceChart } from "@/components/member/PerformanceChart";
import { SkeletonBlock } from "@/components/ui/LoadingSkeleton";
import { SmartSignalPill } from "@/components/ui/SmartSignalPill";
import { formatDateShort, formatTransactionLabel, transactionTone } from "@/lib/format";
import {
  defaultEntitlements,
  entitlementsFromTierHint,
  hasEntitlement,
  storedEntitlementTier,
  type Entitlements,
} from "@/lib/entitlements";
import { tickerHref } from "@/lib/ticker";
import { resolveSmartSignalValue } from "@/lib/smartSignal";
import {
  PORTFOLIO_MODE,
  normalizeMemberPortfolioChartData,
  normalizeMemberPortfolioEventMarkers,
} from "@/lib/portfolioPerformance.mjs";

const REFRESHING_COPY = "Refreshing analytics from disclosed activity.";
const CARD = "overflow-hidden rounded-lg border border-white/10 bg-[#0a1726]/95 shadow-[0_14px_34px_rgba(0,0,0,0.22)]";
const PANEL = "rounded-lg border border-white/8 bg-white/[0.025]";

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

function compactUSD(n: number | null | undefined) {
  if (n == null || !Number.isFinite(n)) return "—";
  const sign = n < 0 ? "-" : "";
  const abs = Math.abs(n);
  if (abs >= 1e9) return `${sign}$${(abs / 1e9).toFixed(1)}B`;
  if (abs >= 1e6) return `${sign}$${(abs / 1e6).toFixed(1)}M`;
  if (abs >= 1e3) return `${sign}$${(abs / 1e3).toFixed(0)}K`;
  return `${sign}$${Math.round(abs)}`;
}

function compactNumber(n: number | null | undefined) {
  if (n == null || !Number.isFinite(n)) return "—";
  const abs = Math.abs(n);
  if (abs >= 1e6) return `${(n / 1e6).toFixed(1)}M`;
  if (abs >= 1e3) return `${(n / 1e3).toFixed(1)}K`;
  return `${Math.round(n)}`;
}

function amountMid(trade: MemberTrade) {
  if (trade.estimated_trade_value != null && Number.isFinite(trade.estimated_trade_value)) return trade.estimated_trade_value;
  const min = trade.amount_range_min;
  const max = trade.amount_range_max;
  if (min != null && max != null) return (min + max) / 2;
  return max ?? min ?? null;
}

function rangeLabel(min: number | null | undefined, max: number | null | undefined) {
  if (min == null && max == null) return "Range unavailable";
  if (min != null && max != null && min !== max) return `${compactUSD(min)} - ${compactUSD(max)}`;
  return compactUSD(max ?? min);
}

function sectorLabel(value: string | null | undefined) {
  const cleaned = (value ?? "").trim();
  if (!cleaned) return "Sector unavailable";
  const normalized = cleaned.toLowerCase();
  if (["equity", "stock", "stocks", "security", "securities", "other", "etf", "fund", "treasury", "crypto"].includes(normalized)) {
    return "Sector unavailable";
  }
  return cleaned;
}

function tradeDirection(tradeType?: string | null): "buy" | "sell" | null {
  const normalized = (tradeType ?? "").trim().toLowerCase();
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

function sortedTrades(items: MemberTrade[]) {
  return [...items].sort((left, right) => {
    const leftDate = Date.parse(left.report_date ?? left.trade_date ?? "");
    const rightDate = Date.parse(right.report_date ?? right.trade_date ?? "");
    return (Number.isFinite(rightDate) ? rightDate : 0) - (Number.isFinite(leftDate) ? leftDate : 0);
  });
}

function daysBetween(later: string | null | undefined, earlier: string | null | undefined) {
  const a = Date.parse(later ?? "");
  const b = Date.parse(earlier ?? "");
  if (!Number.isFinite(a) || !Number.isFinite(b)) return null;
  return Math.max(0, Math.round((a - b) / 86400000));
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

function ActivityDonut({ rows }: { rows: Array<{ label: string; value: number; color: string }> }) {
  const total = rows.reduce((sum, row) => sum + row.value, 0);
  let cursor = 0;
  const gradient = total > 0
    ? rows.map((row) => {
        const start = cursor;
        const end = cursor + (row.value / total) * 100;
        cursor = end;
        return `${row.color} ${start}% ${end}%`;
      }).join(", ")
    : "#1f2937 0% 100%";

  return (
    <div className="mt-2 flex items-center gap-3">
      <div className="relative h-20 w-20 shrink-0 rounded-full" style={{ background: `conic-gradient(${gradient})` }}>
        <div className="absolute inset-5 rounded-full border border-white/10 bg-[#081321]" />
      </div>
      <div className="min-w-0 flex-1 space-y-1.5">
        {rows.length === 0 ? (
          <p className="text-xs text-slate-500">No sector activity yet.</p>
        ) : rows.map((row) => (
          <div key={row.label} className="grid grid-cols-[1fr_auto] items-center gap-2 text-[11px] leading-none">
            <div className="flex min-w-0 items-center gap-2">
              <span className="h-2 w-2 rounded-full" style={{ backgroundColor: row.color }} />
              <span className="truncate text-slate-300">{row.label}</span>
            </div>
            <span className="text-slate-400 tabular-nums">{Math.round((row.value / total) * 100)}%</span>
          </div>
        ))}
      </div>
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

function PortfolioSkeleton() {
  return (
    <section id="member-performance" className={`${CARD} p-3`}>
      <SectionTitle title="Performance" detail="Realistic disclosure lag" />
      <div className="mt-2.5 grid grid-cols-2 gap-px overflow-hidden rounded-md border border-white/8 bg-white/8 sm:grid-cols-3 lg:grid-cols-5">
        {Array.from({ length: 5 }).map((_, idx) => (
            <div key={idx} className="bg-[#081321] px-3 py-2.5">
            <SkeletonBlock className="h-3 w-24" />
            <SkeletonBlock className="mt-3 h-6 w-16" />
          </div>
        ))}
      </div>
      <SkeletonBlock className="mt-2.5 h-28 w-full" />
    </section>
  );
}

function MemberPortfolioPanel({
  portfolio,
  unavailable,
  loading,
  locked,
  selectedLookbackDays,
  lookbackLinks,
  simulatedTradesCount,
}: {
  portfolio: MemberPortfolioPerformance | null;
  unavailable: boolean;
  loading: boolean;
  locked: boolean;
  selectedLookbackDays: number;
  lookbackLinks: Array<{ label: string; value: number; href: string }>;
  simulatedTradesCount: number | null;
}) {
  if (loading) return <PortfolioSkeleton />;
  if (locked) {
    return (
      <section id="member-performance" className={`${CARD} p-3`}>
        <SectionTitle title="Performance" detail="Premium simulation" />
        <h3 className="mt-3 text-lg font-semibold text-white">Portfolio Performance</h3>
        <p className="mt-1 text-xs uppercase tracking-[0.2em] text-emerald-300">Disclosure-lag realistic portfolio</p>
        <p className="mt-2 max-w-3xl text-sm text-white/45">
          The member profile, trade analytics, top tickers, and recent disclosures remain visible. Portfolio simulation is available with Premium and Pro.
        </p>
        <div className="mt-4 max-w-xl">
          <UpgradePrompt
            title="Unlock member portfolio simulation"
            body="Premium and Pro members can view disclosure-lag portfolio performance without limiting the rest of the member profile."
            compact={true}
          />
        </div>
      </section>
    );
  }

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
    { label: "Portfolio Return", value: pct(summary.total_return_pct), valueClass: tone(summary.total_return_pct) },
    { label: "SPY Benchmark", value: pct(summary.benchmark_return_pct), valueClass: tone(summary.benchmark_return_pct) },
    { label: "Outperformance", value: pct(summary.alpha_pct), valueClass: tone(summary.alpha_pct) },
    { label: "Win Rate", value: pct(summary.win_rate_pct), valueClass: "text-white/90" },
    { label: "Sharpe Ratio", value: decimal(summary.sharpe_ratio, 2), valueClass: "text-white/90" },
    { label: "Total Return", value: pct(summary.total_return_pct), valueClass: tone(summary.total_return_pct) },
    { label: "CAGR", value: pct(summary.cagr_pct), valueClass: tone(summary.cagr_pct) },
    { label: "Alpha", value: pct(summary.alpha_pct), valueClass: tone(summary.alpha_pct) },
    { label: "S&P Return", value: pct(summary.benchmark_return_pct), valueClass: tone(summary.benchmark_return_pct) },
    { label: "Max Drawdown", value: pct(summary.max_drawdown_pct), valueClass: tone(summary.max_drawdown_pct == null ? null : -Math.abs(summary.max_drawdown_pct)) },
    { label: "Sharpe", value: decimal(summary.sharpe_ratio, 2), valueClass: "text-white/90" },
    { label: "Simulated Trades", value: numberOrDash(simulatedTradesCount), valueClass: "text-white/90" },
    { label: "Active Tickers", value: numberOrDash(activeTickerPositionsCount), valueClass: "text-white/90" },
  ] : [];

  return (
    <section id="member-performance" className={`${CARD} p-3`}>
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <SectionTitle title="Performance" detail="Realistic disclosure lag" />
          <p className="mt-2 text-xs uppercase tracking-[0.2em] text-emerald-300">Disclosure-lag realistic portfolio</p>
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
              className={`rounded-md border px-3 py-1.5 text-xs transition-colors ${
                option.value === selectedLookbackDays
                  ? "border-sky-400/60 bg-sky-400/10 font-medium text-sky-100"
                  : "border-white/10 bg-slate-950/30 text-white/60 hover:border-emerald-300/30 hover:text-white/85"
              }`}
            >
              {option.label}
            </Link>
          ))}
        </div>
      </div>

      {unavailable ? (
        <p className="mt-4 rounded-lg border border-amber-300/15 bg-amber-300/[0.06] px-4 py-3 text-sm text-amber-100/85">
          {REFRESHING_COPY}
        </p>
      ) : !hasPersistedRun ? (
        <p className="mt-4 rounded-lg border border-white/10 bg-white/[0.03] px-4 py-3 text-sm text-slate-400">
          Portfolio simulation is not available for this lookback yet.
        </p>
      ) : (
        <>
          <div className="mt-2.5 grid grid-cols-2 gap-px overflow-hidden rounded-md border border-white/8 bg-white/8 sm:grid-cols-3 lg:grid-cols-5">
            {metrics.slice(0, 5).map((metric) => (
              <div key={metric.label} className="bg-[#081321] px-3 py-2.5">
                <p className="text-[10px] uppercase tracking-[0.14em] text-white/45">{metric.label}</p>
                <p className={`mt-1.5 text-base font-semibold leading-none tabular-nums ${metric.valueClass}`}>{metric.value}</p>
              </div>
            ))}
          </div>
          <span className="hidden">
            {metrics.slice(5).map((metric) => `${metric.label} ${metric.value}`).join(" ")}
          </span>
          {activePositionsCount != null ? (
            <p className="mt-3 text-xs text-slate-400">Active position rows at end: {numberOrDash(activePositionsCount)}</p>
          ) : null}
          {showNoActiveHoldings ? (
            <p className="mt-4 rounded-lg border border-white/10 bg-white/[0.03] px-4 py-3 text-sm text-slate-400">
              No simulated holdings were active in this window.
            </p>
          ) : showLimitedPriceHistory ? (
            <p className="mt-4 rounded-lg border border-amber-300/15 bg-amber-300/[0.06] px-4 py-3 text-sm text-amber-100/80">
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
            <p className="mt-4 rounded-lg border border-white/10 bg-white/[0.03] px-4 py-3 text-sm text-slate-400">
              Portfolio simulation is not available for this lookback yet.
            </p>
          )}
        </>
      )}
    </section>
  );
}

function MemberHoldingsPanel({
  portfolio,
  loading,
  locked,
}: {
  portfolio: MemberPortfolioPerformance | null;
  loading: boolean;
  locked: boolean;
}) {
  const diagnostics = portfolio?.warmup_diagnostics ?? null;
  const annualCount =
    portfolio?.opening_holdings_from_annual_disclosure ??
    diagnostics?.opening_holdings_from_annual_disclosure ??
    0;
  const annualSymbols =
    portfolio?.annual_disclosure_opening_positions_symbols ??
    diagnostics?.annual_disclosure_opening_positions_symbols ??
    [];
  const annualValue =
    portfolio?.annual_disclosure_opening_positions_value ??
    diagnostics?.annual_disclosure_opening_positions_value ??
    null;
  const estimatedSymbols =
    portfolio?.estimated_opening_positions_symbols ??
    diagnostics?.estimated_opening_positions_symbols ??
    [];
  const symbols = annualSymbols.length > 0 ? annualSymbols : estimatedSymbols;

  return (
    <section id="member-holdings" className={`${CARD} scroll-mt-6 p-3`}>
      <SectionTitle title="Estimated Holdings" detail="Annual disclosures" />
      {loading ? (
        <div className="mt-3 grid grid-cols-2 gap-2 md:grid-cols-4">
          {Array.from({ length: 4 }).map((_, idx) => <SkeletonBlock key={idx} className="h-14 w-full" />)}
        </div>
      ) : locked ? (
        <p className="mt-3 text-sm text-slate-400">
          Holdings estimates use annual financial disclosure baselines when available and are unlocked with portfolio simulation.
        </p>
      ) : (
        <>
          <div className="mt-3 grid grid-cols-2 gap-px overflow-hidden rounded-md border border-white/8 bg-white/8 md:grid-cols-4">
            <div className="bg-[#081321] px-3 py-2.5">
              <p className="text-[9px] font-medium uppercase leading-none tracking-[0.12em] text-slate-500">Annual positions</p>
              <p className="mt-1.5 text-base font-semibold leading-none text-white tabular-nums">{numberOrDash(annualCount)}</p>
            </div>
            <div className="bg-[#081321] px-3 py-2.5">
              <p className="text-[9px] font-medium uppercase leading-none tracking-[0.12em] text-slate-500">Est. value</p>
              <p className="mt-1.5 text-base font-semibold leading-none text-white tabular-nums">{compactUSD(annualValue)}</p>
            </div>
            <div className="bg-[#081321] px-3 py-2.5">
              <p className="text-[9px] font-medium uppercase leading-none tracking-[0.12em] text-slate-500">Visible symbols</p>
              <p className="mt-1.5 text-base font-semibold leading-none text-white tabular-nums">{numberOrDash(symbols.length)}</p>
            </div>
            <div className="bg-[#081321] px-3 py-2.5">
              <p className="text-[9px] font-medium uppercase leading-none tracking-[0.12em] text-slate-500">Basis</p>
              <p className="mt-1.5 text-base font-semibold leading-none text-white">Estimated</p>
            </div>
          </div>
          <p className="mt-3 text-xs leading-5 text-slate-500">
            Holdings are estimated from annual disclosure filings and portfolio warmup diagnostics, not live brokerage positions.
          </p>
          {symbols.length > 0 ? (
            <div className="mt-2 flex flex-wrap gap-1.5">
              {symbols.slice(0, 12).map((symbol) => (
                <TickerPill key={symbol} symbol={symbol} href={tickerHref(symbol)} />
              ))}
            </div>
          ) : null}
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
  const [entitlements, setEntitlements] = useState<Entitlements>(() => entitlementsFromTierHint(storedEntitlementTier()));
  const [entitlementsLoaded, setEntitlementsLoaded] = useState(true);
  const hasInitialAnalytics = Boolean(initialAlphaSummary || initialTrades);
  const [loading, setLoading] = useState(!hasInitialAnalytics);
  const [portfolioLoading, setPortfolioLoading] = useState(false);
  const [alphaUnavailable, setAlphaUnavailable] = useState(false);
  const [portfolioUnavailable, setPortfolioUnavailable] = useState(false);
  const [tradesUnavailable, setTradesUnavailable] = useState(false);
  const canViewPortfolio = hasEntitlement(entitlements, "backtesting");

  useEffect(() => {
    let cancelled = false;
    getEntitlements(undefined, { source: "MemberAnalytics" })
      .then((nextEntitlements) => {
        if (!cancelled) setEntitlements(nextEntitlements);
      })
      .catch(() => {
        if (!cancelled) setEntitlements(defaultEntitlements);
      })
      .finally(() => {
        if (!cancelled) setEntitlementsLoaded(true);
      });

    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    const controller = new AbortController();
    let cancelled = false;
    setLoading(!hasInitialAnalytics);
    setAlphaUnavailable(false);
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

    Promise.allSettled([alphaRequest, tradesRequest]).finally(() => {
      if (!cancelled) setLoading(false);
    });

    return () => {
      cancelled = true;
      controller.abort();
    };
  }, [hasInitialAnalytics, lookbackDays, memberId]);

  useEffect(() => {
    const controller = new AbortController();
    let cancelled = false;

    if (!entitlementsLoaded) return () => undefined;
    if (!canViewPortfolio) {
      setPortfolio(null);
      setPortfolioTradeCountSummary(null);
      setPortfolioUnavailable(false);
      setPortfolioLoading(false);
      return () => undefined;
    }
    if (loading) {
      setPortfolioLoading(true);
      return () => undefined;
    }

    setPortfolioLoading(true);
    setPortfolioUnavailable(false);

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

    Promise.allSettled([portfolioRequest, portfolioTradeCountRequest]).finally(() => {
      if (!cancelled) setPortfolioLoading(false);
    });

    return () => {
      cancelled = true;
      controller.abort();
    };
  }, [canViewPortfolio, entitlementsLoaded, loading, lookbackDays, memberId, portfolioLookbackDays]);

  const recentTrades = useMemo(() => sortedTrades(trades.items), [trades.items]);
  const topTickers = useMemo(() => {
    const counts = new Map<string, { trades: number; buy: number; sell: number; value: number; latestDirection: "buy" | "sell" | null; sectors: Map<string, number> }>();
    trades.items.forEach((trade) => {
      const symbol = String(trade.symbol ?? "").trim().toUpperCase();
      if (!symbol) return;
      const direction = tradeDirection(trade.transaction_type ?? "");
      const existing = counts.get(symbol) ?? { trades: 0, buy: 0, sell: 0, value: 0, latestDirection: null, sectors: new Map<string, number>() };
      existing.trades += 1;
      existing.value += amountMid(trade) ?? 0;
      if (direction === "buy") existing.buy += 1;
      if (direction === "sell") existing.sell += 1;
      const tradeSector = sectorLabel(trade.sector);
      existing.sectors.set(tradeSector, (existing.sectors.get(tradeSector) ?? 0) + 1);
      existing.latestDirection = existing.latestDirection ?? direction;
      counts.set(symbol, existing);
    });
    if (counts.size === 0) {
      return initialTopTickers.map((ticker) => ({ symbol: ticker.symbol, trades: ticker.trades, buy: 0, sell: 0, value: null as number | null, latestDirection: null, sector: "Sector unavailable" }));
    }
    return Array.from(counts.entries())
      .sort((a, b) => b[1].trades - a[1].trades)
      .slice(0, 10)
      .map(([symbol, stats]) => {
        const sector = Array.from(stats.sectors.entries()).sort((a, b) => b[1] - a[1])[0]?.[0] ?? "Sector unavailable";
        return { symbol, ...stats, sector };
      });
  }, [initialTopTickers, trades.items]);

  const activityStats = useMemo(() => {
    let buyCount = 0;
    let sellCount = 0;
    let totalValue = 0;
    let lagTotal = 0;
    let lagCount = 0;
    const groups = new Map<string, number>();
    const months = new Map<string, { label: string; buy: number; sell: number }>();
    const cutoff = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000);
    let net30 = 0;

    for (const trade of trades.items) {
      const direction = tradeDirection(trade.transaction_type ?? "");
      const value = amountMid(trade) ?? 0;
      totalValue += value;
      if (direction === "buy") buyCount += 1;
      if (direction === "sell") sellCount += 1;
      const group = sectorLabel(trade.sector);
      groups.set(group, (groups.get(group) ?? 0) + 1);
      const rawDate = trade.report_date ?? trade.trade_date;
      const date = rawDate ? new Date(rawDate) : null;
      if (date && Number.isFinite(date.getTime())) {
        const key = `${date.getUTCFullYear()}-${String(date.getUTCMonth() + 1).padStart(2, "0")}`;
        const label = date.toLocaleDateString("en-US", { month: "short", year: "2-digit", timeZone: "UTC" });
        const bucket = months.get(key) ?? { label, buy: 0, sell: 0 };
        if (direction === "buy") bucket.buy += 1;
        if (direction === "sell") bucket.sell += 1;
        months.set(key, bucket);
        if (date >= cutoff) {
          if (direction === "buy") net30 += value;
          if (direction === "sell") net30 -= value;
        }
      }
      const lag = daysBetween(trade.report_date, trade.trade_date);
      if (lag != null) {
        lagTotal += lag;
        lagCount += 1;
      }
    }

    const colors = ["#34d399", "#60a5fa", "#a78bfa", "#f59e0b", "#fb7185", "#94a3b8"];
    const sectorRows = Array.from(groups.entries())
      .sort((a, b) => b[1] - a[1])
      .slice(0, 6)
      .map(([label, value], index) => ({ label, value, color: colors[index % colors.length] }));

    return {
      buyCount,
      sellCount,
      totalCount: trades.items.length,
      totalValue,
      net30,
      avgLag: lagCount > 0 ? lagTotal / lagCount : null,
      sectorRows,
      buckets: Array.from(months.entries()).sort((a, b) => a[0].localeCompare(b[0])).slice(-12).map((entry) => entry[1]),
    };
  }, [trades.items]);

  const analyticsStats = [
    { label: "Disclosures", value: numberOrDash(activityStats.totalCount), sub: `Rank inputs: ${numberOrDash(alphaSummary.trades_analyzed)}` },
    { label: "Buy / Sell Ratio", value: `${activityStats.buyCount} / ${activityStats.sellCount}`, sub: activityStats.buyCount >= activityStats.sellCount ? "Net buyer" : "Net seller", valueClass: activityStats.buyCount >= activityStats.sellCount ? "text-emerald-300" : "text-rose-300" },
    { label: "Most Active Sector", value: activityStats.sectorRows[0]?.label ?? "—", sub: `${Math.round(((activityStats.sectorRows[0]?.value ?? 0) / Math.max(1, activityStats.totalCount)) * 100)}% of activity` },
    { label: "Top Ticker", value: topTickers[0]?.symbol ?? "—", sub: `${topTickers[0]?.trades ?? 0} disclosures` },
    { label: "Est. Value", value: compactUSD(activityStats.totalValue), sub: `${lookbackDays}D disclosed range` },
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

  const changeRows = recentTrades.slice(0, 5).map((trade) => {
    const direction = tradeDirection(trade.transaction_type);
    const symbol = trade.symbol ?? trade.security_name ?? "Security";
    return {
      key: `${trade.event_id ?? trade.id}`,
      tone: direction === "sell" ? "border-rose-400/20 bg-rose-400/10 text-rose-300" : direction === "buy" ? "border-emerald-400/20 bg-emerald-400/10 text-emerald-300" : "border-slate-400/20 bg-slate-400/10 text-slate-300",
      title: `${formatTransactionLabel(trade.transaction_type)} in ${symbol}`,
      body: rangeLabel(trade.amount_range_min, trade.amount_range_max),
      date: formatDateShort(trade.report_date ?? trade.trade_date),
    };
  });
  const watchRows = [
    topTickers[0] ? `Position size changes in ${topTickers[0].symbol}` : "New disclosed ticker concentration",
    activityStats.avgLag != null ? `Disclosure lag currently averages ${Math.round(activityStats.avgLag)} days` : "Disclosure lag becomes available as filings update",
    activityStats.buyCount >= activityStats.sellCount ? "Continuation of net buying pattern" : "Continuation of net selling pattern",
    alphaSummary.avg_alpha_pct != null ? `Average alpha trend: ${pct(alphaSummary.avg_alpha_pct)}` : "Outcome analytics as trades become scorable",
    tradesUnavailable ? "Recent activity refresh status" : "Fresh disclosures in the next filing batch",
  ];

  return (
    <div className="space-y-3">
      <div className="grid items-stretch gap-3 xl:grid-cols-[minmax(0,1.4fr)_minmax(260px,0.75fr)_minmax(260px,0.78fr)] xl:[&>section]:h-[158px]">
        <section className={`${CARD} p-3`}>
          <SectionTitle title="Member Activity Summary" detail={`${lookbackDays}D`} />
          <p className="mt-2 truncate text-xs text-slate-500">One of the most active disclosed traders in Congress, summarized from public filings.</p>
          {loading ? <AnalyticsStatsSkeleton /> : <MetricGrid metrics={analyticsStats} />}
          <div className="mt-2 flex flex-wrap items-center gap-x-3 gap-y-1 text-[10px] leading-none text-white/45">
            <span>Scored trades: {alphaSummary.trades_analyzed ?? 0}</span>
            <span>Net flow 30D {activityStats.net30 < 0 ? `-${compactUSD(Math.abs(activityStats.net30))}` : compactUSD(activityStats.net30)}</span>
            {alphaUnavailable && !hasAlphaMetrics ? <span>{REFRESHING_COPY}</span> : null}
          </div>
          <span className="hidden">Trade Outcome Analytics Compact metrics from individually scored disclosures.</span>
        </section>

        <section className={`${CARD} p-3`}>
          <SectionTitle title="Activity by Sector" detail={`${lookbackDays}D`} />
          <ActivityDonut rows={activityStats.sectorRows} />
        </section>

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
      </div>

      <div className="grid items-stretch gap-3 xl:grid-cols-[minmax(0,1.4fr)_minmax(260px,0.75fr)_minmax(260px,0.78fr)] xl:[&>section]:h-[210px]">
        <section className={`${CARD} p-3`}>
          <SectionTitle title="Top Convictions" detail="By disclosed activity" />
          <div className="mt-3 overflow-x-auto">
            <table className="w-full min-w-[560px] text-left text-xs">
              <thead className="text-[10px] uppercase tracking-[0.14em] text-slate-500">
                <tr>
                  <th className="pb-3 font-medium">Ticker</th>
                  <th className="pb-3 font-medium">Category</th>
                  <th className="pb-3 font-medium">Disclosures</th>
                  <th className="pb-3 font-medium">Total est. range</th>
                  <th className="pb-3 font-medium">Activity</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-white/8">
                {topTickers.slice(0, 6).map((ticker) => {
                  const activity = ticker.latestDirection === "sell" ? "Reduced" : ticker.latestDirection === "buy" ? "Increased" : "Observed";
                  return (
                    <tr key={ticker.symbol}>
                      <td className="py-2"><TickerPill symbol={ticker.symbol} href={tickerHref(ticker.symbol)} /></td>
                      <td className="py-2 text-slate-300">{ticker.sector}</td>
                      <td className="py-2 text-slate-300 tabular-nums">{ticker.trades}</td>
                      <td className="py-2 text-slate-300">{compactUSD(ticker.value)}</td>
                      <td className={`py-2 ${ticker.latestDirection === "sell" ? "text-rose-300" : ticker.latestDirection === "buy" ? "text-emerald-300" : "text-slate-400"}`}>{activity}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
            {topTickers.length === 0 ? <p className="py-8 text-sm text-slate-500">No ticker concentration yet.</p> : null}
          </div>
        </section>

        <section id="member-activity-trend" className={`${CARD} scroll-mt-6 p-3`}>
          <SectionTitle title="Activity Trend" detail="Disclosures" />
          <div className="mt-3 flex justify-end gap-4 text-[11px] text-slate-500">
            <span><span className="mr-1 inline-block h-2 w-2 rounded-full bg-emerald-400" />Buys</span>
            <span><span className="mr-1 inline-block h-2 w-2 rounded-full bg-rose-400" />Sells</span>
          </div>
          <MiniBars buckets={activityStats.buckets} />
        </section>

        <section className={`${CARD} p-3`}>
          <SectionTitle title="What to Watch Next" />
          <div className="mt-2 space-y-1.5">
            {watchRows.map((row) => (
              <div key={row} className="flex items-center gap-2 border-b border-white/8 pb-2 last:border-0 last:pb-0">
                <span className="grid h-5 w-5 shrink-0 place-items-center rounded-md border border-emerald-400/20 bg-emerald-400/10 text-[10px] text-emerald-300">•</span>
                <p className="text-xs leading-tight text-slate-300">{row}</p>
              </div>
            ))}
          </div>
        </section>
      </div>

      <div className="grid items-start gap-3 xl:grid-cols-[minmax(0,1fr)_minmax(420px,0.72fr)]">
        <MemberPortfolioPanel
          portfolio={portfolio}
          unavailable={portfolioUnavailable}
          loading={!entitlementsLoaded || portfolioLoading}
          locked={entitlementsLoaded && !canViewPortfolio}
          selectedLookbackDays={portfolioLookbackDays}
          lookbackLinks={portfolioLookbackLinks}
          simulatedTradesCount={simulatedTradesCount}
        />

        <section id="recent-trades" className={`${CARD} min-w-0 p-3 scroll-mt-6`}>
          <SectionTitle title="Recent Disclosed Trades" detail="View all" />
          <div className="mt-3 overflow-x-auto">
            {loading ? (
              <div className="space-y-2">
                {Array.from({ length: 6 }).map((_, idx) => <SkeletonBlock key={idx} className="h-9 w-full" />)}
              </div>
            ) : tradesUnavailable && trades.items.length === 0 ? (
              <p className="rounded-lg border border-white/10 bg-white/[0.03] px-4 py-3 text-sm text-slate-400">
                Recent activity is refreshing from disclosed trades.
              </p>
            ) : recentTrades.length === 0 ? (
              <p className="text-sm text-slate-400">No recent trades for this member.</p>
            ) : (
              <table className="w-full min-w-[620px] text-left text-sm">
                <thead className="text-[10px] uppercase tracking-[0.14em] text-slate-500">
                  <tr>
                    <th className="pb-3 font-medium">Date disclosed</th>
                    <th className="pb-3 font-medium">Ticker</th>
                    <th className="pb-3 font-medium">Type</th>
                    <th className="pb-3 font-medium">Asset type</th>
                    <th className="pb-3 font-medium">Est. range</th>
                    <th className="pb-3 font-medium">Gain/Loss</th>
                    <th className="pb-3 font-medium">Signal</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-white/8">
                  {recentTrades.map((trade) => {
                    const signal = resolveSmartSignalValue(trade as Record<string, unknown>);
                    const sideLabel = formatTransactionLabel(trade.transaction_type ?? "") ?? "Trade";
                    const sideTone = transactionTone(trade.transaction_type ?? "");
                    return (
                      <tr key={trade.event_id ?? trade.id}>
                        <td className="py-2.5 text-slate-300">{trade.report_date ? formatDateShort(trade.report_date) : "—"}</td>
                        <td className="py-2.5"><TickerPill symbol={trade.symbol ?? "—"} href={trade.symbol ? tickerHref(trade.symbol) ?? undefined : undefined} /></td>
                        <td className="py-2.5"><Badge tone={sideTone}>{sideLabel}</Badge></td>
                        <td className="py-2.5 text-slate-300">{trade.asset_class || trade.instrument_type || "Security"}</td>
                        <td className="py-2.5 text-slate-300">
                          {rangeLabel(trade.amount_range_min, trade.amount_range_max)}
                          <span className="hidden">
                            {trade.estimated_price != null ? `$${trade.estimated_price.toFixed(2)}` : ""}
                            {trade.current_price != null ? `$${trade.current_price.toFixed(2)}` : ""}
                            {trade.pnl_source ?? ""}
                          </span>
                        </td>
                        <td className={`py-2.5 font-medium ${tone(trade.pnl_pct)}`}>{pct(trade.pnl_pct)}</td>
                        <td className="py-2.5">
                          {signal.score != null && signal.band ? (
                            <SmartSignalPill score={signal.score} band={signal.band} size="compact" />
                          ) : (
                            <span className="text-[11px] text-slate-500">No signal</span>
                          )}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            )}
          </div>
          <p className="mt-3 text-xs text-slate-500">
            Disclosures are reported by public congressional filing feeds. Full underlying history remains accessible in this table.
          </p>
        </section>
      </div>

      <MemberHoldingsPanel
        portfolio={portfolio}
        loading={!entitlementsLoaded || portfolioLoading}
        locked={entitlementsLoaded && !canViewPortfolio}
      />
    </div>
  );
}
