import Link from "next/link";
import { notFound, redirect } from "next/navigation";
import { Suspense } from "react";
import {
  getInsiderAlphaSummary,
  getInsiderSummary,
  getInsiderStockChart,
  getInsiderTopTickers,
  getInsiderTrades,
} from "@/lib/api";
import { Badge } from "@/components/Badge";
import {
  cardClassName,
  compactInteractiveSurfaceClassName,
  ghostButtonClassName,
  subtlePrimaryButtonClassName,
  tickerLinkClassName,
} from "@/lib/styles";
import { formatDateShort, formatTransactionLabel, transactionTone } from "@/lib/format";
import {
  getInsiderDisplayName,
  insiderDisplayNameFromSlug,
  insiderSlug,
  reportingCikFromInsiderSlug,
  shouldRedirectToCanonicalInsiderSlug,
} from "@/lib/insider";
import { tickerHref } from "@/lib/ticker";
import { TickerPill } from "@/components/ui/TickerPill";
import { PerformanceChart } from "@/components/member/PerformanceChart";
import { PremiumTickerChart, PremiumTickerChartSkeleton } from "@/components/ticker/PremiumTickerChart";
import { SkeletonBlock } from "@/components/ui/LoadingSkeleton";
import { SmartSignalPill } from "@/components/ui/SmartSignalPill";
import { resolveInsiderActivityDisplay } from "@/lib/tradeDisplay";

type Props = {
  params: Promise<{ slug: string }>;
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
};

type Lookback = "30" | "90" | "180" | "365" | "1095";
const LOOKBACK_OPTIONS = [
  { label: "30D", value: "30" },
  { label: "90D", value: "90" },
  { label: "180D", value: "180" },
  { label: "1Y", value: "365" },
  { label: "3Y", value: "1095" },
] as const satisfies readonly { label: string; value: Lookback }[];

type ChartMetric = "return" | "alpha";
type ChartMode = "performance" | "stock";

function one(sp: Record<string, string | string[] | undefined>, key: string): string {
  const value = sp[key];
  return typeof value === "string" ? value : "";
}

function clampLookback(v: string): Lookback {
  return LOOKBACK_OPTIONS.some((option) => option.value === v) ? (v as Lookback) : "90";
}

function chartMetricFromParams(sp: Record<string, string | string[] | undefined>): ChartMetric {
  const metric = one(sp, "am");
  return metric === "alpha" ? "alpha" : "return";
}

function chartModeFromParams(sp: Record<string, string | string[] | undefined>): ChartMode {
  return one(sp, "chart") === "stock" ? "stock" : "performance";
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

function hrefWithParams(
  name: string | null,
  reportingCik: string,
  lookback: Lookback,
  chartMetric: ChartMetric,
  issuer?: string,
  chartMode: ChartMode = "performance",
): string {
  const query = new URLSearchParams();
  query.set("lookback", lookback);
  query.set("chart", chartMode);
  if (chartMetric !== "return") query.set("am", chartMetric);
  if (issuer) query.set("issuer", issuer);
  const slug = insiderSlug(name, reportingCik) ?? reportingCik;
  return `/insider/${encodeURIComponent(slug)}?${query.toString()}`;
}

function buildInsiderBacktestHref(reportingCik: string, lookbackDays: number) {
  const query = new URLSearchParams({
    strategy: "insider",
    scope: "insider",
    insider_cik: reportingCik,
    lookback_days: String(lookbackDays),
    hold_days: "90",
    benchmark: "^GSPC",
  });
  return `/backtesting?${query.toString()}`;
}

function DeferredTopTickersSkeleton() {
  return (
    <div className={`${cardClassName} w-full`}>
      <h2 className="text-lg font-semibold text-white">Top tickers</h2>
      <div className="mt-4 space-y-2">
        {Array.from({ length: 5 }).map((_, idx) => (
          <div key={idx} className="rounded-2xl border border-white/10 bg-white/[0.03] px-3 py-2">
            <SkeletonBlock className="h-4 w-full" />
          </div>
        ))}
      </div>
    </div>
  );
}

async function DeferredTopTickers({
  topTickersPromise,
}: {
  topTickersPromise: Promise<Awaited<ReturnType<typeof getInsiderTopTickers>>>;
}) {
  const topTickers = await topTickersPromise;

  return (
    <div className={`${cardClassName} w-full`}>
      <h2 className="text-lg font-semibold text-white">Top tickers</h2>
      <div className="mt-4 space-y-2">
        {topTickers.items.length === 0 ? (
          <p className="text-sm text-slate-400">No ticker concentration yet.</p>
        ) : (
          topTickers.items.map((ticker) => (
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

async function DeferredCompanyStockChart({
  stockChartPromise,
}: {
  stockChartPromise: Promise<Awaited<ReturnType<typeof getInsiderStockChart>> | null>;
}) {
  const bundle = await stockChartPromise;
  const symbol = bundle?.symbol ?? "Company";

  return (
    <PremiumTickerChart
      bundle={bundle}
      eyebrow="Company stock"
      title={bundle?.symbol ? `${symbol} Stock Chart` : "Company Stock Chart"}
      subtitle="Showing this insider's disclosed buys and sells only."
      allowedMarkerKinds={["insider"]}
      showMarkerControls={false}
      emptyTitle="No company stock chart is available for this insider yet."
      emptyMessage="The chart will appear once this insider has a valid issuer symbol and daily price history."
    />
  );
}

export default async function InsiderPage({ params, searchParams }: Props) {
  const { slug } = await params;
  const reportingCik = reportingCikFromInsiderSlug(slug);
  if (!reportingCik) notFound();
  const sp = (await searchParams) ?? {};
  const lookback = clampLookback(one(sp, "lookback"));
  const chartMetric = chartMetricFromParams(sp);
  const chartMode = chartModeFromParams(sp);
  const issuer = one(sp, "issuer").trim().toUpperCase();

  const lookbackDays = Number(lookback);
  const summaryPromise = getInsiderSummary(reportingCik, lookbackDays, issuer || undefined, { source: "InsiderSummary" });
  const summary = await summaryPromise;

  const resolvedInsiderName = getInsiderDisplayName(summary.insider_name);
  const fallbackSlugName = insiderDisplayNameFromSlug(slug);
  const insiderName = getInsiderDisplayName(resolvedInsiderName, fallbackSlugName) ?? "Unknown Insider";
  const canonicalSlug = insiderSlug(resolvedInsiderName, reportingCik) ?? reportingCik;

  if (shouldRedirectToCanonicalInsiderSlug(slug, canonicalSlug)) {
    const query = new URLSearchParams();
    if (lookback !== "90") query.set("lookback", lookback);
    query.set("chart", chartMode);
    if (chartMetric !== "return") query.set("am", chartMetric);
    if (issuer) query.set("issuer", issuer);
    const suffix = query.toString();
    redirect(`/insider/${encodeURIComponent(canonicalSlug)}${suffix ? `?${suffix}` : ""}`);
  }

  const alphaSummary = await getInsiderAlphaSummary(reportingCik, {
    lookback_days: lookbackDays,
    issuer: issuer || undefined,
    source: "InsiderAlphaSummary",
  });
  const trades = await getInsiderTrades(reportingCik, lookbackDays, 50, issuer || undefined, { source: "InsiderTrades" });
  const topTickersPromise = getInsiderTopTickers(reportingCik, lookbackDays, 10, issuer || undefined, {
    source: "InsiderTopTickers",
  });
  const stockSymbol = issuer || summary.primary_symbol || undefined;
  const stockChartPromise =
    chartMode === "stock"
      ? topTickersPromise
          .then(() =>
            getInsiderStockChart(reportingCik, {
              lookback_days: lookbackDays,
              symbol: stockSymbol,
              source: "InsiderStockChart",
            }),
          )
          .catch((error) => {
            console.error("[insider-stock-chart] bundle unavailable", error);
            return null;
          })
      : Promise.resolve(null);
  const issuerOptions = Array.from(
    new Set(trades.items.map((trade) => trade.symbol).filter((symbol): symbol is string => Boolean(symbol))),
  );

  const roleText = summary.primary_role ?? "Role unavailable";
  const companyText = summary.primary_company_name ?? "Company unavailable";

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

  return (
    <div className="space-y-6">
      <section className={cardClassName}>
        <div className="flex flex-wrap items-start justify-between gap-4">
          <div>
            <p className="text-xs font-semibold uppercase tracking-[0.3em] text-emerald-300">Insider profile</p>
            <h1 className="mt-1 text-3xl font-semibold text-white">
              {issuer && companyText !== "Company unavailable" ? `${insiderName} / ${companyText}` : insiderName}
            </h1>
            <div className="mt-2 flex flex-wrap gap-2 text-xs text-slate-400">
              <span className="rounded-full border border-white/10 bg-slate-900/60 px-2.5 py-1">{companyText}</span>
              <Badge tone="neutral">{roleText}</Badge>
            </div>
          </div>
          <div className="flex flex-wrap items-center gap-2">
            <Link href={buildInsiderBacktestHref(reportingCik, lookbackDays)} prefetch={false} className={subtlePrimaryButtonClassName}>
              Backtest following this insider
            </Link>
            <Link href="/" className={ghostButtonClassName}>Back to feed</Link>
          </div>
        </div>
      </section>

      <section className={cardClassName}>
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div>
            <h2 className="text-lg font-semibold text-white">Insider Alpha Analytics</h2>
            <p className="mt-1 text-sm text-white/45">Average trade metrics summarize scored disclosures individually. Backtests simulate portfolio allocation over time.</p>
          </div>
          <div className="flex flex-wrap gap-2">
            {LOOKBACK_OPTIONS.map((option) => (
              <Link
                key={option.value}
                href={hrefWithParams(insiderName, reportingCik, option.value, chartMetric, issuer || undefined, chartMode)}
                prefetch={false}
                className={`rounded-full border px-3 py-1 text-xs font-semibold ${
                  lookback === option.value
                    ? "border-emerald-400/40 bg-emerald-400/10 text-emerald-200"
                    : "border-white/10 bg-slate-900/60 text-slate-300"
                }`}
              >
                {option.label}
              </Link>
            ))}
          </div>
        </div>

        {alphaSummary.trades_analyzed === 0 && trades.items.length > 0 ? (
          <p className="mt-4 rounded-xl border border-amber-300/25 bg-amber-400/10 px-3 py-2 text-sm text-amber-100">
            No market trades analyzed in this window. Showing recent insider activity below.
          </p>
        ) : null}

        <div className="mt-4 grid gap-3 sm:grid-cols-2 xl:grid-cols-5">
          {analyticsStats.map((stat) => (
            <div key={stat.label} className="rounded-2xl border border-white/10 bg-white/[0.03] px-4 py-3">
              <p className="text-[11px] uppercase tracking-[0.18em] text-white/45">{stat.label}</p>
              <p className={`mt-2 text-xl font-semibold tabular-nums ${stat.valueClass}`}>{stat.value}</p>
            </div>
          ))}
        </div>

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
                  href={hrefWithParams(insiderName, reportingCik, lookback, chartMetric, issuer || undefined, "performance")}
                  prefetch={false}
                  className={`rounded-full px-3 py-1 font-semibold transition ${
                    chartMode === "performance" ? "bg-white/[0.08] text-white" : "text-white/55 hover:text-white/80"
                  }`}
                >
                  Performance Curve
                </Link>
                <Link
                  href={hrefWithParams(insiderName, reportingCik, lookback, chartMetric, issuer || undefined, "stock")}
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
                      href={hrefWithParams(insiderName, reportingCik, lookback, chartMetric, symbol, chartMode)}
                      prefetch={false}
                      className={`rounded-full border px-2.5 py-1 ${
                        (issuer || summary.primary_symbol) === symbol
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
                    href={hrefWithParams(insiderName, reportingCik, lookback, "return", issuer || undefined, chartMode)}
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
                    href={hrefWithParams(insiderName, reportingCik, lookback, "alpha", issuer || undefined, chartMode)}
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
              <Suspense fallback={<PremiumTickerChartSkeleton />}>
                <DeferredCompanyStockChart stockChartPromise={stockChartPromise} />
              </Suspense>
            </div>
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
              {panel.rows.length === 0 ? (
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
          <Suspense fallback={<DeferredTopTickersSkeleton />}>
            <DeferredTopTickers topTickersPromise={topTickersPromise} />
          </Suspense>
        </div>

        <div className={`${cardClassName} w-full min-w-0`}>
          <h2 className="text-lg font-semibold text-white">Recent trades</h2>
          <p className="mt-1 text-xs text-slate-500">
            Displayed quotes are USD. Current foreign prices use spot FX where applicable; historical foreign filing prices use trade-date FX and ADR ratios when normalized. Original reported prices remain shown below.
          </p>
          <div className="mt-4 space-y-3">
            {trades.items.length === 0 ? (
              <p className="text-sm text-slate-400">No insider trades in the selected window.</p>
            ) : (
              trades.items.map((trade) => {
                const tradeRecord = trade as Record<string, unknown>;
                const display = resolveInsiderActivityDisplay(tradeRecord);
                const tradeType = display.tradeType ?? "";
                const sideLabel = formatTransactionLabel(tradeType) ?? "Trade";
                const sideTone = transactionTone(tradeType);

                return (
                  <div
                    key={trade.external_id ?? `${trade.event_id}`}
                    className="relative overflow-hidden rounded-3xl border border-white/5 bg-slate-900/70 p-5 shadow-card"
                  >
                    <div className="grid min-w-0 gap-4 lg:grid-cols-[minmax(280px,1fr)_minmax(110px,.6fr)_minmax(90px,.5fr)_minmax(50px,.55fr)_minmax(100px,.65fr)_minmax(90px,.5fr)_minmax(120px,.5fr)] lg:items-center">
                      <div className="min-w-0">
                        <div className="flex min-w-0 items-center gap-3">
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
                        <div>PnL</div>
                        <div className={`mt-1 text-sm font-semibold tabular-nums ${display.pnl !== null ? pnlClass(display.pnl) : "text-slate-400"}`}>{display.pnl !== null ? formatPnl(display.pnl) : "—"}</div>
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
        </div>
      </div>
    </div>
  );
}
