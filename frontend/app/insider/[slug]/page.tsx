import Link from "next/link";
import { notFound, redirect } from "next/navigation";
import { Suspense } from "react";
import {
  getInsiderAlphaSummary,
  getInsiderSummary,
  getInsiderTopTickers,
  getInsiderTrades,
} from "@/lib/api";
import { Badge, type BadgeTone } from "@/components/Badge";
import {
  cardClassName,
  compactInteractiveSurfaceClassName,
  ghostButtonClassName,
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
import { SkeletonBlock } from "@/components/ui/LoadingSkeleton";

type Props = {
  params: Promise<{ slug: string }>;
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
};

type Lookback = "30" | "90" | "365";

type ChartMetric = "return" | "alpha";

function one(sp: Record<string, string | string[] | undefined>, key: string): string {
  const value = sp[key];
  return typeof value === "string" ? value : "";
}

function clampLookback(v: string): Lookback {
  return v === "30" || v === "90" || v === "365" ? v : "90";
}

function chartMetricFromParams(sp: Record<string, string | string[] | undefined>): ChartMetric {
  const metric = one(sp, "am");
  return metric === "alpha" ? "alpha" : "return";
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

function resolveTradeText(trade: Record<string, unknown>, ...keys: string[]): string | null {
  for (const key of keys) {
    const value = trade[key];
    if (typeof value === "string" && value.trim()) return value.trim();
  }
  return null;
}

function resolveTradeNum(trade: Record<string, unknown>, ...keys: string[]): number | null {
  for (const key of keys) {
    const value = parseNum(trade[key]);
    if (value != null) return value;
  }
  return null;
}

function smartSignalFromTrade(trade: Record<string, unknown>): { label: string; tone: BadgeTone } | null {
  const smartScore = resolveTradeNum(trade, "smart_score", "smartScore");
  const smartBandRaw = resolveTradeText(trade, "smart_band", "smartBand");
  const smartBand = smartBandRaw?.toLowerCase() ?? null;
  if (smartScore !== null) {
    if (smartBand === "strong") return { label: `Smart ${Math.round(smartScore)}`, tone: "pos" };
    if (smartBand === "notable" || smartBand === "mild") return { label: `Smart ${Math.round(smartScore)}`, tone: "neutral" };
    return { label: `Smart ${Math.round(smartScore)}`, tone: "neg" };
  }
  if (smartBand) {
    if (smartBand === "strong") return { label: "Smart Strong", tone: "pos" };
    if (smartBand === "notable" || smartBand === "mild") return { label: "Smart Notable", tone: "neutral" };
    return { label: `Smart ${smartBandRaw}`, tone: "neg" };
  }
  return null;
}

function hrefWithParams(name: string | null, reportingCik: string, lookback: Lookback, chartMetric: ChartMetric): string {
  const query = new URLSearchParams();
  query.set("lookback", lookback);
  if (chartMetric !== "return") query.set("am", chartMetric);
  const slug = insiderSlug(name, reportingCik) ?? reportingCik;
  return `/insider/${encodeURIComponent(slug)}?${query.toString()}`;
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

export default async function InsiderPage({ params, searchParams }: Props) {
  const { slug } = await params;
  const reportingCik = reportingCikFromInsiderSlug(slug);
  if (!reportingCik) notFound();
  const sp = (await searchParams) ?? {};
  const lookback = clampLookback(one(sp, "lookback"));
  const chartMetric = chartMetricFromParams(sp);

  const summary = await getInsiderSummary(reportingCik, Number(lookback));
  const resolvedInsiderName = getInsiderDisplayName(summary.insider_name);
  const fallbackSlugName = insiderDisplayNameFromSlug(slug);
  const insiderName = getInsiderDisplayName(resolvedInsiderName, fallbackSlugName) ?? "Unknown Insider";
  const canonicalSlug = insiderSlug(resolvedInsiderName, reportingCik) ?? reportingCik;

  if (shouldRedirectToCanonicalInsiderSlug(slug, canonicalSlug)) {
    const query = new URLSearchParams();
    if (lookback !== "90") query.set("lookback", lookback);
    if (chartMetric !== "return") query.set("am", chartMetric);
    const suffix = query.toString();
    redirect(`/insider/${encodeURIComponent(canonicalSlug)}${suffix ? `?${suffix}` : ""}`);
  }

  const [alphaSummary, trades] = await Promise.all([
    getInsiderAlphaSummary(reportingCik, { lookback_days: Number(lookback) }),
    getInsiderTrades(reportingCik, Number(lookback), 50),
  ]);
  const topTickersPromise = getInsiderTopTickers(reportingCik, Number(lookback), 10);

  const roleText = summary.primary_role ?? "Role unavailable";
  const companyText = summary.primary_company_name ?? "Company unavailable";

  const analyticsStats = [
    { label: "Trades Analyzed", value: numberOrDash(alphaSummary.trades_analyzed), valueClass: "text-white" },
    { label: "Avg Return", value: pct(alphaSummary.avg_return_pct), valueClass: tone(alphaSummary.avg_return_pct) },
    { label: "Avg Alpha", value: pct(alphaSummary.avg_alpha_pct), valueClass: tone(alphaSummary.avg_alpha_pct) },
    { label: "Win Rate", value: pct0(alphaSummary.win_rate), valueClass: tone(alphaSummary.win_rate == null ? null : (alphaSummary.win_rate - 0.5) * 100) },
    { label: "Avg Holding Days", value: numberOrDash(alphaSummary.avg_holding_days), valueClass: "text-white/90" },
  ];

  const memberSeries = alphaSummary.member_series ?? alphaSummary.performance_series ?? [];
  const benchmarkSeries = alphaSummary.benchmark_series ?? [];
  const chartHasEnoughTrades = memberSeries.filter((point) => typeof point.return_pct === "number").length >= 2;

  return (
    <div className="space-y-6">
      <section className={cardClassName}>
        <div className="flex flex-wrap items-start justify-between gap-4">
          <div>
            <p className="text-xs font-semibold uppercase tracking-[0.3em] text-emerald-300">Insider profile</p>
            <h1 className="mt-1 text-3xl font-semibold text-white">{insiderName}</h1>
            <div className="mt-2 flex flex-wrap gap-2 text-xs text-slate-400">
              <span className="rounded-full border border-white/10 bg-slate-900/60 px-2.5 py-1">{companyText}</span>
              <Badge tone="neutral">{roleText}</Badge>
            </div>
          </div>
          <Link href="/" className={ghostButtonClassName}>Back to feed</Link>
        </div>
      </section>

      <section className={cardClassName}>
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div>
            <h2 className="text-lg font-semibold text-white">Insider Alpha Analytics</h2>
            <p className="mt-1 text-sm text-white/45">Risk-adjusted outcomes for insider transactions in this lookback window.</p>
          </div>
          <div className="flex flex-wrap gap-2">
            {(["30", "90", "365"] as const).map((value) => (
              <Link
                key={value}
                href={hrefWithParams(insiderName, reportingCik, value, chartMetric)}
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
          <div className="flex items-start justify-between gap-3">
            <div>
              <h3 className="text-sm font-semibold uppercase tracking-[0.14em] text-white/70">Performance Curve</h3>
              <p className="mt-1 text-[11px] text-white/40">Insider trade outcomes over time.</p>
            </div>
            <div className="flex items-center gap-2 text-xs">
              <Link
                href={hrefWithParams(insiderName, reportingCik, lookback, "return")}
                className={`rounded-full border px-2.5 py-1 ${
                  chartMetric === "return"
                    ? "border-white/30 bg-white/[0.07] text-white"
                    : "border-white/10 text-white/55 hover:text-white/80"
                }`}
              >
                Return
              </Link>
              <Link
                href={hrefWithParams(insiderName, reportingCik, lookback, "alpha")}
                className={`rounded-full border px-2.5 py-1 ${
                  chartMetric === "alpha"
                    ? "border-white/30 bg-white/[0.07] text-white"
                    : "border-white/10 text-white/55 hover:text-white/80"
                }`}
              >
                Alpha
              </Link>
            </div>
          </div>

          {!chartHasEnoughTrades ? (
            <p className="mt-3 text-sm text-slate-400">Not enough scored trades to render a performance chart.</p>
          ) : (
            <PerformanceChart
              memberSeries={memberSeries}
              benchmarkSeries={benchmarkSeries}
              metric={chartMetric}
              benchmarkLabel={alphaSummary.benchmark_symbol ?? "Benchmark"}
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
                          <Link href={tickerHref(trade.symbol)!} className={`${tickerLinkClassName} truncate`}>
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
          <div className="mt-4 space-y-3">
            {trades.items.length === 0 ? (
              <p className="text-sm text-slate-400">No insider trades in the selected window.</p>
            ) : (
              trades.items.map((trade) => {
                const tradeRecord = trade as Record<string, unknown>;
                const tradeType = resolveTradeText(tradeRecord, "trade_type", "tradeType") ?? "";
                const sideLabel = formatTransactionLabel(tradeType) ?? "Trade";
                const sideTone = transactionTone(tradeType);
                const signal = smartSignalFromTrade(tradeRecord);
                const companyName = resolveTradeText(tradeRecord, "company_name", "companyName") ?? "—";
                const transactionDate = resolveTradeText(tradeRecord, "transaction_date", "trade_date", "transactionDate", "tradeDate");
                const price = resolveTradeNum(tradeRecord, "price");
                const tradeValue = resolveTradeNum(tradeRecord, "trade_value", "tradeValue", "amount_max", "amount_min", "amountMax", "amountMin");
                const pnl = resolveTradeNum(tradeRecord, "pnl_pct", "pnlPct", "pnl");

                return (
                  <div
                    key={trade.external_id ?? `${trade.event_id}`}
                    className="relative overflow-hidden rounded-3xl border border-white/5 bg-slate-900/70 p-5 shadow-card"
                  >
                    <div className="grid min-w-0 gap-4 lg:grid-cols-[minmax(280px,1fr)_minmax(110px,.6fr)_minmax(90px,.5fr)_minmax(50px,.55fr)_minmax(100px,.65fr)_minmax(90px,.5fr)_minmax(120px,.5fr)] lg:items-center">
                      <div className="min-w-0">
                        <div className="flex min-w-0 items-center gap-3">
                          {trade.symbol ? (
                            <TickerPill symbol={trade.symbol} href={tickerHref(trade.symbol) ?? undefined} className="inline-flex shrink-0" />
                          ) : (
                            <TickerPill symbol="—" />
                          )}
                          <div className="min-w-0">
                            <p className="truncate font-semibold text-white">{companyName}</p>
                          </div>
                        </div>
                      </div>

                      <div className="text-xs leading-5 text-slate-400">
                        <div>Trade date</div>
                        <div className="mt-1 text-sm text-slate-200">{transactionDate ? formatDateShort(transactionDate) : "—"}</div>
                      </div>

                      <div className="text-xs leading-5 text-slate-400">
                        <div>Side</div>
                        <div className="mt-1">
                          <Badge tone={sideTone}>{sideLabel}</Badge>
                        </div>
                      </div>

                      <div className="text-xs leading-5 text-slate-400">
                        <div>Price</div>
                        <div className="mt-1 text-sm tabular-nums text-slate-200">{price !== null ? formatMoney(price) : "—"}</div>
                      </div>

                      <div className="text-right text-xs text-slate-400">
                        <div>Trade value</div>
                        <div className="mt-1 text-base font-semibold tabular-nums text-white">{tradeValue !== null ? formatMoney(tradeValue) : "—"}</div>
                      </div>

                      <div className="text-right text-xs text-slate-400">
                        <div>PnL</div>
                        <div className={`mt-1 text-sm font-semibold tabular-nums ${pnl !== null ? pnlClass(pnl) : "text-slate-400"}`}>{pnl !== null ? formatPnl(pnl) : "—"}</div>
                      </div>

                      <div className="text-right text-xs text-slate-400">
                        <div>Signal</div>
                        <div className="mt-1 flex justify-end">{signal ? <Badge tone={signal.tone}>{signal.label}</Badge> : "—"}</div>
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
