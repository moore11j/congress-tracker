import Link from "next/link";
import type { ReactNode } from "react";
import type {
  TickerAnalystConsensusResponse,
  TickerAnalystConsensusSnapshot,
  TickerAnalystConsensusTrendPoint,
} from "@/lib/api";
import { formatDateShort } from "@/lib/format";
import { SkeletonBlock } from "@/components/ui/LoadingSkeleton";

type Props = {
  data: TickerAnalystConsensusResponse | null;
  symbol: string;
};

const panelClass = "rounded-lg border border-white/10 bg-slate-950/55";
const LOADING_LABEL = "Loading";
const EMPTY_LABEL = "-";

function asNumber(value: number | null | undefined): number | null {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function formatPercent(value: number | null | undefined, options?: { signed?: boolean }) {
  const numeric = asNumber(value);
  if (numeric === null) return "-";
  const prefix = options?.signed && numeric > 0 ? "+" : "";
  return `${prefix}${new Intl.NumberFormat("en-US", { maximumFractionDigits: 1 }).format(numeric)}%`;
}

function formatNumber(value: number | null | undefined, options?: Intl.NumberFormatOptions) {
  const numeric = asNumber(value);
  if (numeric === null) return "-";
  return new Intl.NumberFormat("en-US", options).format(numeric);
}

function formatMoney(value: number | null | undefined) {
  const numeric = asNumber(value);
  if (numeric === null) return "-";
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: Math.abs(numeric) >= 100 ? 0 : 2,
  }).format(numeric);
}

function formatNumberOrLoading(value: number | null | undefined, options?: Intl.NumberFormatOptions) {
  return asNumber(value) === null ? EMPTY_LABEL : formatNumber(value, options);
}

function toneForLabel(value: string | null | undefined) {
  const normalized = (value ?? "").toLowerCase();
  if (normalized.includes("bull") || normalized.includes("buy") || normalized.includes("improving")) return "text-emerald-300";
  if (normalized.includes("bear") || normalized.includes("sell") || normalized.includes("deteriorating")) return "text-rose-300";
  if (normalized.includes("mixed") || normalized.includes("stable") || normalized.includes("hold")) return "text-amber-300";
  return "text-slate-300";
}

function toneForPercent(value: number | null | undefined) {
  const numeric = asNumber(value);
  if (numeric === null) return "text-slate-400";
  if (numeric > 0) return "text-emerald-300";
  if (numeric < 0) return "text-rose-300";
  return "text-slate-300";
}

function statusCopy(status: string | null | undefined) {
  const normalized = (status ?? "").toLowerCase();
  if (normalized === "available") return "Available";
  if (normalized === "partial") return "Partial";
  if (normalized === "stale") return "Stale";
  if (normalized === "provider_error") return "Temporarily unavailable";
  return LOADING_LABEL;
}

function DetailMetric({ label, value, tone = "text-white" }: { label: string; value: ReactNode; tone?: string }) {
  return (
    <div className={`${panelClass} p-3`}>
      <p className="text-[10px] font-semibold uppercase tracking-[0.16em] text-slate-500">{label}</p>
      <p className={`mt-1 text-lg font-semibold tabular-nums ${tone}`}>{value}</p>
    </div>
  );
}

function RatingBar({ label, value, total, className }: { label: string; value: number | null | undefined; total: number | null; className: string }) {
  const count = asNumber(value) ?? 0;
  const pct = total && total > 0 ? Math.max(0, Math.min(100, (count / total) * 100)) : 0;
  return (
    <div>
      <div className="flex items-center justify-between gap-3 text-xs">
        <span className="font-medium text-slate-300">{label}</span>
        <span className="tabular-nums text-slate-400">{formatNumber(count, { maximumFractionDigits: 0 })}</span>
      </div>
      <div className="mt-1 h-1.5 overflow-hidden rounded-full bg-white/10">
        <div className={`h-full rounded-full ${className}`} style={{ width: `${pct}%` }} />
      </div>
    </div>
  );
}

function RatingDistribution({ snapshot }: { snapshot: TickerAnalystConsensusSnapshot }) {
  const distribution = snapshot.recommendationDistribution;
  if (!distribution) return null;
  const total = asNumber(distribution.total);
  return (
    <section className={`${panelClass} p-4`}>
      <div className="flex items-center justify-between gap-3">
        <div>
          <p className="text-sm font-semibold text-white">Rating Distribution</p>
          <p className="mt-1 text-xs text-slate-400">{formatNumber(total, { maximumFractionDigits: 0 })} tracked ratings</p>
        </div>
        <span className={`text-sm font-semibold ${toneForLabel(snapshot.recommendationLabel)}`}>{snapshot.recommendationLabel ?? "Unavailable"}</span>
      </div>
      <div className="mt-4 grid gap-3">
        <RatingBar label="Strong Buy" value={distribution.strongBuy} total={total} className="bg-emerald-300" />
        <RatingBar label="Buy" value={distribution.buy} total={total} className="bg-teal-300" />
        <RatingBar label="Hold" value={distribution.hold} total={total} className="bg-amber-300" />
        <RatingBar label="Sell" value={distribution.sell} total={total} className="bg-orange-300" />
        <RatingBar label="Strong Sell" value={distribution.strongSell} total={total} className="bg-rose-300" />
      </div>
    </section>
  );
}

function PriceTargetRange({ snapshot }: { snapshot: TickerAnalystConsensusSnapshot }) {
  const range = snapshot.priceTargetRange;
  if (!range) return null;
  const low = asNumber(range.low);
  const high = asNumber(range.high);
  const consensus = asNumber(range.consensus);
  const median = asNumber(range.median);
  const current = asNumber(snapshot.currentPriceAtSnapshot);
  const values = [low, high, consensus, median, current].filter((value): value is number => value !== null);
  const min = values.length ? Math.min(...values) : null;
  const max = values.length ? Math.max(...values) : null;
  const position = (value: number | null) => {
    if (value === null || min === null || max === null || max <= min) return 50;
    return Math.max(0, Math.min(100, ((value - min) / (max - min)) * 100));
  };

  return (
    <section className={`${panelClass} p-4`}>
      <div className="flex items-start justify-between gap-3">
        <div>
          <p className="text-sm font-semibold text-white">Price Targets</p>
          <p className="mt-1 text-xs text-slate-400">{formatNumber(range.analystCount, { maximumFractionDigits: 0 })} target observations</p>
        </div>
        <p className={`text-sm font-semibold tabular-nums ${toneForPercent(snapshot.impliedUpside?.consensusPct)}`}>
          {formatPercent(snapshot.impliedUpside?.consensusPct, { signed: true })} consensus
        </p>
      </div>
      <div className="mt-5">
        <div className="relative h-2 rounded-full bg-white/10">
          <div className="absolute inset-y-0 rounded-full bg-slate-600/80" style={{ left: `${position(low)}%`, right: `${100 - position(high)}%` }} />
          {current !== null ? <span className="absolute top-1/2 h-4 w-1 -translate-y-1/2 rounded bg-white" style={{ left: `${position(current)}%` }} /> : null}
          {consensus !== null ? <span className="absolute top-1/2 h-4 w-4 -translate-x-1/2 -translate-y-1/2 rounded-full border-2 border-sky-200 bg-sky-400" style={{ left: `${position(consensus)}%` }} /> : null}
          {median !== null ? <span className="absolute top-1/2 h-3 w-3 -translate-x-1/2 -translate-y-1/2 rounded-full border border-emerald-100 bg-emerald-300" style={{ left: `${position(median)}%` }} /> : null}
        </div>
        <div className="mt-3 grid grid-cols-3 gap-2 text-xs">
          <span className="text-slate-400">Low {formatMoney(low)}</span>
          <span className="text-center text-slate-300">Current {formatMoney(current)}</span>
          <span className="text-right text-slate-400">High {formatMoney(high)}</span>
        </div>
      </div>
      <div className="mt-4 grid gap-2 sm:grid-cols-3">
        <DetailMetric label="Consensus" value={formatMoney(consensus)} />
        <DetailMetric label="Median" value={formatMoney(median)} />
        <DetailMetric label="Dispersion" value={formatPercent(snapshot.targetDispersionPct)} />
      </div>
    </section>
  );
}

function dateValue(value: string | null | undefined) {
  if (!value) return null;
  const parsed = new Date(`${value}T00:00:00Z`).getTime();
  return Number.isFinite(parsed) ? parsed : null;
}

function linePath(points: Array<{ x: number; y: number }>) {
  return points.map((point, index) => `${index === 0 ? "M" : "L"} ${point.x.toFixed(1)} ${point.y.toFixed(1)}`).join(" ");
}

function AnalystTrendChart({ points, startDate, endDate }: { points: TickerAnalystConsensusTrendPoint[]; startDate?: string | null; endDate?: string | null }) {
  const dated = points
    .map((point) => ({ ...point, time: dateValue(point.date) }))
    .filter((point): point is TickerAnalystConsensusTrendPoint & { time: number } => point.time !== null)
    .sort((a, b) => a.time - b.time);
  const targetPoints = dated.filter((point) => asNumber(point.consensusTarget) !== null);
  const sentimentPoints = dated.filter((point) => asNumber(point.weightedSentiment) !== null);
  const firstTime = dateValue(startDate) ?? dated[0]?.time ?? null;
  const lastTime = dateValue(endDate) ?? dated[dated.length - 1]?.time ?? null;
  const targetValues = targetPoints.map((point) => asNumber(point.consensusTarget)).filter((value): value is number => value !== null);
  const hasChart = firstTime !== null && lastTime !== null && lastTime > firstTime && (targetPoints.length >= 2 || sentimentPoints.length >= 2);
  const width = 720;
  const height = 268;
  const margin = { top: 24, right: 58, bottom: 42, left: 66 };
  const plotWidth = width - margin.left - margin.right;
  const plotHeight = height - margin.top - margin.bottom;
  const targetMinRaw = targetValues.length ? Math.min(...targetValues) : 0;
  const targetMaxRaw = targetValues.length ? Math.max(...targetValues) : 1;
  const targetPadding = Math.max((targetMaxRaw - targetMinRaw) * 0.12, targetMaxRaw * 0.02, 1);
  const targetMin = Math.max(0, targetMinRaw - targetPadding);
  const targetMax = targetMaxRaw + targetPadding;
  const x = (time: number) => margin.left + ((time - (firstTime ?? time)) / Math.max((lastTime ?? time) - (firstTime ?? time), 1)) * plotWidth;
  const targetY = (value: number) => margin.top + (1 - (value - targetMin) / Math.max(targetMax - targetMin, 1)) * plotHeight;
  const sentimentY = (value: number) => margin.top + (1 - (Math.max(-2, Math.min(2, value)) + 2) / 4) * plotHeight;
  const targetPath = linePath(targetPoints.map((point) => ({ x: x(point.time), y: targetY(asNumber(point.consensusTarget) ?? 0) })));
  const sentimentPath = linePath(sentimentPoints.map((point) => ({ x: x(point.time), y: sentimentY(asNumber(point.weightedSentiment) ?? 0) })));
  const latest = dated[dated.length - 1] ?? null;
  const targetTicks = [targetMin, (targetMin + targetMax) / 2, targetMax];
  const sentimentTicks = [-2, 0, 2];
  const xTicks = [
    { label: startDate ? formatDateShort(startDate) : "", value: firstTime },
    { label: endDate ? formatDateShort(endDate) : "", value: lastTime },
  ].filter((tick): tick is { label: string; value: number } => Boolean(tick.label) && tick.value !== null);

  return (
    <section className={`${panelClass} p-4`}>
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <p className="text-sm font-semibold text-white">Target & Sentiment Trend</p>
          <p className="mt-1 text-xs text-slate-400">Last 90 days</p>
        </div>
        <div className="flex flex-wrap items-center gap-3 text-xs">
          <span className="inline-flex items-center gap-2 text-sky-200"><span className="h-2 w-6 rounded-full bg-sky-400" />Consensus target</span>
          <span className="inline-flex items-center gap-2 text-amber-200"><span className="h-2 w-6 rounded-full bg-amber-300" />Sentiment</span>
        </div>
      </div>
      {hasChart ? (
        <div className="mt-4 overflow-hidden rounded-lg border border-white/10 bg-slate-950/50">
          <svg viewBox={`0 0 ${width} ${height}`} role="img" aria-label="Analyst consensus target and sentiment over the last 90 days" className="h-[268px] w-full">
            <rect x="0" y="0" width={width} height={height} fill="transparent" />
            {[0, 0.5, 1].map((step) => {
              const y = margin.top + step * plotHeight;
              return <line key={step} x1={margin.left} x2={width - margin.right} y1={y} y2={y} stroke="rgba(148,163,184,0.16)" strokeWidth="1" />;
            })}
            <line x1={margin.left} x2={margin.left} y1={margin.top} y2={height - margin.bottom} stroke="rgba(148,163,184,0.32)" />
            <line x1={width - margin.right} x2={width - margin.right} y1={margin.top} y2={height - margin.bottom} stroke="rgba(148,163,184,0.32)" />
            <line x1={margin.left} x2={width - margin.right} y1={height - margin.bottom} y2={height - margin.bottom} stroke="rgba(148,163,184,0.25)" />
            {targetTicks.map((tick) => (
              <text key={`target-${tick}`} x={margin.left - 10} y={targetY(tick) + 4} textAnchor="end" className="fill-slate-400 text-[11px] tabular-nums">
                {formatMoney(tick)}
              </text>
            ))}
            {sentimentTicks.map((tick) => (
              <text key={`sentiment-${tick}`} x={width - margin.right + 10} y={sentimentY(tick) + 4} textAnchor="start" className="fill-slate-400 text-[11px] tabular-nums">
                {formatNumber(tick, { maximumFractionDigits: 0 })}
              </text>
            ))}
            {xTicks.map((tick) => (
              <text key={tick.label} x={x(tick.value)} y={height - 15} textAnchor={tick.value === firstTime ? "start" : "end"} className="fill-slate-500 text-[11px]">
                {tick.label}
              </text>
            ))}
            <text x={margin.left} y={14} className="fill-sky-200 text-[11px] font-semibold">Target</text>
            <text x={width - margin.right} y={14} textAnchor="end" className="fill-amber-200 text-[11px] font-semibold">Sentiment</text>
            {targetPath ? <path d={targetPath} fill="none" stroke="#38bdf8" strokeWidth="3" strokeLinecap="round" strokeLinejoin="round" /> : null}
            {sentimentPath ? <path d={sentimentPath} fill="none" stroke="#fbbf24" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" /> : null}
            {targetPoints.map((point) => (
              <circle key={`target-${point.date}`} cx={x(point.time)} cy={targetY(asNumber(point.consensusTarget) ?? 0)} r="3.5" fill="#38bdf8" stroke="#0f172a" strokeWidth="1.5" />
            ))}
            {sentimentPoints.map((point) => (
              <circle key={`sentiment-${point.date}`} cx={x(point.time)} cy={sentimentY(asNumber(point.weightedSentiment) ?? 0)} r="3" fill="#fbbf24" stroke="#0f172a" strokeWidth="1.5" />
            ))}
          </svg>
        </div>
      ) : (
        <div className="mt-4 rounded-lg border border-white/10 bg-slate-950/50 p-5 text-sm text-slate-400">-</div>
      )}
      <div className="mt-3 grid gap-2 sm:grid-cols-3">
        <DetailMetric label="Current Target" value={formatMoney(latest?.consensusTarget)} tone="text-sky-100" />
        <DetailMetric label="Current Sentiment" value={formatNumber(latest?.weightedSentiment, { maximumFractionDigits: 2 })} tone="text-amber-100" />
        <DetailMetric label="Observations" value={formatNumber(dated.length, { maximumFractionDigits: 0 })} tone="text-slate-200" />
      </div>
    </section>
  );
}

function PremiumLocked() {
  return (
    <section className={`${panelClass} p-4`}>
      <div className="grid gap-4 lg:grid-cols-[minmax(0,1fr)_17rem]">
        <div>
          <p className="text-sm font-semibold text-white">Premium Detail</p>
          <p className="mt-2 text-sm leading-6 text-slate-400">
            Unlock rating distribution, target dispersion, trend changes, and upgrade/downgrade history.
          </p>
          <div className="mt-4 grid gap-2 sm:grid-cols-3">
            <SkeletonBlock className="h-16" />
            <SkeletonBlock className="h-16" />
            <SkeletonBlock className="h-16" />
          </div>
        </div>
        <div className="rounded-lg border border-emerald-300/25 bg-emerald-300/[0.06] p-4">
          <p className="font-semibold text-emerald-100">Analyst detail is Premium</p>
          <p className="mt-2 text-sm leading-6 text-slate-300">Free users get the current summary. Premium adds the full consensus trail.</p>
          <Link href="/account/billing" prefetch={false} className="mt-4 inline-flex h-10 items-center justify-center rounded-lg border border-emerald-400/40 bg-emerald-500/10 px-4 text-sm font-semibold text-emerald-200 transition hover:bg-emerald-500/20">
            Upgrade
          </Link>
        </div>
      </div>
    </section>
  );
}

export function TickerAnalystConsensusSkeleton() {
  return (
    <div className="grid gap-4">
      <SkeletonBlock className="h-32" />
      <div className="grid gap-3 sm:grid-cols-3">
        <SkeletonBlock className="h-20" />
        <SkeletonBlock className="h-20" />
        <SkeletonBlock className="h-20" />
      </div>
      <SkeletonBlock className="h-40" />
    </div>
  );
}

export function TickerAnalystConsensusTab({ data, symbol }: Props) {
  const snapshot = data?.currentSnapshot ?? null;
  const summary = data?.currentSummary ?? null;
  const interpretation = data?.interpretation ?? null;
  const availability = data?.availability?.status ?? snapshot?.availabilityStatus ?? "unavailable";
  const locked = data?.access?.detailsLocked !== false;
  const label = summary?.combinedLabel ?? snapshot?.recommendationLabel ?? interpretation?.combinedLabel ?? "Unavailable";
  const trend = summary?.trendDirection ?? interpretation?.trendDirection ?? "Unavailable";
  const consensusUpside = summary?.consensusImpliedUpsidePct ?? snapshot?.consensusImpliedUpsidePct ?? snapshot?.impliedUpside?.consensusPct ?? null;
  const medianUpside = summary?.medianImpliedUpsidePct ?? snapshot?.medianImpliedUpsidePct ?? snapshot?.impliedUpside?.medianPct ?? null;
  const freshness = data?.freshness ?? interpretation?.freshness ?? null;
  const totalRatings = asNumber(snapshot?.totalRatingCount) ?? asNumber(snapshot?.recommendationDistribution?.total);
  const hasActionWindows = Boolean(data?.gradeEventStats?.days30 || data?.gradeEventStats?.days90);

  if (!data || !snapshot) {
    return (
      <section className={`${panelClass} p-5`}>
        <p className="text-sm font-semibold text-white">Analyst consensus is loading</p>
        <p className="mt-2 text-sm leading-6 text-slate-400">
          Current analyst summary for {symbol} is not available yet. The tab will populate automatically when data is ready.
        </p>
        <p className="mt-4 text-xs font-semibold uppercase tracking-[0.16em] text-slate-500">{statusCopy(availability)}</p>
      </section>
    );
  }

  return (
    <div className="grid gap-4">
      <section className={`${panelClass} p-5`}>
        <div className="flex flex-wrap items-start justify-between gap-4">
          <div>
            <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-500">Current View</p>
            <h3 className={`mt-2 text-2xl font-semibold ${toneForLabel(label)}`}>{label}</h3>
            <p className="mt-2 text-sm leading-6 text-slate-400">
              {trend} analyst sentiment with {summary?.coverageLevel ?? interpretation?.coverageLevel ?? "insufficient"} coverage.
            </p>
          </div>
          <div className="text-right">
            <p className={`text-3xl font-semibold tabular-nums ${toneForPercent(consensusUpside)}`}>{formatPercent(consensusUpside, { signed: true })}</p>
            <p className="mt-1 text-xs font-semibold uppercase tracking-[0.16em] text-slate-500">Consensus upside</p>
          </div>
        </div>
        <div className="mt-5 grid gap-3 sm:grid-cols-3">
          <DetailMetric label="Median Upside" value={formatPercent(medianUpside, { signed: true })} tone={toneForPercent(medianUpside)} />
          <DetailMetric label="Ratings" value={formatNumber(totalRatings, { maximumFractionDigits: 0 })} />
          <DetailMetric label="Freshness" value={freshness?.daysOld === 0 ? "Today" : freshness?.daysOld != null ? `${freshness.daysOld}d old` : statusCopy(availability)} tone="text-slate-200" />
        </div>
        {snapshot.snapshotDate ? (
          <p className="mt-4 text-xs text-slate-500">Snapshot {formatDateShort(snapshot.snapshotDate)}</p>
        ) : null}
      </section>

      {locked ? (
        <PremiumLocked />
      ) : (
        <>
          <div className="grid gap-4 xl:grid-cols-[minmax(0,0.9fr)_minmax(0,1.1fr)]">
            <RatingDistribution snapshot={snapshot} />
            <PriceTargetRange snapshot={snapshot} />
          </div>
          <AnalystTrendChart points={data.trendSeries?.points ?? []} startDate={data.trendSeries?.startDate} endDate={data.trendSeries?.endDate} />
          <section className={`${panelClass} p-4`}>
            <p className="text-sm font-semibold text-white">Rating Actions</p>
            <div className="mt-3 grid gap-3 sm:grid-cols-3">
              <DetailMetric
                label="30d Net"
                value={hasActionWindows ? formatNumberOrLoading(data.gradeEventStats?.days30?.netActions, { maximumFractionDigits: 0 }) : EMPTY_LABEL}
                tone={hasActionWindows ? toneForPercent(data.gradeEventStats?.days30?.netActions) : "text-slate-400"}
              />
              <DetailMetric
                label="90d Net"
                value={hasActionWindows ? formatNumberOrLoading(data.gradeEventStats?.days90?.netActions, { maximumFractionDigits: 0 }) : EMPTY_LABEL}
                tone={hasActionWindows ? toneForPercent(data.gradeEventStats?.days90?.netActions) : "text-slate-400"}
              />
              <DetailMetric label="Latest" value={data.gradeEventStats?.mostRecentEvent?.action ?? "-"} tone="text-slate-200" />
            </div>
          </section>
        </>
      )}
    </div>
  );
}
