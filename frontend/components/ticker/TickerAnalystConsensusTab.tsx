import Link from "next/link";
import type { ReactNode } from "react";
import type {
  TickerAnalystConsensusChange,
  TickerAnalystConsensusResponse,
  TickerAnalystConsensusSnapshot,
} from "@/lib/api";
import { formatDateShort } from "@/lib/format";
import { SkeletonBlock } from "@/components/ui/LoadingSkeleton";

type Props = {
  data: TickerAnalystConsensusResponse | null;
  symbol: string;
};

const panelClass = "rounded-lg border border-white/10 bg-slate-950/55";

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
  return "Backfill pending";
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

function ChangeRow({ label, change }: { label: string; change?: TickerAnalystConsensusChange }) {
  return (
    <div className="grid gap-2 rounded-lg border border-white/10 bg-slate-950/50 p-3 sm:grid-cols-3">
      <div>
        <p className="text-xs font-semibold text-white">{label}</p>
        <p className="mt-1 text-[11px] text-slate-500">{change?.comparisonDate ? `vs ${formatDateShort(change.comparisonDate)}` : "No comparison yet"}</p>
      </div>
      <div>
        <p className="text-[10px] font-semibold uppercase tracking-[0.16em] text-slate-500">Sentiment</p>
        <p className={`mt-1 text-sm font-semibold tabular-nums ${toneForPercent(change?.weightedSentimentChange)}`}>{formatNumber(change?.weightedSentimentChange, { maximumFractionDigits: 2 })}</p>
      </div>
      <div>
        <p className="text-[10px] font-semibold uppercase tracking-[0.16em] text-slate-500">Target</p>
        <p className={`mt-1 text-sm font-semibold tabular-nums ${toneForPercent(change?.consensusTargetChange)}`}>{formatMoney(change?.consensusTargetChange)}</p>
      </div>
    </div>
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

  if (!data || !snapshot) {
    return (
      <section className={`${panelClass} p-5`}>
        <p className="text-sm font-semibold text-white">Analyst consensus is backfilling</p>
        <p className="mt-2 text-sm leading-6 text-slate-400">
          Current analyst summary for {symbol} is not available yet. The tab will populate automatically as the backfill reaches this ticker.
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
          <DetailMetric label="Ratings" value={formatNumber(snapshot.totalRatingCount, { maximumFractionDigits: 0 })} />
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
          <section className={`${panelClass} p-4`}>
            <p className="text-sm font-semibold text-white">Trend Changes</p>
            <div className="mt-3 grid gap-3">
              <ChangeRow label="30 days" change={data.changes?.days30} />
              <ChangeRow label="90 days" change={data.changes?.days90} />
            </div>
          </section>
          <section className={`${panelClass} p-4`}>
            <p className="text-sm font-semibold text-white">Rating Actions</p>
            <div className="mt-3 grid gap-3 sm:grid-cols-3">
              <DetailMetric label="30d Net" value={formatNumber(data.gradeEventStats?.days30?.netActions, { maximumFractionDigits: 0 })} tone={toneForPercent(data.gradeEventStats?.days30?.netActions)} />
              <DetailMetric label="90d Net" value={formatNumber(data.gradeEventStats?.days90?.netActions, { maximumFractionDigits: 0 })} tone={toneForPercent(data.gradeEventStats?.days90?.netActions)} />
              <DetailMetric label="Latest" value={data.gradeEventStats?.mostRecentEvent?.action ?? "-"} tone="text-slate-200" />
            </div>
          </section>
        </>
      )}
    </div>
  );
}
