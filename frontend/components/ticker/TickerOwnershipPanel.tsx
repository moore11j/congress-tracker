"use client";

import { useMemo, useState } from "react";
import Link from "next/link";
import type { TickerOwnershipHolder, TickerOwnershipPoint, TickerOwnershipResponse } from "@/lib/api";
import { formatDateShort } from "@/lib/format";
import { SkeletonBlock } from "@/components/ui/LoadingSkeleton";

type ChartPoint = TickerOwnershipPoint & {
  value: number;
  retail: number;
};

function isFiniteNumber(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value);
}

function clampPct(value: number | null | undefined): number | null {
  if (!isFiniteNumber(value)) return null;
  return Math.max(0, Math.min(value, 100));
}

function formatPct(value: number | null | undefined): string {
  if (!isFiniteNumber(value)) return "-";
  return `${value.toFixed(2)}%`;
}

function formatCompactCurrency(value: number | null | undefined): string {
  if (!isFiniteNumber(value)) return "-";
  const abs = Math.abs(value);
  const sign = value < 0 ? "-" : "";
  if (abs >= 1_000_000_000_000) return `${sign}$${(abs / 1_000_000_000_000).toFixed(1)}T`;
  if (abs >= 1_000_000_000) return `${sign}$${(abs / 1_000_000_000).toFixed(1)}B`;
  if (abs >= 1_000_000) return `${sign}$${(abs / 1_000_000).toFixed(1)}M`;
  if (abs >= 1_000) return `${sign}$${(abs / 1_000).toFixed(1)}K`;
  return `${sign}$${abs.toFixed(0)}`;
}

function formatNumber(value: number | null | undefined): string {
  if (!isFiniteNumber(value)) return "-";
  return new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 }).format(value);
}

function hasOwnershipPct(data: TickerOwnershipResponse | null | undefined): boolean {
  return isFiniteNumber(data?.latest?.institutional_ownership_pct);
}

function hasReportedHoldings(data: TickerOwnershipResponse | null | undefined): boolean {
  if (!data?.latest) return false;
  return (
    data.holders.some((holder) => isFiniteNumber(holder.value_usd) || isFiniteNumber(holder.shares) || isFiniteNumber(holder.ownership_pct))
    || isFiniteNumber(data.latest.total_value_usd)
    || Boolean(data.latest.total_holders && data.latest.total_holders > 0)
  );
}

function trendFor(points: ChartPoint[]) {
  if (points.length < 2) return null;
  const n = points.length;
  const sumX = points.reduce((sum, _point, index) => sum + index, 0);
  const sumY = points.reduce((sum, point) => sum + point.value, 0);
  const sumXY = points.reduce((sum, point, index) => sum + index * point.value, 0);
  const sumXX = points.reduce((sum, _point, index) => sum + index * index, 0);
  const denominator = n * sumXX - sumX * sumX;
  if (denominator === 0) return null;
  const slope = (n * sumXY - sumX * sumY) / denominator;
  const intercept = (sumY - slope * sumX) / n;
  return { start: clampPct(intercept) ?? 0, end: clampPct(intercept + slope * (n - 1)) ?? 0, slope };
}

function normalizedHistory(history: TickerOwnershipPoint[]): ChartPoint[] {
  return history
    .map((point) => {
      const value = clampPct(point.institutional_ownership_pct);
      if (value === null) return null;
      return {
        ...point,
        value,
        retail: clampPct(point.retail_ownership_pct) ?? Math.max(0, 100 - value),
      };
    })
    .filter((point): point is ChartPoint => Boolean(point));
}

function OwnershipChart({ history }: { history: TickerOwnershipPoint[] }) {
  const [activeIndex, setActiveIndex] = useState<number | null>(null);
  const points = normalizedHistory(history);
  const trend = trendFor(points);
  const width = 680;
  const height = 280;
  const left = 46;
  const right = 24;
  const top = 22;
  const bottom = 42;
  const chartWidth = width - left - right;
  const chartHeight = height - top - bottom;
  const xStep = points.length > 1 ? chartWidth / (points.length - 1) : chartWidth / 2;
  const xFor = (index: number) => (points.length > 1 ? left + index * xStep : left + chartWidth / 2);
  const yFor = (value: number) => top + ((100 - value) / 100) * chartHeight;
  const path = points.map((point, index) => `${index === 0 ? "M" : "L"} ${xFor(index)} ${yFor(point.value)}`).join(" ");
  const retailPath = points.map((point, index) => `${index === 0 ? "M" : "L"} ${xFor(index)} ${yFor(point.retail)}`).join(" ");
  const activePoint = activeIndex === null ? null : points[activeIndex] ?? null;
  const activeX = activeIndex === null ? 0 : xFor(activeIndex);
  const activeY = activePoint ? Math.min(yFor(activePoint.value), yFor(activePoint.retail)) : 0;

  if (points.length === 0) {
    return <EmptyState message="Ownership history is not available for this ticker yet." />;
  }

  return (
    <section className="rounded-2xl border border-white/10 bg-slate-950/50 p-4">
      <div className="mb-3 flex flex-wrap items-center justify-between gap-3">
        <h3 className="text-sm font-semibold text-white">Ownership Change</h3>
        <div className="flex items-center gap-3 text-[11px] font-semibold uppercase tracking-[0.12em] text-slate-400">
          <span className="inline-flex items-center gap-1.5"><span className="h-2 w-2 rounded-full bg-blue-400" />Institutional</span>
          <span className="inline-flex items-center gap-1.5"><span className="h-2 w-2 rounded-full bg-emerald-400" />Retail</span>
          <span className="inline-flex items-center gap-1.5"><span className="h-0.5 w-4 border-t border-dashed border-emerald-300" />Trend</span>
        </div>
      </div>
      <div className="relative h-[240px] overflow-hidden rounded-xl border border-white/10 bg-[#07111d]" onMouseLeave={() => setActiveIndex(null)}>
        {activePoint ? (
          <div
            className="pointer-events-none absolute z-10 w-48 rounded-lg border border-white/15 bg-slate-950/95 p-3 text-xs shadow-[0_18px_45px_rgba(0,0,0,0.45)]"
            style={{
              left: `${(activeX / width) * 100}%`,
              top: `${(activeY / height) * 100}%`,
              transform: activeX > width * 0.72 ? "translate(-104%, -18%)" : "translate(12px, -18%)",
            }}
          >
            <p className="font-semibold text-white">{activePoint.period}</p>
            <div className="mt-2 space-y-1">
              <div className="flex items-center justify-between gap-3">
                <span className="inline-flex items-center gap-1.5 text-slate-400"><span className="h-2 w-2 rounded-full bg-blue-400" />Institutional</span>
                <span className="font-semibold tabular-nums text-blue-200">{formatPct(activePoint.value)}</span>
              </div>
              <div className="flex items-center justify-between gap-3">
                <span className="inline-flex items-center gap-1.5 text-slate-400"><span className="h-2 w-2 rounded-full bg-emerald-400" />Retail</span>
                <span className="font-semibold tabular-nums text-emerald-200">{formatPct(activePoint.retail)}</span>
              </div>
            </div>
          </div>
        ) : null}
        <svg viewBox={`0 0 ${width} ${height}`} className="h-full w-full" role="img" aria-label="Institutional ownership history with trend line">
          {[0, 25, 50, 75, 100].map((tick) => {
            const y = yFor(tick);
            return (
              <g key={tick}>
                <line x1={left} x2={width - right} y1={y} y2={y} stroke="rgba(148,163,184,0.12)" />
                <text x={left - 10} y={y + 4} textAnchor="end" className="fill-slate-500 text-[10px]">
                  {tick}%
                </text>
              </g>
            );
          })}
          {path ? <path d={path} fill="none" stroke="#60a5fa" strokeWidth="2.8" /> : null}
          {retailPath ? <path d={retailPath} fill="none" stroke="#34d399" strokeWidth="2.2" opacity="0.82" /> : null}
          {trend && points.length > 1 ? (
            <path
              d={`M ${xFor(0)} ${yFor(trend.start)} L ${xFor(points.length - 1)} ${yFor(trend.end)}`}
              fill="none"
              stroke="#34d399"
              strokeDasharray="6 5"
              strokeWidth="2"
            />
          ) : null}
          {points.map((point, index) => {
            const x = xFor(index);
            const y = yFor(point.value);
            const retailY = yFor(point.retail);
            return (
              <g key={`${point.period}-${index}`}>
                <circle cx={x} cy={y} r="5" fill="#60a5fa" stroke="#bfdbfe" strokeWidth="1.5" />
                <circle cx={x} cy={retailY} r="4" fill="#34d399" stroke="#bbf7d0" strokeWidth="1.2" />
                <circle
                  cx={x}
                  cy={y}
                  r="13"
                  fill="transparent"
                  className="cursor-crosshair"
                  onMouseEnter={() => setActiveIndex(index)}
                  onFocus={() => setActiveIndex(index)}
                  onBlur={() => setActiveIndex(null)}
                  tabIndex={0}
                  aria-label={`${point.period}: institutional ${formatPct(point.value)}, retail ${formatPct(point.retail)}`}
                />
                <circle
                  cx={x}
                  cy={retailY}
                  r="13"
                  fill="transparent"
                  className="cursor-crosshair"
                  onMouseEnter={() => setActiveIndex(index)}
                />
                <text x={x} y={height - 15} textAnchor="middle" className="fill-slate-500 text-[10px]">
                  {point.period.replace(" 20", " '")}
                </text>
                <title>{`${point.period}: institutional ${formatPct(point.value)}, retail ${formatPct(point.retail)}`}</title>
              </g>
            );
          })}
        </svg>
      </div>
    </section>
  );
}

function HolderBreakdown({ holders }: { holders: TickerOwnershipHolder[] }) {
  const visible = holders
    .filter((holder) => isFiniteNumber(holder.ownership_pct) || isFiniteNumber(holder.value_usd) || isFiniteNumber(holder.shares))
    .slice(0, 12);
  if (visible.length === 0) {
    return <p className="text-sm text-slate-400">Holder-level records are not available.</p>;
  }
  return (
    <div className="max-h-44 overflow-auto rounded-xl border border-white/10 bg-slate-950/70">
      <table className="min-w-full table-fixed text-left text-xs">
        <thead className="sticky top-0 z-10 border-b border-white/10 bg-[#07111d] text-[10px] font-semibold uppercase tracking-[0.12em] text-slate-500">
          <tr>
            <th scope="col" className="w-[42%] px-3 py-2">Holder</th>
            <th scope="col" className="w-[20%] px-3 py-2 text-right">Reported Value</th>
            <th scope="col" className="w-[22%] px-3 py-2 text-right">Shares</th>
            <th scope="col" className="w-[16%] px-3 py-2 text-right">Ownership</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-white/10">
          {visible.map((holder) => {
            const pct = clampPct(holder.ownership_pct) ?? 0;
            return (
              <tr key={`${holder.cik}-${holder.holder_name}`} className="hover:bg-white/[0.035]">
                <td className="truncate px-3 py-2 font-semibold text-slate-100">{holder.holder_name || "Institution"}</td>
                <td className="px-3 py-2 text-right tabular-nums text-slate-300">{formatCompactCurrency(holder.value_usd)}</td>
                <td className="px-3 py-2 text-right tabular-nums text-slate-300">{formatNumber(holder.shares)}</td>
                <td className="px-3 py-2 text-right font-semibold tabular-nums text-blue-200">{pct > 0 ? formatPct(pct) : "-"}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function InstitutionalHoldersSection({ data }: { data: TickerOwnershipResponse }) {
  const latest = data.latest;
  return (
    <section className="rounded-2xl border border-white/10 bg-slate-950/50 p-3">
      <div className="mb-2 flex flex-col gap-2 sm:flex-row sm:items-start sm:justify-between">
        <div>
          <h3 className="text-sm font-semibold text-white">Institutional Holders</h3>
          <p className="mt-1 text-xs text-slate-500">
            {latest?.period ?? "Latest quarter"}{latest?.latest_filing_date ? ` / Filed ${formatDateShort(latest.latest_filing_date)}` : ""}
          </p>
        </div>
        <p className="text-xs font-semibold tabular-nums text-slate-400">{formatNumber(data.holders.length)} records</p>
      </div>
      <HolderBreakdown holders={data.holders} />
    </section>
  );
}

function ReportedHoldingsSummary({ data }: { data: TickerOwnershipResponse }) {
  const latest = data.latest;
  return (
    <section className="rounded-2xl border border-white/10 bg-slate-950/50 p-4">
      <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
        <div>
          <h3 className="text-sm font-semibold text-white">Reported Institutional Holdings</h3>
          <p className="mt-1 text-xs text-slate-500">
            {latest?.period ?? "Latest quarter"}{latest?.latest_filing_date ? ` / Filed ${formatDateShort(latest.latest_filing_date)}` : ""}
          </p>
        </div>
        <p className="rounded-full border border-amber-300/25 bg-amber-300/10 px-3 py-1 text-xs font-semibold text-amber-100">
          Float unavailable
        </p>
      </div>
      <div className="mt-4 grid gap-2 sm:grid-cols-3">
        <SummaryTile label="Holders" value={formatNumber(latest?.total_holders)} />
        <SummaryTile label="Reported Value" value={formatCompactCurrency(latest?.total_value_usd)} />
        <SummaryTile label="Holder Records" value={formatNumber(data.holders.length)} />
      </div>
      {data.message ? <p className="mt-4 text-sm leading-6 text-slate-400">{data.message}</p> : null}
      <div className="mt-4">
        <HolderBreakdown holders={data.holders} />
      </div>
    </section>
  );
}

function OwnershipSplit({ data }: { data: TickerOwnershipResponse }) {
  const latest = data.latest;
  const institutional = clampPct(latest?.institutional_ownership_pct) ?? 0;
  const retail = clampPct(latest?.retail_ownership_pct) ?? Math.max(0, 100 - institutional);

  return (
    <section className="rounded-2xl border border-white/10 bg-slate-950/50 p-4">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <h3 className="text-sm font-semibold text-white">Institutional vs Retail</h3>
          <p className="mt-1 text-xs text-slate-500">
            {latest?.period ?? "Latest quarter"}{latest?.latest_filing_date ? ` / Filed ${formatDateShort(latest.latest_filing_date)}` : ""}
          </p>
        </div>
        <div className="grid grid-cols-2 gap-2 text-right">
          <div>
            <p className="text-[10px] font-semibold uppercase tracking-[0.12em] text-slate-500">Institutions</p>
            <p className="text-sm font-semibold tabular-nums text-blue-200">{formatPct(institutional)}</p>
          </div>
          <div>
            <p className="text-[10px] font-semibold uppercase tracking-[0.12em] text-slate-500">Retail</p>
            <p className="text-sm font-semibold tabular-nums text-emerald-200">{formatPct(retail)}</p>
          </div>
        </div>
      </div>
      <div className="mt-4 overflow-hidden rounded-xl border border-white/10 bg-slate-950">
        <div className="flex h-16 w-full">
          <div
            className="relative h-full bg-blue-500/85 text-left"
            style={{ width: `${institutional}%` }}
          >
            <span className="absolute inset-x-3 top-1/2 -translate-y-1/2 truncate text-xs font-semibold text-white">
              Institutional {formatPct(institutional)}
            </span>
          </div>
          <div className="relative h-full bg-emerald-500/80" style={{ width: `${retail}%` }}>
            <span className="absolute inset-x-3 top-1/2 -translate-y-1/2 truncate text-xs font-semibold text-white">
              Retail {formatPct(retail)}
            </span>
          </div>
        </div>
      </div>
      <div className="mt-3 grid gap-2 sm:grid-cols-3">
        <SummaryTile label="Holders" value={formatNumber(latest?.total_holders)} />
        <SummaryTile
          label={isFiniteNumber(latest?.total_institutional_shares) ? "Institutional Shares" : "Reported Value"}
          value={isFiniteNumber(latest?.total_institutional_shares) ? formatNumber(latest?.total_institutional_shares) : formatCompactCurrency(latest?.total_value_usd)}
        />
        <SummaryTile
          label={isFiniteNumber(latest?.float_shares) ? "Float Shares" : "Holder Records"}
          value={isFiniteNumber(latest?.float_shares) ? formatNumber(latest?.float_shares) : formatNumber(data.holders.length)}
        />
      </div>
    </section>
  );
}

function SummaryTile({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-xl border border-white/10 bg-white/[0.035] px-3 py-2.5">
      <p className="text-[10px] font-semibold uppercase tracking-[0.13em] text-slate-500">{label}</p>
      <p className="mt-1 truncate text-sm font-semibold tabular-nums text-slate-100">{value}</p>
    </div>
  );
}

function EmptyState({ message }: { message: string }) {
  return (
    <div className="rounded-xl border border-dashed border-white/15 bg-white/[0.025] px-4 py-5">
      <p className="text-sm text-slate-400">{message}</p>
    </div>
  );
}

function LockedState() {
  return (
    <div className="rounded-2xl border border-emerald-300/20 bg-emerald-300/10 p-5">
      <p className="text-sm font-semibold text-emerald-100">Ownership breakdown requires Pro.</p>
      <p className="mt-2 text-sm leading-6 text-slate-300">Institutional holder percentages and ownership trends use 13F filings.</p>
      <Link href="/pricing" className="mt-4 inline-flex rounded-xl border border-emerald-300/40 bg-emerald-300/10 px-3 py-2 text-sm font-semibold text-emerald-100 transition hover:bg-emerald-300/15">
        Upgrade to Pro
      </Link>
    </div>
  );
}

export function TickerOwnershipSkeleton() {
  return (
    <div className="space-y-3">
      <SkeletonBlock className="h-40 rounded-2xl" />
      <SkeletonBlock className="h-72 rounded-2xl" />
    </div>
  );
}

export function TickerOwnershipPanel({ data, locked = false }: { data: TickerOwnershipResponse | null; locked?: boolean }) {
  const displayData = useMemo(() => data, [data]);
  if (locked || displayData?.locked || displayData?.status === "pro_locked") return <LockedState />;
  if (
    !displayData
    || displayData.status === "unavailable"
    || !displayData.latest
    || (displayData.status === "no_data" && !hasReportedHoldings(displayData))
    || (!hasOwnershipPct(displayData) && !hasReportedHoldings(displayData))
  ) {
    return <EmptyState message={displayData?.message || "Ownership data is not available for this ticker yet."} />;
  }
  return (
    <div className="space-y-4">
      {hasOwnershipPct(displayData) ? (
        <>
          <OwnershipSplit data={displayData} />
          <OwnershipChart history={displayData.history} />
          <InstitutionalHoldersSection data={displayData} />
        </>
      ) : (
        <ReportedHoldingsSummary data={displayData} />
      )}
      {displayData.tooltip ? <p className="text-xs leading-5 text-slate-500">{displayData.tooltip}</p> : null}
    </div>
  );
}
