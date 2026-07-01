"use client";

import { useMemo, useState } from "react";
import type { InstitutionHoldingItem } from "@/lib/api";
import { cardClassName } from "@/lib/styles";
import { formatDateShort } from "@/lib/format";

type Props = {
  holdings: InstitutionHoldingItem[];
  totalReportedValue?: number | null;
  holdingsCount?: number | null;
  reportPeriod: string;
  filingDate?: string | null;
  hasRetryableFiling?: boolean;
};

type AllocationSlice = {
  key: string;
  symbol: string;
  issuerName: string;
  valueUsd: number;
  shares: number | null;
  weightPct: number;
  color: string;
  start: number;
  end: number;
};

const TOP_HOLDINGS_LIMIT = 10;
const SLICE_COLORS = [
  "#38bdf8",
  "#a78bfa",
  "#f59e0b",
  "#f472b6",
  "#22d3ee",
  "#c084fc",
  "#f97316",
  "#60a5fa",
  "#eab308",
  "#818cf8",
  "#94a3b8",
];

export function HoldingsAllocationChart({
  holdings,
  totalReportedValue,
  holdingsCount,
  reportPeriod,
  filingDate,
  hasRetryableFiling = false,
}: Props) {
  const chartSlices = useMemo(
    () => buildAllocationSlices(holdings, totalReportedValue, holdingsCount),
    [holdings, holdingsCount, totalReportedValue],
  );
  const [activeKey, setActiveKey] = useState<string | null>(chartSlices[0]?.key ?? null);
  const activeSlice = chartSlices.find((slice) => slice.key === activeKey) ?? chartSlices[0] ?? null;
  const hasChart = chartSlices.length >= 2;

  return (
    <section className={`${cardClassName} min-w-0 overflow-hidden`} data-testid="institution-holdings-allocation">
      <div className="mb-5 flex min-w-0 flex-col justify-between gap-3 lg:flex-row lg:items-start">
        <div className="min-w-0">
          <h2 className="text-lg font-semibold text-white">Reported holdings allocation</h2>
          <p className="mt-1 max-w-3xl text-sm leading-6 text-slate-400">
            Based on the institution&apos;s most recent processed 13F filing. Values reflect reported quarter-end holdings from the filing date.
          </p>
        </div>
        <div className="flex min-w-0 flex-wrap gap-2 text-xs text-slate-300">
          <span className="rounded-full border border-white/10 bg-slate-950/45 px-3 py-1">
            Report period: <span className="font-semibold text-slate-100">{reportPeriod}</span>
          </span>
          <span className="rounded-full border border-white/10 bg-slate-950/45 px-3 py-1">
            Filing date: <span className="font-semibold text-slate-100">{formatDateShort(filingDate ?? null)}</span>
          </span>
        </div>
      </div>

      {!hasChart ? (
        <div className="rounded-2xl border border-dashed border-white/10 bg-slate-950/30 p-6 text-sm text-slate-400">
          {hasRetryableFiling
            ? "Filing detected; reported holdings are not available yet."
            : "No reported holdings are available for this institution yet."}
        </div>
      ) : (
        <div className="grid min-w-0 gap-6 lg:grid-cols-[minmax(220px,340px)_minmax(0,1fr)] lg:items-center">
          <div className="mx-auto flex w-full max-w-[320px] min-w-0 flex-col items-center">
            <svg
              viewBox="0 0 100 100"
              role="img"
              aria-label="Reported holdings allocation by reported value"
              className="aspect-square w-full max-w-[280px] drop-shadow-[0_18px_35px_rgba(15,23,42,0.35)]"
            >
              <circle cx="50" cy="50" r="41" fill="rgba(15, 23, 42, 0.45)" />
              {chartSlices.map((slice) => (
                <path
                  key={slice.key}
                  d={donutPath(slice.start, slice.end)}
                  fill={slice.color}
                  stroke="rgba(15,23,42,0.78)"
                  strokeWidth="0.9"
                  tabIndex={0}
                  className="cursor-pointer transition duration-150 hover:opacity-90 focus:outline-none"
                  opacity={activeSlice?.key === slice.key ? 1 : 0.78}
                  onMouseEnter={() => setActiveKey(slice.key)}
                  onFocus={() => setActiveKey(slice.key)}
                >
                  <title>{sliceTooltip(slice)}</title>
                </path>
              ))}
              <circle cx="50" cy="50" r="25" fill="rgba(2, 6, 23, 0.92)" />
              <text x="50" y="47" textAnchor="middle" className="fill-slate-200 text-[6px] font-semibold">
                Top {Math.min(TOP_HOLDINGS_LIMIT, holdingsCount ?? TOP_HOLDINGS_LIMIT)}
              </text>
              <text x="50" y="56" textAnchor="middle" className="fill-slate-400 text-[4px]">
                reported value
              </text>
            </svg>
            {activeSlice ? (
              <div className="mt-4 w-full rounded-2xl border border-white/10 bg-slate-950/70 p-4 text-sm shadow-xl">
                <p className="truncate font-semibold text-white">
                  {activeSlice.symbol} - {activeSlice.issuerName}
                </p>
                <dl className="mt-3 grid grid-cols-2 gap-x-3 gap-y-2 text-xs">
                  <dt className="text-slate-500">Reported value</dt>
                  <dd className="text-right font-semibold tabular-nums text-slate-100">{formatCompactCurrency(activeSlice.valueUsd)}</dd>
                  <dt className="text-slate-500">Portfolio weight</dt>
                  <dd className="text-right font-semibold tabular-nums text-slate-100">{formatPct(activeSlice.weightPct)}</dd>
                  <dt className="text-slate-500">Shares</dt>
                  <dd className="text-right font-semibold tabular-nums text-slate-100">{formatShares(activeSlice.shares)}</dd>
                </dl>
              </div>
            ) : null}
          </div>

          <div className="grid min-w-0 gap-2 sm:grid-cols-2">
            {chartSlices.map((slice) => (
              <button
                key={slice.key}
                type="button"
                className="flex min-w-0 items-center gap-3 rounded-xl border border-white/10 bg-slate-950/35 px-3 py-2 text-left transition hover:border-white/20 hover:bg-slate-900/55 focus:outline-none focus:ring-2 focus:ring-sky-300/40"
                onMouseEnter={() => setActiveKey(slice.key)}
                onFocus={() => setActiveKey(slice.key)}
              >
                <span className="h-3 w-3 flex-none rounded-full" style={{ backgroundColor: slice.color }} aria-hidden="true" />
                <span className="min-w-0 flex-1">
                  <span className="block truncate text-sm font-semibold text-slate-100">{slice.symbol}</span>
                  <span className="block truncate text-xs text-slate-500">{slice.issuerName}</span>
                </span>
                <span className="flex-none text-right text-xs tabular-nums text-slate-300">
                  <span className="block font-semibold text-slate-100">{formatPct(slice.weightPct)}</span>
                  <span className="block">{formatCompactCurrency(slice.valueUsd)}</span>
                </span>
              </button>
            ))}
          </div>
        </div>
      )}
    </section>
  );
}

function buildAllocationSlices(
  holdings: InstitutionHoldingItem[],
  totalReportedValue?: number | null,
  holdingsCount?: number | null,
): AllocationSlice[] {
  const sorted = holdings
    .map((holding, index) => ({
      ...holding,
      key: `${holding.symbol ?? "holding"}-${holding.cusip ?? index}`,
      value_usd: numericValue(holding.value_usd),
      shares: numericValue(holding.shares),
    }))
    .filter((holding) => holding.value_usd > 0)
    .sort((left, right) => right.value_usd - left.value_usd);

  const topHoldings = sorted.slice(0, TOP_HOLDINGS_LIMIT);
  const topValue = topHoldings.reduce((sum, holding) => sum + holding.value_usd, 0);
  const fallbackTotal = sorted.reduce((sum, holding) => sum + holding.value_usd, 0);
  const rawTotal = numericValue(totalReportedValue);
  const totalValue = Math.max(rawTotal, fallbackTotal, topValue);
  const shouldShowOther = (holdingsCount ?? sorted.length) > TOP_HOLDINGS_LIMIT && totalValue > topValue;

  const rawSlices: Array<Omit<AllocationSlice, "weightPct" | "start" | "end">> = topHoldings.map((holding, index) => ({
    key: holding.key,
    symbol: holding.symbol ?? "-",
    issuerName: holding.issuer_name ?? "Company unavailable",
    valueUsd: holding.value_usd,
    shares: holding.shares,
    color: SLICE_COLORS[index % SLICE_COLORS.length],
  }));

  if (shouldShowOther) {
    rawSlices.push({
      key: "other",
      symbol: "Other",
      issuerName: "Remaining reported holdings",
      valueUsd: Math.max(0, totalValue - topValue),
      shares: null,
      color: SLICE_COLORS[TOP_HOLDINGS_LIMIT % SLICE_COLORS.length],
    });
  }

  if (rawSlices.length < 2 || totalValue <= 0) return [];

  let cursor = 0;
  return rawSlices.map((slice) => {
    const weightPct = (slice.valueUsd / totalValue) * 100;
    const start = cursor;
    const end = cursor + weightPct / 100;
    cursor = end;
    return { ...slice, weightPct, start, end };
  });
}

function donutPath(startFraction: number, endFraction: number): string {
  const outerRadius = 43;
  const innerRadius = 25;
  const start = Math.max(0, Math.min(0.9999, startFraction));
  const end = Math.max(start, Math.min(0.9999, endFraction));
  const largeArc = end - start > 0.5 ? 1 : 0;
  const outerStart = polarToCartesian(50, 50, outerRadius, end);
  const outerEnd = polarToCartesian(50, 50, outerRadius, start);
  const innerStart = polarToCartesian(50, 50, innerRadius, start);
  const innerEnd = polarToCartesian(50, 50, innerRadius, end);

  return [
    `M ${outerStart.x} ${outerStart.y}`,
    `A ${outerRadius} ${outerRadius} 0 ${largeArc} 0 ${outerEnd.x} ${outerEnd.y}`,
    `L ${innerStart.x} ${innerStart.y}`,
    `A ${innerRadius} ${innerRadius} 0 ${largeArc} 1 ${innerEnd.x} ${innerEnd.y}`,
    "Z",
  ].join(" ");
}

function polarToCartesian(cx: number, cy: number, radius: number, fraction: number) {
  const angle = fraction * Math.PI * 2 - Math.PI / 2;
  return {
    x: roundCoord(cx + radius * Math.cos(angle)),
    y: roundCoord(cy + radius * Math.sin(angle)),
  };
}

function roundCoord(value: number): number {
  return Math.round(value * 1000) / 1000;
}

function numericValue(value: number | string | null | undefined): number {
  if (typeof value === "number") return Number.isFinite(value) ? value : 0;
  if (typeof value !== "string") return 0;
  const parsed = Number(value.replace(/[$,]/g, "").trim());
  return Number.isFinite(parsed) ? parsed : 0;
}

function sliceTooltip(slice: AllocationSlice): string {
  return [
    `${slice.symbol} - ${slice.issuerName}`,
    `Reported value: ${formatCompactCurrency(slice.valueUsd)}`,
    `Portfolio weight: ${formatPct(slice.weightPct)}`,
    `Shares: ${formatShares(slice.shares)}`,
  ].join("\n");
}

function formatCompactCurrency(value: number | null | undefined): string {
  if (value === null || value === undefined || Number.isNaN(value)) return "-";
  const abs = Math.abs(value);
  if (abs >= 1_000_000_000) return `$${(value / 1_000_000_000).toFixed(abs >= 10_000_000_000 ? 0 : 1)}B`;
  if (abs >= 1_000_000) return `$${(value / 1_000_000).toFixed(abs >= 10_000_000 ? 0 : 1)}M`;
  if (abs >= 1_000) return `$${(value / 1_000).toFixed(abs >= 10_000 ? 0 : 1)}K`;
  return `$${Math.round(value).toLocaleString()}`;
}

function formatPct(value: number | null | undefined): string {
  if (value === null || value === undefined || Number.isNaN(value)) return "-";
  return `${value.toFixed(value >= 10 ? 1 : 2)}%`;
}

function formatShares(value: number | null | undefined): string {
  if (value === null || value === undefined || Number.isNaN(value)) return "-";
  const abs = Math.abs(value);
  if (abs >= 1_000_000_000) return `${(value / 1_000_000_000).toFixed(abs >= 10_000_000_000 ? 0 : 1)}B`;
  if (abs >= 1_000_000) return `${(value / 1_000_000).toFixed(abs >= 10_000_000 ? 0 : 1)}M`;
  if (abs >= 1_000) return `${(value / 1_000).toFixed(abs >= 10_000 ? 0 : 1)}K`;
  return Math.round(value).toLocaleString();
}
