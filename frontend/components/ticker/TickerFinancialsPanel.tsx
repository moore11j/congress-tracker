"use client";

import type { ReactNode } from "react";
import { useMemo, useState } from "react";
import type {
  TickerEarningsPoint,
  TickerFinancialForecast,
  TickerFinancialForecasts,
  TickerFinancialsPoint,
  TickerFinancialsResponse,
} from "@/lib/api";
import { formatDateShort } from "@/lib/format";
import { SkeletonBlock } from "@/components/ui/LoadingSkeleton";

type PeriodMode = "annual" | "quarterly";
type ChartMetric = "revenue" | "netIncome";
type HoverPoint = {
  x: number;
  y: number;
  period: string;
  date?: string | null;
  value: number | null;
  yoyGrowth: number | null;
  tone?: "pos" | "neg" | "neutral";
  secondary?: string | null;
  isForecast?: boolean;
  estimateLow?: number | null;
  estimateHigh?: number | null;
  epsActual?: number | null;
  epsEstimate?: number | null;
  surprise?: number | null;
  result?: string;
};

type EpsChartPoint = TickerEarningsPoint & {
  isForecast?: boolean;
};

type ChartPoint = TickerFinancialsPoint & {
  value: number | null;
  yoyGrowth: number | null;
  isForecast?: boolean;
  estimateLow?: number | null;
  estimateHigh?: number | null;
};

const EMPTY_MESSAGE = "Financial data is not available for this ticker yet.";

function isFiniteNumber(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value);
}

function formatCompactCurrency(value: number | null | undefined): string {
  if (!isFiniteNumber(value)) return "-";
  const sign = value < 0 ? "-" : "";
  const abs = Math.abs(value);
  if (abs >= 1_000_000_000_000) return `${sign}$${(abs / 1_000_000_000_000).toFixed(1)}T`;
  if (abs >= 1_000_000_000) return `${sign}$${(abs / 1_000_000_000).toFixed(1)}B`;
  if (abs >= 1_000_000) return `${sign}$${(abs / 1_000_000).toFixed(1)}M`;
  if (abs >= 1_000) return `${sign}$${(abs / 1_000).toFixed(1)}K`;
  return `${sign}$${abs.toFixed(0)}`;
}

function formatEps(value: number | null | undefined): string {
  if (!isFiniteNumber(value)) return "-";
  const sign = value < 0 ? "-" : "";
  return `${sign}$${Math.abs(value).toFixed(2)}`;
}

function formatPct(value: number | null | undefined): string {
  if (!isFiniteNumber(value)) return "-";
  const sign = value > 0 ? "+" : "";
  return `${sign}${value.toFixed(1)}%`;
}

function formatMargin(value: number | null | undefined): string {
  if (!isFiniteNumber(value)) return "-";
  return `${value.toFixed(1)}%`;
}

function formatMultiple(value: number | null | undefined): string {
  if (!isFiniteNumber(value)) return "Unavailable";
  return `${value.toFixed(1)}x`;
}

function formatEstimateRange(low: number | null | undefined, high: number | null | undefined): string | null {
  if (isFiniteNumber(low) && isFiniteNumber(high)) return `${formatCompactCurrency(low)} - ${formatCompactCurrency(high)}`;
  if (isFiniteNumber(low)) return `Low ${formatCompactCurrency(low)}`;
  if (isFiniteNumber(high)) return `High ${formatCompactCurrency(high)}`;
  return null;
}

function formatSignedEps(value: number | null | undefined): string {
  if (!isFiniteNumber(value)) return "-";
  const sign = value > 0 ? "+" : value < 0 ? "-" : "";
  return `${sign}$${Math.abs(value).toFixed(2)}`;
}

function pointKey(point: TickerFinancialsPoint, index: number) {
  return `${point.period}-${point.date ?? index}`;
}

function defaultMode(annual: TickerFinancialsPoint[], quarterly: TickerFinancialsPoint[]): PeriodMode {
  return quarterly.length > 0 ? "quarterly" : "annual";
}

function seriesWithYoy(points: TickerFinancialsPoint[], metric: ChartMetric, mode: PeriodMode): ChartPoint[] {
  const lag = mode === "quarterly" ? 4 : 1;
  return points.map((point, index) => {
    const value = point[metric] ?? null;
    const prior = points[index - lag]?.[metric] ?? null;
    const yoyGrowth =
      isFiniteNumber(value) && isFiniteNumber(prior) && prior !== 0
        ? ((value - prior) / Math.abs(prior)) * 100
        : null;
    return { ...point, value, yoyGrowth };
  });
}

function comparableForecastPoint(forecast: TickerFinancialForecast | null | undefined, metric: ChartMetric): ChartPoint | null {
  if (!forecast) return null;
  const value = metric === "revenue" ? forecast.revenueEstimate : forecast.earningsEstimate;
  if (!isFiniteNumber(value)) return null;
  const estimateLow = metric === "revenue" ? forecast.revenueLow : forecast.earningsLow;
  const estimateHigh = metric === "revenue" ? forecast.revenueHigh : forecast.earningsHigh;
  return {
    period: forecast.period || "Forecast",
    date: forecast.date ?? null,
    revenue: metric === "revenue" ? value : null,
    netIncome: metric === "netIncome" ? value : null,
    value,
    yoyGrowth: null,
    estimateLow: estimateLow ?? null,
    estimateHigh: estimateHigh ?? null,
  };
}

function seriesWithForecast(points: TickerFinancialsPoint[], metric: ChartMetric, mode: PeriodMode, forecast?: TickerFinancialForecast | null): ChartPoint[] {
  const forecastPoint = comparableForecastPoint(forecast, metric);
  const actual = seriesWithYoy(points, metric, mode);
  if (!forecastPoint) return actual;

  const lag = mode === "quarterly" ? 4 : 1;
  const forecastValue = forecastPoint[metric] ?? null;
  const comparableIndex = actual.length - lag;
  const prior = comparableIndex >= 0 ? actual[comparableIndex]?.value : actual[actual.length - 1]?.value;
  const yoyGrowth =
    isFiniteNumber(forecastValue) && isFiniteNumber(prior) && prior !== 0 ? ((forecastValue - prior) / Math.abs(prior)) * 100 : null;
  return [...actual, { ...forecastPoint, value: forecastValue, yoyGrowth, isForecast: true }];
}

function tooltipPositionStyle(x: number, y: number, width: number, height: number, tooltipWidthRem = 10.75, tooltipHeightRem = 6.5) {
  const leftPct = (x / width) * 100;
  const topPct = (y / height) * 100;
  return {
    left: `clamp(0.5rem, calc(${leftPct}% - ${tooltipWidthRem / 2}rem), calc(100% - ${tooltipWidthRem + 0.5}rem))`,
    top: `clamp(0.5rem, calc(${topPct}% + 0.75rem), calc(100% - ${tooltipHeightRem + 0.5}rem))`,
  };
}

function normalizedEarningsResult(item: TickerEarningsPoint): "beat" | "miss" | "inline" | "unknown" {
  if (!isFiniteNumber(item.epsActual) || !isFiniteNumber(item.epsEstimate)) return "unknown";
  const delta = item.epsActual - item.epsEstimate;
  if (Math.abs(delta) < 0.005) return "inline";
  return delta > 0 ? "beat" : "miss";
}

function earningsSurprise(item: TickerEarningsPoint): number | null {
  if (isFiniteNumber(item.surprise)) return item.surprise;
  if (!isFiniteNumber(item.epsActual) || !isFiniteNumber(item.epsEstimate)) return null;
  return item.epsActual - item.epsEstimate;
}

function earningsSurprisePct(item: TickerEarningsPoint): number | null {
  if (isFiniteNumber(item.surprisePct)) return item.surprisePct;
  const surprise = earningsSurprise(item);
  if (!isFiniteNumber(surprise) || !isFiniteNumber(item.epsEstimate) || item.epsEstimate === 0) return null;
  return (surprise / Math.abs(item.epsEstimate)) * 100;
}

function isFutureQuarterlyEstimate(item: TickerEarningsPoint): boolean {
  if (isFiniteNumber(item.epsActual) || !isFiniteNumber(item.epsEstimate)) return false;
  const parsed = Date.parse(item.date);
  if (!Number.isFinite(parsed)) return false;
  const today = new Date();
  today.setHours(0, 0, 0, 0);
  return parsed >= today.getTime();
}

function ModeToggle({
  mode,
  annualAvailable,
  quarterlyAvailable,
  onChange,
}: {
  mode: PeriodMode;
  annualAvailable: boolean;
  quarterlyAvailable: boolean;
  onChange: (mode: PeriodMode) => void;
}) {
  if (!annualAvailable || !quarterlyAvailable) {
    return <span className="text-[11px] font-semibold uppercase tracking-[0.14em] text-slate-500">{quarterlyAvailable ? "Quarterly" : "Annual"}</span>;
  }
  return (
    <div className="flex rounded-lg border border-white/10 bg-slate-950/70 p-0.5">
      {(["annual", "quarterly"] as PeriodMode[]).map((item) => (
        <button
          key={item}
          type="button"
          onClick={() => onChange(item)}
          className={`rounded-md px-2.5 py-1 text-[11px] font-semibold capitalize transition ${
            mode === item ? "bg-emerald-400/15 text-emerald-200" : "text-slate-400 hover:bg-white/5 hover:text-slate-200"
          }`}
        >
          {item}
        </button>
      ))}
    </div>
  );
}

function FinancialChart({
  title,
  metric,
  valueLabel,
  annual,
  quarterly,
  forecasts,
  positiveNegative,
}: {
  title: string;
  metric: ChartMetric;
  valueLabel: string;
  annual: TickerFinancialsPoint[];
  quarterly: TickerFinancialsPoint[];
  forecasts?: TickerFinancialForecasts | null;
  positiveNegative?: boolean;
}) {
  const [mode, setMode] = useState<PeriodMode>(defaultMode(annual, quarterly));
  const [hover, setHover] = useState<HoverPoint | null>(null);
  const points = mode === "quarterly" && quarterly.length > 0 ? quarterly : annual;
  const selectedForecast = mode === "quarterly" ? forecasts?.nextQuarter : forecasts?.nextFiscalYear;
  const normalized = seriesWithForecast(points, metric, mode, selectedForecast).filter((point) => isFiniteNumber(point.value));
  const values = normalized.flatMap((point) => [point.value, point.estimateLow, point.estimateHigh]).filter(isFiniteNumber);
  const minValue = positiveNegative ? Math.min(0, ...values) : 0;
  const maxValue = Math.max(0, ...values);
  const range = maxValue - minValue || 1;
  const width = 640;
  const height = 250;
  const left = 54;
  const right = 18;
  const top = 22;
  const bottom = 42;
  const chartWidth = width - left - right;
  const chartHeight = height - top - bottom;
  const zeroY = top + ((maxValue - 0) / range) * chartHeight;
  const barGap = normalized.length > 0 ? chartWidth / normalized.length : chartWidth;
  const barWidth = Math.max(14, Math.min(44, barGap * 0.56));

  if (normalized.length === 0) {
    return (
      <FinancialSection title={title} action={<ModeToggle mode={mode} annualAvailable={annual.length > 0} quarterlyAvailable={quarterly.length > 0} onChange={setMode} />}>
        <UnavailableState message={`${title} data is not available yet.`} />
      </FinancialSection>
    );
  }

  const hasForecast = normalized.some((point) => point.isForecast);
  const rangeLabel = hover ? formatEstimateRange(hover.estimateLow, hover.estimateHigh) : null;
  const action = (
    <div className="flex flex-wrap items-center justify-end gap-3">
      {hasForecast ? (
        <span className="inline-flex items-center gap-1.5 text-[11px] font-semibold uppercase tracking-[0.12em] text-slate-400">
          <span className="h-2 w-4 rounded-full border border-dashed border-slate-400 bg-slate-400/20" />
          Forecast
        </span>
      ) : null}
      <ModeToggle mode={mode} annualAvailable={annual.length > 0} quarterlyAvailable={quarterly.length > 0} onChange={setMode} />
    </div>
  );

  return (
    <FinancialSection title={title} action={action}>
      <div className="relative h-[250px] w-full overflow-hidden rounded-xl border border-white/10 bg-[#07111d]">
        <svg viewBox={`0 0 ${width} ${height}`} className="h-full w-full" role="img" aria-label={`${title} chart`}>
          <defs>
            <linearGradient id={`${metric}-bar-pos`} x1="0" x2="0" y1="0" y2="1">
              <stop offset="0%" stopColor="#34d399" stopOpacity="0.9" />
              <stop offset="100%" stopColor="#10b981" stopOpacity="0.45" />
            </linearGradient>
            <linearGradient id={`${metric}-bar-neutral`} x1="0" x2="0" y1="0" y2="1">
              <stop offset="0%" stopColor="#22d3ee" stopOpacity="0.88" />
              <stop offset="100%" stopColor="#0891b2" stopOpacity="0.42" />
            </linearGradient>
          </defs>
          {[0, 0.5, 1].map((tick) => {
            const y = top + tick * chartHeight;
            const value = maxValue - tick * range;
            return (
              <g key={tick}>
                <line x1={left} x2={width - right} y1={y} y2={y} stroke="rgba(148,163,184,0.12)" />
                <text x={left - 10} y={y + 4} textAnchor="end" className="fill-slate-500 text-[11px]">
                  {formatCompactCurrency(value)}
                </text>
              </g>
            );
          })}
          {positiveNegative ? <line x1={left} x2={width - right} y1={zeroY} y2={zeroY} stroke="rgba(226,232,240,0.28)" /> : null}
          {normalized.map((point, index) => {
            const value = Number(point.value);
            const x = left + index * barGap + (barGap - barWidth) / 2;
            const y = top + ((maxValue - Math.max(value, 0)) / range) * chartHeight;
            const yZero = top + ((maxValue - Math.min(value, 0)) / range) * chartHeight;
            const barY = Math.min(y, yZero);
            const barH = Math.max(2, Math.abs(yZero - y));
            const isNegative = value < 0;
            const fill = point.isForecast ? "rgba(148,163,184,0.2)" : positiveNegative ? (isNegative ? "#fb7185" : "url(#netIncome-bar-pos)") : "url(#revenue-bar-neutral)";
            const centerX = x + barWidth / 2;
            const topY = isNegative ? yZero + barH : barY;
            const lowY = isFiniteNumber(point.estimateLow) ? top + ((maxValue - point.estimateLow) / range) * chartHeight : null;
            const highY = isFiniteNumber(point.estimateHigh) ? top + ((maxValue - point.estimateHigh) / range) * chartHeight : null;
            return (
              <g key={pointKey(point, index)}>
                {point.isForecast && lowY !== null && highY !== null ? (
                  <g opacity="0.9">
                    <line x1={centerX} x2={centerX} y1={highY} y2={lowY} stroke="rgba(203,213,225,0.8)" strokeDasharray="3 3" />
                    <line x1={centerX - 7} x2={centerX + 7} y1={highY} y2={highY} stroke="rgba(203,213,225,0.8)" />
                    <line x1={centerX - 7} x2={centerX + 7} y1={lowY} y2={lowY} stroke="rgba(203,213,225,0.8)" />
                  </g>
                ) : null}
                <rect
                  x={x}
                  y={barY}
                  width={barWidth}
                  height={barH}
                  rx="4"
                  fill={fill}
                  stroke={point.isForecast ? "rgba(203,213,225,0.85)" : "none"}
                  strokeDasharray={point.isForecast ? "4 3" : undefined}
                  opacity={hover?.period === point.period ? 1 : 0.78}
                  onMouseEnter={() =>
                    setHover({
                      x: centerX,
                      y: Math.max(top + 8, Math.min(topY, height - 92)),
                      period: point.period,
                      date: point.date,
                      value,
                      yoyGrowth: point.yoyGrowth,
                      tone: value > 0 ? "pos" : value < 0 ? "neg" : "neutral",
                      isForecast: point.isForecast,
                      estimateLow: point.estimateLow,
                      estimateHigh: point.estimateHigh,
                    })
                  }
                  onMouseLeave={() => setHover(null)}
                />
                <text x={centerX} y={height - 16} textAnchor="middle" className="fill-slate-500 text-[10px]">
                  {point.period.replace(" 20", " '")}
                </text>
              </g>
            );
          })}
        </svg>
        {hover ? (
          <div
            className="pointer-events-none absolute z-10 w-fit min-w-0 max-w-[10.75rem] whitespace-nowrap rounded-lg border border-white/15 bg-[#050b13]/95 p-2.5 text-xs shadow-[0_18px_45px_rgba(0,0,0,0.42)]"
            style={tooltipPositionStyle(hover.x, hover.y, width, height)}
          >
            <p className="font-semibold text-white">{hover.period}</p>
            {hover.isForecast ? <p className="mt-0.5 text-[10px] font-semibold uppercase tracking-[0.12em] text-slate-400">Forecast</p> : null}
            <p className={`mt-1 tabular-nums ${hover.tone === "neg" ? "text-rose-300" : "text-emerald-300"}`}>
              {valueLabel} {formatCompactCurrency(hover.value)}
            </p>
            {hover.isForecast && rangeLabel ? (
              <p className="mt-1 text-slate-400">
                Range {rangeLabel}
              </p>
            ) : null}
            <p className="mt-1 text-slate-400">YoY {formatPct(hover.yoyGrowth)}</p>
          </div>
        ) : null}
      </div>
    </FinancialSection>
  );
}

function EpsSurpriseSection({ earnings, forecasts }: { earnings: TickerEarningsPoint[]; forecasts?: TickerFinancialForecasts | null }) {
  const [hover, setHover] = useState<HoverPoint | null>(null);
  const quarterlyItems = earnings
    .filter((item) => isFiniteNumber(item.epsActual) || isFiniteNumber(item.epsEstimate))
    .map((item) => ({ ...item, isForecast: isFutureQuarterlyEstimate(item) }));
  const nextQuarterEps = forecasts?.nextQuarter?.epsEstimate;
  const nextQuarterForecastPoint: EpsChartPoint | null = isFiniteNumber(nextQuarterEps)
    ? {
        date: forecasts?.nextQuarter?.date ?? "",
        period: forecasts?.nextQuarter?.period ? `${forecasts.nextQuarter.period} Forecast` : "Quarter Forecast",
        epsActual: null,
        epsEstimate: nextQuarterEps,
        surprise: null,
        surprisePct: null,
        result: "unknown",
        isForecast: true,
      }
    : null;
  const hasNextQuarterForecast = nextQuarterForecastPoint
    ? quarterlyItems.some((item) => (nextQuarterForecastPoint.date && item.date === nextQuarterForecastPoint.date) || item.period === nextQuarterForecastPoint.period.replace(" Forecast", ""))
    : false;
  const items = [...quarterlyItems, ...(nextQuarterForecastPoint && !hasNextQuarterForecast ? [nextQuarterForecastPoint] : [])]
    .sort((leftItem, rightItem) => (leftItem.date || "").localeCompare(rightItem.date || ""))
    .slice(-6);
  const chartItems: EpsChartPoint[] = items;
  const latest = [...items].reverse().slice(0, 4);
  const width = 640;
  const height = 288;
  const left = 42;
  const right = 18;
  const top = 22;
  const bottom = 38;
  const chartWidth = width - left - right;
  const chartHeight = height - top - bottom;
  const values = chartItems.flatMap((item) => [item.epsActual, item.epsEstimate]).filter(isFiniteNumber);
  const minValue = Math.min(0, ...values);
  const maxValue = Math.max(0, ...values);
  const range = maxValue - minValue || 1;
  const xStep = chartItems.length > 1 ? chartWidth / (chartItems.length - 1) : chartWidth / 2;
  const xFor = (index: number) => (chartItems.length > 1 ? left + index * xStep : left + chartWidth / 2);
  const yFor = (value: number) => top + ((maxValue - value) / range) * chartHeight;
  const pathFor = (points: EpsChartPoint[], key: "epsActual" | "epsEstimate") => {
    let started = false;
    return points
      .map((item, index) => {
        const value = item[key];
        if (item.isForecast) return "";
        if (!isFiniteNumber(value)) return "";
        const command = started ? "L" : "M";
        started = true;
        return `${command} ${xFor(index)} ${yFor(value)}`;
      })
      .filter(Boolean)
      .join(" ");
  };
  const actualPath = pathFor(items, "epsActual");
  const estimatePath = pathFor(items, "epsEstimate");
  const forecastSegments = chartItems
    .map((item, index) => {
      if (!item.isForecast || !isFiniteNumber(item.epsEstimate)) return null;
      for (let anchorIndex = index - 1; anchorIndex >= 0; anchorIndex -= 1) {
        const anchorValue = chartItems[anchorIndex]?.epsEstimate ?? chartItems[anchorIndex]?.epsActual;
        if (isFiniteNumber(anchorValue)) {
          return {
            x1: xFor(anchorIndex),
            y1: yFor(anchorValue),
            x2: xFor(index),
            y2: yFor(item.epsEstimate),
          };
        }
      }
      return null;
    })
    .filter((segment): segment is { x1: number; y1: number; x2: number; y2: number } => Boolean(segment));

  if (items.length === 0) {
    return (
      <FinancialSection title="EPS Actual vs Estimate">
        <UnavailableState message="EPS actual and estimate history is not available yet." />
      </FinancialSection>
    );
  }

  return (
    <FinancialSection
      title="EPS Actual vs Estimate"
      action={
        <div className="flex items-center gap-3 text-[11px] font-semibold uppercase tracking-[0.12em] text-slate-400">
          <span className="inline-flex items-center gap-1.5"><span className="h-2 w-2 rounded-full bg-emerald-300" />Actual</span>
          <span className="inline-flex items-center gap-1.5"><span className="h-2 w-2 rounded-full bg-cyan-300" />Estimate</span>
        </div>
      }
    >
      <div className="grid items-stretch gap-4 xl:grid-cols-[minmax(0,1.2fr)_minmax(280px,.8fr)]">
        <div className="relative h-[288px] overflow-hidden rounded-xl border border-white/10 bg-[#07111d]">
          <svg viewBox={`0 0 ${width} ${height}`} className="h-full w-full" role="img" aria-label="EPS actual versus estimate chart">
            {[0, 0.5, 1].map((tick) => {
              const y = top + tick * chartHeight;
              const value = maxValue - tick * range;
              return (
                <g key={tick}>
                  <line x1={left} x2={width - right} y1={y} y2={y} stroke="rgba(148,163,184,0.12)" />
                  <text x={left - 9} y={y + 4} textAnchor="end" className="fill-slate-500 text-[10px]">
                    {value.toFixed(1)}
                  </text>
                </g>
              );
            })}
            {estimatePath ? <path d={estimatePath} fill="none" stroke="#22d3ee" strokeDasharray="4 4" strokeWidth="2" /> : null}
            {actualPath ? <path d={actualPath} fill="none" stroke="#34d399" strokeWidth="2.4" /> : null}
            {forecastSegments.map((segment) => (
              <path key={`${segment.x1}-${segment.x2}`} d={`M ${segment.x1} ${segment.y1} L ${segment.x2} ${segment.y2}`} fill="none" stroke="#94a3b8" strokeDasharray="5 5" strokeWidth="2" />
            ))}
            {chartItems.map((item, index) => {
              const x = xFor(index);
              const actualY = isFiniteNumber(item.epsActual) ? yFor(item.epsActual) : null;
              const estimateY = isFiniteNumber(item.epsEstimate) ? yFor(item.epsEstimate) : null;
              const result = normalizedEarningsResult(item);
              const surprise = earningsSurprise(item);
              const surprisePct = earningsSurprisePct(item);
              const hoverPayload = {
                x,
                y: Math.max(top + 8, Math.min(actualY ?? estimateY ?? top, height - 112)),
                period: item.period,
                date: item.date,
                value: item.epsActual ?? null,
                yoyGrowth: surprisePct,
                epsActual: item.epsActual ?? null,
                epsEstimate: item.epsEstimate ?? null,
                surprise,
                result,
                isForecast: item.isForecast,
                tone: result === "miss" ? "neg" : result === "beat" ? "pos" : "neutral",
              } satisfies HoverPoint;
              return (
                <g key={`${item.period}-${item.date}`}>
                  {estimateY !== null ? (
                    <circle
                      cx={x}
                      cy={estimateY}
                      r={item.isForecast ? "5" : "4"}
                      fill={item.isForecast ? "rgba(148,163,184,0.16)" : "#22d3ee"}
                      stroke={item.isForecast ? "#cbd5e1" : "none"}
                      strokeDasharray={item.isForecast ? "3 2" : undefined}
                      opacity="0.85"
                      onMouseEnter={() => setHover(hoverPayload)}
                      onMouseLeave={() => setHover(null)}
                    />
                  ) : null}
                  {actualY !== null ? (
                    <circle
                      cx={x}
                      cy={actualY}
                      r="5"
                      fill={result === "miss" ? "#fb7185" : "#34d399"}
                      onMouseEnter={() => setHover(hoverPayload)}
                      onMouseLeave={() => setHover(null)}
                    />
                  ) : null}
                  <circle
                    cx={x}
                    cy={actualY ?? estimateY ?? top}
                    r="13"
                    fill="transparent"
                    onMouseEnter={() => setHover(hoverPayload)}
                    onMouseLeave={() => setHover(null)}
                  />
                  <text x={x} y={height - 12} textAnchor="middle" className="fill-slate-500 text-[10px]">
                    {item.period.replace(" 20", " '")}
                  </text>
                </g>
              );
            })}
          </svg>
          {hover ? (
            <div
              className="pointer-events-none absolute z-10 w-fit min-w-0 max-w-[12rem] whitespace-nowrap rounded-lg border border-white/15 bg-[#050b13]/95 p-2.5 text-xs shadow-[0_18px_45px_rgba(0,0,0,0.42)]"
              style={tooltipPositionStyle(hover.x, hover.y, width, height, 12, 7.75)}
            >
              <p className="font-semibold text-white">{hover.period}</p>
              {hover.isForecast ? <p className="mt-0.5 text-[10px] font-semibold uppercase tracking-[0.12em] text-slate-400">Forecast</p> : null}
              <p className="mt-0.5 text-slate-500">{formatDateShort(hover.date ?? null)}</p>
              <p className={hover.tone === "neg" ? "mt-1 text-rose-300" : "mt-1 text-emerald-300"}>Actual {formatEps(hover.epsActual)}</p>
              <p className="mt-1 text-cyan-200">Estimate {formatEps(hover.epsEstimate)}</p>
              <p className="mt-1 text-slate-400">
                Surprise {formatSignedEps(hover.surprise)} / {formatPct(hover.yoyGrowth)}
              </p>
              <p className="mt-1 text-slate-400">{hover.result === "unknown" ? "Unknown" : hover.result === "inline" ? "In line" : hover.result === "beat" ? "Beat" : "Miss"}</p>
            </div>
          ) : null}
        </div>
        <div className="flex h-[288px] flex-col overflow-hidden rounded-xl border border-white/10">
          {latest.map((item) => {
            const result = normalizedEarningsResult(item);
            const surprise = earningsSurprise(item);
            const surprisePct = earningsSurprisePct(item);
            return (
            <div key={`${item.date}-${item.period}`} className="grid min-h-0 flex-1 grid-cols-[minmax(0,1fr)_auto] gap-3 border-b border-white/10 px-3 py-2.5 last:border-b-0">
              <div className="min-w-0">
                <p className="truncate text-sm font-semibold text-slate-100">{item.period}</p>
                <p className="text-xs text-slate-500">{formatDateShort(item.date)}</p>
                <div className="mt-2 grid grid-cols-2 gap-x-3 gap-y-1 text-[11px] text-slate-400">
                  <span>Actual <span className="tabular-nums text-slate-200">{formatEps(item.epsActual)}</span></span>
                  <span>Estimate <span className="tabular-nums text-cyan-200">{formatEps(item.epsEstimate)}</span></span>
                </div>
              </div>
              <div className="text-right">
                <ResultPill result={result} />
                <p className={result === "miss" ? "mt-2 text-xs tabular-nums text-rose-300" : "mt-2 text-xs tabular-nums text-emerald-300"}>
                  {formatSignedEps(surprise)} / {formatPct(surprisePct)}
                </p>
              </div>
            </div>
            );
          })}
        </div>
      </div>
    </FinancialSection>
  );
}

function ResultPill({ result }: { result: string }) {
  const label = result === "beat" ? "Beat" : result === "miss" ? "Miss" : result === "inline" ? "In line" : "Unknown";
  const className =
    result === "beat"
      ? "border-emerald-300/30 bg-emerald-300/10 text-emerald-200"
      : result === "miss"
        ? "border-rose-300/30 bg-rose-300/10 text-rose-200"
        : "border-white/10 bg-white/[0.04] text-slate-300";
  return <span className={`inline-flex rounded-full border px-2 py-0.5 text-[10px] font-semibold uppercase tracking-[0.12em] ${className}`}>{label}</span>;
}

function FinancialSection({ title, action, children }: { title: string; action?: ReactNode; children: ReactNode }) {
  return (
    <section className="rounded-2xl border border-white/10 bg-slate-950/50 p-4">
      <div className="mb-3 flex flex-wrap items-center justify-between gap-3">
        <h3 className="text-sm font-semibold text-white">{title}</h3>
        {action}
      </div>
      {children}
    </section>
  );
}

function SummaryTile({ label, value, tone, muted }: { label: string; value: string; tone?: "pos" | "neg" | "neutral"; muted?: boolean }) {
  const toneClass = muted ? "text-slate-500" : tone === "pos" ? "text-emerald-300" : tone === "neg" ? "text-rose-300" : "text-slate-100";
  return (
    <div className="rounded-xl border border-white/10 bg-white/[0.035] px-3 py-2.5">
      <p className="text-[10px] font-semibold uppercase tracking-[0.13em] text-slate-500">{label}</p>
      <p className={`mt-1 truncate text-sm font-semibold tabular-nums ${toneClass}`}>{value}</p>
    </div>
  );
}

function UnavailableState({ message = EMPTY_MESSAGE }: { message?: string }) {
  return (
    <div className="rounded-xl border border-dashed border-white/15 bg-white/[0.025] px-4 py-5">
      <p className="text-sm text-slate-400">{message}</p>
    </div>
  );
}

export function TickerFinancialsSkeleton() {
  return (
    <div className="space-y-4">
      <div className="grid gap-2 sm:grid-cols-2 xl:grid-cols-6">
        {Array.from({ length: 6 }, (_, index) => (
          <div key={index} className="rounded-xl border border-white/10 bg-white/[0.035] px-3 py-2.5">
            <SkeletonBlock className="h-2 w-20" />
            <SkeletonBlock className="mt-2 h-4 w-24" />
          </div>
        ))}
      </div>
      <div className="grid gap-4 xl:grid-cols-2">
        <SkeletonBlock className="h-72 rounded-2xl" />
        <SkeletonBlock className="h-72 rounded-2xl" />
      </div>
      <SkeletonBlock className="h-64 rounded-2xl" />
    </div>
  );
}

export function TickerFinancialsPanel({ data }: { data: TickerFinancialsResponse | null }) {
  const summary = data?.summary;
  const annual = Array.isArray(data?.annual) ? data.annual : [];
  const quarterly = Array.isArray(data?.quarterly) ? data.quarterly : [];
  const earnings = Array.isArray(data?.earnings) ? data.earnings : [];
  const forecasts = data?.forecasts ?? null;
  const hasAnyData = Boolean(annual.length || quarterly.length || earnings.length);
  const marginTiles = useMemo(
    () => [
      { label: "Gross Margin", value: formatMargin(summary?.grossMargin) },
      { label: "Operating Margin", value: formatMargin(summary?.operatingMargin) },
      { label: "FCF TTM", value: formatCompactCurrency(summary?.freeCashFlowTtm) },
      { label: "Operating Cash Flow TTM", value: formatCompactCurrency(summary?.operatingCashFlowTtm) },
    ],
    [summary],
  );

  if (!data || data.status === "unavailable" || !hasAnyData) {
    return <UnavailableState message={data?.message || EMPTY_MESSAGE} />;
  }

  return (
    <div className="space-y-4">
      <div className="grid gap-2 sm:grid-cols-2 xl:grid-cols-6">
        <SummaryTile label="Revenue TTM" value={formatCompactCurrency(summary?.revenueTtm)} />
        <SummaryTile label="Net Income TTM" value={formatCompactCurrency(summary?.netIncomeTtm)} tone={isFiniteNumber(summary?.netIncomeTtm) && summary.netIncomeTtm < 0 ? "neg" : "pos"} />
        <SummaryTile label="EPS TTM" value={formatEps(summary?.epsTtm)} tone={isFiniteNumber(summary?.epsTtm) && summary.epsTtm < 0 ? "neg" : "pos"} />
        <SummaryTile label="Trailing P/E" value={formatMultiple(summary?.trailingPE)} muted={!isFiniteNumber(summary?.trailingPE)} />
        <SummaryTile label="Forward P/E" value={formatMultiple(summary?.forwardPE)} muted={!isFiniteNumber(summary?.forwardPE)} />
        <SummaryTile label="Next Earnings" value={formatDateShort(summary?.nextEarningsDate ?? null)} />
      </div>

      <div className="grid gap-4 xl:grid-cols-2">
        <FinancialChart title="Revenue Trend" metric="revenue" valueLabel="Revenue" annual={annual} quarterly={quarterly} forecasts={forecasts} />
        <FinancialChart title="Earnings Trend" metric="netIncome" valueLabel="Net Income" annual={annual} quarterly={quarterly} forecasts={forecasts} positiveNegative />
      </div>

      <EpsSurpriseSection earnings={earnings} forecasts={forecasts} />

      {marginTiles.some((tile) => tile.value !== "-") ? (
        <FinancialSection title="Margin / Cash Quality">
          <div className="grid gap-2 sm:grid-cols-2 xl:grid-cols-4">
            {marginTiles.map((tile) => (
              <SummaryTile key={tile.label} label={tile.label} value={tile.value} />
            ))}
          </div>
        </FinancialSection>
      ) : null}
    </div>
  );
}
