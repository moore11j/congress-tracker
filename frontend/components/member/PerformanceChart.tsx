"use client";

import { useMemo, useState, type MouseEvent } from "react";
import type { BenchmarkPerformancePoint, MemberPerformancePoint } from "@/lib/api";
import { getSvgLocalPoint } from "@/lib/chartPointer";

type Metric = "return" | "alpha";

type Props = {
  memberSeries: MemberPerformancePoint[];
  benchmarkSeries: BenchmarkPerformancePoint[];
  metric: Metric;
  benchmarkLabel: string;
  subjectLabel?: string;
};

const WIDTH = 1000;
const HEIGHT = 320;
const MARGIN = { top: 18, right: 84, bottom: 34, left: 64 };

function pct(value: number | null | undefined, digits = 1) {
  if (value == null || !Number.isFinite(value)) return "-";
  return `${value.toFixed(digits)}%`;
}

function formatDateCompact(raw: string | null | undefined) {
  if (!raw) return "-";
  const date = new Date(raw);
  if (!Number.isFinite(date.getTime())) return raw;
  return date.toLocaleDateString("en-US", { month: "short", day: "numeric" });
}

function formatDateFull(raw: string | null | undefined) {
  if (!raw) return "-";
  const date = new Date(raw);
  if (!Number.isFinite(date.getTime())) return raw;
  return date.toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" });
}

function toTime(raw: string | null | undefined) {
  if (!raw) return null;
  const date = new Date(raw);
  const time = date.getTime();
  return Number.isFinite(time) ? time : null;
}

function scaleBounds(values: number[], padRatio = 0.14) {
  const min = Math.min(...values);
  const max = Math.max(...values);
  const spread = Math.max(max - min, 1.2);
  const padding = spread * padRatio;
  return { min: min - padding, max: max + padding, range: Math.max(spread + padding * 2, 1) };
}

export function PerformanceChart({
  memberSeries,
  benchmarkSeries,
  metric,
  benchmarkLabel,
  subjectLabel = "Profile",
}: Props) {
  const [activeIndex, setActiveIndex] = useState<number | null>(null);

  const chart = useMemo(() => {
    const innerWidth = WIDTH - MARGIN.left - MARGIN.right;
    const innerHeight = HEIGHT - MARGIN.top - MARGIN.bottom;

    const profilePoints = memberSeries
      .map((point) => {
        const time = toTime(point.asof_date);
        const value = metric === "alpha" ? point.cumulative_alpha_pct : (point.strategy_return_pct ?? point.cumulative_return_pct);
        if (time == null || typeof value !== "number" || !Number.isFinite(value)) return null;
        return { time, value, point };
      })
      .filter(Boolean)
      .sort((a, b) => (a!.time - b!.time) || (a!.point.event_id - b!.point.event_id)) as Array<{
      time: number;
      value: number;
      point: MemberPerformancePoint;
    }>;

    const benchmarkPoints = benchmarkSeries
      .map((point) => {
        const time = toTime(point.asof_date);
        const value = point.cumulative_return_pct;
        if (time == null || typeof value !== "number" || !Number.isFinite(value)) return null;
        return { time, value, point };
      })
      .filter(Boolean)
      .sort((a, b) => a!.time - b!.time) as Array<{
      time: number;
      value: number;
      point: BenchmarkPerformancePoint;
    }>;

    if (profilePoints.length < 2) return null;

    const allTimes = [...profilePoints.map((point) => point.time), ...benchmarkPoints.map((point) => point.time)];
    const minTime = Math.min(...allTimes);
    const maxTime = Math.max(...allTimes);
    const timeSpan = Math.max(maxTime - minTime, 1);
    const xFor = (time: number) => MARGIN.left + ((time - minTime) / timeSpan) * innerWidth;

    const domainValues =
      metric === "return"
        ? [...profilePoints.map((point) => point.value), ...benchmarkPoints.map((point) => point.value)]
        : profilePoints.map((point) => point.value);
    const bounds = scaleBounds(domainValues);
    const yFor = (value: number) => MARGIN.top + innerHeight - ((value - bounds.min) / bounds.range) * innerHeight;

    const points = profilePoints.map((item) => ({
      ...item,
      x: xFor(item.time),
      y: yFor(item.value),
    }));
    const benchmarkRenderPoints = benchmarkPoints.map((item) => ({
      ...item,
      x: xFor(item.time),
      y: yFor(item.value),
    }));

    const profilePath = points.map((point) => `${point.x},${point.y}`).join(" ");
    const benchmarkPath = benchmarkRenderPoints.map((point) => `${point.x},${point.y}`).join(" ");
    const yTicks = Array.from({ length: 5 }, (_, index) => {
      const ratio = index / 4;
      const value = bounds.max - ratio * bounds.range;
      return { value, y: MARGIN.top + ratio * innerHeight };
    });
    const tickIndexes = Array.from(
      new Set([0, Math.floor((points.length - 1) / 3), Math.floor(((points.length - 1) * 2) / 3), points.length - 1]),
    ).sort((a, b) => a - b);

    return { innerWidth, xFor, points, benchmarkRenderPoints, profilePath, benchmarkPath, yTicks, tickIndexes };
  }, [memberSeries, benchmarkSeries, metric]);

  if (!chart) return null;

  const activePoint = activeIndex == null ? null : chart.points[activeIndex] ?? null;
  const activeBenchmarkPoint =
    activePoint == null || chart.benchmarkRenderPoints.length === 0
      ? null
      : chart.benchmarkRenderPoints.reduce((nearest, point) =>
          Math.abs(point.time - activePoint.time) < Math.abs(nearest.time - activePoint.time) ? point : nearest,
        );

  const handleMove = (event: MouseEvent<SVGSVGElement>) => {
    const local = getSvgLocalPoint(event.currentTarget, event.clientX, event.clientY);
    if (!local) {
      setActiveIndex(null);
      return;
    }
    const clampedX = Math.max(MARGIN.left, Math.min(WIDTH - MARGIN.right, local.x));
    let nearestIndex = 0;
    let nearestDistance = Math.abs(chart.points[0].x - clampedX);
    for (let index = 1; index < chart.points.length; index += 1) {
      const distance = Math.abs(chart.points[index].x - clampedX);
      if (distance < nearestDistance) {
        nearestDistance = distance;
        nearestIndex = index;
      }
    }
    setActiveIndex(nearestIndex);
  };

  const label = subjectLabel.trim() || "Profile";

  return (
    <div className="mt-3 rounded-2xl border border-white/10 bg-[#07111d] p-3">
      <div className="mb-3 flex flex-wrap items-center justify-between gap-3">
        <div className="flex flex-wrap items-center gap-3 text-xs text-slate-400">
          <span className="inline-flex items-center gap-2">
            <span className="h-2.5 w-2.5 rounded-full bg-emerald-300" />
            {label}
          </span>
          {metric === "return" ? (
            <span className="inline-flex items-center gap-2">
              <span className="h-2.5 w-2.5 rounded-full bg-slate-300" />
              {benchmarkLabel}
            </span>
          ) : null}
        </div>
        <span className="text-xs uppercase tracking-[0.18em] text-slate-500">
          {metric === "return" ? "Equal-Weight Outcome Return" : "Equal-Weight Outcome Alpha"}
        </span>
      </div>

      <div className="relative">
        <svg
          viewBox={`0 0 ${WIDTH} ${HEIGHT}`}
          className="h-[320px] w-full"
          onMouseMove={handleMove}
          onMouseLeave={() => setActiveIndex(null)}
        >
          {chart.yTicks.map((tick) => (
            <g key={`y-${tick.y}`}>
              <line x1={MARGIN.left} x2={WIDTH - MARGIN.right} y1={tick.y} y2={tick.y} stroke="rgba(148,163,184,0.12)" strokeWidth="1" />
              <text x={WIDTH - MARGIN.right + 8} y={tick.y + 4} textAnchor="start" className="fill-slate-300/55 text-[11px] tabular-nums">
                {pct(tick.value)}
              </text>
            </g>
          ))}

          {chart.tickIndexes.map((index) => {
            const point = chart.points[index];
            return (
              <g key={`x-${point.point.asof_date}-${index}`}>
                <line x1={point.x} x2={point.x} y1={MARGIN.top} y2={HEIGHT - MARGIN.bottom} stroke="rgba(148,163,184,0.08)" strokeWidth="1" />
                <text x={point.x} y={HEIGHT - 10} textAnchor="middle" className="fill-slate-400 text-[11px]">
                  {formatDateCompact(point.point.asof_date)}
                </text>
              </g>
            );
          })}

          {metric === "return" && chart.benchmarkPath ? (
            <polyline fill="none" stroke="rgba(226,232,240,0.78)" strokeDasharray="6 4" strokeWidth="2" points={chart.benchmarkPath} />
          ) : null}
          <polyline fill="none" stroke="rgba(110,231,183,0.96)" strokeWidth="2.8" strokeLinecap="round" strokeLinejoin="round" points={chart.profilePath} />

          {activePoint ? (
            <>
              <line x1={activePoint.x} x2={activePoint.x} y1={MARGIN.top} y2={HEIGHT - MARGIN.bottom} stroke="rgba(167,243,208,0.24)" strokeWidth="1.2" />
              <circle cx={activePoint.x} cy={activePoint.y} r={4} fill="rgba(167,243,208,0.96)" stroke="rgba(16,185,129,0.6)" strokeWidth="1.2" />
              {metric === "return" && activeBenchmarkPoint ? (
                <circle cx={activeBenchmarkPoint.x} cy={activeBenchmarkPoint.y} r={3.2} fill="rgba(226,232,240,0.92)" stroke="rgba(148,163,184,0.65)" strokeWidth="1.1" />
              ) : null}
            </>
          ) : null}
        </svg>

        {activePoint ? (
          <div
            className="pointer-events-none absolute top-4 z-10 w-60 rounded-2xl border border-white/10 bg-slate-950/95 px-3 py-3 text-sm shadow-xl"
            style={{ left: `min(calc(${((activePoint.x / WIDTH) * 100).toFixed(2)}% + 12px), calc(100% - 15.5rem))` }}
          >
            <div className="flex items-center justify-between gap-3">
              <div className="text-xs uppercase tracking-[0.18em] text-slate-500">{metric === "return" ? "Return" : "Alpha"}</div>
              <div className="text-xs text-slate-400">{formatDateFull(activePoint.point.asof_date)}</div>
            </div>
            <div className="mt-3 space-y-2">
              <div className="flex items-center justify-between gap-3">
                <span className="text-slate-400">{label} return</span>
                <span className="font-semibold text-emerald-200">{pct(activePoint.point.strategy_return_pct ?? activePoint.point.cumulative_return_pct)}</span>
              </div>
              <div className="flex items-center justify-between gap-3">
                <span className="text-slate-400">{benchmarkLabel} return</span>
                <span className="font-semibold text-slate-100">
                  {pct(activePoint.point.running_benchmark_return_pct ?? activeBenchmarkPoint?.point.cumulative_return_pct)}
                </span>
              </div>
              <div className="flex items-center justify-between gap-3">
                <span className="text-slate-400">Alpha</span>
                <span className="font-semibold text-white">{pct(activePoint.point.cumulative_alpha_pct)}</span>
              </div>
              <div className="flex items-center justify-between gap-3">
                <span className="text-slate-400">Active outcomes</span>
                <span className="font-semibold text-white">{activePoint.point.active_positions ?? "-"}</span>
              </div>
            </div>
          </div>
        ) : null}
      </div>
    </div>
  );
}
