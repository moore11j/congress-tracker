"use client";

import { useMemo, useState } from "react";
import type { BacktestTimelinePoint } from "@/lib/api";

type Props = {
  timeline: BacktestTimelinePoint[];
};

const WIDTH = 1000;
const HEIGHT = 320;
const MARGIN = { top: 18, right: 84, bottom: 34, left: 64 };

function formatPrice(value: number) {
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 0,
  }).format(value);
}

function formatPriceTight(value: number) {
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 2,
  }).format(value);
}

function dateLabel(value: string) {
  const parsed = new Date(value);
  if (!Number.isFinite(parsed.getTime())) return value;
  return parsed.toLocaleDateString("en-US", { month: "short", day: "numeric" });
}

function scaleBounds(values: number[]) {
  const min = Math.min(...values);
  const max = Math.max(...values);
  const spread = Math.max(max - min, 1);
  const padding = spread * 0.14;
  return { min: Math.max(min - padding, 0), max: max + padding, range: Math.max(spread + padding * 2, 1) };
}

export function BacktestChart({ timeline }: Props) {
  const [activeIndex, setActiveIndex] = useState<number | null>(null);

  const chart = useMemo(() => {
    if (timeline.length === 0) return null;
    const innerWidth = WIDTH - MARGIN.left - MARGIN.right;
    const innerHeight = HEIGHT - MARGIN.top - MARGIN.bottom;
    const xStep = innerWidth / Math.max(timeline.length - 1, 1);
    const bounds = scaleBounds(timeline.flatMap((point) => [point.strategy_value, point.benchmark_value]));
    const yFor = (value: number) => MARGIN.top + innerHeight - (((value - bounds.min) / bounds.range) * innerHeight);
    const points = timeline.map((point, index) => ({
      ...point,
      x: MARGIN.left + index * xStep,
      strategyY: yFor(point.strategy_value),
      benchmarkY: yFor(point.benchmark_value),
    }));
    const strategyPath = points.map((point) => `${point.x},${point.strategyY}`).join(" ");
    const benchmarkPath = points.map((point) => `${point.x},${point.benchmarkY}`).join(" ");
    const yTicks = Array.from({ length: 5 }, (_, index) => {
      const ratio = index / 4;
      const value = bounds.max - ratio * bounds.range;
      return { value, y: MARGIN.top + ratio * innerHeight };
    });
    const tickIndexes = Array.from(new Set([0, Math.floor((points.length - 1) / 3), Math.floor(((points.length - 1) * 2) / 3), points.length - 1])).sort((a, b) => a - b);
    return { innerWidth, points, strategyPath, benchmarkPath, yTicks, tickIndexes, xStep };
  }, [timeline]);

  if (!chart) {
    return (
      <div className="rounded-2xl border border-white/10 bg-[#07111d] px-4 py-10 text-center text-sm text-slate-400">
        Not enough data points to draw a curve yet.
      </div>
    );
  }

  const hoveredPoint = activeIndex == null ? null : chart.points[activeIndex] ?? null;

  return (
    <div className="rounded-2xl border border-white/10 bg-[#07111d] p-3">
      <div className="mb-3 flex flex-wrap items-center justify-between gap-3">
        <div className="flex flex-wrap items-center gap-3 text-xs text-slate-400">
          <span className="inline-flex items-center gap-2">
            <span className="h-2.5 w-2.5 rounded-full bg-emerald-300" />
            Strategy
          </span>
          <span className="inline-flex items-center gap-2">
            <span className="h-2.5 w-2.5 rounded-full bg-slate-300" />
            S&amp;P 500
          </span>
        </div>
        <span className="text-xs uppercase tracking-[0.18em] text-slate-500">Portfolio Value ($)</span>
      </div>

      <div className="relative">
        <svg
          viewBox={`0 0 ${WIDTH} ${HEIGHT}`}
          className="h-[320px] w-full"
          onMouseLeave={() => setActiveIndex(null)}
          onMouseMove={(event) => {
            const rect = event.currentTarget.getBoundingClientRect();
            const relativeX = ((event.clientX - rect.left) / rect.width) * chart.innerWidth;
            const rawIndex = Math.round(relativeX / Math.max(chart.xStep, 1));
            const nextIndex = Math.min(Math.max(rawIndex, 0), chart.points.length - 1);
            setActiveIndex(nextIndex);
          }}
        >
          {chart.yTicks.map((tick) => (
            <g key={`y-${tick.y}`}>
              <line x1={MARGIN.left} x2={WIDTH - MARGIN.right} y1={tick.y} y2={tick.y} stroke="rgba(148,163,184,0.12)" strokeWidth="1" />
              <text x={WIDTH - MARGIN.right + 8} y={tick.y + 4} textAnchor="start" className="fill-slate-300/55 text-[11px] tabular-nums">
                {formatPrice(tick.value)}
              </text>
            </g>
          ))}

          {chart.tickIndexes.map((index) => {
            const point = chart.points[index];
            return (
              <g key={`x-${point.date}-${index}`}>
                <line x1={point.x} x2={point.x} y1={MARGIN.top} y2={HEIGHT - MARGIN.bottom} stroke="rgba(148,163,184,0.08)" strokeWidth="1" />
                <text x={point.x} y={HEIGHT - 10} textAnchor="middle" className="fill-slate-400 text-[11px]">
                  {dateLabel(point.date)}
                </text>
              </g>
            );
          })}

          <polyline fill="none" stroke="rgba(226,232,240,0.78)" strokeDasharray="6 4" strokeWidth="2" points={chart.benchmarkPath} />
          <polyline fill="none" stroke="rgba(110,231,183,0.96)" strokeWidth="2.8" strokeLinecap="round" strokeLinejoin="round" points={chart.strategyPath} />

          {hoveredPoint ? (
            <>
              <line x1={hoveredPoint.x} x2={hoveredPoint.x} y1={MARGIN.top} y2={HEIGHT - MARGIN.bottom} stroke="rgba(167,243,208,0.24)" strokeWidth="1.2" />
              <circle cx={hoveredPoint.x} cy={hoveredPoint.strategyY} r={4} fill="rgba(167,243,208,0.96)" stroke="rgba(16,185,129,0.6)" strokeWidth="1.2" />
              <circle cx={hoveredPoint.x} cy={hoveredPoint.benchmarkY} r={3.2} fill="rgba(226,232,240,0.92)" stroke="rgba(148,163,184,0.65)" strokeWidth="1.1" />
            </>
          ) : null}
        </svg>

        {hoveredPoint ? (
          <div
            className="pointer-events-none absolute top-4 z-10 w-56 rounded-2xl border border-white/10 bg-slate-950/95 px-3 py-3 text-sm shadow-xl"
            style={{ left: `min(calc(${((hoveredPoint.x / WIDTH) * 100).toFixed(2)}% + 12px), calc(100% - 15rem))` }}
          >
            <div className="text-xs uppercase tracking-[0.18em] text-slate-500">{dateLabel(hoveredPoint.date)}</div>
            <div className="mt-3 space-y-2">
              <div className="flex items-center justify-between gap-3">
                <span className="text-slate-400">Strategy value</span>
                <span className="font-semibold text-emerald-200">{formatPriceTight(hoveredPoint.strategy_value)}</span>
              </div>
              <div className="flex items-center justify-between gap-3">
                <span className="text-slate-400">Benchmark value</span>
                <span className="font-semibold text-slate-100">{formatPriceTight(hoveredPoint.benchmark_value)}</span>
              </div>
              <div className="flex items-center justify-between gap-3">
                <span className="text-slate-400">Active positions</span>
                <span className="font-semibold text-white">{hoveredPoint.active_positions}</span>
              </div>
            </div>
          </div>
        ) : null}
      </div>
    </div>
  );
}
