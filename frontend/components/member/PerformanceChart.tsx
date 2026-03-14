"use client";

import { useMemo, useState, type MouseEvent } from "react";
import type { BenchmarkPerformancePoint, MemberPerformancePoint } from "@/lib/api";
import { getSvgLocalPoint } from "@/lib/chartPointer";

type Metric = "return" | "alpha";

type MemberChartPoint = {
  x: number;
  y: number;
  index: number;
  value: number;
  point: MemberPerformancePoint;
};

type BenchmarkChartPoint = {
  x: number;
  y: number;
  value: number;
  point: BenchmarkPerformancePoint;
};

type Props = {
  memberSeries: MemberPerformancePoint[];
  benchmarkSeries: BenchmarkPerformancePoint[];
  metric: Metric;
  benchmarkLabel: string;
};

const WIDTH = 1000;
const HEIGHT = 320;
const MARGIN = { top: 18, right: 58, bottom: 34, left: 58 };
const POINT_HIT_RADIUS = 11;

function pct(value: number | null | undefined, digits = 1) {
  if (value == null || !Number.isFinite(value)) return "—";
  return `${value.toFixed(digits)}%`;
}

function formatDateCompact(raw: string | null | undefined) {
  if (!raw) return "—";
  const date = new Date(raw);
  if (!Number.isFinite(date.getTime())) return raw;
  return date.toLocaleDateString("en-US", { month: "short", day: "numeric" });
}

function formatDateFull(raw: string | null | undefined) {
  if (!raw) return "—";
  const date = new Date(raw);
  if (!Number.isFinite(date.getTime())) return raw;
  return date.toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" });
}

function clamp(value: number, min: number, max: number) {
  return Math.max(min, Math.min(max, value));
}

function toTime(raw: string | null | undefined) {
  if (!raw) return null;
  const date = new Date(raw);
  const time = date.getTime();
  return Number.isFinite(time) ? time : null;
}

function createScale(values: number[]) {
  const rawMin = Math.min(...values);
  const rawMax = Math.max(...values);
  const spread = Math.max(rawMax - rawMin, 1.2);
  const pad = spread * 0.16;
  const min = rawMin - pad;
  const max = rawMax + pad;
  const range = Math.max(max - min, 1);
  return { min, max, range };
}

export function PerformanceChart({ memberSeries, benchmarkSeries, metric, benchmarkLabel }: Props) {
  const [hoveredIndex, setHoveredIndex] = useState<number | null>(null);

  const chart = useMemo(() => {
    const innerWidth = WIDTH - MARGIN.left - MARGIN.right;
    const innerHeight = HEIGHT - MARGIN.top - MARGIN.bottom;

    const memberPoints = memberSeries
      .map((point, index) => {
        const time = toTime(point.asof_date);
        const value = metric === "alpha" ? point.cumulative_alpha_pct : point.cumulative_return_pct;
        if (time == null || typeof value !== "number" || !Number.isFinite(value)) return null;
        return { index, time, value, point };
      })
      .filter(Boolean)
      .sort((a, b) => (a!.time - b!.time) || (a!.point.event_id - b!.point.event_id)) as Array<{ index: number; time: number; value: number; point: MemberPerformancePoint }>;

    const benchmarkPoints =
      metric === "return"
        ? benchmarkSeries
            .map((point) => {
              const time = toTime(point.asof_date);
              const value = point.cumulative_return_pct;
              if (time == null || typeof value !== "number" || !Number.isFinite(value)) return null;
              return { time, value, point };
            })
            .filter(Boolean)
            .sort((a, b) => a!.time - b!.time) as Array<{ time: number; value: number; point: BenchmarkPerformancePoint }>
        : [];

    if (memberPoints.length < 2) return null;

    const allTimes = [...memberPoints.map((p) => p.time), ...benchmarkPoints.map((p) => p.time)];
    const minTime = Math.min(...allTimes);
    const maxTime = Math.max(...allTimes);
    const timeSpan = Math.max(maxTime - minTime, 1);

    const xFor = (time: number) => MARGIN.left + ((time - minTime) / timeSpan) * innerWidth;

    const memberScale = createScale(memberPoints.map((p) => p.value));
    const memberYFor = (value: number) =>
      MARGIN.top + innerHeight - ((value - memberScale.min) / memberScale.range) * innerHeight;

    const benchmarkScale = benchmarkPoints.length > 0 ? createScale(benchmarkPoints.map((p) => p.value)) : null;
    const benchmarkYFor = (value: number) => {
      if (!benchmarkScale) return memberYFor(value);
      return MARGIN.top + innerHeight - ((value - benchmarkScale.min) / benchmarkScale.range) * innerHeight;
    };

    const memberRenderPoints: MemberChartPoint[] = memberPoints.map((item) => ({
      ...item,
      x: xFor(item.time),
      y: memberYFor(item.value),
    }));

    const benchmarkRenderPoints: BenchmarkChartPoint[] = benchmarkPoints.map((item) => ({
      ...item,
      x: xFor(item.time),
      y: benchmarkYFor(item.value),
    }));

    const memberPath = memberRenderPoints.map((item) => `${item.x},${item.y}`).join(" ");
    const benchmarkPath = benchmarkRenderPoints.map((item) => `${item.x},${item.y}`).join(" ");

    const axisTicks = 5;
    const memberYTicks = Array.from({ length: axisTicks }, (_, index) => {
      const ratio = index / (axisTicks - 1);
      const value = memberScale.max - ratio * memberScale.range;
      return { value, y: MARGIN.top + ratio * innerHeight };
    });

    const benchmarkYTicks = benchmarkScale
      ? Array.from({ length: axisTicks }, (_, index) => {
          const ratio = index / (axisTicks - 1);
          const value = benchmarkScale.max - ratio * benchmarkScale.range;
          return { value, y: MARGIN.top + ratio * innerHeight };
        })
      : [];

    const xTicks = Array.from({ length: 6 }, (_, tick) => {
      const ratio = tick / 5;
      const time = minTime + ratio * timeSpan;
      return {
        x: MARGIN.left + ratio * innerWidth,
        label: formatDateCompact(new Date(time).toISOString()),
      };
    });

    return { memberRenderPoints, benchmarkRenderPoints, memberPath, benchmarkPath, memberYTicks, benchmarkYTicks, xTicks };
  }, [memberSeries, benchmarkSeries, metric]);

  if (!chart) return null;

  const activePoint =
    hoveredIndex == null ? null : chart.memberRenderPoints.find((point) => point.index === hoveredIndex) ?? null;

  const handleMove = (event: MouseEvent<SVGSVGElement>) => {
    if (!chart.memberRenderPoints.length) return;
    const local = getSvgLocalPoint(event.currentTarget, event.clientX, event.clientY);
    if (!local) {
      setHoveredIndex(null);
      return;
    }

    let nearest = chart.memberRenderPoints[0];
    let distance = Math.hypot(local.x - nearest.x, local.y - nearest.y);
    for (const point of chart.memberRenderPoints) {
      const nextDistance = Math.hypot(local.x - point.x, local.y - point.y);
      if (nextDistance < distance) {
        nearest = point;
        distance = nextDistance;
      }
    }

    setHoveredIndex(distance <= POINT_HIT_RADIUS ? nearest.index : null);
  };

  return (
    <div className="mt-3">
      <div className="relative w-full overflow-hidden rounded-xl border border-white/10 bg-[#06111c] p-3">
        <svg viewBox={`0 0 ${WIDTH} ${HEIGHT}`} className="h-[320px] w-full" onMouseMove={handleMove} onMouseLeave={() => setHoveredIndex(null)}>
          {chart.memberYTicks.map((tick) => (
            <g key={`y-grid-${tick.y}`}>
              <line x1={MARGIN.left} x2={WIDTH - MARGIN.right} y1={tick.y} y2={tick.y} stroke="rgba(148,163,184,0.12)" strokeWidth="1" />
              <text x={WIDTH - MARGIN.right + 8} y={tick.y + 4} textAnchor="start" className="fill-cyan-100/55 text-[11px] tabular-nums">{pct(tick.value)}</text>
            </g>
          ))}
          {chart.xTicks.map((tick, idx) => (
            <g key={`x-grid-${idx}`}>
              <line x1={tick.x} x2={tick.x} y1={MARGIN.top} y2={HEIGHT - MARGIN.bottom} stroke="rgba(148,163,184,0.08)" strokeWidth="1" />
              <text x={tick.x} y={HEIGHT - 10} textAnchor="middle" className="fill-white/45 text-[11px]">{tick.label}</text>
            </g>
          ))}

          {metric === "return" && chart.benchmarkPath && (
            <polyline fill="none" stroke="rgba(148,163,184,0.72)" strokeDasharray="5 4" strokeWidth="2" points={chart.benchmarkPath} />
          )}

          <polyline fill="none" stroke="rgba(14,165,233,0.95)" strokeWidth="2.8" points={chart.memberPath} strokeLinecap="round" strokeLinejoin="round" />

          {chart.memberRenderPoints.map((point) => {
            const active = point.index === activePoint?.index;
            return (
              <circle
                key={`${point.point.event_id}-${point.index}`}
                cx={point.x}
                cy={point.y}
                r={active ? 4.2 : 2.4}
                fill={active ? "rgba(34,211,238,0.96)" : "rgba(56,189,248,0.52)"}
                stroke={active ? "rgba(224,242,254,0.9)" : "rgba(125,211,252,0.35)"}
                strokeWidth={active ? 1.3 : 1}
                style={{ opacity: activePoint ? (active ? 1 : 0.35) : 0.72 }}
              />
            );
          })}

          {metric === "return" && chart.benchmarkYTicks.map((tick) => (
            <text key={`left-${tick.y}`} x={MARGIN.left - 8} y={tick.y + 4} textAnchor="end" className="fill-slate-200/50 text-[11px] tabular-nums">{pct(tick.value)}</text>
          ))}
        </svg>

        {activePoint && (
          <div
            className="pointer-events-none absolute z-20 min-w-[240px] rounded-lg border border-white/15 bg-[#071626]/95 px-3 py-2.5 text-xs text-white/85 shadow-[0_12px_30px_rgba(2,6,23,0.5)] backdrop-blur"
            style={{ left: `${clamp((activePoint.x / WIDTH) * 100 + 2, 2, 65)}%`, top: `${clamp((activePoint.y / HEIGHT) * 100 - 12, 2, 62)}%` }}
          >
            <div className="flex items-center justify-between gap-2">
              <p className="font-semibold tracking-wide text-white">{(activePoint.point.symbol ?? "—").toUpperCase()}</p>
              <p className="text-[11px] text-white/55">{formatDateFull(activePoint.point.asof_date)}</p>
            </div>
            <p className="mt-0.5 text-[11px] uppercase tracking-[0.08em] text-white/45">{activePoint.point.trade_type || "Trade"}</p>
            <div className="mt-2 grid grid-cols-2 gap-x-3 gap-y-1.5 text-[11px] tabular-nums">
              <span className="text-white/55">Running member</span><span className="text-right font-medium text-cyan-200">{pct(activePoint.point.cumulative_return_pct)}</span>
              {metric === "return" ? (
                <>
                  <span className="text-white/55">Running {benchmarkLabel}</span><span className="text-right">{pct(activePoint.point.running_benchmark_return_pct)}</span>
                </>
              ) : (
                <>
                  <span className="text-white/55">Running alpha</span><span className="text-right">{pct(activePoint.point.cumulative_alpha_pct)}</span>
                </>
              )}
              <span className="text-white/55">Trade return</span><span className="text-right">{pct(activePoint.point.return_pct)}</span>
              <span className="text-white/55">Alpha</span><span className="text-right">{pct(activePoint.point.alpha_pct)}</span>
              <span className="text-white/55">Holding days</span><span className="text-right">{activePoint.point.holding_days ?? "—"}</span>
            </div>
          </div>
        )}
      </div>

      <div className="mt-2 flex flex-wrap items-center gap-3 text-[11px] text-white/45">
        <span className="uppercase tracking-[0.08em] text-white/55">{metric === "return" ? `Running return % vs ${benchmarkLabel}` : "Running alpha %"}</span>
        <span className="inline-flex items-center gap-1.5"><span className="h-[2px] w-4 rounded bg-sky-400" /> Member</span>
        {metric === "return" && <span className="inline-flex items-center gap-1.5"><span className="h-[2px] w-4 border-t border-dashed border-slate-300/80" /> {benchmarkLabel}</span>}
      </div>
    </div>
  );
}
