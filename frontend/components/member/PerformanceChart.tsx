"use client";

import { useMemo, useState, type MouseEvent } from "react";
import type { MemberPerformancePoint } from "@/lib/api";

type Metric = "return" | "alpha";

type ChartPoint = {
  x: number;
  y: number;
  index: number;
  value: number;
  point: MemberPerformancePoint;
};

type Props = {
  series: MemberPerformancePoint[];
  metric: Metric;
  benchmarkSymbol: string;
};

const WIDTH = 680;
const HEIGHT = 240;
const MARGIN = { top: 14, right: 12, bottom: 30, left: 52 };

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
  return date.toLocaleDateString("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
  });
}

function clamp(value: number, min: number, max: number) {
  return Math.max(min, Math.min(max, value));
}

function valueForMetric(point: MemberPerformancePoint, metric: Metric) {
  return metric === "alpha" ? point.cumulative_alpha_pct : point.cumulative_return_pct;
}

export function PerformanceChart({ series, metric, benchmarkSymbol }: Props) {
  const [hoveredIndex, setHoveredIndex] = useState<number | null>(null);

  const chart = useMemo(() => {
    const innerWidth = WIDTH - MARGIN.left - MARGIN.right;
    const innerHeight = HEIGHT - MARGIN.top - MARGIN.bottom;

    const memberValues = series.map((point) => valueForMetric(point, metric));
    const memberPoints = memberValues
      .map((value, index) => {
        if (typeof value !== "number" || !Number.isFinite(value)) return null;
        const x =
          series.length === 1
            ? MARGIN.left + innerWidth / 2
            : MARGIN.left + (index / (series.length - 1)) * innerWidth;
        return { index, x, value, point: series[index] };
      })
      .filter(Boolean) as Array<{ index: number; x: number; value: number; point: MemberPerformancePoint }>;

    const benchmarkPoints =
      metric === "return"
        ? series
            .map((point, index) => {
              const value = point.cumulative_benchmark_return_pct;
              if (typeof value !== "number" || !Number.isFinite(value)) return null;
              const x =
                series.length === 1
                  ? MARGIN.left + innerWidth / 2
                  : MARGIN.left + (index / (series.length - 1)) * innerWidth;
              return { index, x, value };
            })
            .filter(Boolean) as Array<{ index: number; x: number; value: number }>
        : [];

    const allValues = [...memberPoints.map((item) => item.value), ...benchmarkPoints.map((item) => item.value)];
    if (!allValues.length || memberPoints.length < 2) return null;

    const rawMin = Math.min(...allValues);
    const rawMax = Math.max(...allValues);
    const spread = Math.max(rawMax - rawMin, 1);
    const pad = spread * 0.14;
    const min = rawMin - pad;
    const max = rawMax + pad;
    const range = Math.max(max - min, 1);

    const yFor = (value: number) => MARGIN.top + innerHeight - ((value - min) / range) * innerHeight;

    const points: ChartPoint[] = memberPoints.map((item) => ({
      ...item,
      y: yFor(item.value),
    }));

    const benchmarkPath = benchmarkPoints.map((item) => `${item.x},${yFor(item.value)}`).join(" ");
    const memberPath = points.map((item) => `${item.x},${item.y}`).join(" ");

    const yTicks = Array.from({ length: 5 }, (_, index) => {
      const ratio = index / 4;
      const value = max - ratio * range;
      return {
        value,
        y: MARGIN.top + ratio * innerHeight,
      };
    });

    const desiredXTicks = 6;
    const xTickIndexes = Array.from(
      new Set(
        Array.from({ length: Math.min(desiredXTicks, series.length) }, (_, tick) => {
          if (series.length === 1) return 0;
          return Math.round((tick / (Math.min(desiredXTicks, series.length) - 1)) * (series.length - 1));
        }),
      ),
    );

    const xTicks = xTickIndexes.map((index) => {
      const x =
        series.length === 1
          ? MARGIN.left + innerWidth / 2
          : MARGIN.left + (index / (series.length - 1)) * innerWidth;
      return {
        index,
        x,
        label: formatDateCompact(series[index]?.asof_date),
      };
    });

    return {
      points,
      memberPath,
      benchmarkPath,
      yTicks,
      xTicks,
      min,
      max,
      innerHeight,
      innerWidth,
    };
  }, [series, metric]);

  if (!chart) return null;

  const activePoint = hoveredIndex == null ? null : chart.points.find((point) => point.index === hoveredIndex) ?? null;

  const handleMove = (event: MouseEvent<SVGSVGElement>) => {
    if (!chart.points.length) return;
    const bounds = event.currentTarget.getBoundingClientRect();
    const mouseX = ((event.clientX - bounds.left) / bounds.width) * WIDTH;
    let nearest = chart.points[0];
    let distance = Math.abs(mouseX - nearest.x);
    for (const point of chart.points) {
      const nextDistance = Math.abs(mouseX - point.x);
      if (nextDistance < distance) {
        nearest = point;
        distance = nextDistance;
      }
    }
    setHoveredIndex(nearest.index);
  };

  return (
    <div className="mt-3">
      <div className="relative overflow-hidden rounded-xl border border-white/10 bg-[#06111c] p-2.5">
        <svg
          viewBox={`0 0 ${WIDTH} ${HEIGHT}`}
          className="h-56 w-full"
          onMouseMove={handleMove}
          onMouseLeave={() => setHoveredIndex(null)}
        >
          {chart.yTicks.map((tick) => (
            <g key={`y-grid-${tick.y}`}>
              <line
                x1={MARGIN.left}
                x2={WIDTH - MARGIN.right}
                y1={tick.y}
                y2={tick.y}
                stroke="rgba(148,163,184,0.16)"
                strokeWidth="1"
              />
              <text
                x={MARGIN.left - 8}
                y={tick.y + 4}
                textAnchor="end"
                className="fill-white/50 text-[11px] tabular-nums"
              >
                {pct(tick.value)}
              </text>
            </g>
          ))}

          {chart.xTicks.map((tick) => (
            <g key={`x-grid-${tick.index}`}>
              <line
                x1={tick.x}
                x2={tick.x}
                y1={MARGIN.top}
                y2={HEIGHT - MARGIN.bottom}
                stroke="rgba(148,163,184,0.1)"
                strokeWidth="1"
              />
              <text x={tick.x} y={HEIGHT - 10} textAnchor="middle" className="fill-white/45 text-[11px]">
                {tick.label}
              </text>
            </g>
          ))}

          <line
            x1={MARGIN.left}
            x2={MARGIN.left}
            y1={MARGIN.top}
            y2={HEIGHT - MARGIN.bottom}
            stroke="rgba(148,163,184,0.32)"
            strokeWidth="1"
          />
          <line
            x1={MARGIN.left}
            x2={WIDTH - MARGIN.right}
            y1={HEIGHT - MARGIN.bottom}
            y2={HEIGHT - MARGIN.bottom}
            stroke="rgba(148,163,184,0.32)"
            strokeWidth="1"
          />

          {metric === "return" && chart.benchmarkPath && (
            <polyline
              fill="none"
              stroke="rgba(148,163,184,0.68)"
              strokeDasharray="6 4"
              strokeWidth="1.8"
              points={chart.benchmarkPath}
            />
          )}

          <polyline
            fill="none"
            stroke="rgba(14,165,233,0.95)"
            strokeWidth="2.5"
            points={chart.memberPath}
            strokeLinecap="round"
            strokeLinejoin="round"
          />

          {activePoint && (
            <line
              x1={activePoint.x}
              x2={activePoint.x}
              y1={MARGIN.top}
              y2={HEIGHT - MARGIN.bottom}
              stroke="rgba(56,189,248,0.45)"
              strokeWidth="1"
              strokeDasharray="4 4"
            />
          )}

          {chart.points.map((point) => {
            const active = point.index === activePoint?.index;
            return (
              <circle
                key={`${point.point.event_id}-${point.index}`}
                cx={point.x}
                cy={point.y}
                r={active ? 4.8 : 2.1}
                fill={active ? "rgba(34,211,238,0.98)" : "rgba(56,189,248,0.4)"}
                stroke={active ? "rgba(186,230,253,0.85)" : "rgba(125,211,252,0.25)"}
                strokeWidth={active ? 1.4 : 1}
                style={{ opacity: activePoint ? (active ? 1 : 0.25) : 0.18 }}
              />
            );
          })}
        </svg>

        {activePoint && (
          <div
            className="pointer-events-none absolute z-20 min-w-[220px] rounded-lg border border-white/15 bg-[#071626]/95 px-3 py-2.5 text-xs text-white/85 shadow-[0_12px_30px_rgba(2,6,23,0.5)] backdrop-blur"
            style={{
              left: `${clamp((activePoint.x / WIDTH) * 100 + 2, 2, 65)}%`,
              top: `${clamp((activePoint.y / HEIGHT) * 100 - 12, 2, 62)}%`,
            }}
          >
            <div className="flex items-center justify-between gap-2">
              <p className="font-semibold tracking-wide text-white">{(activePoint.point.symbol ?? "—").toUpperCase()}</p>
              <p className="text-[11px] text-white/55">{formatDateFull(activePoint.point.asof_date)}</p>
            </div>
            <p className="mt-0.5 text-[11px] uppercase tracking-[0.08em] text-white/45">
              {activePoint.point.trade_type || "Trade"}
            </p>
            <div className="mt-2 grid grid-cols-2 gap-x-3 gap-y-1.5 text-[11px] tabular-nums">
              <span className="text-white/55">Cumulative</span>
              <span className="text-right font-medium text-cyan-200">{pct(activePoint.value)}</span>
              <span className="text-white/55">Trade return</span>
              <span className="text-right">{pct(activePoint.point.return_pct)}</span>
              <span className="text-white/55">Benchmark</span>
              <span className="text-right">{pct(activePoint.point.benchmark_return_pct)}</span>
              <span className="text-white/55">Alpha</span>
              <span className="text-right">{pct(activePoint.point.alpha_pct)}</span>
              <span className="text-white/55">Holding days</span>
              <span className="text-right">{activePoint.point.holding_days ?? "—"}</span>
            </div>
          </div>
        )}
      </div>

      <div className="mt-2 flex flex-wrap items-center gap-3 text-[11px] text-white/45">
        <span className="uppercase tracking-[0.08em] text-white/55">
          {metric === "return" ? "Running return %" : "Running alpha %"}
          {metric === "return" ? ` vs ${benchmarkSymbol}` : ""}
        </span>
        <span className="inline-flex items-center gap-1.5">
          <span className="h-[2px] w-4 rounded bg-sky-400" /> Solid = Member
        </span>
        {metric === "return" && (
          <span className="inline-flex items-center gap-1.5">
            <span className="h-[2px] w-4 border-t border-dashed border-slate-300/80" /> Dashed = Benchmark
          </span>
        )}
      </div>
    </div>
  );
}
