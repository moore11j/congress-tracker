"use client";

import { useMemo, useState } from "react";
import { formatDateShort } from "@/lib/format";

type DecisionTrendPoint = {
  date: string;
  score: number;
};

type RenderedPoint = DecisionTrendPoint & {
  x: number;
  y: number;
};

const width = 260;
const height = 96;
const padding = {
  top: 9,
  right: 10,
  bottom: 22,
  left: 32,
};

function clamp(value: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, value));
}

function scoreBand(score: number): string {
  if (score <= 19) return "Inactive";
  if (score <= 39) return "Weak";
  if (score <= 59) return "Moderate";
  if (score <= 79) return "Strong";
  return "Very strong";
}

function confirmationLabel(score: number, direction?: string | null): string {
  const normalized = (direction ?? "").toLowerCase();
  if (score <= 19 && normalized === "neutral") return "Inactive";
  if (normalized === "mixed") return "Conflicted confirmation";
  if (normalized === "neutral" || !normalized) return "No clear direction";
  return `${scoreBand(score)} ${normalized}`;
}

function formatAxisDate(value: string): string {
  const formatted = formatDateShort(value);
  return formatted ? formatted.replace(/, \d{4}$/, "") : value;
}

function chartStroke(direction?: string | null): string {
  if (direction === "bearish") return "stroke-rose-400";
  if (direction === "mixed") return "stroke-amber-300";
  return "stroke-emerald-300";
}

export function DecisionTrendChart({
  history,
  direction,
}: {
  history?: DecisionTrendPoint[];
  direction?: string | null;
}) {
  const [hoveredIndex, setHoveredIndex] = useState<number | null>(null);
  const points = useMemo(
    () => Array.isArray(history) ? history.filter((point) => Number.isFinite(point.score)).slice(-30) : [],
    [history],
  );

  const chart = useMemo(() => {
    if (points.length < 2) return null;

    const scores = points.map((point) => clamp(point.score, 0, 100));
    const rawMin = Math.min(...scores);
    const rawMax = Math.max(...scores);
    const spread = Math.max(rawMax - rawMin, 1);
    const yMin = clamp(Math.floor((rawMin - Math.max(4, spread * 0.15)) / 5) * 5, 0, 100);
    const yMax = clamp(Math.ceil((rawMax + Math.max(4, spread * 0.15)) / 5) * 5, 0, 100);
    const finalMin = yMax === yMin ? clamp(yMin - 5, 0, 95) : yMin;
    const finalMax = yMax === yMin ? clamp(yMax + 5, 5, 100) : yMax;
    const range = Math.max(finalMax - finalMin, 1);
    const innerWidth = width - padding.left - padding.right;
    const innerHeight = height - padding.top - padding.bottom;
    const xStep = innerWidth / Math.max(points.length - 1, 1);
    const rendered = points.map((point, index): RenderedPoint => {
      const score = clamp(point.score, 0, 100);
      return {
        ...point,
        x: padding.left + index * xStep,
        y: padding.top + ((finalMax - score) / range) * innerHeight,
      };
    });
    const yTicks = [finalMax, Math.round((finalMax + finalMin) / 2), finalMin];
    const xTicks = [0, Math.floor((points.length - 1) / 2), points.length - 1]
      .filter((value, index, values) => values.indexOf(value) === index);

    return {
      rendered,
      yTicks,
      xTicks,
      yFor: (score: number) => padding.top + ((finalMax - score) / range) * innerHeight,
    };
  }, [points]);

  if (!chart) {
    return (
      <div className="flex h-24 items-center justify-center rounded-md border border-white/10 bg-slate-950/35 px-3 text-xs font-medium text-slate-500">
        Score history unavailable
      </div>
    );
  }

  const hovered = hoveredIndex === null ? null : chart.rendered[hoveredIndex] ?? null;
  const linePoints = chart.rendered.map((point) => `${point.x.toFixed(1)},${point.y.toFixed(1)}`).join(" ");
  const stroke = chartStroke(direction);

  return (
    <div className="relative h-24 w-full">
      <svg
        className="h-full w-full overflow-visible"
        viewBox={`0 0 ${width} ${height}`}
        role="img"
        aria-label="Confirmation score history"
        onPointerMove={(event) => {
          const rect = event.currentTarget.getBoundingClientRect();
          const pointerX = ((event.clientX - rect.left) / rect.width) * width;
          let nearestIndex = 0;
          let nearestDistance = Number.POSITIVE_INFINITY;
          chart.rendered.forEach((point, index) => {
            const distance = Math.abs(point.x - pointerX);
            if (distance < nearestDistance) {
              nearestDistance = distance;
              nearestIndex = index;
            }
          });
          setHoveredIndex(nearestIndex);
        }}
        onPointerLeave={() => setHoveredIndex(null)}
      >
        <line x1={padding.left} y1={padding.top} x2={padding.left} y2={height - padding.bottom} className="stroke-white/15" />
        <line x1={padding.left} y1={height - padding.bottom} x2={width - padding.right} y2={height - padding.bottom} className="stroke-white/15" />
        {chart.yTicks.map((tick) => {
          const y = chart.yFor(tick);
          return (
            <g key={tick}>
              <line x1={padding.left} y1={y} x2={width - padding.right} y2={y} className="stroke-white/10" strokeDasharray="2 4" />
              <text x={padding.left - 6} y={y + 3} textAnchor="end" className="fill-slate-500 text-[10px] tabular-nums">
                {tick}
              </text>
            </g>
          );
        })}
        {chart.xTicks.map((index) => {
          const point = chart.rendered[index];
          const anchor = index === 0 ? "start" : index === points.length - 1 ? "end" : "middle";
          return (
            <text key={`${point.date}-${index}`} x={point.x} y={height - 5} textAnchor={anchor} className="fill-slate-500 text-[10px]">
              {formatAxisDate(point.date)}
            </text>
          );
        })}
        <polyline points={linePoints} fill="none" className={stroke} strokeWidth="2.75" strokeLinecap="round" strokeLinejoin="round" />
        {hovered ? (
          <g>
            <line x1={hovered.x} y1={padding.top} x2={hovered.x} y2={height - padding.bottom} className="stroke-white/25" />
            <circle cx={hovered.x} cy={hovered.y} r="4.2" className={`${stroke} fill-slate-950`} strokeWidth="2.2" />
          </g>
        ) : null}
      </svg>

      {hovered ? (
        <div
          className="pointer-events-none absolute z-10 min-w-36 rounded-md border border-white/10 bg-slate-950/95 px-3 py-2 text-xs shadow-xl shadow-black/40 ring-1 ring-white/5"
          style={{
            left: `${clamp((hovered.x / width) * 100, 18, 82)}%`,
            top: `${clamp((hovered.y / height) * 100, 16, 58)}%`,
            transform: "translate(-50%, -115%)",
          }}
        >
          <p className="font-semibold text-slate-100">{formatDateShort(hovered.date) ?? hovered.date}</p>
          <p className="mt-1 tabular-nums text-slate-300">Score {Math.round(hovered.score)} / 100</p>
          <p className="mt-1 font-medium text-slate-400">{confirmationLabel(hovered.score, direction)}</p>
        </div>
      ) : null}
    </div>
  );
}
