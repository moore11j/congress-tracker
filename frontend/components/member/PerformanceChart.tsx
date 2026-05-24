"use client";

import { useMemo, useState, type MouseEvent } from "react";
import type { BenchmarkPerformancePoint, MemberPerformancePoint } from "@/lib/api";
import { getSvgLocalPoint } from "@/lib/chartPointer";

type Metric = "return" | "alpha";

export type MemberPortfolioEventMarker = {
  id: string;
  date: string;
  symbol: string;
  side: string;
  trade_date?: string | null;
  filing_date?: string | null;
  value?: number | null;
  price?: number | null;
  return_pct?: number | null;
};

type Props = {
  memberSeries: MemberPerformancePoint[];
  benchmarkSeries: BenchmarkPerformancePoint[];
  metric: Metric;
  benchmarkLabel: string;
  subjectLabel?: string;
  chartLabel?: string;
  events?: MemberPortfolioEventMarker[];
};

const WIDTH = 1000;
const HEIGHT = 320;
const MARGIN = { top: 18, right: 84, bottom: 34, left: 64 };
const READOUT_EDGE_OFFSET = 14;
const READOUT_WIDTH = "min(330px, calc(100% - 24px))";

function pct(value: number | null | undefined, digits = 2) {
  if (value == null || !Number.isFinite(value)) return "-";
  const sign = value > 0 ? "+" : "";
  return `${sign}${value.toFixed(digits)}%`;
}

function money(value: number | null | undefined, digits = 2) {
  if (value == null || !Number.isFinite(value)) return "-";
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: Math.abs(value) >= 1000 ? 0 : digits,
    maximumFractionDigits: Math.abs(value) >= 1000 ? 0 : digits,
  }).format(value);
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

function dateKey(raw: string | null | undefined) {
  if (!raw) return null;
  return raw.slice(0, 10);
}

function scaleBounds(values: number[], padRatio = 0.14) {
  const min = Math.min(...values);
  const max = Math.max(...values);
  const spread = Math.max(max - min, 1.2);
  const padding = spread * padRatio;
  return { min: min - padding, max: max + padding, range: Math.max(spread + padding * 2, 1) };
}

function clamp(value: number, min: number, max: number) {
  return Math.max(min, Math.min(max, value));
}

function readoutHorizontalStyle(x: number) {
  const percent = `${((x / WIDTH) * 100).toFixed(2)}%`;
  const maxLeft = `calc(100% - ${READOUT_WIDTH} - 12px)`;
  const preferredLeft =
    x > WIDTH - MARGIN.right - 180
      ? `calc(${percent} - ${READOUT_EDGE_OFFSET}px - ${READOUT_WIDTH})`
      : `calc(${percent} + ${READOUT_EDGE_OFFSET}px)`;
  return { left: `clamp(12px, ${preferredLeft}, ${maxLeft})` };
}

function readoutVerticalStyle(y: number) {
  return y > HEIGHT / 2 ? { bottom: "12px" } : { top: "12px" };
}

function markerTone(side: string) {
  return side.toLowerCase() === "sell"
    ? { fill: "#fb7185", stroke: "rgba(127,29,29,0.9)", label: "Sell marker down arrow" }
    : { fill: "#34d399", stroke: "rgba(6,78,59,0.9)", label: "Buy marker up arrow" };
}

export function PerformanceChart({
  memberSeries,
  benchmarkSeries,
  metric,
  benchmarkLabel,
  subjectLabel = "Profile",
  chartLabel,
  events = [],
}: Props) {
  const [activeReadout, setActiveReadout] = useState<{ index: number; pinned: boolean } | null>(null);

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

    const eventGroups = new Map<string, MemberPortfolioEventMarker[]>();
    for (const event of events) {
      const key = dateKey(event.date);
      if (!key) continue;
      const list = eventGroups.get(key) ?? [];
      list.push(event);
      eventGroups.set(key, list);
    }
    const eventMarkers = [...eventGroups.entries()]
      .map(([date, groupedEvents]) => {
        const time = toTime(date);
        if (time == null) return null;
        const nearest = points.reduce((current, point) =>
          Math.abs(point.time - time) < Math.abs(current.time - time) ? point : current,
        );
        return {
          date,
          x: nearest.x,
          y: nearest.y,
          events: groupedEvents.map((event, index) => {
            const offset = (index - (groupedEvents.length - 1) / 2) * 16;
            const isSell = event.side.toLowerCase() === "sell";
            return {
              event,
              x: clamp(nearest.x + offset, MARGIN.left + 10, WIDTH - MARGIN.right - 10),
              y: clamp(nearest.y + (isSell ? 16 : -16), MARGIN.top + 14, HEIGHT - MARGIN.bottom - 14),
            };
          }),
        };
      })
      .filter(Boolean) as Array<{
      date: string;
      x: number;
      y: number;
      events: Array<{ event: MemberPortfolioEventMarker; x: number; y: number }>;
    }>;

    return { innerWidth, xFor, points, benchmarkRenderPoints, profilePath, benchmarkPath, yTicks, tickIndexes, eventGroups, eventMarkers };
  }, [memberSeries, benchmarkSeries, metric, events]);

  if (!chart) return null;

  const activePoint = activeReadout == null ? null : chart.points[activeReadout.index] ?? null;
  const activeBenchmarkPoint =
    activePoint == null || chart.benchmarkRenderPoints.length === 0
      ? null
      : chart.benchmarkRenderPoints.reduce((nearest, point) =>
          Math.abs(point.time - activePoint.time) < Math.abs(nearest.time - activePoint.time) ? point : nearest,
        );
  const activeDate = dateKey(activePoint?.point.asof_date);
  const activeEvents = activeDate ? chart.eventGroups.get(activeDate) ?? [] : [];

  const handleMove = (event: MouseEvent<SVGSVGElement>) => {
    if (activeReadout?.pinned) return;
    const local = getSvgLocalPoint(event.currentTarget, event.clientX, event.clientY);
    if (!local) {
      setActiveReadout(null);
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
    setActiveReadout({ index: nearestIndex, pinned: false });
  };

  const handleClick = (event: MouseEvent<SVGSVGElement>) => {
    const local = getSvgLocalPoint(event.currentTarget, event.clientX, event.clientY);
    if (!local) {
      setActiveReadout(null);
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
    setActiveReadout((current) =>
      current?.pinned && current.index === nearestIndex ? null : { index: nearestIndex, pinned: true },
    );
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
          {chartLabel ?? (metric === "return" ? "Equal-Weight Outcome Return" : "Equal-Weight Outcome Alpha")}
        </span>
      </div>

      <div className="relative">
        <svg
          viewBox={`0 0 ${WIDTH} ${HEIGHT}`}
          className="h-[320px] w-full"
          onMouseMove={handleMove}
          onMouseLeave={() => setActiveReadout((current) => (current?.pinned ? current : null))}
          onClick={handleClick}
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

          {chart.eventMarkers.map((marker) => (
            <g key={`event-${marker.date}`}>
              <line x1={marker.x} x2={marker.x} y1={MARGIN.top} y2={HEIGHT - MARGIN.bottom} stroke="rgba(52,211,153,0.14)" strokeWidth="1" />
              {marker.events.map(({ event, x, y }) => {
                const tone = markerTone(event.side);
                const isSell = event.side.toLowerCase() === "sell";
                return (
                  <path
                    key={event.id}
                    d={isSell ? "M0 10 L9 0 H4 V-10 H-4 V0 H-9 Z" : "M0 -10 L9 0 H4 V10 H-4 V0 H-9 Z"}
                    transform={`translate(${x} ${y})`}
                    fill={tone.fill}
                    stroke={tone.stroke}
                    strokeWidth="1.5"
                    aria-label={tone.label}
                    className="drop-shadow-[0_2px_4px_rgba(0,0,0,0.5)]"
                  />
                );
              })}
              {marker.events.length > 1 ? (
                <text x={marker.x} y={marker.y - 7} textAnchor="middle" className="fill-emerald-100 text-[10px] font-semibold">
                  {marker.events.length}
                </text>
              ) : null}
            </g>
          ))}

          {activePoint ? (
            <>
              <line x1={activePoint.x} x2={activePoint.x} y1={MARGIN.top} y2={HEIGHT - MARGIN.bottom} stroke="rgba(226,232,240,0.55)" strokeWidth="1.2" />
              <circle cx={activePoint.x} cy={activePoint.y} r={4} fill="rgba(167,243,208,0.96)" stroke="rgba(16,185,129,0.6)" strokeWidth="1.2" />
              {metric === "return" && activeBenchmarkPoint ? (
                <circle cx={activeBenchmarkPoint.x} cy={activeBenchmarkPoint.y} r={3.2} fill="rgba(226,232,240,0.92)" stroke="rgba(148,163,184,0.65)" strokeWidth="1.1" />
              ) : null}
            </>
          ) : null}
        </svg>

        {activePoint ? (
          <div
            className="pointer-events-none absolute z-20 rounded-lg border border-white/15 bg-[#050b13]/95 p-3 text-xs text-slate-200 shadow-[0_18px_45px_rgba(0,0,0,0.48)] backdrop-blur"
            style={{
              width: READOUT_WIDTH,
              maxHeight: "calc(100% - 24px)",
              overflowY: "auto",
              ...readoutHorizontalStyle(activePoint.x),
              ...readoutVerticalStyle(activePoint.y),
            }}
          >
            <div className="flex items-start justify-between gap-3">
              <div>
                <p className="font-semibold text-white">{formatDateFull(activePoint.point.asof_date)}</p>
                <p className="mt-0.5 text-[10px] uppercase tracking-[0.14em] text-slate-500">
                  {activeReadout?.pinned ? "Pinned readout" : "Crosshair readout"}
                </p>
              </div>
              <p className="rounded border border-emerald-300/25 bg-emerald-300/10 px-2 py-1 font-semibold text-emerald-100">
                {money(activePoint.point.strategy_value)}
              </p>
            </div>
            <div className="mt-3 grid grid-cols-2 gap-x-4 gap-y-1.5">
              <span className="text-slate-500">Portfolio value</span>
              <span className="text-right tabular-nums text-white">{money(activePoint.point.strategy_value)}</span>
              <span className="text-slate-500">{benchmarkLabel} value</span>
              <span className="text-right tabular-nums text-slate-100">{money(activePoint.point.benchmark_value)}</span>
              <span className="text-slate-500">Portfolio return</span>
              <span className="text-right tabular-nums text-slate-100">{pct(activePoint.point.strategy_return_pct ?? activePoint.point.cumulative_return_pct)}</span>
              <span className="text-slate-500">{benchmarkLabel} return</span>
              <span className="text-right tabular-nums text-slate-100">
                {pct(activePoint.point.running_benchmark_return_pct ?? activeBenchmarkPoint?.point.cumulative_return_pct)}
              </span>
              <span className="text-slate-500">Relative vs benchmark</span>
              <span className={`text-right tabular-nums ${activePoint.point.cumulative_alpha_pct != null && activePoint.point.cumulative_alpha_pct >= 0 ? "text-emerald-300" : "text-rose-300"}`}>
                {pct(activePoint.point.cumulative_alpha_pct)}
              </span>
              <span className="text-slate-500">Active positions</span>
              <span className="text-right tabular-nums text-white">{activePoint.point.active_positions ?? "-"}</span>
            </div>
            <div className="mt-3 border-t border-white/10 pt-3">
              <p className="text-[10px] font-semibold uppercase tracking-[0.14em] text-slate-500">Events on this marker</p>
              {activeEvents.length > 0 ? (
                <div className="mt-2 max-h-32 space-y-2 overflow-hidden">
                  {activeEvents.slice(0, 4).map((event) => (
                    <div key={event.id} className="rounded border border-white/10 bg-white/[0.035] px-2 py-1.5">
                      <div className="flex items-center justify-between gap-3">
                        <span className="font-semibold text-white">{event.symbol}</span>
                        <span className={event.side.toLowerCase() === "sell" ? "text-rose-300" : "text-emerald-300"}>{event.side}</span>
                      </div>
                      <p className="mt-0.5 text-slate-400">
                        {formatDateCompact(event.trade_date ?? event.date)}
                        {event.value != null ? ` / ${money(event.value, 0)}` : ""}
                        {event.price != null ? ` @ ${money(event.price)}` : ""}
                        {event.return_pct != null ? ` / ${pct(event.return_pct)}` : ""}
                      </p>
                    </div>
                  ))}
                  {activeEvents.length > 4 ? <p className="text-slate-500">+{activeEvents.length - 4} more trades</p> : null}
                </div>
              ) : (
                <p className="mt-2 text-slate-500">No trades on this date.</p>
              )}
            </div>
          </div>
        ) : null}
      </div>
    </div>
  );
}
