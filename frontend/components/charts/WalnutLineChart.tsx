"use client";

import { useCallback, useEffect, useId, useMemo, useRef, useState, type PointerEvent, type ReactNode } from "react";
import { chartBounds, nearestChartIndex } from "@/components/charts/chartPerformanceUtils";
import { formatChartCompact } from "@/components/charts/chartFormatters";

export type WalnutLineSeries = { key: string; label: string; color: string; dashed?: boolean; areaColor?: string; values: readonly number[] };
export type WalnutLinePoint = { label: string };

type Props = {
  data: readonly WalnutLinePoint[];
  series: readonly WalnutLineSeries[];
  ariaLabel: string;
  height?: number;
  formatValue?: (value: number) => string;
  valueFormat?: "number" | "currencyCompact";
  renderTooltip?: (index: number) => ReactNode;
};

const WIDTH = 1000;
const MARGIN = { top: 18, right: 84, bottom: 34, left: 64 };

export function WalnutLineChart({ data, series, ariaLabel, height = 320, formatValue, valueFormat, renderTooltip }: Props) {
  const svgRef = useRef<SVGSVGElement>(null);
  const frame = useRef<number | null>(null);
  const latestIndex = useRef<number | null>(null);
  const [activeIndex, setActiveIndex] = useState<number | null>(null);
  const [revealed, setRevealed] = useState(false);
  const [reducedMotion, setReducedMotion] = useState(false);
  const clipId = useId().replace(/:/g, "");
  const innerWidth = WIDTH - MARGIN.left - MARGIN.right;
  const innerHeight = height - MARGIN.top - MARGIN.bottom;
  const displayedValue = formatValue ?? (valueFormat === "currencyCompact" ? formatChartCompact : valueFormat === "number" ? (value: number) => new Intl.NumberFormat("en-US", { notation: "compact", maximumFractionDigits: 1 }).format(value) : (value: number) => String(value));

  useEffect(() => {
    const media = window.matchMedia("(prefers-reduced-motion: reduce)");
    const update = () => setReducedMotion(media.matches);
    update();
    media.addEventListener("change", update);
    return () => media.removeEventListener("change", update);
  }, []);
  useEffect(() => {
    const timer = requestAnimationFrame(() => setRevealed(true));
    return () => cancelAnimationFrame(timer);
  }, []);
  useEffect(() => () => { if (frame.current !== null) cancelAnimationFrame(frame.current); }, []);

  const chart = useMemo(() => {
    if (data.length < 2 || series.length === 0) return null;
    const values = series.flatMap((item) => item.values).filter(Number.isFinite);
    if (values.length === 0) return null;
    const bounds = chartBounds(values);
    const xValues = data.map((_, index) => MARGIN.left + (index / Math.max(data.length - 1, 1)) * innerWidth);
    const yFor = (value: number) => MARGIN.top + innerHeight - ((value - bounds.min) / bounds.range) * innerHeight;
    const paths = series.map((item) => ({
      ...item,
      path: data.map((_, index) => `${xValues[index]},${yFor(item.values[index] ?? bounds.min)}`).join(" "),
      area: item.areaColor ? `${data.map((_, index) => `${xValues[index]},${yFor(item.values[index] ?? bounds.min)}`).join(" ")} ${xValues[xValues.length - 1]},${height - MARGIN.bottom} ${xValues[0]},${height - MARGIN.bottom}` : null,
      yFor,
    }));
    const yTicks = Array.from({ length: 5 }, (_, index) => ({
      y: MARGIN.top + (index / 4) * innerHeight,
      value: bounds.max - (index / 4) * bounds.range,
    }));
    const tickIndexes = [...new Set([0, Math.floor((data.length - 1) / 3), Math.floor(((data.length - 1) * 2) / 3), data.length - 1])];
    return { xValues, yFor, paths, yTicks, tickIndexes };
  }, [data, innerHeight, innerWidth, series]);

  const scheduleIndex = useCallback((clientX: number) => {
    const svg = svgRef.current;
    if (!svg || !chart) return;
    const rect = svg.getBoundingClientRect();
    const localX = MARGIN.left + ((clientX - rect.left) / Math.max(rect.width, 1)) * innerWidth;
    const index = nearestChartIndex(chart.xValues, Math.max(MARGIN.left, Math.min(WIDTH - MARGIN.right, localX)));
    if (index < 0 || index === latestIndex.current) return;
    latestIndex.current = index;
    if (frame.current !== null) return;
    frame.current = requestAnimationFrame(() => {
      frame.current = null;
      setActiveIndex(latestIndex.current);
    });
  }, [chart, innerWidth]);

  const handlePointerDown = (event: PointerEvent<SVGSVGElement>) => {
    if (event.pointerType !== "mouse") event.currentTarget.setPointerCapture(event.pointerId);
    scheduleIndex(event.clientX);
  };
  const active = activeIndex == null || !chart ? null : { index: activeIndex, x: chart.xValues[activeIndex] };
  if (!chart) return <div className="flex items-center justify-center text-sm text-slate-400" style={{ height }}>Not enough data points to draw a curve yet.</div>;

  return (
    <div className="relative" onKeyDown={(event) => {
      if (event.key === "Escape") setActiveIndex(null);
      if (event.key === "ArrowLeft" || event.key === "ArrowRight") {
        event.preventDefault();
        setActiveIndex((current) => Math.max(0, Math.min(data.length - 1, (current ?? 0) + (event.key === "ArrowLeft" ? -1 : 1))));
      }
    }}>
      <svg ref={svgRef} viewBox={`0 0 ${WIDTH} ${height}`} className="w-full outline-none" style={{ height, touchAction: "pan-y" }} role="img" aria-label={ariaLabel} tabIndex={0}
        onPointerDown={handlePointerDown} onPointerMove={(event) => scheduleIndex(event.clientX)} onPointerLeave={(event) => { if (event.pointerType === "mouse") setActiveIndex(null); }}>
        <defs><clipPath id={clipId}><rect x={MARGIN.left} y={MARGIN.top} width={innerWidth} height={innerHeight} /></clipPath></defs>
        {chart.yTicks.map((tick) => <g key={tick.y}><line x1={MARGIN.left} x2={WIDTH - MARGIN.right} y1={tick.y} y2={tick.y} stroke="rgba(148,163,184,0.12)" /><text x={WIDTH - MARGIN.right + 8} y={tick.y + 4} className="fill-slate-300/55 text-[11px] tabular-nums">{displayedValue(tick.value)}</text></g>)}
        {chart.tickIndexes.map((index) => <g key={index}><line x1={chart.xValues[index]} x2={chart.xValues[index]} y1={MARGIN.top} y2={height - MARGIN.bottom} stroke="rgba(148,163,184,0.08)" /><text x={chart.xValues[index]} y={height - 10} textAnchor="middle" className="fill-slate-400 text-[11px]">{data[index].label}</text></g>)}
        <g clipPath={`url(#${clipId})`} style={{ clipPath: reducedMotion || revealed ? "inset(0 0 0 0)" : "inset(0 100% 0 0)", transition: reducedMotion ? "none" : "clip-path 560ms cubic-bezier(.22,1,.36,1)" }}>
          {chart.paths.map((item) => item.area ? <polygon key={`${item.key}-area`} points={item.area} fill={item.areaColor} /> : null)}
          {chart.paths.map((item) => <polyline key={item.key} fill="none" stroke={item.color} strokeDasharray={item.dashed ? "6 4" : undefined} strokeWidth={item.dashed ? 2 : 2.8} strokeLinecap="round" strokeLinejoin="round" vectorEffect="non-scaling-stroke" points={item.path} />)}
        </g>
        {active ? <><line x1={active.x} x2={active.x} y1={MARGIN.top} y2={height - MARGIN.bottom} stroke="rgba(167,243,208,0.3)" strokeWidth="1.2" />{chart.paths.map((item) => <circle key={item.key} cx={active.x} cy={item.yFor(item.values[active.index] ?? 0)} r={item.dashed ? 3.2 : 4} fill={item.color} stroke="rgba(2,6,23,.8)" strokeWidth="1.2" />)}</> : null}
      </svg>
      {active ? <div className="pointer-events-none absolute top-4 z-10 w-56 rounded-2xl border border-white/10 bg-slate-950/95 px-3 py-3 text-sm shadow-xl" style={{ left: `clamp(12px, calc(${((active.x / WIDTH) * 100).toFixed(2)}% + 12px), calc(100% - 15rem))` }}>{renderTooltip ? renderTooltip(active.index) : <DefaultTooltip label={data[active.index].label} series={series} index={active.index} formatValue={displayedValue} />}</div> : null}
      <span className="sr-only">Use left and right arrow keys to inspect chart values. Touch and drag across the chart to scrub values.</span>
    </div>
  );
}

function DefaultTooltip({ label, series, index, formatValue }: { label: string; series: readonly WalnutLineSeries[]; index: number; formatValue: (value: number) => string }) {
  return <><div className="text-xs uppercase tracking-[0.18em] text-slate-500">{label}</div><div className="mt-3 space-y-2">{series.map((item) => <div key={item.key} className="flex items-center justify-between gap-3"><span className="text-slate-400">{item.label}</span><span className="font-semibold tabular-nums text-slate-100">{formatValue(item.values[index] ?? 0)}</span></div>)}</div></>;
}
