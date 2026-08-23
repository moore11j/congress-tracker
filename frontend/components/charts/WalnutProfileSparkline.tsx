"use client";

import { useEffect, useRef, useState, type PointerEvent } from "react";

type Point = { label: string; value: number };

export function WalnutProfileSparkline({ id, metricLabel, valueFormat, points }: { id: string; metricLabel: string; valueFormat?: string; points: Point[] }) {
  const [revealed, setRevealed] = useState(false);
  const [activeIndex, setActiveIndex] = useState<number | null>(null);
  const frame = useRef<number | null>(null);
  const values = points.map((point) => point.value);
  const max = Math.max(...values, 1);
  const min = Math.min(...values, max);
  const range = Math.max(max - min, max * 0.18, 1);
  const plotTop = 6;
  const plotBottom = 56;
  const chartPoints = points.map((point, index) => {
    const x = points.length === 1 ? 59 : (index / (points.length - 1)) * 118 + 1;
    const y = plotBottom - ((point.value - min) / range) * (plotBottom - plotTop);
    return { x, y: Math.max(plotTop, Math.min(plotBottom, y)) };
  });
  const line = chartPoints.map((point) => `${point.x},${point.y}`).join(" ");
  const area = `M ${chartPoints[0]?.x ?? 1} ${plotBottom} L ${line} L ${chartPoints.at(-1)?.x ?? 119} ${plotBottom} Z`;
  const active = activeIndex == null ? null : chartPoints[activeIndex];
  const activePoint = activeIndex == null ? null : points[activeIndex];

  useEffect(() => {
    const animationFrame = requestAnimationFrame(() => setRevealed(true));
    return () => cancelAnimationFrame(animationFrame);
  }, []);
  useEffect(() => () => { if (frame.current !== null) cancelAnimationFrame(frame.current); }, []);

  const inspect = (event: PointerEvent<SVGSVGElement>) => {
    const rect = event.currentTarget.getBoundingClientRect();
    const index = Math.max(0, Math.min(chartPoints.length - 1, Math.round(((event.clientX - rect.left) / Math.max(rect.width, 1)) * (chartPoints.length - 1))));
    if (frame.current !== null) cancelAnimationFrame(frame.current);
    frame.current = requestAnimationFrame(() => { setActiveIndex(index); frame.current = null; });
  };

  return <div className="relative z-30 min-w-0" aria-label={`${metricLabel}: prior period compared with latest period`}>
    <p className="mb-1 truncate text-[9px] font-semibold uppercase tracking-[.1em] text-slate-500">Y · {metricLabel}</p>
    <div className="grid grid-cols-[2.35rem_minmax(0,1fr)] gap-1">
      <div aria-hidden className="flex h-14 flex-col justify-between pb-0.5 text-right text-[8px] leading-none tabular-nums text-slate-500"><span>{formatValue(max, valueFormat)}</span><span>{formatValue(min, valueFormat)}</span></div>
      <svg viewBox="0 0 120 60" preserveAspectRatio="none" className="h-14 w-full overflow-visible" role="img" aria-label={`${metricLabel}, from ${points[0]?.label ?? "latest period"} to ${points.at(-1)?.label ?? "latest period"}`} style={{ touchAction: "manipulation" }} onPointerMove={inspect} onPointerDown={inspect} onPointerLeave={(event) => { if (event.pointerType === "mouse") setActiveIndex(null); }}>
        <defs><linearGradient id={`${id}-area`} x1="0" x2="0" y1="0" y2="1"><stop stopColor="#42d3a7" stopOpacity=".42" /><stop offset="1" stopColor="#42d3a7" stopOpacity=".04" /></linearGradient><clipPath id={`${id}-clip`}><rect x="0" y="0" width="120" height="60" /></clipPath></defs>
        <path d="M 1 18 H 119 M 1 37 H 119" stroke="rgba(148,163,184,.16)" strokeWidth=".6" vectorEffect="non-scaling-stroke" />
        <g clipPath={`url(#${id}-clip)`} style={{ clipPath: revealed ? "inset(0 0 0 0)" : "inset(0 100% 0 0)", transition: "clip-path 460ms cubic-bezier(.22,1,.36,1)" }}><path d={area} fill={`url(#${id}-area)`} /><polyline points={line} fill="none" stroke="#78f3c3" strokeWidth="1.8" vectorEffect="non-scaling-stroke" /></g>
        {active ? <><line x1={active.x} x2={active.x} y1="4" y2="57" stroke="rgba(226,232,240,.5)" strokeDasharray="2 2" vectorEffect="non-scaling-stroke" /><circle cx={active.x} cy={active.y} r="2.6" fill="#78f3c3" stroke="#07101f" strokeWidth=".8" vectorEffect="non-scaling-stroke" /></> : null}
      </svg>
    </div>
    <div className="mt-1 grid grid-cols-[2.35rem_minmax(0,1fr)] gap-1 text-[8px] uppercase tracking-[.08em] text-slate-500"><span>Y</span><span className="flex justify-between"><span>{points[0]?.label ?? "Latest"}</span><span>{points.at(-1)?.label ?? "Latest"}</span></span></div>
    <p className="mt-1 text-right text-[8px] font-semibold uppercase tracking-[.08em] text-slate-500">X · reporting period</p>
    {activePoint ? <div role="status" className="pointer-events-none absolute bottom-full left-1/2 z-[60] mb-2 w-max max-w-52 -translate-x-1/2 rounded-md border border-emerald-300/30 bg-slate-950/95 px-3 py-2 text-left text-xs shadow-[0_18px_45px_rgba(0,0,0,.55)] backdrop-blur"><p className="font-semibold text-white">{activePoint.label}</p><p className="mt-0.5 text-slate-300">{metricLabel}: <span className="font-semibold tabular-nums text-emerald-200">{formatValue(activePoint.value, valueFormat)}</span></p></div> : null}
  </div>;
}

function formatValue(value: number, valueFormat?: string) {
  return valueFormat === "currency" ? new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", notation: "compact", maximumFractionDigits: 1 }).format(value) : new Intl.NumberFormat("en-US", { notation: "compact", maximumFractionDigits: 1 }).format(value);
}
