"use client";

import { useEffect, useRef, useState, type KeyboardEvent, type PointerEvent } from "react";

type ActivityPoint = { label: string; positive: number; negative?: number; line?: number };
type ValueFormat = "number" | "currencyCompact";

function formatValue(value: number, format: ValueFormat) {
  return new Intl.NumberFormat("en-US", format === "currencyCompact" ? { style: "currency", currency: "USD", notation: "compact", maximumFractionDigits: 1 } : { notation: "compact", maximumFractionDigits: 1 }).format(value);
}

export function WalnutActivityBarChart({ data, ariaLabel, positiveLabel, negativeLabel, lineLabel, height = 224, valueFormat = "currencyCompact", lineValueFormat = "number" }: { data: ActivityPoint[]; ariaLabel: string; positiveLabel: string; negativeLabel?: string; lineLabel?: string; height?: number; valueFormat?: ValueFormat; lineValueFormat?: ValueFormat }) {
  const [activeIndex, setActiveIndex] = useState<number | null>(null);
  const [revealed, setRevealed] = useState(false);
  const [reducedMotion, setReducedMotion] = useState(false);
  const frame = useRef<number | null>(null);
  const barMax = Math.max(...data.map((point) => Math.max(Math.max(0, point.positive), Math.abs(Math.min(0, point.negative ?? 0)))), 1);
  const lineMax = Math.max(...data.map((point) => Math.max(0, point.line ?? 0)), 1);
  const hasNegative = Boolean(negativeLabel) && data.some((point) => (point.negative ?? 0) !== 0);
  const zeroY = hasNegative ? 50 : 92;
  const positiveHeight = hasNegative ? 39 : 76;
  const negativeHeight = hasNegative ? 39 : 0;
  const xFor = (index: number) => 6 + (index / Math.max(data.length - 1, 1)) * 88;
  const linePoints = lineLabel ? data.map((point, index) => `${xFor(index)},${92 - (Math.max(0, point.line ?? 0) / lineMax) * 76}`).join(" ") : "";
  const active = activeIndex == null ? null : data[activeIndex];

  useEffect(() => {
    const media = window.matchMedia("(prefers-reduced-motion: reduce)");
    const update = () => setReducedMotion(media.matches);
    update();
    media.addEventListener("change", update);
    return () => media.removeEventListener("change", update);
  }, []);
  useEffect(() => {
    const animationFrame = requestAnimationFrame(() => setRevealed(true));
    return () => cancelAnimationFrame(animationFrame);
  }, []);
  useEffect(() => () => { if (frame.current !== null) cancelAnimationFrame(frame.current); }, []);

  const activateFromX = (clientX: number, bounds: DOMRect) => {
    if (!data.length) return;
    const index = Math.max(0, Math.min(data.length - 1, Math.round(((clientX - bounds.left) / Math.max(bounds.width, 1)) * (data.length - 1))));
    if (frame.current !== null) cancelAnimationFrame(frame.current);
    frame.current = requestAnimationFrame(() => { setActiveIndex(index); frame.current = null; });
  };
  const onPointerMove = (event: PointerEvent<SVGSVGElement>) => activateFromX(event.clientX, event.currentTarget.getBoundingClientRect());
  const onPointerDown = (event: PointerEvent<SVGSVGElement>) => { event.currentTarget.setPointerCapture(event.pointerId); activateFromX(event.clientX, event.currentTarget.getBoundingClientRect()); };
  const onKeyDown = (event: KeyboardEvent<SVGSVGElement>) => {
    if (event.key === "Escape") return setActiveIndex(null);
    if (event.key !== "ArrowLeft" && event.key !== "ArrowRight") return;
    event.preventDefault();
    setActiveIndex((current) => Math.max(0, Math.min(data.length - 1, (current ?? 0) + (event.key === "ArrowLeft" ? -1 : 1))));
  };

  if (!data.length) return <p className="flex h-48 items-center justify-center text-sm text-slate-400">No activity is available for this period.</p>;
  return <div className="relative min-w-0" style={{ height }}>
    <div className="mb-3 flex flex-wrap gap-x-4 gap-y-1 text-[10px] text-slate-400"><span className="inline-flex items-center gap-1.5"><i className="h-2 w-2 rounded-sm bg-emerald-400" />{positiveLabel}</span>{negativeLabel ? <span className="inline-flex items-center gap-1.5"><i className="h-2 w-2 rounded-sm bg-rose-400" />{negativeLabel}</span> : null}{lineLabel ? <span className="inline-flex items-center gap-1.5"><i className="h-2 w-2 rounded-full bg-blue-400" />{lineLabel}</span> : null}</div>
    <svg viewBox="0 0 100 100" preserveAspectRatio="none" className="h-[calc(100%-2rem)] w-full overflow-visible outline-none" role="img" aria-label={ariaLabel} tabIndex={0} style={{ touchAction: "pan-y" }} onPointerMove={onPointerMove} onPointerDown={onPointerDown} onPointerLeave={(event) => { if (event.pointerType === "mouse") setActiveIndex(null); }} onKeyDown={onKeyDown}>
      {[12, 31, 50, 69, 88].map((y) => <line key={y} x1="2" x2="98" y1={y} y2={y} stroke="rgba(148,163,184,.14)" vectorEffect="non-scaling-stroke" />)}
      <line x1="2" x2="98" y1={zeroY} y2={zeroY} stroke="rgba(148,163,184,.32)" vectorEffect="non-scaling-stroke" />
      {data.map((point, index) => {
        const x = xFor(index);
        const width = Math.max(1.1, Math.min(3.2, 30 / data.length));
        const positive = (Math.max(0, point.positive) / barMax) * positiveHeight;
        const negative = (Math.abs(Math.min(0, point.negative ?? 0)) / barMax) * negativeHeight;
        const emphasized = activeIndex == null || activeIndex === index;
        return <g key={point.label} opacity={emphasized ? 1 : .38} style={{ transition: "opacity 140ms ease-out" }}><rect x={x - width - .35} y={zeroY - positive} width={width} height={positive} rx=".55" fill="#42d3a7" style={{ transformBox: "fill-box", transformOrigin: "center bottom", transform: `scaleY(${revealed || reducedMotion ? 1 : 0})`, transition: reducedMotion ? "opacity 120ms ease-out" : `transform 460ms cubic-bezier(.22,1,.36,1) ${index * 18}ms, opacity 140ms ease-out` }} />{negativeLabel ? <rect x={x + .35} y={zeroY} width={width} height={negative} rx=".55" fill="#fb7185" style={{ transformBox: "fill-box", transformOrigin: "center top", transform: `scaleY(${revealed || reducedMotion ? 1 : 0})`, transition: reducedMotion ? "opacity 120ms ease-out" : `transform 460ms cubic-bezier(.22,1,.36,1) ${index * 18}ms, opacity 140ms ease-out` }} /> : null}</g>;
      })}
      {lineLabel ? <polyline points={linePoints} fill="none" stroke="#60a5fa" strokeWidth="1.65" vectorEffect="non-scaling-stroke" style={{ opacity: revealed || reducedMotion ? 1 : 0, transition: reducedMotion ? "none" : "opacity 360ms ease-out 180ms" }} /> : null}
      {activeIndex !== null ? <line x1={xFor(activeIndex)} x2={xFor(activeIndex)} y1="6" y2="94" stroke="rgba(226,232,240,.5)" strokeDasharray="2 2" vectorEffect="non-scaling-stroke" /> : null}
    </svg>
    <div className="pointer-events-none absolute inset-x-0 bottom-0 grid gap-1 text-[10px] text-slate-500" style={{ gridTemplateColumns: `repeat(${data.length}, minmax(0, 1fr))` }}>{data.map((point, index) => <span key={point.label} className={`truncate text-center ${activeIndex === index ? "text-slate-200" : ""}`}>{point.label}</span>)}</div>
    {active ? <div className="pointer-events-none absolute left-1/2 top-7 z-10 w-52 -translate-x-1/2 rounded-md border border-white/10 bg-slate-950/95 px-3 py-2 text-xs shadow-xl"><p className="font-semibold text-white">{active.label}</p><div className="mt-1.5 space-y-1 text-slate-300"><p><span className="text-emerald-300">{positiveLabel}</span> {formatValue(active.positive, valueFormat)}</p>{negativeLabel ? <p><span className="text-rose-300">{negativeLabel}</span> {formatValue(Math.abs(active.negative ?? 0), valueFormat)}</p> : null}{lineLabel ? <p><span className="text-blue-300">{lineLabel}</span> {formatValue(active.line ?? 0, lineValueFormat)}</p> : null}</div></div> : null}
    <span className="sr-only">Use left and right arrow keys or touch and drag across the chart to inspect a period.</span>
  </div>;
}
