"use client";

import { useEffect, useRef, useState, type PointerEvent } from "react";

export function WalnutProfileSparkline({ id, values }: { id: string; values: number[] }) {
  const [revealed, setRevealed] = useState(false);
  const [activeIndex, setActiveIndex] = useState<number | null>(null);
  const frame = useRef<number | null>(null);
  const max = Math.max(...values, 1);
  const min = Math.min(...values, max);
  const range = Math.max(max - min, max * 0.18, 1);
  const plotTop = 6;
  const plotBottom = 56;
  const points = values.map((value, index) => {
    const x = (index / Math.max(values.length - 1, 1)) * 118 + 1;
    const y = plotBottom - ((value - min) / range) * (plotBottom - plotTop);
    return { x, y: Math.max(plotTop, Math.min(plotBottom, y)) };
  });
  const line = points.map((point) => `${point.x},${point.y}`).join(" ");
  const area = `M 1 ${plotBottom} L ${line} L 119 ${plotBottom} Z`;
  const active = activeIndex == null ? null : points[activeIndex];

  useEffect(() => {
    const animationFrame = requestAnimationFrame(() => setRevealed(true));
    return () => cancelAnimationFrame(animationFrame);
  }, []);
  useEffect(() => () => { if (frame.current !== null) cancelAnimationFrame(frame.current); }, []);

  const inspect = (event: PointerEvent<SVGSVGElement>) => {
    const rect = event.currentTarget.getBoundingClientRect();
    const index = Math.max(0, Math.min(points.length - 1, Math.round(((event.clientX - rect.left) / Math.max(rect.width, 1)) * (points.length - 1))));
    if (frame.current !== null) cancelAnimationFrame(frame.current);
    frame.current = requestAnimationFrame(() => { setActiveIndex(index); frame.current = null; });
  };

  return <svg viewBox="0 0 120 60" preserveAspectRatio="none" className="h-14 w-full overflow-visible" role="img" aria-label="Illustrative recent trend; open this profile for the detailed activity chart" style={{ touchAction: "manipulation" }} onPointerMove={inspect} onPointerDown={inspect} onPointerLeave={(event) => { if (event.pointerType === "mouse") setActiveIndex(null); }}>
    <defs><linearGradient id={`${id}-area`} x1="0" x2="0" y1="0" y2="1"><stop stopColor="#42d3a7" stopOpacity=".42" /><stop offset="1" stopColor="#42d3a7" stopOpacity=".04" /></linearGradient><clipPath id={`${id}-clip`}><rect x="0" y="0" width="120" height="60" /></clipPath></defs>
    <g clipPath={`url(#${id}-clip)`} style={{ clipPath: revealed ? "inset(0 0 0 0)" : "inset(0 100% 0 0)", transition: "clip-path 460ms cubic-bezier(.22,1,.36,1)" }}><path d={area} fill={`url(#${id}-area)`} /><polyline points={line} fill="none" stroke="#78f3c3" strokeWidth="1.8" vectorEffect="non-scaling-stroke" /></g>
    {active ? <><line x1={active.x} x2={active.x} y1="4" y2="57" stroke="rgba(226,232,240,.5)" strokeDasharray="2 2" vectorEffect="non-scaling-stroke" /><circle cx={active.x} cy={active.y} r="2.6" fill="#78f3c3" stroke="#07101f" strokeWidth=".8" vectorEffect="non-scaling-stroke" /></> : null}
  </svg>;
}
