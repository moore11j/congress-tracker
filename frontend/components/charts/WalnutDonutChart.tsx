"use client";

import { useEffect, useState, type KeyboardEvent, type PointerEvent } from "react";

type Segment = { label: string; value: number; color: string };

export function WalnutDonutChart({ segments, value, label, ariaLabel, size = 144 }: { segments: Segment[]; value: string; label: string; ariaLabel: string; size?: number }) {
  const [activeIndex, setActiveIndex] = useState<number | null>(null);
  const [revealed, setRevealed] = useState(false);
  const [reducedMotion, setReducedMotion] = useState(false);
  const total = segments.reduce((sum, segment) => sum + Math.max(0, segment.value), 0);
  let offset = 0;
  const active = activeIndex == null ? null : segments[activeIndex];

  useEffect(() => {
    const media = window.matchMedia("(prefers-reduced-motion: reduce)");
    const update = () => setReducedMotion(media.matches);
    update();
    media.addEventListener("change", update);
    return () => media.removeEventListener("change", update);
  }, []);
  useEffect(() => {
    const frame = requestAnimationFrame(() => setRevealed(true));
    return () => cancelAnimationFrame(frame);
  }, []);

  const selectWithKeyboard = (event: KeyboardEvent<SVGSVGElement>) => {
    if (event.key === "Escape") return setActiveIndex(null);
    if (event.key !== "ArrowLeft" && event.key !== "ArrowRight") return;
    event.preventDefault();
    setActiveIndex((current) => Math.max(0, Math.min(segments.length - 1, (current ?? 0) + (event.key === "ArrowLeft" ? -1 : 1))));
  };
  const selectWithTouch = (event: PointerEvent<SVGCircleElement>, index: number) => {
    event.currentTarget.setPointerCapture(event.pointerId);
    setActiveIndex(index);
  };

  return (
    <div className="relative mx-auto shrink-0" style={{ height: size, width: size }}>
      <svg viewBox="0 0 42 42" className="h-full w-full -rotate-90 outline-none" role="img" aria-label={ariaLabel} tabIndex={0} style={{ touchAction: "manipulation" }} onKeyDown={selectWithKeyboard} onPointerLeave={(event) => { if (event.pointerType === "mouse") setActiveIndex(null); }}>
        {segments.map((segment, index) => {
          const dash = total ? (Math.max(0, segment.value) / total) * 100 : 0;
          const dashOffset = -offset;
          offset += dash;
          return <circle key={`${segment.label}-${index}`} cx="21" cy="21" r="15.915" fill="none" stroke={segment.color} strokeWidth="7" strokeDasharray={revealed || reducedMotion ? `${dash} ${100 - dash}` : "0 100"} strokeDashoffset={dashOffset} opacity={activeIndex == null || activeIndex === index ? 1 : 0.45} className="cursor-pointer outline-none transition-opacity duration-150 focus:opacity-100" style={{ transition: reducedMotion ? "opacity 120ms ease-out" : "stroke-dasharray 460ms cubic-bezier(.22,1,.36,1), opacity 150ms ease-out" }} tabIndex={0} onPointerEnter={() => setActiveIndex(index)} onPointerDown={(event) => selectWithTouch(event, index)} onFocus={() => setActiveIndex(index)}><title>{`${segment.label}: ${total ? ((Math.max(0, segment.value) / total) * 100).toFixed(1) : "0.0"}%`}</title></circle>;
        })}
      </svg>
      <div className="pointer-events-none absolute inset-0 flex flex-col items-center justify-center text-center">
        <b className="max-w-24 truncate text-lg tabular-nums text-white">{active ? `${total ? ((Math.max(0, active.value) / total) * 100).toFixed(1) : "0.0"}%` : value}</b>
        <span className="max-w-24 truncate text-[9px] uppercase tracking-[.12em] text-slate-500">{active?.label ?? label}</span>
      </div>
      <span className="sr-only">Use left and right arrow keys, or touch a segment, to inspect the breakdown.</span>
    </div>
  );
}
