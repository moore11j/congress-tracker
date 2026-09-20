"use client";

import { useEffect, useRef, useState, type ReactNode } from "react";
import { GripHorizontal, PanelBottom, RotateCcw, X } from "lucide-react";

/** Position is local to the chart, retained while inspecting other points. */
export function MovableChartTooltip({ children, anchor, height, onClose }: { children: ReactNode; anchor: number; height: number; onClose: () => void }) {
  const box = useRef<HTMLDivElement>(null);
  const drag = useRef<{ x: number; y: number; left: number; top: number } | null>(null);
  const [position, setPosition] = useState<{ left: number; top: number } | null>(null);
  const [docked, setDocked] = useState(false);
  const clamp = (left: number, top: number) => ({
    left: Math.max(0, Math.min(left, (box.current?.parentElement?.clientWidth ?? 224) - (box.current?.offsetWidth ?? 224))),
    top: Math.max(0, Math.min(top, height - (box.current?.offsetHeight ?? 120))),
  });
  useEffect(() => {
    const parent = box.current?.parentElement;
    if (!parent) return;
    const observer = new ResizeObserver(() => setPosition(p => p ? clamp(p.left, p.top) : p));
    observer.observe(parent);
    return () => observer.disconnect();
  // The clamp reads current DOM dimensions rather than captured state.
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [height]);
  return <div ref={box} className={`${docked ? "relative mt-2" : "absolute"} z-10 w-56 max-w-full rounded-2xl border border-white/15 bg-slate-950/95 p-3 text-sm shadow-xl`}
    style={docked ? undefined : position ?? { top: 16, left: `clamp(0px, ${anchor}%, max(0px, calc(100% - 14rem)))` }}
    onKeyDown={event => { event.stopPropagation(); if (event.key === "Escape") onClose(); }}>
    <div className="mb-2 flex items-center gap-1 border-b border-white/10 pb-2 text-slate-400">
      <button type="button" disabled={docked} aria-label="Move chart info; drag or use arrow keys" title="Drag to move · arrow keys also work" className="flex min-h-8 flex-1 cursor-grab items-center gap-1 text-xs active:cursor-grabbing disabled:cursor-default" style={{ touchAction: "none" }}
        onPointerDown={event => {
          if (docked || event.button !== 0) return;
          const rect = box.current!.getBoundingClientRect(), parent = box.current!.parentElement!.getBoundingClientRect();
          drag.current = { x: event.clientX, y: event.clientY, left: rect.left - parent.left, top: rect.top - parent.top };
          event.currentTarget.setPointerCapture(event.pointerId); event.preventDefault();
        }}
        onPointerMove={event => { if (drag.current) setPosition(clamp(drag.current.left + event.clientX - drag.current.x, drag.current.top + event.clientY - drag.current.y)); }}
        onPointerUp={() => { drag.current = null; }} onPointerCancel={() => { drag.current = null; }} onLostPointerCapture={() => { drag.current = null; }}
        onKeyDown={event => {
          if (!event.key.startsWith("Arrow") || docked) return;
          event.preventDefault();
          const left = box.current?.offsetLeft ?? 0, top = box.current?.offsetTop ?? 16, step = event.shiftKey ? 40 : 10;
          setPosition(clamp(left + (event.key === "ArrowRight" ? step : event.key === "ArrowLeft" ? -step : 0), top + (event.key === "ArrowDown" ? step : event.key === "ArrowUp" ? -step : 0)));
        }}><GripHorizontal size={14} />Move</button>
      <button type="button" className="p-1.5" aria-label={docked ? "Float chart info" : "Dock chart info below graph"} title={docked ? "Float over graph" : "Move below graph"} onClick={() => setDocked(v => !v)}><PanelBottom size={14} /></button>
      <button type="button" className="p-1.5" aria-label="Reset chart info position" onClick={() => { setPosition(null); setDocked(false); }}><RotateCcw size={13} /></button>
      <button type="button" className="p-1.5" aria-label="Close chart info" onClick={onClose}><X size={14} /></button>
    </div>{children}
  </div>;
}
