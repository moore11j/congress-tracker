"use client";

import { useEffect, useState } from "react";
import type { ProfileSectorMover } from "@/lib/api";

const COLORS: Record<string, string> = { Congress: "#42d3a7", Insider: "#3b82f6", Institution: "#a855f7", Department: "#f6b91a" };

export function WalnutSectorMovementBars({ rows }: { rows: ProfileSectorMover[] }) {
  const [activeSector, setActiveSector] = useState<string | null>(null);
  const [revealed, setRevealed] = useState(false);
  const max = Math.max(...rows.map((row) => row.current_value), 1);
  useEffect(() => {
    const frame = requestAnimationFrame(() => setRevealed(true));
    return () => cancelAnimationFrame(frame);
  }, []);
  if (!rows.length) return <p className="flex h-44 items-center justify-center text-sm text-slate-400">No sector-mapped activity is available.</p>;
  const active = rows.find((row) => row.sector === activeSector);
  return <div><div className="space-y-2.5">{rows.map((row, index) => <button key={row.sector} type="button" className={`grid w-full grid-cols-[minmax(0,7.5rem)_minmax(0,1fr)_3.5rem] items-center gap-2 rounded-sm text-left text-xs outline-none transition focus-visible:ring-2 focus-visible:ring-emerald-300/70 ${activeSector === null || activeSector === row.sector ? "opacity-100" : "opacity-40"}`} onPointerEnter={() => setActiveSector(row.sector)} onPointerLeave={(event) => { if (event.pointerType === "mouse") setActiveSector(null); }} onFocus={() => setActiveSector(row.sector)} onBlur={() => setActiveSector(null)} onClick={() => setActiveSector((current) => current === row.sector ? null : row.sector)} aria-pressed={activeSector === row.sector} aria-label={`${row.sector}: ${row.current_value.toLocaleString()} recent activities, ${row.previous_value.toLocaleString()} prior activities, ${row.change >= 0 ? "+" : ""}${row.change.toLocaleString()} change`}><span className="truncate text-slate-300">{row.sector}</span><span className="flex h-2.5 overflow-hidden rounded-sm bg-slate-800">{row.segments.map((segment) => segment.value ? <i key={segment.type} className="h-full" style={{ width: revealed ? `${(segment.value / max) * 100}%` : "0%", backgroundColor: COLORS[segment.type] ?? "#94a3b8", transition: `width 440ms cubic-bezier(.22,1,.36,1) ${index * 35}ms` }} /> : null)}</span><span className={`text-right font-semibold tabular-nums ${row.change >= 0 ? "text-emerald-300" : "text-rose-300"}`}>{row.change >= 0 ? "+" : ""}{row.change.toLocaleString()}</span></button>)}</div>{active ? <div className="mt-3 rounded-md border border-white/10 bg-slate-950/70 px-3 py-2 text-xs text-slate-300"><span className="font-semibold text-white">{active.sector}</span><span className="mx-2 text-slate-600">•</span>{active.current_value.toLocaleString()} recent vs {active.previous_value.toLocaleString()} prior activities</div> : null}<div className="mt-4 flex flex-wrap gap-x-3 gap-y-1 text-[10px] text-slate-400">{Object.entries(COLORS).map(([category, color]) => <span key={category} className="inline-flex items-center gap-1.5"><i className="h-2 w-2 rounded-full" style={{ backgroundColor: color }} />{category}</span>)}</div></div>;
}
