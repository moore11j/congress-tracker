"use client";

import { useEffect, useMemo, useState } from "react";
import type { ProfileSectorPeriod } from "@/lib/api";

const COLORS = ["#42d3a7", "#3b82f6", "#a855f7", "#f6b91a", "#fb7185", "#60a5fa", "#a3e635", "#94a3b8"];

export function WalnutDepartmentSpendingMix({ rows }: { rows: ProfileSectorPeriod[] }) {
  const [activePeriod, setActivePeriod] = useState(() => Math.max(rows.length - 1, 0));
  const [revealed, setRevealed] = useState(false);
  const labels = useMemo(() => {
    const latest = rows[rows.length - 1]?.segments.map((segment) => segment.label) ?? [];
    return [...latest, ...Array.from(new Set(rows.flatMap((row) => row.segments.map((segment) => segment.label)))).filter((label) => !latest.includes(label))];
  }, [rows]);

  useEffect(() => {
    if (window.matchMedia("(prefers-reduced-motion: reduce)").matches) {
      setRevealed(true);
      return;
    }
    const frame = requestAnimationFrame(() => setRevealed(true));
    return () => cancelAnimationFrame(frame);
  }, []);

  if (!rows.length) return <p className="flex h-36 items-center justify-center text-sm text-slate-400">No sector-mapped spending is available.</p>;

  const active = rows[Math.min(activePeriod, rows.length - 1)] ?? rows[0];
  return <div>
    <div className="flex h-36 items-end gap-2" role="group" aria-label="Department spending mix by reporting period">
      {rows.map((row, index) => {
        const selected = active.period === row.period;
        return <button key={row.period} type="button" className={`flex h-full min-w-6 flex-1 flex-col-reverse overflow-hidden rounded-sm bg-slate-900 outline-none transition focus-visible:ring-2 focus-visible:ring-emerald-300/70 ${selected ? "ring-1 ring-emerald-300/50 opacity-100" : "opacity-60 hover:opacity-100"}`} onPointerEnter={() => setActivePeriod(index)} onFocus={() => setActivePeriod(index)} onClick={() => setActivePeriod(index)} aria-pressed={selected} aria-label={`${row.period}: ${row.segments.map((segment) => `${segment.label} ${segment.percent.toFixed(1)}%`).join(", ")}`}>
          {labels.map((label, segmentIndex) => {
            const segment = row.segments.find((item) => item.label === label);
            return segment ? <i key={label} className="block w-full" style={{ height: revealed ? `${Math.max(segment.percent, 0.8)}%` : "0%", backgroundColor: COLORS[segmentIndex % COLORS.length], transition: `height 460ms cubic-bezier(.22,1,.36,1) ${index * 45 + segmentIndex * 18}ms` }} /> : null;
          })}
        </button>;
      })}
    </div>
    <div className="mt-2 flex justify-between gap-2 text-[10px] text-slate-500"><span>{rows[0]?.period}</span><span>{rows[rows.length - 1]?.period}</span></div>
    <div className="mt-3 rounded-md border border-white/10 bg-slate-950/70 px-3 py-2 text-xs text-slate-300" aria-live="polite"><span className="font-semibold text-white">{active.period}</span><span className="mx-2 text-slate-600">•</span>{active.segments.slice(0, 6).map((segment) => <span key={segment.label} className="mr-3 inline-flex items-center gap-1.5"><i className="h-2 w-2 rounded-full" style={{ backgroundColor: COLORS[labels.indexOf(segment.label) % COLORS.length] }} />{segment.label} {segment.percent.toFixed(1)}%</span>)}</div>
    <div className="mt-3 flex flex-wrap gap-x-3 gap-y-1.5">{labels.slice(0, 8).map((label, index) => <span key={label} className="inline-flex items-center gap-1.5 text-[10px] text-slate-400"><i className="h-2 w-2 rounded-full" style={{ backgroundColor: COLORS[index % COLORS.length] }} />{label}</span>)}</div>
  </div>;
}
