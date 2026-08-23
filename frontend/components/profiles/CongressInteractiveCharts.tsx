"use client";

import { useEffect, useState } from "react";
import type { CongressOverviewResponse, ProfileSectorPeriod } from "@/lib/api";
import { WalnutLineChart } from "@/components/charts/WalnutLineChart";

const COLORS = ["#42d3a7", "#3b82f6", "#a855f7", "#f6b91a", "#fb7185", "#60a5fa", "#a3e635", "#94a3b8"];
const compact = (value: number) => new Intl.NumberFormat("en-US", { notation: "compact", maximumFractionDigits: 1 }).format(value);
const money = (value: number) => new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", notation: "compact", maximumFractionDigits: 1 }).format(value);

export function CongressSnapshotChart({ points }: { points: Array<{ label: string; value: number }> }) {
  return <WalnutLineChart data={points.map((point) => ({ label: point.label }))} series={[{ key: "congress-trades", label: "Trades", color: "#55e3b0", areaColor: "rgba(66,211,167,.22)", values: points.map((point) => point.value) }]} ariaLabel="Monthly Congress trade count trend" height={192} valueFormat="number" />;
}

export function CongressMetricTrend({ points, tone }: { points: Array<{ label: string; value: number }>; tone: "green" | "red" | "blue" }) {
  const color = tone === "red" ? "#fb7185" : tone === "blue" ? "#60a5fa" : "#55e3b0";
  return <WalnutLineChart data={points.map((point) => ({ label: point.label }))} series={[{ key: "metric", label: "Reported value", color, areaColor: `${color}33`, values: points.map((point) => point.value) }]} ariaLabel="Congress metric trend" height={142} valueFormat="number" />;
}

export function CongressSectorExposure({ rows }: { rows: ProfileSectorPeriod[] }) {
  const [activePeriod, setActivePeriod] = useState(0);
  const [revealed, setRevealed] = useState(false);
  const periods = rows.slice(-5);
  const labels = [...new Set(periods.flatMap((row) => row.segments.map((segment) => segment.label)))];
  useEffect(() => { const frame = requestAnimationFrame(() => setRevealed(true)); return () => cancelAnimationFrame(frame); }, []);
  if (!periods.length) return <p className="py-8 text-sm text-slate-400">No sector exposure is available.</p>;
  const active = periods[activePeriod] ?? periods[0];
  return <div><div className="flex h-48 items-end gap-4 border-b border-white/10 px-2">{periods.map((row, index) => <button key={row.period} type="button" className={`flex flex-1 flex-col items-center gap-2 rounded-sm outline-none transition focus-visible:ring-2 focus-visible:ring-emerald-300/70 ${activePeriod === index ? "opacity-100" : "opacity-55 hover:opacity-100"}`} onPointerEnter={() => setActivePeriod(index)} onFocus={() => setActivePeriod(index)} onClick={() => setActivePeriod(index)} aria-pressed={activePeriod === index} aria-label={`${row.period} sector exposure`}><span className="flex h-40 w-full max-w-10 flex-col-reverse overflow-hidden rounded-sm bg-slate-900">{labels.map((label, segmentIndex) => { const segment = row.segments.find((item) => item.label === label); return segment ? <i key={label} style={{ height: revealed ? `${Math.max(segment.percent, .6)}%` : "0%", backgroundColor: COLORS[segmentIndex % COLORS.length], transition: `height 460ms cubic-bezier(.22,1,.36,1) ${index * 55 + segmentIndex * 20}ms` }} /> : null; })}</span><span className="text-[10px] text-slate-500">{row.period}</span></button>)}</div><div className="mt-3 flex flex-wrap gap-x-3 gap-y-1 text-[10px] text-slate-400">{active.segments.slice(0, 6).map((segment) => <span key={segment.label} className="inline-flex items-center gap-1.5"><i className="h-2 w-2 rounded-full" style={{ backgroundColor: COLORS[labels.indexOf(segment.label) % COLORS.length] }} />{segment.label} {segment.percent.toFixed(1)}%</span>)}</div></div>;
}

export function CongressNetSectorBars({ rows }: { rows: NonNullable<CongressOverviewResponse["sector_activity"]> }) {
  const [activeSector, setActiveSector] = useState<string | null>(null);
  const [revealed, setRevealed] = useState(false);
  const visible = rows.slice(0, 8);
  const max = Math.max(...visible.map((row) => Math.abs(row.current_value)), 1);
  useEffect(() => { const frame = requestAnimationFrame(() => setRevealed(true)); return () => cancelAnimationFrame(frame); }, []);
  if (!visible.length) return <p className="py-8 text-sm text-slate-400">No sector activity is available.</p>;
  const active = visible.find((row) => row.sector === activeSector);
  return <div><div className="space-y-3">{visible.map((row, index) => { const width = (Math.abs(row.current_value) / max) * 50; return <button key={row.sector} type="button" className={`grid w-full grid-cols-[7rem_minmax(0,1fr)_4rem] items-center gap-2 rounded-sm text-left text-xs outline-none transition focus-visible:ring-2 focus-visible:ring-emerald-300/70 ${activeSector === null || activeSector === row.sector ? "opacity-100" : "opacity-40"}`} onPointerEnter={() => setActiveSector(row.sector)} onPointerLeave={(event) => { if (event.pointerType === "mouse") setActiveSector(null); }} onFocus={() => setActiveSector(row.sector)} onBlur={() => setActiveSector(null)} onClick={() => setActiveSector((current) => current === row.sector ? null : row.sector)} aria-pressed={activeSector === row.sector}><span className="truncate text-slate-300">{row.sector}</span><span className="relative h-2.5 rounded bg-slate-800"><i className="absolute inset-y-0 left-1/2 w-px bg-slate-500/40" /><i className={`absolute top-0 h-full rounded ${row.current_value >= 0 ? "bg-emerald-400" : "bg-rose-400"}`} style={{ left: row.current_value >= 0 ? "50%" : `${50 - (revealed ? width : 0)}%`, width: `${revealed ? width : 0}%`, transition: `width 430ms cubic-bezier(.22,1,.36,1) ${index * 35}ms, left 430ms cubic-bezier(.22,1,.36,1) ${index * 35}ms` }} /></span><span className={row.current_value >= 0 ? "text-right text-emerald-300" : "text-right text-rose-300"}>{money(row.current_value)}</span></button>; })}</div>{active ? <p className="mt-3 rounded-md border border-white/10 bg-slate-950/70 px-3 py-2 text-xs text-slate-300"><span className="font-semibold text-white">{active.sector}</span><span className="mx-2 text-slate-600">•</span>Net activity {money(active.current_value)}</p> : null}<div className="mt-3 flex justify-between text-[10px] text-slate-500"><span>{money(-max)}</span><span>$0</span><span>{money(max)}</span></div></div>;
}
