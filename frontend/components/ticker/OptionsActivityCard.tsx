"use client";

import { useEffect, useRef, useState } from "react";
import { getOptionsActivity } from "@/lib/api";
import type { OptionsFlowSummary } from "@/lib/types";
import { WalnutLineChart } from "@/components/charts/WalnutLineChart";

const cash = (n: number) => new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", notation: "compact", maximumFractionDigits: 1 }).format(n);
const count = (n: number) => new Intl.NumberFormat("en-US").format(n);

export function OptionsActivityCard({ summary }: { summary: OptionsFlowSummary }) {
  const [value, setValue] = useState(summary), [busy, setBusy] = useState(false), [error, setError] = useState("");
  const pending = useRef<AbortController | null>(null);
  useEffect(() => { pending.current?.abort(); setValue(summary); setBusy(false); setError(""); return () => pending.current?.abort(); }, [summary]);
  const load = async () => {
    pending.current?.abort(); const controller = new AbortController(); pending.current = controller;
    setBusy(true); setError("");
    try { const result = await getOptionsActivity(summary.ticker, controller.signal); if (!controller.signal.aborted) setValue(result); }
    catch (e) { if (!controller.signal.aborted) setError(e instanceof Error ? e.message : "Activity could not be loaded. Try again shortly."); }
    finally { if (!controller.signal.aborted) setBusy(false); }
  };
  const activity = value.data_basis === "historical_daily_bars" && value.coverage;
  const metrics = value.metrics;
  return <div className="min-w-0 rounded-lg border border-sky-300/15 bg-sky-300/[.035] p-4">
    <div className="flex flex-wrap items-center justify-between gap-2"><p className="text-[10px] font-semibold uppercase tracking-wider text-slate-400">Options activity</p><span className="text-[10px] font-medium uppercase text-sky-200">{activity ? value.label : "Historical"}</span></div>
    {activity ? <>
      <p className="mt-2 text-xs text-slate-400">30-day history · expiration {value.coverage!.expiration} only</p>
      <dl className="mt-3 grid grid-cols-2 gap-3 text-xs">
        <div><dt className="text-slate-400">Call contracts traded</dt><dd className="mt-1 font-semibold text-sky-200">{count(metrics.call_volume ?? 0)}</dd></div>
        <div><dt className="text-slate-400">Put contracts traded</dt><dd className="mt-1 font-semibold text-amber-200">{count(metrics.put_volume ?? 0)}</dd></div>
        <div><dt className="text-slate-400">Call premium traded · est.</dt><dd className="mt-1 text-slate-100">{cash(metrics.call_premium ?? 0)}</dd></div>
        <div><dt className="text-slate-400">Put premium traded · est.</dt><dd className="mt-1 text-slate-100">{cash(metrics.put_premium ?? 0)}</dd></div>
      </dl>
      <p className="mt-3 text-xs text-slate-300">{metrics.volume_multiple != null ? `${metrics.volume_multiple.toFixed(2)}× volume in the latest observed session vs. the prior ${metrics.baseline_sessions} active sessions${metrics.volume_multiple >= 2 ? " · activity spike" : ""}.` : "More history is needed to compare session volume."}</p>
      {value.history && value.history.length > 1 ? <div className="mt-3"><WalnutLineChart height={190} width={420} minValue={0} valueFormat="number" draggableTooltip ariaLabel="Historical call and put volume for the selected expiration" data={value.history.map(d => ({ label: d.date.slice(5) }))} series={[{ key: "calls", label: "Calls", color: "#7dd3fc", values: value.history.map(d => d.call_volume) }, { key: "puts", label: "Puts", color: "#fcd34d", values: value.history.map(d => d.put_volume) }]} /><p className="text-[10px] text-slate-400">Blue: calls · Amber: puts · Contracts traded per session</p></div> : null}
      <p className="mt-3 text-[10px] leading-4 text-slate-500">Latest trade date: {value.latest_flow_date}. {value.coverage!.listed_contracts} currently listed standard contracts checked. Other expirations and adjusted contracts excluded. Premium ≈ daily volume × VWAP × 100, summed across the window.</p>
    </> : <p className="mt-3 text-xs leading-5 text-slate-400">Load historical call/put volume and estimated premium for one listed expiration at least three weeks away.</p>}
    <p className="mt-3 text-[10px] leading-4 text-slate-400">Call-heavy and put-heavy describe premium activity. Buying versus selling is unknown; this does not confirm bullish or bearish sentiment.</p>
    <button onClick={load} disabled={busy} className="mt-3 rounded-lg border border-white/10 px-3 py-2 text-xs text-slate-200 hover:bg-white/5 disabled:opacity-50">{busy ? "Loading historical activity…" : activity ? "Reload activity" : "Load activity"}</button>
    {busy ? <p role="status" className="mt-2 text-xs text-slate-400">Checking listed contracts and historical sessions…</p> : null}
    {error ? <p role="alert" className="mt-2 text-xs text-amber-200">{error}</p> : null}
  </div>;
}
