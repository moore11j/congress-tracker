"use client";

import { useEffect, useRef, useState } from "react";
import { ApiError, getStrategies, getBacktestPresets, runBacktest, suggestMemberInsiders, getInstitutionPerformance, searchRetirementParticipants } from "@/lib/api";
import { annualizeReturn } from "@/lib/retirementCalculator";
import { congressRetirementBacktest, retirementBacktestReturn, retirementCongressSources } from "@/lib/retirementBacktest";

export type ImportedReturn = { name: string; rate: number; href: string; period: string; basis: string; warnings?: string[]; assumptions?: string[] };
type Source = { id: string; name: string; href: string; kind: string; issuer?: string; rate?: number | null; period?: string; locked?: boolean };

export function RetirementReturnPicker({ onApply }: { onApply: (source: ImportedReturn, target: "saving" | "retirement" | "both") => void }) {
  const [category, setCategory] = useState("strategy");
  const [query, setQuery] = useState("");
  const [sources, setSources] = useState<Source[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [preview, setPreview] = useState<ImportedReturn | null>(null);
  const [target, setTarget] = useState<"saving" | "retirement" | "both">("saving");
  const requestId = useRef(0);
  const selectionId = useRef(0);
  useEffect(() => {
    const id = ++requestId.current;
    selectionId.current++;
    const abort = new AbortController();
    setPreview(null); setSources([]); setError("");
    if (category !== "strategy" && query.trim().length < 2) { setLoading(false); return () => abort.abort(); }
    setLoading(true);
    const timer = setTimeout(async () => {
      try {
        let items: Source[];
        if (category === "strategy") {
          const data = await getStrategies({ period: "max", sort: "cagr" });
          items = data.items.filter((s) => s.name.toLowerCase().includes(query.toLowerCase())).map((s) => ({ id: s.slug, name: s.name, href: `/strategies/${encodeURIComponent(s.slug)}`, kind: "strategy", rate: s.performance?.cagrPct, locked: s.access?.locked, period: `${s.latestRun?.backtestStartDate ?? "Inception"} to ${s.performance?.asOfDate ?? s.latestRun?.backtestEndDate ?? "latest available"}` }));
        } else if (category === "member") {
          const data = await suggestMemberInsiders(query, 20, { signal: abort.signal, source: "RetirementCalculator" });
          items = retirementCongressSources(data.items);
        } else {
          const data = await searchRetirementParticipants(query, abort.signal);
          items = data.items.filter((s) => s.kind === category).map((s) => ({ id: s.id, name: s.label, href: s.href, kind: s.kind })).filter((s) => Boolean(s.id));
        }
        const unique = [...new Map(items.map((item) => [`${item.kind}:${item.id}:${item.issuer ?? ""}`, item])).values()];
        if (id === requestId.current && !abort.signal.aborted) setSources(unique);
      } catch { if (id === requestId.current && !abort.signal.aborted) setError("Sources could not be loaded. Try searching again; custom returns remain available."); }
      finally { if (id === requestId.current && !abort.signal.aborted) setLoading(false); }
    }, 300);
    return () => { clearTimeout(timer); abort.abort(); selectionId.current++; };
  }, [category, query]);

  async function select(source: Source) {
    const id = ++selectionId.current;
    setPreview(null); setError(""); setLoading(true);
    try {
      if (source.locked) throw new Error("This source requires an eligible Walnut plan. Open its profile to review access.");
      let rate = source.rate;
      let period = source.period ?? "";
      let basis = "Historical strategy CAGR";
      let warnings: string[] = [];
      let assumptions: string[] = [];
      if (source.kind === "institution") {
        const data = await getInstitutionPerformance(source.id);
        const item = [...(data.items ?? [])].filter((item) => annualizeReturn(item.return_pct, item.start_date, item.end_date) !== null && (item.coverage_pct ?? 0) >= (item.minimum_coverage_pct ?? data.minimum_coverage_pct ?? 0)).sort((a, b) => Date.parse(a.start_date!) - Date.parse(b.start_date!))[0];
        rate = item ? annualizeReturn(item.return_pct, item.start_date, item.end_date) : null;
        period = item ? `${item.start_date} to ${item.end_date}` : "";
        basis = "Annualized reported-holdings return · 13F price model";
      } else if (source.kind === "member") {
        const presets = await getBacktestPresets();
        if (id !== selectionId.current) return;
        if (!presets.access.signed_in) throw new Error("Sign in to run a Congress backtest. Backtesting requires an eligible Walnut plan; custom returns remain free.");
        if (!presets.access.can_run) throw new Error("Your plan does not include backtesting. Open the backtester to review access, or enter a custom return.");
        const result = retirementBacktestReturn(await runBacktest(congressRetirementBacktest(source.id, presets.today)));
        ({ rate, period, basis, warnings, assumptions } = result);
      }
      if (typeof rate !== "number" || !Number.isFinite(rate)) throw new Error("No usable annualized return is available for this source. Choose another source or enter a custom return.");
      if (rate < -99 || rate > 100) throw new Error("This historical return is outside the calculator’s supported range (-99% to 100%). Choose another source or a custom assumption.");
      if (id === selectionId.current) setPreview({ name: source.kind === "member" ? `${source.name} · simulated strategy` : source.name, href: source.href, rate, period, basis, warnings, assumptions });
    } catch (e) { if (id === selectionId.current) setError(e instanceof ApiError && [401, 402, 403].includes(e.status) ? "Sign in with an eligible Walnut plan to import this return. Custom calculations are free." : e instanceof Error ? e.message : "Performance is temporarily unavailable."); }
    finally { if (id === selectionId.current) setLoading(false); }
  }
  return <div className="mt-4 space-y-3 rounded-xl border border-white/10 bg-slate-950/60 p-4">
    <div className="grid gap-3 sm:grid-cols-2"><label className="text-xs text-slate-400">Source type<select className="retirement-input" value={category} onChange={(e) => setCategory(e.target.value)}><option value="strategy">Strategies</option><option value="member">Congress members (backtest)</option><option value="institution">Institutions</option></select></label><label className="text-xs text-slate-400">Search by name<input className="retirement-input" value={query} onChange={(e) => setQuery(e.target.value)} placeholder={category === "strategy" ? "Find a strategy" : "Enter at least 2 characters"} /></label></div>
    {category === "member" ? <p className="text-xs leading-5 text-slate-400">Run the existing backtester over the previous three years using disclosure dates, 90-day holds, and monthly rebalancing. This models a strategy, not the member’s actual account. <a href="/backtesting" className="text-emerald-200 underline">Backtesting access</a> requires sign-in and an eligible plan.</p> : null}
    {loading ? <p role="status" className="motion-safe:animate-pulse text-xs text-emerald-200">Loading historical performance…</p> : null}
    {error ? <p role="alert" className="text-xs leading-5 text-amber-200">{error}</p> : null}
    <div className="max-h-44 overflow-y-auto">{sources.map((source) => <button key={`${source.kind}-${source.id}-${source.issuer ?? ""}`} type="button" disabled={loading} onClick={() => void select(source)} className="flex w-full items-center justify-between gap-3 border-b border-white/5 px-2 py-3 text-left text-sm text-slate-200 hover:bg-white/5 disabled:opacity-50"><span>{source.name}</span><span className="shrink-0 text-xs text-emerald-300">{source.locked ? "Plan required" : source.kind === "member" ? "Backtest →" : "Preview →"}</span></button>)}</div>
    {!loading && !error && !sources.length && (category === "strategy" || query.trim().length >= 2) ? <p className="text-xs text-slate-400">No matching sources. Try another name.</p> : null}
    {preview ? <div className="space-y-3 rounded-lg bg-emerald-300/5 p-3">
      <div className="flex items-start justify-between gap-3"><a href={preview.href} target="_blank" rel="noreferrer" className="text-sm font-medium text-emerald-200 underline underline-offset-4">{preview.name}</a><strong className="text-xl text-emerald-200">{preview.rate.toFixed(2)}%</strong></div>
      <p className="text-xs leading-5 text-slate-400">{preview.basis}<br />{preview.period}</p>
      {preview.warnings?.map((warning) => <p key={warning} className="text-xs leading-5 text-amber-200">{warning}</p>)}
      {preview.assumptions?.length ? <details className="text-xs leading-5 text-slate-400"><summary className="cursor-pointer text-emerald-200">Backtest assumptions</summary><ul className="mt-2 list-disc space-y-1 pl-4">{preview.assumptions.map((item) => <li key={item}>{item}</li>)}</ul><a href="/backtesting" className="mt-2 inline-block underline">Open the full backtester</a></details> : null}
      <label className="block text-xs text-slate-400">Apply to<select className="retirement-input" value={target} onChange={(e) => setTarget(e.target.value as typeof target)}><option value="saving">Before retirement</option><option value="retirement">During retirement</option><option value="both">Both periods</option></select></label><button type="button" onClick={() => onApply(preview, target)} className="rounded-lg bg-emerald-300 px-3 py-2 text-sm font-semibold text-slate-950">Use this annual return</button>
    </div> : null}
    <p className="text-xs leading-5 text-slate-500">Historical and simulated returns are scenario inputs, not forecasts. They may reflect disclosure delays, incomplete holdings, or backtesting assumptions. Past performance does not guarantee future results.</p>
  </div>;
}
