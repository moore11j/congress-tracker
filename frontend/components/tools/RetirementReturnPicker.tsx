"use client";

import { useEffect, useRef, useState } from "react";
import { ApiError, getStrategies, getMemberPortfolioPerformance, getInsiderPortfolioPerformance, getInstitutionPerformance, searchRetirementParticipants } from "@/lib/api";
import { annualizeReturn } from "@/lib/retirementCalculator";

export type ImportedReturn = { name: string; rate: number; href: string; period: string; basis: string };
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
        } else {
          const data = await searchRetirementParticipants(query, abort.signal);
          items = data.items.filter((s) => s.kind === category).map((s) => ({ id: s.kind === "insider" ? s.id.split(":").at(-2) ?? "" : s.id, name: s.kind === "insider" && s.symbol ? `${s.label} · ${s.symbol}` : s.label, href: s.href, kind: s.kind, issuer: s.symbol ?? undefined })).filter((s) => Boolean(s.id));
        }
        const unique = [...new Map(items.map((item) => [`${item.kind}:${item.id}:${item.issuer ?? ""}`, item])).values()];
        if (id === requestId.current && !abort.signal.aborted) setSources(unique);
      } catch { if (id === requestId.current && !abort.signal.aborted) setError("Sources could not be loaded. Try searching again; custom returns remain available."); }
      finally { if (id === requestId.current && !abort.signal.aborted) setLoading(false); }
    }, 300);
    return () => { clearTimeout(timer); abort.abort(); };
  }, [category, query]);

  async function select(source: Source) {
    const id = ++selectionId.current;
    setPreview(null); setError(""); setLoading(true);
    try {
      if (source.locked) throw new Error("This source requires an eligible Walnut plan. Open its profile to review access.");
      let rate = source.rate;
      let period = source.period ?? "";
      let basis = "Historical strategy CAGR";
      if (source.kind === "institution") {
        const data = await getInstitutionPerformance(source.id);
        const item = [...(data.items ?? [])].filter((item) => annualizeReturn(item.return_pct, item.start_date, item.end_date) !== null && (item.coverage_pct ?? 0) >= (item.minimum_coverage_pct ?? data.minimum_coverage_pct ?? 0)).sort((a, b) => Date.parse(a.start_date!) - Date.parse(b.start_date!))[0];
        rate = item ? annualizeReturn(item.return_pct, item.start_date, item.end_date) : null;
        period = item ? `${item.start_date} to ${item.end_date}` : "";
        basis = "Annualized reported-holdings return · 13F price model";
      } else if (source.kind !== "strategy") {
        const data = source.kind === "member" ? await getMemberPortfolioPerformance(source.id, { lookback_days: 1095, mode: "realistic_disclosure_lag" }) : await getInsiderPortfolioPerformance(source.id, source.issuer);
        if (data.curve_quality_status === "poor") throw new Error("This portfolio has insufficient data quality for a return assumption.");
        rate = data.summary?.cagr_pct;
        period = `${data.effective_start_date ?? data.start_date ?? "Available history"} to ${data.effective_end_date ?? data.end_date ?? "latest available"}`;
        basis = "Replicated portfolio CAGR · disclosure-date model";
      }
      if (typeof rate !== "number" || !Number.isFinite(rate)) throw new Error("No usable annualized return is available for this source. Choose another source or enter a custom return.");
      if (rate < -99 || rate > 100) throw new Error("This historical return is outside the calculator’s supported range (-99% to 100%). Choose another source or a custom assumption.");
      if (id === selectionId.current) setPreview({ name: source.name, href: source.href, rate, period, basis });
    } catch (e) { if (id === selectionId.current) setError(e instanceof ApiError && [401, 403].includes(e.status) ? "Sign in with an eligible Walnut plan to import this portfolio. Custom calculations are free." : e instanceof Error ? e.message : "Performance is temporarily unavailable."); }
    finally { if (id === selectionId.current) setLoading(false); }
  }
  return <div className="mt-4 space-y-3 rounded-xl border border-white/10 bg-slate-950/60 p-4">
    <div className="grid gap-3 sm:grid-cols-2"><label className="text-xs text-slate-400">Source type<select className="retirement-input" value={category} onChange={(e) => setCategory(e.target.value)}><option value="strategy">Strategies</option><option value="member">Congress members</option><option value="insider">Insiders</option><option value="institution">Institutions</option></select></label><label className="text-xs text-slate-400">Search by name<input className="retirement-input" value={query} onChange={(e) => setQuery(e.target.value)} placeholder={category === "strategy" ? "Find a strategy" : "Enter at least 2 characters"} /></label></div>
    {loading ? <p role="status" className="motion-safe:animate-pulse text-xs text-emerald-200">Loading historical performance…</p> : null}
    {error ? <p role="alert" className="text-xs leading-5 text-amber-200">{error}</p> : null}
    <div className="max-h-44 overflow-y-auto">{sources.map((source) => <button key={`${source.kind}-${source.id}-${source.issuer ?? ""}`} type="button" disabled={loading} onClick={() => void select(source)} className="flex w-full items-center justify-between gap-3 border-b border-white/5 px-2 py-3 text-left text-sm text-slate-200 hover:bg-white/5 disabled:opacity-50"><span>{source.name}</span><span className="shrink-0 text-xs text-emerald-300">{source.locked ? "Plan required" : "Preview →"}</span></button>)}</div>
    {!loading && !error && !sources.length && (category === "strategy" || query.trim().length >= 2) ? <p className="text-xs text-slate-400">No matching sources. Try another name.</p> : null}
    {preview ? <div className="space-y-3 rounded-lg bg-emerald-300/5 p-3"><div className="flex items-start justify-between gap-3"><a href={preview.href} target="_blank" rel="noreferrer" className="text-sm font-medium text-emerald-200 underline underline-offset-4">{preview.name}</a><strong className="text-xl text-emerald-200">{preview.rate.toFixed(2)}%</strong></div><p className="text-xs leading-5 text-slate-400">{preview.basis}<br />{preview.period}</p><label className="block text-xs text-slate-400">Apply to<select className="retirement-input" value={target} onChange={(e) => setTarget(e.target.value as typeof target)}><option value="saving">Before retirement</option><option value="retirement">During retirement</option><option value="both">Both periods</option></select></label><button type="button" onClick={() => onApply(preview, target)} className="rounded-lg bg-emerald-300 px-3 py-2 text-sm font-semibold text-slate-950">Use this annual return</button></div> : null}
    <p className="text-xs leading-5 text-slate-500">Historical and simulated returns are scenario inputs, not forecasts. They may reflect disclosure delays, incomplete holdings, or backtesting assumptions. Past performance does not guarantee future results.</p>
  </div>;
}
