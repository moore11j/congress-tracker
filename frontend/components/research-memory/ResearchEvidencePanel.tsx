"use client";

import { useEffect, useState } from "react";
import { getResearchMemoryMatches, getTickerOperationalIntelligence, type ResearchMemoryEvidenceMatch, type ResearchMemoryThesis, type ResearchSourceCoverage } from "@/lib/api";
import { researchDate, researchSourceHref, researchSourceLabels } from "@/lib/researchEvidence";
import { ResearchCoverage } from "./ResearchCoverage";

const relationships: Record<string, { title: string; color: string }> = {
  supports: { title: "Supports an assumption", color: "text-emerald-200 bg-emerald-300/10" },
  contradicts: { title: "Challenges an assumption", color: "text-rose-200 bg-rose-300/10" },
  related: { title: "Relevant context", color: "text-sky-200 bg-sky-300/10" },
  potential_invalidator: { title: "Review invalidation condition", color: "text-amber-200 bg-amber-300/10" },
};

export function ResearchEvidencePanel({ thesis }: { thesis: ResearchMemoryThesis }) {
  const [items, setItems] = useState<ResearchMemoryEvidenceMatch[] | null>(null);
  const [coverage, setCoverage] = useState<ResearchSourceCoverage[]>([]);
  const [failed, setFailed] = useState(false);
  const [retry, setRetry] = useState(0);
  const [filter, setFilter] = useState("all");
  useEffect(() => {
    let active = true;
    setItems(null); setFailed(false);
    getResearchMemoryMatches(thesis.id).then((value) => { if (active) setItems(value.items); }).catch(() => { if (active) setFailed(true); });
    getTickerOperationalIntelligence(thesis.ticker).then((value) => { if (active) setCoverage(value.coverage ?? []); }).catch(() => { if (active) setCoverage([]); });
    return () => { active = false; };
  }, [thesis.id, thesis.ticker, thesis.updated_at, retry]);
  const visible = (items ?? []).filter((item) => filter === "all" || item.relationship === filter);
  return <section className="rounded-xl border border-white/10 bg-slate-900/70 p-4 sm:p-5">
    <p className="text-xs font-semibold uppercase tracking-[0.16em] text-emerald-300">Research evidence</p>
    <h2 className="mt-2 text-xl font-semibold text-white">What changed—and why it matters</h2>
    <p className="mt-2 max-w-3xl text-sm leading-6 text-slate-400">Each development is linked to the assumption it affects. This is not a thesis-health score.</p>
    {coverage.length ? <div className="mt-4"><ResearchCoverage items={coverage}/></div> : <p className="mt-3 text-xs text-slate-500">Source coverage is currently unavailable.</p>}
    {thesis.status !== "active" ? <p className="mt-3 text-sm text-amber-200">{thesis.status === "draft" ? "Activate this thesis to begin matching new evidence." : "Matching is paused. Previously matched evidence remains available."}</p> : null}
    <div className="mt-4 flex flex-wrap gap-2" role="group" aria-label="Filter matched evidence">{[["all", "All evidence"], ["supports", "Supporting"], ["contradicts", "Challenging"], ["related", "Context"]].map(([value, title]) => <button key={value} type="button" aria-pressed={filter === value} onClick={() => setFilter(value)} className={`min-h-10 rounded-lg border px-3 text-xs font-semibold transition ${filter === value ? "border-emerald-300/30 bg-emerald-300/10 text-emerald-100" : "border-white/10 text-slate-400 hover:text-white"}`}>{title}</button>)}</div>
    {failed ? <div className="mt-4 text-sm text-slate-400">Matched evidence is temporarily unavailable. <button type="button" onClick={() => setRetry((value) => value + 1)} className="font-semibold text-emerald-200">Try again</button></div> : items === null ? <p className="mt-4 animate-pulse text-sm text-slate-400" role="status">Loading matched evidence…</p> : !visible.length ? <p className="mt-4 rounded-lg border border-dashed border-white/10 p-4 text-sm leading-6 text-slate-400">No matching evidence in this view yet. This does not confirm or disprove your thesis. New evidence is compared with eligible assumptions after activation.</p> : <div className="mt-4 space-y-4">{visible.map((item) => {
      const evidence = item.evidence_snapshot;
      const treatment = relationships[item.relationship] ?? relationships.related;
      const href = researchSourceHref(evidence.source_url);
      return <article key={item.id} className="min-w-0 rounded-lg border border-white/10 bg-slate-950/40 p-4">
        <div className="flex flex-wrap items-center gap-2 text-xs"><span className={`rounded-full px-2.5 py-1 ${treatment.color}`}>{treatment.title}</span><span className="text-slate-500">{researchSourceLabels[evidence.source_type ?? ""] ?? "Company evidence"}{evidence.published_at ? ` · ${researchDate(evidence.published_at)}` : " · Publication date unavailable"}</span></div>
        <h3 className="mt-3 font-semibold text-white">{evidence.headline || "Source development"}</h3>
        {evidence.summary ? <p className="mt-2 text-sm leading-6 text-slate-300">{evidence.summary}</p> : null}
        <div className="mt-3 grid gap-3 sm:grid-cols-2"><div><p className="text-[11px] font-semibold uppercase tracking-wider text-slate-500">Your assumption at comparison</p><p className="mt-1 text-sm text-slate-300">{item.claim_snapshot?.subject ?? "Assumption"}{item.claim_snapshot?.expected_direction ? ` · ${item.claim_snapshot.expected_direction}` : ""}</p></div><div><p className="text-[11px] font-semibold uppercase tracking-wider text-slate-500">Why it matters</p><p className="mt-1 text-sm leading-6 text-slate-300">{item.reason}</p></div></div>
        {evidence.watch_item ? <p className="mt-3 rounded-lg bg-emerald-300/5 p-3 text-sm text-emerald-100"><span className="font-semibold">Watch next: </span>{evidence.watch_item}</p> : null}
        <p className="mt-3 text-xs text-slate-500">{item.confidence} confidence in this relationship{item.confidence === "low" ? " · Interpretation is uncertain; review the source before relying on it." : ". Source statements may include management expectations rather than realized outcomes."}</p>
        {evidence.evidence_excerpt ? <details className="mt-3 text-sm"><summary className="cursor-pointer font-semibold text-slate-300">Inspect evidence</summary><blockquote className="mt-2 border-l-2 border-emerald-300/30 pl-3 text-sm leading-6 text-slate-400">{evidence.evidence_excerpt}</blockquote>{evidence.source_locator ? <p className="mt-2 break-all text-[10px] text-slate-500">Reference: {evidence.source_locator}</p> : null}</details> : null}
        {href ? <a href={href} target="_blank" rel="noreferrer" className="mt-3 inline-block text-xs font-semibold text-emerald-200 hover:text-emerald-100">Read original source ↗</a> : null}
      </article>;
    })}</div>}
  </section>;
}
