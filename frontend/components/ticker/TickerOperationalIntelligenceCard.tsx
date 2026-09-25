"use client";

import Link from "next/link";
import { useId, useState } from "react";
import { ResearchCoverage } from "@/components/research-memory/ResearchCoverage";
import { researchSourceHref } from "@/lib/researchEvidence";
import { collectResearchFindings, findingCategories, selectResearchFindings, type FindingCategory, type ResearchFinding } from "@/lib/tickerResearchFindings";
import { useTickerOperationalIntelligence } from "./TickerOperationalIntelligenceProvider";
import { ResearchMemoryAccessNotice, useResearchMemoryAccess } from "@/components/research-memory/ResearchMemoryAccess";

const SOURCE_LABELS: Record<string, string> = { news_article: "News", press_release: "Press release", earnings_transcript: "Earnings call" };
const FILTER_LABELS = { all: "All findings", catalysts: "Catalysts", risks: "Risks", opportunities: "Opportunities", watch_next: "Watch next" } as const;
const EMPTY_LABELS = { all: "No material operating-source signals have been processed for this ticker yet.", catalysts: "No catalysts identified in the checked sources.", risks: "No risks identified in the checked sources.", opportunities: "No positive developments with explicit future milestones identified.", watch_next: "No explicit future milestones were extracted." };
const TAG_CLASSES: Record<FindingCategory, string> = { catalysts: "text-violet-200 bg-violet-400/10", risks: "text-rose-200 bg-rose-400/10", opportunities: "text-sky-200 bg-sky-400/10", watch_next: "text-emerald-200 bg-emerald-400/10" };

function publicationLabel(value?: string | null) {
  if (!value || Number.isNaN(Date.parse(value))) return "Publication date unavailable";
  return new Intl.DateTimeFormat("en-US", { month: "short", day: "numeric", year: "numeric", timeZone: "UTC" }).format(new Date(value));
}

export function ResearchFindingRow({ finding }: { finding: ResearchFinding }) {
  const { item } = finding;
  const href = researchSourceHref(item.source_url);
  return <article className="grid min-w-0 gap-2 py-4 md:grid-cols-[7.5rem_minmax(0,1fr)] md:gap-4">
    <div className="flex flex-wrap gap-x-2 text-xs leading-5 text-slate-400 md:block">
      <p className="text-[11px] font-medium uppercase tracking-wider">{SOURCE_LABELS[item.source_type] ?? item.source_type}</p>
      <p>{publicationLabel(item.published_at)}</p>
    </div>
    <div className="min-w-0">
      <div className="flex flex-wrap gap-1.5">{finding.categories.map((category) => <span key={category} className={`rounded px-1.5 py-0.5 text-[11px] font-medium ${TAG_CLASSES[category]}`}>{findingCategories[category]}</span>)}{item.materiality === "high" ? <span className="rounded bg-amber-300/10 px-1.5 py-0.5 text-[11px] text-amber-200">Material</span> : null}</div>
      <p className="mt-1.5 text-sm font-semibold leading-6 text-slate-100 [overflow-wrap:anywhere]">{item.title}</p>
      <p className="mt-1 text-sm leading-6 text-slate-400 [overflow-wrap:anywhere]">{item.summary}</p>
      <div className="mt-1 flex flex-wrap items-center gap-x-4 gap-y-1 text-xs">
        {href ? <a href={href} target="_blank" rel="noreferrer" className="rounded py-1 font-semibold text-emerald-200 hover:text-emerald-100 focus-visible:outline focus-visible:outline-2 focus-visible:outline-emerald-300">Read source ↗</a> : null}
        <span className="text-slate-400">{item.confidence ? `${item.confidence} extraction confidence` : "Extraction confidence unavailable"}</span>
      </div>
      <details className="mt-1 text-sm text-slate-400">
        <summary className="w-fit cursor-pointer rounded py-1 text-xs font-medium text-slate-300 hover:text-white focus-visible:outline focus-visible:outline-2 focus-visible:outline-emerald-300">Evidence &amp; interpretation</summary>
        <div className="mt-2 space-y-3 border-l border-white/10 pl-3 leading-6 [overflow-wrap:anywhere]">{finding.variants.map(({ category, item: variant }, index) => <div key={`${category}-${index}`}>
          <p className="text-xs font-semibold text-slate-300">{findingCategories[category]}</p><p>{variant.title}</p><p>{variant.summary}</p>
          {variant.evidence_excerpt ? <><p className="mt-2 text-xs font-medium text-slate-300">Evidence excerpt</p><blockquote className="italic">{variant.evidence_excerpt}</blockquote></> : <p className="text-xs">No retained excerpt is available for this finding.</p>}
          <p className="mt-1 text-xs">{SOURCE_LABELS[variant.source_type] ?? variant.source_type} · {publicationLabel(variant.published_at)} · {variant.confidence ?? "Unspecified"} extraction confidence · {variant.materiality} materiality</p>
          {researchSourceHref(variant.source_url) ? <a href={researchSourceHref(variant.source_url)!} target="_blank" rel="noreferrer" className="text-xs text-emerald-200 underline underline-offset-4">Read original source ↗</a> : null}
        </div>)}</div>
      </details>
    </div>
  </article>;
}

export function TickerOperationalIntelligenceCard({ symbol }: { symbol: string }) {
  const access = useResearchMemoryAccess();
  const { data, failed, disabled, retry } = useTickerOperationalIntelligence();
  const [selection, setSelection] = useState<{ symbol: string; category: FindingCategory | "all" }>({ symbol, category: "all" });
  const [sort, setSort] = useState<"latest" | "material">("latest");
  const resultsId = useId();
  const category = selection.symbol === symbol ? selection.category : "all";
  const findings = collectResearchFindings(data);
  const visible = selectResearchFindings(findings, category, sort);
  if (process.env.NEXT_PUBLIC_RESEARCH_OPERATIONAL_INTELLIGENCE_ENABLED === "false") return null;
  if (access.status !== "allowed") return <section className="mt-6 border-t border-white/10 pt-5" aria-label="Company developments"><ResearchMemoryAccessNotice {...access} title="Company developments" body="Unlock source-grounded catalysts, risks, opportunities, and what to watch next from news, press releases, and earnings calls with Premium." /></section>;
  if (disabled) return null;
  return <section className="mt-6 border-t border-white/10 pt-5" aria-label="Company developments">
    <div className="flex flex-wrap items-start justify-between gap-3"><div><h2 className="text-xl font-semibold text-white">Company developments</h2><p className="mt-1 text-sm text-slate-300">The business behind the ticker</p></div><Link href={`/monitoring/research?ticker=${encodeURIComponent(symbol)}`} className="rounded-lg border border-emerald-300/25 px-3 py-2 text-xs font-semibold text-emerald-200 hover:bg-emerald-300/10">Build a thesis →</Link></div>
    <p className="mt-2 text-sm leading-6 text-slate-400">Source-grounded company developments from news, official releases, and enabled earnings-call coverage.</p>
    {data?.coverage ? <div className="mt-4"><ResearchCoverage items={data.coverage} compact /></div> : null}
    <div className="mt-2 flex flex-wrap items-baseline gap-x-3 gap-y-1 text-xs leading-5 text-slate-400"><p>Recent evidence · past {data?.lookback_days ?? 120} days.</p><details><summary className="cursor-pointer rounded py-1 text-slate-300 focus-visible:outline focus-visible:outline-2 focus-visible:outline-emerald-300">Coverage notes</summary><p className="max-w-3xl pb-2">News and press-release analysis uses provider excerpts, which may omit details from the full source. Releases may describe the company or its partners. Coverage describes what has been checked; an empty section does not mean the business has no risks.</p></details></div>
    {!data && !failed ? <div className="mt-4 space-y-3" role="status" aria-label="Loading operating-source intelligence">{[0, 1, 2].map((row) => <div key={row} className="h-16 animate-pulse border-b border-white/10 bg-white/[0.025]" />)}<span className="sr-only">Loading company developments…</span></div> : failed ? <div className="mt-4 flex flex-wrap items-center gap-3 py-3" role="alert"><p className="text-sm text-slate-400">Operational-source intelligence is temporarily unavailable.</p><button type="button" onClick={retry} className="rounded px-2 py-2 text-sm font-semibold text-emerald-200 hover:text-emerald-100">Try again</button></div> : <>
      <div className="mt-4 flex flex-wrap items-center justify-between gap-x-4 border-b border-white/10">
        <div className="flex flex-wrap gap-x-4" role="group" aria-label="Filter company developments">{(Object.keys(FILTER_LABELS) as (FindingCategory | "all")[]).map((key) => <button key={key} type="button" aria-pressed={category === key} aria-controls={resultsId} onClick={() => setSelection({ symbol, category: key })} className={`border-b-2 py-3 text-xs font-medium transition focus-visible:outline focus-visible:outline-2 focus-visible:outline-emerald-300 ${category === key ? "border-violet-400 text-violet-200" : "border-transparent text-slate-400 hover:text-white"}`}>{FILTER_LABELS[key]} <span className="text-slate-500">{key === "all" ? findings.length : findings.filter((finding) => finding.categories.includes(key)).length}</span></button>)}</div>
        <label className="flex items-center gap-2 py-2 text-xs text-slate-400"><span className="sr-only">Sort company developments</span><select value={sort} onChange={(event) => setSort(event.target.value as "latest" | "material")} className="min-h-9 rounded border-0 bg-slate-950 px-2 text-slate-300 focus-visible:outline focus-visible:outline-2 focus-visible:outline-emerald-300"><option value="latest">Latest first</option><option value="material">Material first</option></select></label>
      </div>
      <div id={resultsId} className="divide-y divide-white/10">{visible.length ? visible.map((finding) => <ResearchFindingRow key={finding.id} finding={finding} />) : <p className="py-6 text-sm text-slate-400">{EMPTY_LABELS[category]}</p>}</div>
      <p className="mt-2 text-xs text-slate-500" role="status">{visible.length} finding{visible.length === 1 ? "" : "s"} shown · Empty categories do not mean the company has no risks.</p>
    </>}
  </section>;
}
