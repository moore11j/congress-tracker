"use client";

import Link from "next/link";
import { type OperationalIntelligenceItem } from "@/lib/api";
import { ResearchCoverage } from "@/components/research-memory/ResearchCoverage";
import { researchSourceHref } from "@/lib/researchEvidence";
import { useTickerOperationalIntelligence } from "./TickerOperationalIntelligenceProvider";

const SOURCE_LABELS: Record<string, string> = {
  news_article: "News",
  press_release: "Press release",
  earnings_transcript: "Earnings call",
};

function publicationLabel(value: string | null | undefined): string | null {
  if (!value) return null;
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return null;
  return new Intl.DateTimeFormat("en-US", { month: "short", day: "numeric", year: "numeric", timeZone: "UTC" }).format(parsed);
}

function SourceItem({ item }: { item: OperationalIntelligenceItem }) {
  const href = researchSourceHref(item.source_url);
  const date = publicationLabel(item.published_at);
  const body = <>
    <div className="flex flex-wrap items-center gap-x-2 gap-y-1 text-[11px] font-medium uppercase tracking-[0.11em] text-slate-500">
      <span>{SOURCE_LABELS[item.source_type] ?? "Source document"}</span>
      {date ? <span aria-label={`Published ${date}`}>· {date}</span> : null}
      {item.materiality === "high" ? <span className="text-amber-200">Material</span> : null}
      {item.confidence ? <span aria-label={`Extraction confidence: ${item.confidence}`}>· {item.confidence} extraction confidence</span> : null}
    </div>
    <p className="mt-1.5 font-medium leading-5 text-slate-100">{item.title}</p>
    <p className="mt-1 text-xs leading-5 text-slate-400">{item.summary}</p>
    {href ? <span className="mt-2 inline-block text-xs font-semibold text-emerald-200">Read source ↗</span> : null}
  </>;
  const shellClassName = "rounded-lg border border-white/10 bg-slate-950/35 p-3 transition hover:border-emerald-300/30 hover:bg-slate-950/55";
  const linkClassName = "block transition focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-emerald-300/50 hover:text-white";
  return <article className={shellClassName}>{href ? <a href={href} target="_blank" rel="noreferrer" className={linkClassName} aria-label={`Open source: ${item.title}`}>{body}</a> : <div>{body}</div>}{item.evidence_excerpt ? <details className="mt-2 text-xs leading-5 text-slate-400"><summary className="cursor-pointer font-semibold text-slate-300 hover:text-white">Evidence excerpt</summary><blockquote className="mt-1 border-l border-emerald-300/35 pl-2 italic text-slate-400">{item.evidence_excerpt}</blockquote></details> : null}</article>;
}

function SignalGroup({ title, items, empty }: { title: string; items: OperationalIntelligenceItem[]; empty: string }) {
  return <div className="min-w-0"><h3 className="text-xs font-semibold uppercase tracking-[0.14em] text-slate-500">{title}</h3><div className="mt-2 space-y-2">{items.length ? items.map((item) => <SourceItem key={item.id} item={item} />) : <p className="rounded-lg border border-dashed border-white/10 px-3 py-3 text-sm leading-5 text-slate-500">{empty}</p>}</div></div>;
}

function LoadingGroups() {
  return <div className="mt-4 grid gap-5 lg:grid-cols-3" aria-label="Loading operating-source intelligence"><div className="h-36 animate-pulse rounded-lg border border-white/10 bg-white/[0.035]" /><div className="h-36 animate-pulse rounded-lg border border-white/10 bg-white/[0.035]" /><div className="h-36 animate-pulse rounded-lg border border-white/10 bg-white/[0.035]" /></div>;
}

export function TickerOperationalIntelligenceCard({ symbol }: { symbol: string }) {
  const { data, failed, disabled, retry } = useTickerOperationalIntelligence();
  if (disabled) return null;
  return <section className="mt-5 rounded-lg border border-white/10 bg-slate-950/40 px-5 py-4">
    <div className="flex flex-wrap items-start justify-between gap-3"><div><p className="text-xs font-semibold uppercase tracking-[0.16em] text-emerald-300">Company developments</p><h2 className="mt-2 text-xl font-semibold text-white">The business behind the ticker</h2><p className="mt-2 max-w-3xl text-sm leading-6 text-slate-400">Source-grounded company developments from news, official releases, and enabled earnings-call coverage.</p></div><Link href={`/monitoring/research?ticker=${encodeURIComponent(symbol)}`} className="rounded-lg border border-emerald-300/25 px-3 py-2 text-xs font-semibold text-emerald-200 hover:bg-emerald-300/10">Build a thesis →</Link></div>
    <p className="mt-2 max-w-3xl text-xs leading-5 text-slate-500">News and press-release analysis uses provider excerpts, which may omit details from the full source. Releases may describe the company or its partners.</p>
    {data?.coverage ? <div className="mt-4"><ResearchCoverage items={data.coverage}/><p className="mt-2 text-xs text-slate-500">Recent evidence · past {data.lookback_days ?? 120} days. Coverage describes what has been checked; an empty section does not mean the business has no risks.</p></div> : null}
    {!data && !failed ? <LoadingGroups /> : failed ? <div className="mt-4 flex flex-wrap items-center gap-3 rounded-lg border border-white/10 bg-slate-950/35 p-3"><p className="text-sm text-slate-400">Operational-source intelligence is temporarily unavailable.</p><button type="button" onClick={retry} className="text-sm font-semibold text-emerald-200 hover:text-emerald-100">Try again</button></div> : data?.status === "empty" ? <p className="mt-4 rounded-lg border border-dashed border-white/10 px-3 py-3 text-sm leading-5 text-slate-500">No material operating-source signals have been processed for this ticker yet.</p> : <div className="mt-4 grid gap-5 md:grid-cols-2"><SignalGroup title="Catalysts" items={data?.catalysts ?? []} empty="No catalysts identified in the checked sources."/><SignalGroup title="Risks" items={data?.risks ?? []} empty="No risks identified in the checked sources."/><SignalGroup title="Opportunities" items={data?.opportunities ?? []} empty="No positive developments with explicit future milestones identified."/><SignalGroup title="What to watch next" items={data?.watch_next ?? []} empty="No explicit future milestones were extracted."/></div>}
  </section>;
}
