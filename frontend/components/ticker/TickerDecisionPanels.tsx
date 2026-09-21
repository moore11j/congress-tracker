"use client";

import type { TickerDecisionLayer } from "@/lib/api";
import { formatDateShort } from "@/lib/format";
import { researchSourceHref } from "@/lib/researchEvidence";
import { mergeOperationalOverview, type OverviewDecisionItem } from "@/lib/tickerOperationalOverview";
import { useTickerOperationalIntelligence } from "./TickerOperationalIntelligenceProvider";

function DecisionItem({ item }: { item: OverviewDecisionItem }) {
  const href = researchSourceHref(item.sourceUrl);
  const date = item.date ? formatDateShort(item.date) : item.freshness;
  return <div className="min-w-0 border-l border-white/10 pl-3">
    {item.evidenceId ? <>
      <p className="text-sm leading-6 text-slate-100">{item.description}</p>
      <div className="mt-1 flex flex-wrap items-center gap-x-2 gap-y-1 text-[11px] text-slate-500">
        <span>{item.title}{date ? ` · ${date}` : ""}</span>
        {href ? <a href={href} target="_blank" rel="noreferrer" className="text-emerald-200 hover:text-emerald-100">Source ↗</a> : null}
        <a href="#research" className="text-emerald-200 hover:text-emerald-100">Research details →</a>
      </div>
    </> : <>
      <div className="flex flex-wrap items-start justify-between gap-x-2 gap-y-1"><p className="text-sm font-semibold leading-5 text-slate-100">{item.title}</p>{date ? <span className="text-xs text-slate-500">{date}</span> : null}</div>
      <p className="mt-1 text-xs leading-5 text-slate-400">{item.description}</p>
    </>}
  </div>;
}

function DecisionPanel({ title, items = [], empty }: { title: string; items?: OverviewDecisionItem[]; empty: string }) {
  return <section className="min-w-0 rounded-lg border border-white/10 bg-slate-950/40 p-5">
    <h3 className="mb-4 text-xs font-semibold uppercase tracking-[0.16em] text-slate-200">{title}</h3>
    {items.length ? <div className="space-y-4">{items.map((item, index) => <DecisionItem key={`${item.evidenceId ?? item.category}-${index}`} item={item} />)}</div> : <p className="text-sm leading-6 text-slate-500">{empty}</p>}
  </section>;
}

export function TickerDecisionPanels({ layer }: { layer: TickerDecisionLayer }) {
  const { data, failed, disabled, retry } = useTickerOperationalIntelligence();
  const combined = mergeOperationalOverview(layer, disabled ? null : data);
  return <>
    <div className="mt-5 grid gap-4 lg:grid-cols-3">
      <DecisionPanel title="WHAT CHANGED (30D)" items={combined.what_changed} empty="No meaningful dated changes are available for this window." />
      <DecisionPanel title="CATALYSTS" items={combined.catalysts} empty="No catalysts identified in the available evidence." />
      <DecisionPanel title="RISKS" items={combined.risks} empty="No risks identified in the available evidence; this does not mean the business has no risks." />
    </div>
    <section className="mt-5 rounded-lg border border-white/10 bg-slate-950/40 px-5 py-4">
      <h3 className="mb-4 text-xs font-semibold uppercase tracking-[0.16em] text-slate-200">WHAT TO WATCH NEXT</h3>
      {combined.watch_items?.length ? <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-3">{combined.watch_items.map((item, index) => <DecisionItem key={`${item.category}-${index}`} item={item} />)}</div> : <p className="text-sm text-slate-500">No specific watch items are available yet.</p>}
    </section>
    {!disabled ? <p className="mt-2 text-xs leading-5 text-slate-500" role="status">
      {failed ? <>Company developments are temporarily unavailable; other confirmation inputs remain visible. <button type="button" onClick={retry} className="text-emerald-200">Retry</button></> : !data ? "Loading company developments…" : <>Source-grounded company findings are summarized here; <a href="#research" className="text-emerald-200 hover:text-emerald-100">see coverage and evidence in Research</a>. These findings do not change the confirmation score.</>}
    </p> : null}
  </>;
}
