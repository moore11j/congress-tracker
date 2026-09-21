"use client";

import type { TickerDecisionLayer } from "@/lib/api";
import { formatDateShort } from "@/lib/format";
import { researchSourceHref } from "@/lib/researchEvidence";
import { mergeOperationalOverview, type OverviewDecisionItem } from "@/lib/tickerOperationalOverview";
import { useTickerOperationalIntelligence } from "./TickerOperationalIntelligenceProvider";

function openResearch() {
  // The user may have selected Overview after previously following #research.
  // Re-select the tab even when the URL fragment is already unchanged.
  window.location.hash = "research";
  window.dispatchEvent(new HashChangeEvent("hashchange"));
}

function DecisionItem({ item }: { item: OverviewDecisionItem }) {
  const href = researchSourceHref(item.sourceUrl);
  const date = item.date ? formatDateShort(item.date) : item.freshness;
  return <div className="min-w-0 py-2.5 [overflow-wrap:anywhere]">
    {item.evidenceId ? <>
      <p className="text-sm leading-6 text-slate-100">{item.description}</p>
      <div className="mt-1 flex flex-wrap items-center gap-x-2 gap-y-1 text-[11px] text-slate-500">
        <span>{item.title}{date ? ` · ${date}` : ""}</span>
        {href ? <a href={href} target="_blank" rel="noreferrer" className="text-emerald-200 hover:text-emerald-100">Source ↗</a> : null}
        <a href="#research" onClick={openResearch} className="text-emerald-200 hover:text-emerald-100">Research details →</a>
      </div>
    </> : <>
      <div className="flex flex-wrap items-start justify-between gap-x-2 gap-y-1"><p className="text-sm font-semibold leading-5 text-slate-100">{item.title}</p>{date ? <span className="text-xs text-slate-500">{date}</span> : null}</div>
      <p className="mt-1 text-xs leading-5 text-slate-400">{item.description}</p>
    </>}
  </div>;
}

function DecisionPanel({ title, items = [], empty }: { title: string; items?: OverviewDecisionItem[]; empty: string }) {
  return <section className="min-w-0">
    <h3 className="border-b border-white/10 pb-2 text-xs font-semibold uppercase tracking-[0.14em] text-slate-300">{title}</h3>
    {items.length ? <div className="divide-y divide-white/[0.06]">{items.map((item, index) => <DecisionItem key={`${item.evidenceId ?? item.category}-${index}`} item={item} />)}</div> : <p className="py-3 text-sm leading-6 text-slate-400">{empty}</p>}
  </section>;
}

export function TickerDecisionPanels({ layer }: { layer: TickerDecisionLayer }) {
  const { data, failed, disabled, retry } = useTickerOperationalIntelligence();
  const combined = mergeOperationalOverview(layer, disabled ? null : data);
  return <>
    <div className="mt-5 grid items-start gap-5 lg:grid-cols-2 lg:gap-6">
      <div className="min-w-0 space-y-5 lg:border-r lg:border-white/10 lg:pr-6">
        <DecisionPanel title="CATALYSTS" items={combined.catalysts} empty="No catalysts identified in the available evidence." />
        <DecisionPanel title="WHAT CHANGED (30D)" items={combined.what_changed} empty="No meaningful dated changes are available for this window." />
      </div>
      <div className="min-w-0 space-y-5">
        <DecisionPanel title="RISKS" items={combined.risks} empty="No risks identified in the available evidence; this does not mean the business has no risks." />
        <DecisionPanel title="WHAT TO WATCH NEXT" items={combined.watch_items} empty="No specific watch items are available yet." />
      </div>
    </div>
    {!disabled ? <p className="mt-2 text-xs leading-5 text-slate-500" role="status">
      {failed ? <>Company developments are temporarily unavailable; other confirmation inputs remain visible. <button type="button" onClick={retry} className="text-emerald-200">Retry</button></> : !data ? "Loading company developments…" : <>Source-grounded company findings are summarized here; <a href="#research" onClick={openResearch} className="text-emerald-200 hover:text-emerald-100">see coverage and evidence in Research</a>. These findings do not change the confirmation score.</>}
    </p> : null}
  </>;
}
