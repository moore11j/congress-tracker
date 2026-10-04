"use client";

import type { TickerDecisionLayer } from "@/lib/api";
import { formatDateShort } from "@/lib/format";
import { researchSourceHref } from "@/lib/researchEvidence";
import { mergeOperationalOverview, type OverviewDecisionItem } from "@/lib/tickerOperationalOverview";
import { useTickerOperationalIntelligence } from "./TickerOperationalIntelligenceProvider";
import { ContextualUpgrade } from "@/components/billing/ContextualUpgrade";
import { ArrowUpRight, ShieldAlert, Clock3, ScanLine } from "lucide-react";

const PANEL_STYLES = {
  CATALYSTS: { label: "Catalysts", icon: ArrowUpRight, heading: "text-emerald-300", surface: "border-emerald-400/20 bg-emerald-400/[0.035]", hint: "What could move the business" },
  RISKS: { label: "Risks", icon: ShieldAlert, heading: "text-rose-300", surface: "border-rose-400/20 bg-rose-400/[0.035]", hint: "What could challenge the outlook" },
  "WHAT CHANGED (30D)": { label: "What changed", icon: ScanLine, heading: "text-sky-300", surface: "border-sky-400/20 bg-sky-400/[0.035]", hint: "Developments over the last 30 days" },
  "WHAT TO WATCH NEXT": { label: "Watch next", icon: Clock3, heading: "text-amber-300", surface: "border-amber-400/20 bg-amber-400/[0.035]", hint: "The next checkpoints" },
} as const;

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
      <p className="text-sm leading-6 text-slate-300">{item.description}</p>
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

function DecisionPanel({ title, items = [], empty, locked = false }: { title: keyof typeof PANEL_STYLES; items?: OverviewDecisionItem[]; empty: string; locked?: boolean }) {
  const style = PANEL_STYLES[title];
  const Icon = style.icon;
  const renderItem = (item: OverviewDecisionItem, index: number) => <DecisionItem key={`${item.evidenceId ?? item.category}-${index}`} item={item} />;
  return <section className={`min-w-0 rounded-xl border p-4 ${style.surface}`}>
    <div className={`flex items-center gap-2.5 ${style.heading}`}>
      <Icon className="h-4 w-4 shrink-0" aria-hidden="true" />
      <h3 className="text-sm font-semibold tracking-wide">{style.label}</h3>
    </div>
    <p className="mb-2 mt-1.5 text-xs text-slate-400">{style.hint}</p>
    <div className={locked ? "pointer-events-none select-none opacity-70 blur-[2.5px]" : ""} aria-hidden={locked || undefined} inert={locked}>
      {items.length ? <>
        <div className="divide-y divide-white/[0.06]">{items.slice(0, 2).map(renderItem)}</div>
        {items.length > 2 ? <details className="group border-t border-white/10 pt-2">
          <summary className={`cursor-pointer rounded py-1 text-xs font-semibold focus-visible:outline focus-visible:outline-2 ${style.heading}`}>More {style.label.toLowerCase()} ({items.length - 2})</summary>
          <div className="divide-y divide-white/[0.06]">{items.slice(2).map(renderItem)}</div>
        </details> : null}
      </> : <p className="py-3 text-sm leading-6 text-slate-400">{empty}</p>}
    </div>
    {locked ? <div className="mt-2"><ContextualUpgrade title={title === "CATALYSTS" ? "Understand the catalysts" : title === "RISKS" ? "Understand the risks" : title === "WHAT CHANGED (30D)" ? "See what changed" : "Know what to watch next"} body="Unlock the evidence and context behind this ticker with Premium." feature="ticker_confirmation" compact /></div> : null}
  </section>;
}

export function TickerDecisionPanels({ layer, locked = false }: { layer: TickerDecisionLayer; locked?: boolean }) {
  const { data, failed, disabled, retry } = useTickerOperationalIntelligence();
  const combined = mergeOperationalOverview(layer, disabled ? null : data);
  return <>
    <div className="mt-5 grid items-start gap-4 lg:grid-cols-2">
      <div className="min-w-0 space-y-4">
        <DecisionPanel locked={locked} title="CATALYSTS" items={combined.catalysts} empty="No catalysts identified in the available evidence." />
        <DecisionPanel locked={locked} title="WHAT CHANGED (30D)" items={combined.what_changed} empty="No meaningful dated changes are available for this window." />
      </div>
      <div className="min-w-0 space-y-4">
        <DecisionPanel locked={locked} title="RISKS" items={combined.risks} empty="No risks identified in the available evidence; this does not mean the business has no risks." />
        <DecisionPanel locked={locked} title="WHAT TO WATCH NEXT" items={combined.watch_items} empty="No specific watch items are available yet." />
      </div>
    </div>
    {!disabled && !locked ? <p className="mt-2 text-xs leading-5 text-slate-500" role="status">
      {failed ? <>Company developments are temporarily unavailable; other confirmation inputs remain visible. <button type="button" onClick={retry} className="text-emerald-200">Retry</button></> : !data ? "Loading company developments…" : <>Source-grounded company findings are summarized here; <a href="#research" onClick={openResearch} className="text-emerald-200 hover:text-emerald-100">see coverage and evidence in Research</a>. These findings do not change the confirmation score.</>}
    </p> : null}
  </>;
}
