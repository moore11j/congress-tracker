import type { ResearchSourceCoverage } from "@/lib/api";
import { researchCheckTime, researchSourceLabels } from "@/lib/researchEvidence";

const labels: Record<string, string> = { ready: "Checked", empty: "No documents returned", disabled: "Not enabled", not_checked: "Awaiting coverage", partial: "Partially analyzed", refreshing: "Checking sources", unavailable: "Temporarily unavailable", stale: "Refresh overdue", pending: "Awaiting coverage" };

export function ResearchCoverage({ items, compact = false }: { items: ResearchSourceCoverage[]; compact?: boolean }) {
  if (compact) return <div aria-label="Source coverage" className="text-xs">
    <div className="flex flex-wrap gap-x-6 gap-y-2">{items.map((item) => <p key={item.source_type} className="flex flex-wrap items-center gap-x-2 gap-y-1">
      <span className={item.status === "ready" ? "text-emerald-300" : "text-amber-200"} aria-hidden="true">{item.status === "ready" ? "✓" : "○"}</span>
      <span className="font-semibold text-slate-200">{researchSourceLabels[item.source_type] ?? item.source_type}</span>
      <span className={item.status === "ready" ? "text-emerald-300" : "text-amber-200"}>{labels[item.status] ?? "Awaiting coverage"}</span>
      <span className="text-slate-400">· {item.documents_seen} document{item.documents_seen === 1 ? "" : "s"} returned</span>
    </p>)}</div>
    <details className="mt-2 text-slate-400">
      <summary className="w-fit cursor-pointer rounded py-1 text-emerald-200 focus-visible:outline focus-visible:outline-2 focus-visible:outline-emerald-300">Coverage details</summary>
      <dl className="mt-1 grid gap-3 sm:grid-cols-3">{items.map((item) => <div key={item.source_type}>
        <dt className="font-semibold text-slate-300">{researchSourceLabels[item.source_type] ?? item.source_type}</dt>
        <dd className="mt-1 leading-5">Last attempted: {item.last_checked_at ? researchCheckTime(item.last_checked_at) : "Not yet checked"}<br />Last successful check: {item.last_success_at ? researchCheckTime(item.last_success_at) : "No completed source check yet"}</dd>
      </div>)}</dl>
    </details>
  </div>;
  return <div className="grid gap-2 sm:grid-cols-3" aria-label="Source coverage">{items.map((item) => <div key={item.source_type} className="min-w-0 rounded-lg border border-white/10 bg-slate-950/30 px-3 py-2.5"><div className="flex flex-wrap items-center justify-between gap-2"><span className="text-xs font-semibold text-slate-300">{researchSourceLabels[item.source_type] ?? item.source_type}</span><span className={`text-[11px] ${["ready", "empty"].includes(item.status) ? "text-emerald-300" : "text-amber-200/90"}`}>{labels[item.status] ?? "Awaiting coverage"}</span></div><p className="mt-1 text-[11px] text-slate-500">{item.last_success_at ? `${item.documents_seen} documents returned · ${researchCheckTime(item.last_success_at)}` : "No completed source check yet"}</p></div>)}</div>;
}
