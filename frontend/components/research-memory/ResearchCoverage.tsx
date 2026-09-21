import type { ResearchSourceCoverage } from "@/lib/api";
import { researchDate, researchSourceLabels } from "@/lib/researchEvidence";

const labels: Record<string, string> = { ready: "Checked", empty: "No recent documents", disabled: "Not enabled", not_checked: "Awaiting coverage", partial: "Analysis in progress", refreshing: "Checking sources", unavailable: "Temporarily unavailable", stale: "Refresh overdue", pending: "Awaiting coverage" };

export function ResearchCoverage({ items }: { items: ResearchSourceCoverage[] }) {
  return <div className="grid gap-2 sm:grid-cols-3" aria-label="Source coverage">{items.map((item) => <div key={item.source_type} className="min-w-0 rounded-lg border border-white/10 bg-slate-950/30 px-3 py-2.5"><div className="flex flex-wrap items-center justify-between gap-2"><span className="text-xs font-semibold text-slate-300">{researchSourceLabels[item.source_type] ?? item.source_type}</span><span className={`text-[11px] ${["ready", "empty"].includes(item.status) ? "text-emerald-300" : "text-amber-200/90"}`}>{labels[item.status] ?? "Awaiting coverage"}</span></div><p className="mt-1 text-[11px] text-slate-500">{item.last_success_at ? `Last successful check ${researchDate(item.last_success_at)}` : "No completed source check yet"}</p></div>)}</div>;
}
