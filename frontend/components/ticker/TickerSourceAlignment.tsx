import type { CrossSourceDivergence, CrossSourceDivergenceSource } from "@/lib/api";

function tone(state: string) {
  if (state === "strong_divergence") return "border-rose-300/35 bg-rose-300/10 text-rose-200";
  if (state === "moderate_divergence") return "border-amber-300/35 bg-amber-300/10 text-amber-200";
  if (state === "mild_divergence") return "border-sky-300/35 bg-sky-300/10 text-sky-200";
  if (state === "aligned") return "border-emerald-300/30 bg-emerald-300/10 text-emerald-200";
  return "border-slate-400/30 text-slate-400";
}

function Sources({ sources, direction }: { sources: CrossSourceDivergenceSource[]; direction: "bullish" | "bearish" | "neutral" }) {
  if (!sources.length) return null;
  return <div className="mt-2 flex flex-wrap items-center gap-x-3 gap-y-2 text-xs" aria-label={`${direction} evidence`}>
    {sources.map((source) => <span key={source.key} className="inline-flex items-center gap-1.5 text-slate-300"><span aria-hidden="true" className={direction === "bullish" ? "text-emerald-300" : direction === "bearish" ? "text-rose-300" : "text-slate-500"}>●</span><span className="sr-only">{direction}: </span>{source.label}</span>)}
  </div>;
}

export function TickerSourceAlignment({ divergence }: { divergence?: CrossSourceDivergence | null }) {
  if (!divergence) return <p className="text-sm text-slate-400">Source alignment unavailable.</p>;
  const total = divergence.bullish_source_count + divergence.bearish_source_count;
  const bullishShare = total > 0 ? divergence.bullish_source_count / total * 100 : 0;
  const bearishShare = total > 0 ? divergence.bearish_source_count / total * 100 : 0;
  return <section aria-label="Cross-Source Divergence" className="min-w-0">
    <div className="flex flex-wrap items-center justify-between gap-2"><h3 className="text-xs font-semibold uppercase tracking-wider text-slate-400">Source alignment</h3><span className={`rounded-full border px-2 py-0.5 text-xs font-semibold ${tone(divergence.state)}`}>{divergence.label}</span></div>
    <div className="mt-3 flex flex-wrap gap-x-3 gap-y-1 text-xs tabular-nums"><span className="text-slate-400">Source count</span><span className="text-emerald-300">{divergence.bullish_source_count} bullish</span><span className="text-rose-300">{divergence.bearish_source_count} bearish</span></div>
    <div className="mt-2 flex h-1 overflow-hidden rounded-full bg-slate-800" aria-hidden="true"><span className="bg-emerald-400" style={{ width: `${bullishShare}%` }} /><span className="bg-rose-400" style={{ width: `${bearishShare}%` }} /></div>
    {divergence.source_breakdown_available ? <><Sources sources={divergence.bullish_sources ?? []} direction="bullish" /><Sources sources={divergence.bearish_sources ?? []} direction="bearish" /><Sources sources={divergence.neutral_sources ?? []} direction="neutral" /></> : <p className="mt-2 text-xs text-slate-400">Source breakdown unavailable.</p>}
    <details className="mt-2 text-xs leading-5 text-slate-400"><summary className="w-fit cursor-pointer rounded py-1 text-emerald-200 focus-visible:outline focus-visible:outline-2 focus-visible:outline-emerald-300">Alignment &amp; methodology details</summary>
      <p className="mt-1 text-slate-300">{divergence.explanation ?? divergence.public_explanation}</p>
      <p className="mt-2">Divergence measures weighted disagreement between sources. The bar shows source counts, not their weight in the rating.</p>
      <p className="mt-1">{divergence.active_source_count} active sources evaluated · {divergence.methodology_version}</p>
    </details>
  </section>;
}
