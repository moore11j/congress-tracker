import type { ResearchMemoryMarket } from "@/lib/api";
import { researchCheckTime } from "@/lib/researchEvidence";

function money(value: number | null | undefined, signed = false) {
  if (value == null || !Number.isFinite(value)) return "Unavailable";
  return new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", signDisplay: signed ? "exceptZero" : "auto" }).format(value);
}
function change(value: number | null | undefined, suffix: string) {
  if (value == null || !Number.isFinite(value)) return "Unavailable";
  const decimals = suffix === " pts" ? 0 : value !== 0 && Math.abs(value) < 0.01 ? 4 : 2;
  return `${value > 0 ? "+" : ""}${value.toFixed(decimals)}${suffix}`;
}
function color(value: number | null | undefined) {
  return value == null || value === 0 ? "text-slate-300" : value > 0 ? "text-emerald-300" : "text-rose-300";
}
function score(value: number | null | undefined) { return value == null ? "Unavailable" : `${value}/100`; }

export function ResearchThesisMarket({ market }: { market?: ResearchMemoryMarket }) {
  if (!market) return <p className="mt-4 text-xs text-slate-500">Ticker price and creation snapshot unavailable.</p>;
  const { baseline, current } = market;
  return <div className="mt-4 space-y-3 rounded-lg border border-white/10 bg-slate-950/40 p-3">
    <div className="flex flex-wrap items-start justify-between gap-3">
      <div><p className="text-[11px] uppercase tracking-wider text-slate-500">Latest ticker price · USD</p><p className="mt-1 text-xl font-semibold tabular-nums text-white">{money(current.price)}</p><p className={`mt-1 text-xs tabular-nums ${color(current.day_change)}`}>Daily: {money(current.day_change, true)} {current.day_change_percent != null ? `(${change(current.day_change_percent, "%")})` : ""}</p></div>
      <div className="text-right"><p className="text-[11px] uppercase tracking-wider text-slate-500">Since thesis creation</p><p className={`mt-1 font-semibold tabular-nums ${color(market.price_change)}`}>{money(market.price_change, true)}</p><p className={`mt-1 text-sm tabular-nums ${color(market.price_change_percent)}`}>{change(market.price_change_percent, "%")}</p></div>
    </div>
    <div className="grid gap-3 border-t border-white/10 pt-3 sm:grid-cols-2">
      <div><p className="text-[11px] text-slate-500">Price at creation</p><p className="mt-1 text-sm font-semibold tabular-nums text-slate-200">{money(baseline.price)}</p><p className="mt-1 text-[10px] leading-4 text-slate-500">{baseline.price_as_of ? `${baseline.kind === "historical" ? "Historical reference" : "Captured quote"} · ${researchCheckTime(baseline.price_as_of)}` : "No historical price available"}</p></div>
      <div><p className="text-[11px] text-slate-500">Confirmation · at creation → now</p><p className="mt-1 text-sm font-semibold tabular-nums text-slate-200">{score(baseline.score)} → {score(current.score)}{market.score_change != null ? <span className="ml-2 text-xs font-normal text-slate-400">({change(market.score_change, " pts")})</span> : null}</p><p className="mt-1 text-[10px] capitalize text-slate-400">{baseline.direction ?? "Unknown"} → {current.direction ?? "Unknown"}</p><p className="mt-1 text-[10px] leading-4 text-slate-500">{baseline.score_as_of ? `Starting score · ${researchCheckTime(baseline.score_as_of)}` : "No creation-time score available"}</p>{market.score_methodology_changed ? <p className="mt-1 text-[10px] text-amber-200">Scoring method changed; point change unavailable.</p> : null}</div>
    </div>
    <p className="text-[10px] leading-4 text-slate-500">Price as of {researchCheckTime(current.price_as_of) ?? "unavailable"}{current.is_stale ? " · Update overdue" : ""}. Confirmation as of {researchCheckTime(current.score_as_of) ?? "unavailable"}.</p>
    {baseline.kind === "historical" ? <p className="text-[10px] leading-4 text-slate-500">Starting values reconstructed from historical data before creation.</p> : null}
    {market.price_change_unavailable_reason === "split_review_required" ? <p className="text-[10px] text-amber-200">Stock split since creation; price comparison requires adjustment.</p> : null}
    <p className="text-[10px] leading-4 text-slate-500">Price change excludes dividends. Confirmation measures evidence agreement, not thesis performance.</p>
  </div>;
}
