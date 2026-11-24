"use client";

import Link from "next/link";
import { useEffect, useState } from "react";
import { getCachedLeaderboard, type CachedLeaderboardResponse } from "@/lib/api";
import { confirmationLabel } from "@/lib/confirmationLabel";
import { tickerHref } from "@/lib/ticker";

function text(value: unknown): string { return typeof value === "string" ? value : ""; }
function date(value: unknown): string {
  if (typeof value !== "string" || !Number.isFinite(Date.parse(value))) return "Update time unavailable";
  return new Intl.DateTimeFormat("en-US", { month: "short", day: "numeric", year: "numeric", hour: "numeric", minute: "2-digit", timeZone: "America/New_York", timeZoneName: "short" }).format(new Date(value));
}

export function DiscoveryStocks({ onResearch }: { onResearch?: (symbol: string) => void } = {}) {
  const [snapshot, setSnapshot] = useState<CachedLeaderboardResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [failed, setFailed] = useState(false);
  const [attempt, setAttempt] = useState(0);
  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setFailed(false);
    getCachedLeaderboard("top-stocks", { source: "Discover" })
      .then((result) => { if (!cancelled) setSnapshot(result); })
      .catch(() => { if (!cancelled) setFailed(true); })
      .finally(() => { if (!cancelled) setLoading(false); });
    return () => { cancelled = true; };
  }, [attempt]);

  return <DiscoveryStocksView snapshot={snapshot} loading={loading} failed={failed} onRetry={() => setAttempt((value) => value + 1)} onResearch={onResearch} />;
}

export function DiscoveryStocksView({ snapshot, loading, failed, onRetry, onResearch }: {
  snapshot: CachedLeaderboardResponse | null;
  loading: boolean;
  failed: boolean;
  onRetry: () => void;
  onResearch?: (symbol: string) => void;
}) {
  if (loading) return <div role="status" aria-busy="true" className="border-y border-white/10 py-8 text-sm text-slate-400">Loading stock research…</div>;
  if (failed || !snapshot) return <div role="status" className="rounded-lg border border-amber-300/20 bg-amber-300/5 p-5"><h3 className="font-semibold text-white">Stock research is temporarily unavailable</h3><p className="mt-2 text-sm text-slate-400">We could not load the rankings. This does not mean there are no stocks to investigate.</p><button type="button" onClick={onRetry} className="mt-4 rounded-md border border-white/20 px-4 py-2 text-sm text-white">Try again</button></div>;
  const rows = snapshot.items.filter((row) => tickerHref(text(row.symbol)));
  return <div className="space-y-4">
    <p className="text-[11px] leading-5 text-slate-500">Ranking updated: {date(snapshot.generated_at)}</p>
    {rows.length === 0 ? <p role="status" className="border-y border-white/10 py-6 text-sm text-slate-400">{snapshot.empty_message || "No ranked stocks are available in this snapshot. Coverage may be incomplete."}</p> : <div className="divide-y divide-white/10 border-y border-white/10">
      {rows.map((row) => {
        const symbol = text(row.symbol);
        const href = tickerHref(symbol)!;
        const score = snapshot.preview === false && typeof row.confirmation_score === "number" && Number.isFinite(row.confirmation_score) ? row.confirmation_score : null;
        return <article key={symbol} className="grid min-w-0 grid-cols-[30px_minmax(0,1fr)] gap-x-3 py-5 sm:grid-cols-[36px_minmax(0,1fr)_auto] sm:gap-x-4 sm:py-6">
          <span className="pt-1 font-mono text-xs tabular-nums text-slate-500">#{typeof row.rank === "number" ? row.rank : "—"}</span>
          <div className="min-w-0">
            <div className="flex flex-wrap items-baseline gap-x-3 gap-y-1"><h3 className="text-lg font-semibold tracking-tight text-white"><Link href={href} prefetch={false} onClick={event => { if (onResearch && event.button === 0 && !event.metaKey && !event.ctrlKey && !event.shiftKey && !event.altKey) { event.preventDefault(); onResearch(symbol); } }} className="hover:text-emerald-200">{symbol}</Link></h3><p className="min-w-0 break-words text-xs text-slate-400">{text(row.company_name) !== symbol ? text(row.company_name) : ""}</p></div>
            <p className="mt-2 text-sm leading-6 text-slate-300">{text(row.why_ranked) || "Review the company’s evidence, coverage and risks."}</p>
            <Link href={href} prefetch={false} onClick={event => { if (onResearch && event.button === 0 && !event.metaKey && !event.ctrlKey && !event.shiftKey && !event.altKey) { event.preventDefault(); onResearch(symbol); } }} className="mt-3 inline-flex items-center gap-2 text-xs font-medium text-emerald-200 hover:text-white">Review evidence and risks <span aria-hidden="true">→</span></Link>
          </div>
          {score !== null ? <div className="col-start-2 mt-4 border-l border-white/10 pl-3 sm:col-start-3 sm:row-start-1 sm:mt-0 sm:max-w-[150px] sm:pl-5 sm:text-right"><p className="text-[10px] text-slate-400">30-day Confirmation Score</p><p className="mt-1 text-2xl font-semibold tabular-nums text-white">{score}<span className="text-xs font-normal text-slate-500"> / 100</span></p><p className="mt-1 text-xs text-slate-300">{confirmationLabel(score, text(row.confirmation_direction), text(row.confirmation_band))}</p></div> : !snapshot.preview ? <p className="col-start-2 mt-3 text-xs text-slate-500">Confirmation score unavailable.</p> : null}
        </article>;
      })}
    </div>}
    {snapshot.preview && <p className="text-xs leading-5 text-slate-400">Detailed scores and evidence require Premium or Pro.</p>}
    {!!snapshot.locked_ranks?.length && <p className="text-sm text-slate-400">Ranks 3–5 are the public preview. <Link href="/login?mode=register&return_to=%2Fdiscover" prefetch={false} className="text-emerald-200">Create a free account</Link> to see ranks 1 and 2.</p>}
    <p className="max-w-2xl text-[11px] leading-5 text-slate-500">Rankings describe available evidence, not predicted returns. Check each company’s source dates, coverage and risks.</p>
  </div>;
}
