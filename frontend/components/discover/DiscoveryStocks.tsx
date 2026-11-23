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

export function DiscoveryStocks() {
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

  return <DiscoveryStocksView snapshot={snapshot} loading={loading} failed={failed} onRetry={() => setAttempt((value) => value + 1)} />;
}

export function DiscoveryStocksView({ snapshot, loading, failed, onRetry }: {
  snapshot: CachedLeaderboardResponse | null;
  loading: boolean;
  failed: boolean;
  onRetry: () => void;
}) {
  if (loading) return <div role="status" aria-busy="true" className="rounded-2xl border border-white/10 p-6 text-sm text-slate-400">Loading stock research…</div>;
  if (failed || !snapshot) return <div role="status" className="rounded-2xl border border-amber-300/20 bg-amber-300/5 p-6"><h3 className="font-semibold text-white">Stock research is temporarily unavailable</h3><p className="mt-2 text-sm text-slate-400">We could not load the rankings. This does not mean there are no stocks to investigate.</p><button type="button" onClick={onRetry} className="mt-4 rounded-lg border border-white/20 px-4 py-2 text-sm text-white">Try again</button></div>;
  const rows = snapshot.items.filter((row) => tickerHref(text(row.symbol)));
  return <div className="space-y-4">
    <p className="text-xs text-slate-400">Ranking updated: {date(snapshot.generated_at)}. Review each company’s evidence dates before drawing conclusions.</p>
    {rows.length === 0 ? <p role="status" className="rounded-2xl border border-white/10 p-6 text-sm text-slate-400">{snapshot.empty_message || "No ranked stocks are available in this snapshot. Coverage may be incomplete."}</p> : <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
      {rows.map((row) => {
        const symbol = text(row.symbol);
        const href = tickerHref(symbol)!;
        const score = snapshot.preview === false && typeof row.confirmation_score === "number" && Number.isFinite(row.confirmation_score) ? row.confirmation_score : null;
        return <article key={symbol} className="flex min-w-0 flex-col rounded-2xl border border-white/10 bg-[#081825] p-5 sm:p-6">
          <div className="flex items-start justify-between gap-3"><div className="min-w-0"><h3 className="text-xl font-semibold text-white"><Link href={href} prefetch={false} className="hover:text-emerald-200">{symbol}</Link></h3><p className="mt-1 break-words text-sm text-slate-400">{text(row.company_name)}</p></div><span className="rounded-lg border border-white/10 px-2 py-1 text-xs text-slate-400">#{typeof row.rank === "number" ? row.rank : "—"}</span></div>
          {score !== null ? <div className="mt-5"><p className="text-xs text-slate-400">30-day Confirmation Score</p><p className="mt-1 text-2xl font-semibold text-emerald-200">{score}<span className="text-sm text-slate-500"> / 100</span></p><p className="mt-1 text-xs text-slate-300">{confirmationLabel(score, text(row.confirmation_direction), text(row.confirmation_band))}</p></div> : snapshot.preview ? <p className="mt-5 text-xs text-slate-400">Detailed scores and evidence require Premium or Pro.</p> : <p className="mt-5 text-xs text-slate-400">Confirmation score unavailable.</p>}
          <p className="mt-4 text-sm leading-6 text-slate-300">{text(row.why_ranked) || "Open the company’s research to inspect its evidence, coverage and risks."}</p>
          <Link href={href} prefetch={false} className="mt-auto pt-5 text-sm font-semibold text-emerald-200 hover:text-white">Review evidence and risks <span aria-hidden="true">→</span></Link>
        </article>;
      })}
    </div>}
    {!!snapshot.locked_ranks?.length && <p className="text-sm text-slate-400">Ranks 3–5 are the public preview. <Link href="/login?mode=register&return_to=%2Fdiscover" prefetch={false} className="text-emerald-200">Create a free account</Link> to see ranks 1 and 2.</p>}
    <p className="text-xs leading-5 text-slate-500">Rankings describe available evidence. They do not predict returns or establish that a stock is a suitable investment.</p>
  </div>;
}
