"use client";

import Link from "next/link";
import { useEffect, useState } from "react";
import { getTickerResearchMemories, hasClientAuthHint } from "@/lib/api";

export function TickerResearchMemoryCard({ symbol }: { symbol: string }) {
  if (process.env.NEXT_PUBLIC_RESEARCH_MEMORY_ENABLED === "false") return null;
  const [items, setItems] = useState<{ id: string; title: string; status: string }[] | null>(null);
  const authenticated = hasClientAuthHint();
  useEffect(() => { if (!authenticated) { setItems([]); return; } getTickerResearchMemories(symbol).then((response) => setItems(response.items)).catch(() => setItems(null)); }, [authenticated, symbol]);
  const active = items ?? [];
  return <section className="mt-5 rounded-lg border border-white/10 bg-slate-950/40 px-5 py-4"><p className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-400">Your Research</p>{!authenticated ? <div className="mt-2 flex flex-wrap items-center justify-between gap-3"><p className="text-sm text-slate-300">Create a private thesis for this ticker.</p><Link href={`/monitoring/research?ticker=${encodeURIComponent(symbol)}`} className="text-sm font-semibold text-emerald-200 hover:text-emerald-100">Create thesis</Link></div> : items === null ? <p className="mt-2 text-sm text-slate-500">Loading Research Memory…</p> : active.length === 0 ? <div className="mt-2 flex flex-wrap items-center justify-between gap-3"><p className="text-sm text-slate-300">No active thesis</p><Link href={`/monitoring/research?ticker=${encodeURIComponent(symbol)}`} className="text-sm font-semibold text-emerald-200 hover:text-emerald-100">Create thesis</Link></div> : <div className="mt-2 flex flex-wrap items-end justify-between gap-3"><div><p className="text-sm font-semibold text-white">{active.length === 1 ? active[0].title : `${active.length} active theses`}</p>{active.length > 1 ? <p className="mt-1 text-xs text-slate-400">{active.slice(0, 2).map((item) => item.title).join(" · ")}</p> : <p className="mt-1 text-xs text-emerald-200">Active — evidence monitoring is not running yet.</p>}</div><Link href={active.length === 1 ? `/monitoring/research/${active[0].id}` : "/monitoring/research"} className="text-sm font-semibold text-emerald-200 hover:text-emerald-100">View Research Memory</Link></div>}</section>;
}
