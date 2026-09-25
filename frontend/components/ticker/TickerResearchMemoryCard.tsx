"use client";

import Link from "next/link";
import { useEffect, useState } from "react";
import { ApiError, getTickerResearchMemories } from "@/lib/api";
import { ResearchMemoryAccessNotice, useResearchMemoryAccess } from "@/components/research-memory/ResearchMemoryAccess";

export function TickerResearchMemoryCard({ symbol }: { symbol: string }) {
  const access = useResearchMemoryAccess();
  const enabled = process.env.NEXT_PUBLIC_RESEARCH_MEMORY_ENABLED !== "false";
  const [state, setState] = useState<{ symbol: string; items: { id: string; title: string; status: string }[]; status: "loading" | "ready" | "error" | "locked" }>({ symbol, items: [], status: "loading" });
  const [attempt, setAttempt] = useState(0);
  useEffect(() => {
    if (!enabled || access.status !== "allowed") return;
    let active = true;
    setState({ symbol, items: [], status: "loading" });
    getTickerResearchMemories(symbol).then(response => {
      if (active) setState({ symbol, items: response.items, status: "ready" });
    }).catch(error => {
      if (active) setState({ symbol, items: [], status: error instanceof ApiError && [401, 402, 403].includes(error.status) ? "locked" : "error" });
    });
    return () => { active = false; };
  }, [symbol, enabled, access.status, attempt]);
  if (!enabled) return null;
  const notice = access.status !== "allowed" ? access.status : state.status === "locked" ? "locked" : null;
  if (notice) return <section className="mt-5" aria-label="Your Research"><ResearchMemoryAccessNotice status={notice} retry={access.retry} title="Your Research" body="Create a private investment thesis and follow source-linked evidence with Research Memory. Available with Premium." /></section>;
  const loading = state.symbol !== symbol || state.status === "loading";
  const items = loading ? [] : state.items;
  return <section className="mt-5 rounded-lg border border-white/10 bg-slate-950/40 px-5 py-4">
    <p className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-400">Your Research</p>
    {loading ? <p role="status" className="mt-2 text-sm text-slate-400">Loading Research Memory…</p> : state.status === "error" ? <div role="alert" className="mt-2 text-sm text-slate-400">Unable to load Research Memory. <button type="button" onClick={() => setAttempt(value => value + 1)} className="min-h-10 px-2 text-emerald-200">Try again</button></div> : <div className="mt-2 flex flex-wrap items-end justify-between gap-3">
      <div><p className="text-sm font-semibold text-white">{items.length === 0 ? "No active thesis" : items.length === 1 ? items[0].title : `${items.length} active theses`}</p>
        {items.length > 1 ? <p className="mt-1 text-xs text-slate-400">{items.slice(0, 2).map(item => item.title).join(" · ")}</p> : items.length === 1 ? <p className="mt-1 text-xs text-emerald-200">Active — operating-source evidence is matched as it is processed.</p> : null}
      </div><Link href={items.length === 0 ? `/monitoring/research?ticker=${encodeURIComponent(symbol)}` : items.length === 1 ? `/monitoring/research/${items[0].id}` : "/monitoring/research"} className="text-sm font-semibold text-emerald-200 hover:text-emerald-100">{items.length ? "View Research Memory" : "Create thesis"}</Link>
    </div>}
  </section>;
}
