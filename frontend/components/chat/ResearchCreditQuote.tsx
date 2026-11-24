"use client";
import { useEffect, useState } from "react";
import { creditTasks, type ResearchMode } from "@/lib/researchCredits";

export function ResearchCreditQuote({ mode, onModeChange }: { mode: ResearchMode; onModeChange: (mode: ResearchMode) => void }) {
  const task = creditTasks.find(item => item.id === mode)!;
  const [balance, setBalance] = useState<{ available: number; billing_hold: boolean } | null>(null);
  const [status, setStatus] = useState("Balance not connected");
  useEffect(() => {
    setBalance(null);
    if (mode === "browse") return;
    const abort = new AbortController();
    setStatus("Checking sandbox balance…");
    fetch("/api/ask/credits/balance", { signal: abort.signal, cache: "no-store" })
      .then(async response => {
        if (!response.ok) throw new Error("Unavailable");
        const data = await response.json();
        if (data.sandbox !== true || !Number.isSafeInteger(data.available) || data.available < 0 || typeof data.billing_hold !== "boolean")
          throw new Error("Invalid balance");
        if (!abort.signal.aborted) setBalance(data);
      }).catch(() => { if (!abort.signal.aborted) setStatus("Balance not connected"); });
    return () => abort.abort();
  }, [mode]);
  return <div className="mb-2 space-y-2 border-b border-white/10 pb-3 text-xs">
    <div className="flex flex-wrap items-center justify-between gap-2">
      <label className="sr-only" htmlFor="research-mode">Research type</label>
      <select id="research-mode" value={mode} onChange={event => {
        if (creditTasks.some(item => item.id === event.target.value)) onModeChange(event.target.value as ResearchMode);
      }} className="min-h-11 max-w-full rounded-lg border border-white/10 bg-[#151c23] px-2 text-slate-200">
        {creditTasks.map(item => <option key={item.id} value={item.id}>{item.name} · {item.credits ? `${item.credits} credits` : "Free"}</option>)}
      </select>
      <span className="text-emerald-200">{task.credits ? `${task.credits} credits per task` : "0 credits"}</span>
    </div>
    <p className="text-slate-400">{task.description}</p>
    {mode !== "browse" && <div className="space-y-1 text-slate-400" role="status">
      <p>{balance ? balance.billing_hold ? "Billing review required" : `${balance.available.toLocaleString()} credits available${balance.available >= task.credits ? ` · ${(balance.available - task.credits).toLocaleString()} after this task` : " · Not enough for this task"}` : status}</p>
      <p>Includes one brief clarification using the same evidence. Your plan’s feature gates still apply.</p>
      <p className="text-amber-200/80">Price preview. AI answers are not connected yet; sending is disabled and no credits will be used.</p>
    </div>}
  </div>;
}
