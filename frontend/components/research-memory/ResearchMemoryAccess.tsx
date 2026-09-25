"use client";

import { createContext, useContext, useEffect, useState, type ReactNode } from "react";
import { ApiError, getEntitlements } from "@/lib/api";
import { hasEntitlement } from "@/lib/entitlements";
import { ContextualUpgrade } from "@/components/billing/ContextualUpgrade";

export type ResearchAccessStatus = "loading" | "allowed" | "locked" | "error";
const AccessContext = createContext<{ status: ResearchAccessStatus; retry: () => void }>({ status: "loading", retry() {} });

export function ResearchMemoryAccessProvider({ children }: { children: ReactNode }) {
  const [status, setStatus] = useState<ResearchAccessStatus>("loading");
  const [attempt, setAttempt] = useState(0);
  useEffect(() => {
    let active = true;
    setStatus("loading");
    getEntitlements(undefined, { source: "ResearchMemoryAccess" }).then((entitlements) => {
      if (active) setStatus(entitlements.status === "temporarily_unavailable" ? "error" : hasEntitlement(entitlements, "view_research_memory") ? "allowed" : "locked");
    }).catch((error) => {
      if (active) setStatus(error instanceof ApiError && [401, 402, 403].includes(error.status) ? "locked" : "error");
    });
    return () => { active = false; };
  }, [attempt]);
  return <AccessContext.Provider value={{ status, retry: () => setAttempt(value => value + 1) }}>{children}</AccessContext.Provider>;
}

export function useResearchMemoryAccess() { return useContext(AccessContext); }

export function ResearchMemoryAccessNotice({ status, retry, title = "Research Memory", body = "Turn your investment thesis into private, structured research with source-linked evidence. Available with Premium." }: {
  status: ResearchAccessStatus; retry: () => void; title?: string; body?: string;
}) {
  if (status === "allowed") return null;
  if (status === "locked") return <ContextualUpgrade title={title} body={body} feature="view_research_memory" />;
  if (status === "error") return <div role="alert" className="rounded-lg border border-white/10 p-4 text-sm text-slate-400">Unable to check Research Memory access. <button type="button" onClick={retry} className="min-h-10 rounded px-2 font-semibold text-emerald-200">Try again</button></div>;
  return <div role="status" className="rounded-lg border border-white/10 p-4 text-sm text-slate-400">Checking Research Memory access…</div>;
}

export function ResearchMemoryGate({ children }: { children: ReactNode }) {
  const access = useResearchMemoryAccess();
  if (process.env.NEXT_PUBLIC_RESEARCH_MEMORY_ENABLED === "false") return <p className="p-4 text-sm text-slate-400">Research Memory is not available.</p>;
  return access.status === "allowed" ? <>{children}</> : <ResearchMemoryAccessNotice {...access} />;
}

export function ResearchMemoryAccessBoundary({ children }: { children: ReactNode }) {
  return <ResearchMemoryAccessProvider><ResearchMemoryGate>{children}</ResearchMemoryGate></ResearchMemoryAccessProvider>;
}
