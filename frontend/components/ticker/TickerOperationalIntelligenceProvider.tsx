"use client";

import { createContext, useContext, useEffect, useState, type ReactNode } from "react";
import { ApiError, getTickerOperationalIntelligence, type TickerOperationalIntelligence } from "@/lib/api";

type State = { data: TickerOperationalIntelligence | null; failed: boolean; disabled: boolean; retry: () => void };
const IntelligenceContext = createContext<State>({ data: null, failed: false, disabled: true, retry: () => {} });

// One prepared-data request shared by Overview and Research. Switching tabs
// never fetches providers or invokes a model, and never duplicates this request.
export function TickerOperationalIntelligenceProvider({ symbol, children }: { symbol: string; children: ReactNode }) {
  const enabled = process.env.NEXT_PUBLIC_RESEARCH_OPERATIONAL_INTELLIGENCE_ENABLED !== "false";
  const [state, setState] = useState<{ symbol: string; data: TickerOperationalIntelligence | null; failed: boolean; disabled: boolean }>({ symbol, data: null, failed: false, disabled: !enabled });
  const [attempt, setAttempt] = useState(0);
  useEffect(() => {
    if (!enabled) return;
    let active = true;
    setState({ symbol, data: null, failed: false, disabled: false });
    getTickerOperationalIntelligence(symbol).then((data) => {
      if (active) setState({ symbol, data, failed: false, disabled: false });
    }).catch((error) => {
      if (active) setState({ symbol, data: null, failed: !(error instanceof ApiError && error.status === 404), disabled: error instanceof ApiError && error.status === 404 });
    });
    return () => { active = false; };
  }, [symbol, enabled, attempt]);
  const current = state.symbol === symbol ? state : { data: null, failed: false, disabled: !enabled };
  return <IntelligenceContext.Provider value={{ ...current, disabled: !enabled || current.disabled, retry: () => setAttempt((value) => value + 1) }}>{children}</IntelligenceContext.Provider>;
}

export function useTickerOperationalIntelligence() { return useContext(IntelligenceContext); }
