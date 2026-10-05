"use client";

import { useEffect, useState } from "react";
import { getEntitlements } from "@/lib/api";
import { hasEntitlement, type Entitlements } from "@/lib/entitlements";
import { SignalMixerWorkbench } from "@/components/backtesting/SignalMixerWorkbench";
import { ResearchToolHeader } from "./ResearchToolHeader";

export function SignalMixerTool({ initialEntitlements, today }: { initialEntitlements: Entitlements; today: string }) {
  const [entitlements, setEntitlements] = useState(initialEntitlements);
  const [checking, setChecking] = useState(!hasEntitlement(initialEntitlements, "backtesting"));
  useEffect(() => {
    let cancelled = false;
    getEntitlements().then(value => { if (!cancelled) setEntitlements(value); }).catch(() => {}).finally(() => { if (!cancelled) setChecking(false); });
    return () => { cancelled = true; };
  }, []);
  return <div className="research-tool">
    <ResearchToolHeader active="mixer" title="Signal Mixer" description="What happened after two signals lined up? Choose an earlier company event and a later purchase disclosure, then explore the stock’s returns against the S&P 500." />
    <div className="mb-6 grid gap-3 md:grid-cols-3">{[
      ["01", "Choose a sequence", "Start with an analyst upgrade followed by an insider purchase filing, or choose your own pair."],
      ["02", "Find historical matches", "Search the selected disclosure dates and keep only companies that meet your conditions."],
      ["03", "See what followed", "Compare 30-, 90- and 365-day stock returns with SPY. This studies events; it does not allocate a portfolio."],
    ].map(([step, title, text]) => <div key={step} className="tool-step"><span className="text-xs font-semibold text-emerald-300">{step}</span><h2 className="mt-2 text-sm font-semibold text-white">{title}</h2><p className="mt-2 text-xs leading-5 text-slate-400">{text}</p></div>)}</div>
    {checking ? <p role="status" className="mb-3 text-sm text-slate-400">Checking account access…</p> : null}
    <SignalMixerWorkbench canRun={!checking && hasEntitlement(entitlements, "backtesting")} today={today} />
  </div>;
}
