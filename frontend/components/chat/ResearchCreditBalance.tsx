"use client";
import Link from "next/link";
import { useResearchCreditBalance } from "@/lib/useResearchCreditBalance";

export function ResearchCreditBalance({ compact = false }: { compact?: boolean }) {
  const { balance, status } = useResearchCreditBalance();
  return <Link href="/ask/credits" className={compact
    ? "flex min-h-11 items-center rounded-lg border border-white/10 px-3 text-xs text-emerald-200"
    : "block rounded-xl border border-white/10 p-4 text-sm text-slate-300"} aria-live="polite">
    {balance ? <><strong>{balance.available.toLocaleString()} credits</strong>
      {!compact && <span className="mt-1 block">{balance.monthly.toLocaleString()} monthly · {balance.purchased.toLocaleString()} purchased · {balance.reserved.toLocaleString()} reserved</span>}
      {balance.billing_hold && <span className="block">Billing review required</span>}</>
      : compact ? "Credits" : <>Research credit balance · {status}</>}
  </Link>;
}
