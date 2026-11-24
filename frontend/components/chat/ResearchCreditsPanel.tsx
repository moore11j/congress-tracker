"use client";
import Link from "next/link";
import { useEffect, useState, useRef } from "react";
import { ArrowLeft, ArrowUpRight } from "lucide-react";
import { creditAmountCents, creditPlans, creditTasks } from "@/lib/researchCredits";

type Balance = { monthly: number; purchased: number; available: number; reserved: number; billing_hold: boolean; sandbox: true };
export function ResearchCreditsPanel() {
  const [amount, setAmount] = useState("10");
  const [balance, setBalance] = useState<Balance | null>(null);
  const [status, setStatus] = useState("Connecting to the billing sandbox…");
  const [busy, setBusy] = useState(false);
  const purchase = useRef<{ cents: number; id: string } | null>(null);
  const cents = creditAmountCents(amount);
  useEffect(() => {
    const abort = new AbortController();
    fetch("/api/ask/credits/balance", { signal: abort.signal, cache: "no-store" })
      .then(async response => {
        if (!response.ok) {
          setStatus(response.status === 401 || response.status === 403
            ? "Sign in with a verified sandbox account to test billing."
            : "Billing sandbox is not connected yet. Purchases are unavailable."); return;
        }
        const data = await response.json();
        if (data.sandbox !== true || ![data.monthly, data.purchased, data.available, data.reserved].every(n => Number.isSafeInteger(n) && n >= 0))
          throw new Error("Invalid balance");
        setBalance(data); setStatus(data.billing_hold ? "Billing review required. New purchases are paused." : "Sandbox balance. These credits are for testing only.");
      }).catch(() => { if (!abort.signal.aborted) setStatus("Billing sandbox is unavailable. No balance has been confirmed."); });
    return () => abort.abort();
  }, []);
  async function checkout() {
    if (!cents || busy || !balance || balance.billing_hold) return;
    setBusy(true);
    if (!purchase.current || purchase.current.cents !== cents) purchase.current = { cents, id: crypto.randomUUID() };
    try {
      const response = await fetch("/api/ask/credits/checkout", { method: "POST", headers: { "content-type": "application/json" },
        body: JSON.stringify({ amount_cents: cents, request_id: purchase.current.id }) });
      const data = await response.json();
      if (!response.ok) throw new Error(typeof data.detail === "string" ? data.detail : "Sandbox checkout could not be opened.");
      const url = new URL(data.url);
      if (data.sandbox !== true || url.protocol !== "https:" || url.host !== "checkout.stripe.com" || !String(data.id).startsWith("cs_test_"))
        throw new Error("Only Stripe sandbox checkout is available here.");
      window.location.assign(url.href);
    } catch (error) { setStatus(error instanceof Error ? error.message : "Checkout is unavailable."); }
    finally { setBusy(false); }
  }
  return <div className="min-h-[100dvh] bg-[#151c23] px-4 py-7 text-slate-200 sm:px-8">
    <div className="mx-auto max-w-5xl">
      <Link href="/ask" className="inline-flex min-h-11 items-center gap-2 text-sm text-emerald-200"><ArrowLeft className="h-4 w-4" /> Back to Ask Walnut</Link>
      <div className="mt-6 flex flex-wrap items-center gap-3"><h1 className="text-3xl font-medium tracking-tight text-white">Research credits</h1><span className="rounded-full border border-amber-200/20 px-3 py-1 text-xs text-amber-200">Billing preview</span></div>
      <p className="mt-3 max-w-2xl text-sm leading-6 text-slate-400">Your plan unlocks research tools and data. Credits cover generated answers. Searching for a company and opening its chart use no credits.</p>
      <div role="status" className="mt-6 rounded-xl border border-white/10 p-4 text-sm text-slate-300">{status} {balance && !balance.billing_hold && <span className="mt-2 block font-medium text-white">{balance.available.toLocaleString()} available · {balance.monthly.toLocaleString()} monthly · {balance.purchased.toLocaleString()} purchased · {balance.reserved.toLocaleString()} reserved</span>}</div>
      <section aria-label="Monthly plan credits" className="mt-8 grid gap-4 md:grid-cols-3">{creditPlans.map(plan => <article key={plan.name} className="rounded-2xl border border-white/10 p-5">
        <h2 className="text-base font-medium text-white">{plan.name}</h2><p className="mt-4 text-2xl font-medium text-white">{plan.monthly}<span className="ml-1 text-sm font-normal text-slate-500">/month</span></p>
        <p className="mt-2 text-sm text-emerald-200">{plan.credits.toLocaleString()} credits each month</p><p className="mt-3 text-xs leading-5 text-slate-400">{plan.annual ? `${plan.annual}/year with annual billing. Credits still refresh monthly.` : "Try sourced answers with a verified account."}</p>
      </article>)}</section>
      <div className="mt-8 grid gap-8 md:grid-cols-2">
        <section><h2 className="text-lg font-medium text-white">Add credits when you need them</h2><p className="mt-2 text-sm text-slate-400">$1 buys 100 credits. Purchased credits carry forward.</p>
          <div className="mt-4 grid grid-cols-4 gap-2">{[5, 10, 20, 50].map(value => <button key={value} type="button" onClick={() => setAmount(String(value))} aria-pressed={amount === String(value)} className={`min-h-11 rounded-lg border text-sm ${amount === String(value) ? "border-emerald-300/60 bg-emerald-300/10 text-emerald-200" : "border-white/10 text-slate-300"}`}>${value}</button>)}</div>
          <label className="mt-5 block text-sm text-slate-300" htmlFor="credit-amount">Custom amount (USD)</label><input id="credit-amount" inputMode="decimal" value={amount} onChange={e => setAmount(e.target.value)} className="mt-2 min-h-11 w-full rounded-lg border border-white/15 bg-[#0d141b] px-3 text-base text-white" aria-describedby="credit-amount-note" />
          <p id="credit-amount-note" className="mt-2 text-sm text-slate-400">{cents ? `${cents.toLocaleString()} credits for $${(cents / 100).toFixed(2)}` : "Enter $5 to $1,000 for this billing test."}</p>
          <button type="button" disabled={!cents || !balance || balance.billing_hold || busy} onClick={() => void checkout()} className="mt-4 flex min-h-12 w-full items-center justify-center gap-2 rounded-lg bg-emerald-300 px-4 text-sm font-medium text-slate-950 disabled:cursor-not-allowed disabled:opacity-40">{busy ? "Opening sandbox checkout…" : "Test checkout"}<ArrowUpRight className="h-4 w-4" /></button>
          <p className="mt-3 text-xs leading-5 text-slate-500">Test payments only. Returning from checkout does not confirm payment; the balance updates after Stripe verifies it.</p>
        </section>
        <section><h2 className="text-lg font-medium text-white">Know the cost before you ask</h2><dl className="mt-4 divide-y divide-white/10">{creditTasks.map(task => <div key={task.name} className="flex justify-between gap-4 py-4 text-sm"><dt className="text-slate-300">{task.name}</dt><dd className="shrink-0 text-emerald-200">{task.credits ? `${task.credits} credits` : "Free"}</dd></div>)}</dl>
          <p className="mt-4 text-sm leading-6 text-slate-400">Monthly credits reset each cycle. Extra credits do not unlock Premium or Pro features. Automatic top-ups are off.</p>
          <p className="mt-4 text-xs leading-5 text-slate-500">Preview allowances and task prices. AI answers and automatic monthly grants are still being connected. No live subscription changes are made here.</p>
        </section>
      </div>
    </div>
  </div>;
}
