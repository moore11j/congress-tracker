import Link from "next/link";
import { AccountAccessPanel } from "@/components/billing/AccountAccessPanel";
import { BillingAccountPanel } from "@/components/billing/BillingAccountPanel";

const rowClassName = "grid gap-3 border-t border-white/10 py-4 text-sm sm:grid-cols-[1.2fr_0.8fr_0.8fr]";
const cellClassName = "text-slate-300";

export const dynamic = "force-dynamic";

export default function BillingPage() {
  return (
    <div className="space-y-8">
      <AccountAccessPanel />
      <BillingAccountPanel />

      <section id="compare" className="rounded-lg border border-white/10 bg-slate-900/70 p-5">
        <div className="flex flex-wrap items-start justify-between gap-4">
          <div>
            <p className="text-xs font-semibold uppercase tracking-wide text-emerald-300">Plans</p>
            <h2 className="mt-1 text-2xl font-semibold text-white">Keep the core product generous.</h2>
            <p className="mt-2 max-w-2xl text-sm text-slate-300">
              Premium is for heavier monitoring, more saved research paths, and alert-first email workflows.
            </p>
          </div>
          <Link
            href="/watchlists"
            prefetch={false}
            className="inline-flex items-center justify-center rounded-lg border border-white/10 px-4 py-2 text-sm font-semibold text-slate-200 transition hover:border-white/20 hover:text-white"
          >
            Back to work
          </Link>
        </div>

        <div className="mt-6 overflow-hidden rounded-lg border border-white/10">
          <div className="grid gap-3 bg-slate-950/60 px-4 py-3 text-xs font-semibold uppercase tracking-wide text-slate-400 sm:grid-cols-[1.2fr_0.8fr_0.8fr]">
            <div>Feature</div>
            <div>Free</div>
            <div>Premium</div>
          </div>
          <PlanRow feature="Watchlists" free="3 lists" premium="25 lists" />
          <PlanRow feature="Tickers per watchlist" free="15 tickers" premium="100 tickers" />
          <PlanRow feature="Saved views" free="5 views" premium="50 views" />
          <PlanRow feature="Monitoring inbox" free="8 sources" premium="100 sources" />
          <PlanRow feature="Email digests and alerts" free="Not included" premium="Included" />
        </div>
      </section>
    </div>
  );
}

function PlanRow({ feature, free, premium }: { feature: string; free: string; premium: string }) {
  return (
    <div className={`${rowClassName} px-4`}>
      <div className="font-medium text-white">{feature}</div>
      <div className={cellClassName}>{free}</div>
      <div className={cellClassName}>{premium}</div>
    </div>
  );
}
