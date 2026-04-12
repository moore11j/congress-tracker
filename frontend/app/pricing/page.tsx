import Link from "next/link";
import { PricingActions } from "@/components/billing/PricingActions";

const rowClassName = "grid gap-3 border-t border-white/10 py-4 text-sm sm:grid-cols-[1.2fr_0.8fr_0.8fr]";
const cellClassName = "text-slate-300";

export default function PricingPage() {
  return (
    <div className="mx-auto max-w-5xl space-y-8">
      <section className="rounded-lg border border-white/10 bg-slate-900/80 p-6 shadow-2xl shadow-black/30">
        <div className="flex flex-wrap items-start justify-between gap-5">
          <div>
            <p className="text-xs font-semibold uppercase tracking-[0.3em] text-emerald-300">Plans & Pricing</p>
            <h1 className="mt-3 text-3xl font-semibold text-white">Choose the research pace that fits.</h1>
            <p className="mt-2 max-w-2xl text-sm leading-6 text-slate-300">
              Free stays useful for light research. Premium is built for daily monitoring, higher limits, and alert-first workflows.
            </p>
          </div>
          <PricingActions />
        </div>
      </section>

      <section className="overflow-hidden rounded-lg border border-white/10 bg-slate-900/70">
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
      </section>

      <div className="flex flex-wrap gap-3 text-sm">
        <Link href="/login" className="rounded-lg border border-white/10 px-4 py-2 font-semibold text-slate-200 transition hover:border-white/20 hover:text-white">
          Login / Register
        </Link>
        <Link href="/account/billing" className="rounded-lg border border-white/10 px-4 py-2 font-semibold text-slate-200 transition hover:border-white/20 hover:text-white">
          Manage billing
        </Link>
      </div>
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
