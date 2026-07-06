"use client";

import dynamic from "next/dynamic";
import type { PlanConfig } from "@/lib/api";

const PricingPlanner = dynamic(
  () => import("@/components/billing/PricingPlanner").then((module) => module.PricingPlanner),
  {
    ssr: false,
    loading: () => <PricingFallback />,
  },
);

export function PricingPlannerDeferred({ config }: { config: PlanConfig }) {
  return <PricingPlanner config={config} />;
}

function PricingFallback() {
  return (
    <section className="rounded-lg border border-white/10 bg-slate-950/60 p-4 shadow-2xl shadow-black/25">
      <div className="flex flex-wrap items-end justify-between gap-4">
        <div>
          <p className="text-xs font-semibold uppercase tracking-[0.24em] text-emerald-300">Plans & Pricing</p>
          <h1 className="mt-2 text-3xl font-semibold text-white">Walnut Market Terminal plans</h1>
          <p className="mt-2 max-w-3xl text-sm leading-6 text-slate-400">
            Compact access, limits, and workflow capacity for Free, Premium, and Pro.
          </p>
        </div>
      </div>
      <div className="mt-5 grid gap-3 lg:grid-cols-3" aria-hidden="true">
        {["Free", "Premium", "Pro"].map((plan) => (
          <div key={plan} className="min-h-64 rounded-lg border border-white/10 bg-slate-900/70 p-4">
            <div className="h-5 w-24 rounded bg-white/10" />
            <div className="mt-5 h-8 w-28 rounded bg-white/10" />
            <div className="mt-6 grid grid-cols-2 gap-2">
              {Array.from({ length: 6 }).map((_, index) => (
                <div key={index} className="h-14 rounded-md border border-white/10 bg-slate-950/50" />
              ))}
            </div>
          </div>
        ))}
      </div>
    </section>
  );
}
