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
      <p className="text-xs font-semibold uppercase tracking-[0.24em] text-emerald-300">Plans & Pricing</p>
      <h1 className="mt-2 text-3xl font-semibold text-white">Walnut Market Terminal plans</h1>
      <p className="mt-2 max-w-3xl text-sm leading-6 text-slate-400">
        Compact access, limits, and workflow capacity for Free, Premium, and Pro.
      </p>
      <div className="mt-5 h-2 rounded-full bg-white/10" aria-hidden="true">
        <div className="h-full w-1/3 rounded-full bg-emerald-300/60" />
      </div>
    </section>
  );
}
