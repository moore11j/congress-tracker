"use client";

import Link from "next/link";
import { VisibleEvent } from "@/components/analytics/VisibleEvent";
import { trackEvent } from "@/lib/productAnalytics";

export function ContextualUpgrade({ title, body, feature, tier = "Premium", compact = false }: { title: string; body: string; feature: string; tier?: "Premium" | "Pro"; compact?: boolean }) {
  const properties = { gated_feature: feature, target_plan: tier.toLowerCase() };
  return <VisibleEvent name="upgrade_prompt_viewed" properties={properties} className={`rounded-lg border border-emerald-300/25 bg-emerald-300/[0.06] ${compact ? "p-3" : "p-4"}`}>
    <p className="font-semibold text-emerald-100">{title}</p>
    <p className="mt-1 text-sm leading-6 text-slate-300">{body}</p>
    <Link href="/pricing" prefetch={false} onClick={() => trackEvent("upgrade_prompt_clicked", { ...properties, destination_page: "/pricing" })} className="mt-3 inline-flex min-h-10 items-center justify-center rounded-lg border border-emerald-400/40 bg-emerald-500/10 px-4 py-2 text-sm font-semibold text-emerald-200 transition hover:bg-emerald-500/20">Unlock with {tier}</Link>
  </VisibleEvent>;
}
