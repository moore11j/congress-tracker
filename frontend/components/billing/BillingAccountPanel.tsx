"use client";

import { useEffect, useState } from "react";
import { getEntitlements } from "@/lib/api";
import {
  defaultEntitlements,
  type Entitlements,
} from "@/lib/entitlements";

export function BillingAccountPanel() {
  const [entitlements, setEntitlements] = useState<Entitlements>(defaultEntitlements);

  useEffect(() => {
    let cancelled = false;
    getEntitlements()
      .then((next) => {
        if (!cancelled) setEntitlements(next);
      })
      .catch(() => {
        if (!cancelled) setEntitlements(defaultEntitlements);
      });
    return () => {
      cancelled = true;
    };
  }, []);

  return (
    <section className="rounded-lg border border-white/10 bg-slate-900/70 p-5">
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div>
          <p className="text-xs font-semibold uppercase tracking-wide text-emerald-300">Account</p>
          <h1 className="mt-1 text-3xl font-semibold text-white">
            {entitlements.tier === "premium" ? "Premium" : "Free"}
          </h1>
          <p className="mt-2 max-w-2xl text-sm text-slate-300">
            Free stays useful for research. Premium raises workflow limits and unlocks alert-first digests.
          </p>
        </div>
        <a
          href="#compare"
          className="inline-flex items-center justify-center rounded-lg border border-emerald-300/40 bg-emerald-300/10 px-4 py-2 text-sm font-semibold text-emerald-100 transition hover:bg-emerald-300/15"
        >
          Compare plans
        </a>
      </div>

      <div className="mt-5 grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
        <Metric label="Watchlists" value={entitlements.limits.watchlists} />
        <Metric label="Tickers per list" value={entitlements.limits.watchlist_tickers} />
        <Metric label="Saved views" value={entitlements.limits.saved_views} />
        <Metric label="Inbox sources" value={entitlements.limits.monitoring_sources} />
      </div>
    </section>
  );
}

function Metric({ label, value }: { label: string; value: number }) {
  return (
    <div className="rounded-lg border border-white/10 bg-slate-950/40 p-4">
      <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">{label}</div>
      <div className="mt-2 text-2xl font-semibold text-white">{value}</div>
    </div>
  );
}
