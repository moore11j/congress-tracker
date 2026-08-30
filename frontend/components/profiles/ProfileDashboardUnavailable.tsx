"use client";

import type { ReactNode } from "react";

type ProfileDashboardKind = "profiles" | "congress" | "insiders";

const COPY: Record<ProfileDashboardKind, { eyebrow: string; title: string; description: string }> = {
  profiles: {
    eyebrow: "PROFILES",
    title: "Profiles snapshot temporarily unavailable",
    description: "We could not read the prepared profile snapshot just now. Your access has not changed; retry to load the latest cached view.",
  },
  congress: {
    eyebrow: "CONGRESS",
    title: "Congress snapshot temporarily unavailable",
    description: "We could not read the prepared Congress snapshot just now. Retry to load the latest cached view.",
  },
  insiders: {
    eyebrow: "INSIDERS",
    title: "Insider snapshot temporarily unavailable",
    description: "We could not read the prepared insider snapshot just now. Retry to load the latest cached view.",
  },
};

export function ProfileDashboardUnavailable({ kind, filter }: { kind: ProfileDashboardKind; filter?: ReactNode }) {
  const copy = COPY[kind];

  return (
    <main className="relative min-w-0 overflow-hidden pb-3">
      <div aria-hidden className="pointer-events-none absolute inset-x-0 top-0 h-48 opacity-60 [background:radial-gradient(ellipse_at_70%_0%,rgba(52,211,153,.16),transparent_38%),radial-gradient(ellipse_at_25%_0%,rgba(59,130,246,.11),transparent_44%)]" />
      <header className="relative z-10 flex flex-col gap-3 pt-2 md:flex-row md:items-end md:justify-between">
        <div>
          <p className="text-xs font-semibold uppercase tracking-[.28em] text-emerald-300">{copy.eyebrow}</p>
          <h1 className="mt-2 text-3xl font-semibold leading-tight text-white md:text-4xl">{copy.title}</h1>
          <p className="mt-2 max-w-3xl text-sm leading-6 text-slate-300">{copy.description}</p>
        </div>
        {filter}
      </header>
      <section className="relative z-10 mt-4 rounded-lg border border-slate-700/70 bg-slate-950/65 p-5 shadow-[0_18px_50px_rgba(0,0,0,.18)]">
        <p className="text-sm leading-6 text-slate-400">This page only uses prepared snapshot data. No live refresh is running in your request.</p>
        <button type="button" onClick={() => window.location.reload()} className="mt-4 inline-flex items-center rounded-md border border-emerald-300/40 bg-emerald-400/10 px-4 py-2 text-sm font-semibold text-emerald-100 transition hover:bg-emerald-400/20">
          Retry cached snapshot
        </button>
      </section>
    </main>
  );
}
