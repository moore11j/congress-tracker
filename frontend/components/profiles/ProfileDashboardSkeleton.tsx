import type { ReactNode } from "react";

type SkeletonVariant = "congress" | "insiders" | "institutions" | "departments";

const COPY: Record<SkeletonVariant, { eyebrow: string; title: string; subtitle: string; snapshot: string }> = {
  congress: {
    eyebrow: "CONGRESS",
    title: "Congress Trading",
    subtitle: "Track disclosed trades, portfolio activity, and market positioning across members of Congress.",
    snapshot: "Congress trading snapshot",
  },
  insiders: {
    eyebrow: "INSIDERS",
    title: "Corporate Insider Activity",
    subtitle: "Track purchases and sales from executives, directors, and major shareholders.",
    snapshot: "Insider trading snapshot",
  },
  institutions: {
    eyebrow: "INSTITUTIONS",
    title: "Institutional Holdings",
    subtitle: "Track institutional portfolios, quarterly position changes, accumulation, and sector exposure.",
    snapshot: "Institutional holdings snapshot",
  },
  departments: {
    eyebrow: "DEPARTMENTS",
    title: "Government Contract Activity",
    subtitle: "Track department spending, contract awards, vendors, and public-company exposure.",
    snapshot: "Government contract snapshot",
  },
};

export function ProfileDashboardSkeleton({ variant, filter }: { variant: SkeletonVariant; filter?: ReactNode }) {
  const copy = COPY[variant];

  return (
    <main className="relative min-w-0 space-y-3 overflow-hidden pb-3" aria-busy="true">
      <div aria-hidden className="pointer-events-none absolute inset-x-0 top-0 h-48 opacity-60 [background:radial-gradient(ellipse_at_70%_0%,rgba(52,211,153,.16),transparent_38%),radial-gradient(ellipse_at_25%_0%,rgba(59,130,246,.11),transparent_44%)]" />
      <header className="relative z-10 flex flex-col gap-3 pt-2 md:flex-row md:items-end md:justify-between">
        <div>
          <p className="text-xs font-semibold uppercase tracking-[.28em] text-emerald-300">{copy.eyebrow}</p>
          <h1 className="mt-2 text-3xl font-semibold leading-tight text-white md:text-4xl">{copy.title}</h1>
          <p className="mt-2 text-sm leading-6 text-slate-300">{copy.subtitle}</p>
        </div>
        {filter}
      </header>
      <section className="relative z-10 grid gap-3 xl:grid-cols-[1.7fr_.9fr]">
        <SkeletonPanel title={copy.snapshot} tall />
        <div className="grid gap-3 sm:grid-cols-2">
          {Array.from({ length: 4 }).map((_, index) => <SkeletonCard key={index} />)}
        </div>
      </section>
      <section className="grid gap-3 sm:grid-cols-2 xl:grid-cols-5">
        {Array.from({ length: 5 }).map((_, index) => <SkeletonCard key={index} />)}
      </section>
      <section className="grid gap-3 xl:grid-cols-2">
        <SkeletonPanel title="Loading table" rows={6} />
        <SkeletonPanel title="Loading table" rows={6} />
      </section>
      <section className="grid gap-3 xl:grid-cols-[1.05fr_.85fr_.8fr]">
        <SkeletonPanel title="Loading chart" rows={5} />
        <SkeletonPanel title="Loading chart" rows={5} />
        <SkeletonPanel title="Loading mix" rows={4} />
      </section>
      <section className="grid gap-3 xl:grid-cols-[.85fr_1fr_1.15fr]">
        <SkeletonPanel title="Loading breakdown" rows={5} />
        <SkeletonPanel title="Loading movers" rows={5} />
        <SkeletonPanel title="Loading activity" rows={5} />
      </section>
      <SkeletonPanel title="Loading activity over time" tall />
    </main>
  );
}

function SkeletonPanel({ title, rows = 3, tall = false }: { title: string; rows?: number; tall?: boolean }) {
  return (
    <section className="min-w-0 rounded-lg border border-slate-700/70 bg-slate-950/65 p-4 shadow-[0_18px_50px_rgba(0,0,0,.18)]">
      <div className="flex items-start justify-between gap-3">
        <div>
          <p className="h-3 w-36 animate-pulse rounded bg-slate-700/70" aria-label={title} />
          <p className="mt-2 h-2 w-28 animate-pulse rounded bg-slate-800/80" />
        </div>
        <p className="h-2 w-12 animate-pulse rounded bg-slate-800/80" />
      </div>
      <div className={tall ? "mt-5 h-44" : "mt-5 space-y-3"}>
        {tall ? <div className="h-full animate-pulse rounded-md bg-gradient-to-b from-emerald-300/15 to-slate-900/20" /> : Array.from({ length: rows }).map((_, index) => <p key={index} className="h-3 animate-pulse rounded bg-slate-800/80" style={{ width: `${92 - index * 7}%` }} />)}
      </div>
    </section>
  );
}

function SkeletonCard() {
  return (
    <div className="min-w-0 rounded-lg border border-slate-700/70 bg-slate-950/65 p-4">
      <p className="h-2.5 w-24 animate-pulse rounded bg-slate-700/70" />
      <p className="mt-4 h-6 w-28 animate-pulse rounded bg-slate-800/80" />
      <p className="mt-3 h-3 w-20 animate-pulse rounded bg-slate-800/70" />
    </div>
  );
}
