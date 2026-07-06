import { FeedPageClient } from "@/components/feed/FeedPageClient";
import type { Metadata } from "next";
import { Suspense } from "react";

// PR summary: Home feed ships a static shell first; the client hydrates mode-aware filters and the unified event tape after page load.
export const dynamic = "force-static";

export const metadata: Metadata = {
  alternates: {
    canonical: "/",
  },
};

function FeedShellFallback() {
  return (
    <div className="space-y-8">
      <section className="flex flex-col gap-6">
        <div className="flex flex-col gap-2">
          <p className="text-xs font-semibold uppercase tracking-[0.3em] text-emerald-300">Live Market Flow</p>
          <h1 className="text-4xl font-semibold text-white sm:text-5xl">Unified disclosure and market intelligence feed.</h1>
          <p className="max-w-2xl text-sm text-slate-400">
            One intelligence workflow: switch between All, Congress, Insider, Government Contracts, and Institutional Activity with mode-aware filters.
          </p>
        </div>
        <div className="rounded-3xl border border-white/10 bg-white/[0.03] p-5">
          <div className="h-4 w-44 rounded bg-white/10" />
          <div className="mt-4 grid gap-3 sm:grid-cols-3">
            <div className="h-11 rounded-xl bg-white/10" />
            <div className="h-11 rounded-xl bg-white/10" />
            <div className="h-11 rounded-xl bg-white/10" />
          </div>
        </div>
      </section>
      <div className="rounded-3xl border border-white/10 bg-white/[0.03] p-5">
        <div className="h-4 w-36 rounded bg-white/10" />
        <div className="mt-4 space-y-3">
          <div className="h-12 rounded-xl bg-white/10" />
          <div className="h-12 rounded-xl bg-white/10" />
          <div className="h-12 rounded-xl bg-white/10" />
        </div>
      </div>
    </div>
  );
}

export default function FeedPage() {
  return (
    <Suspense fallback={<FeedShellFallback />}>
      <FeedPageClient />
    </Suspense>
  );
}
