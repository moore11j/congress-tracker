"use client";

import { useEffect } from "react";

export default function TickerRouteError({
  error,
  reset,
}: {
  error: Error & { digest?: string };
  reset: () => void;
}) {
  useEffect(() => {
    console.error("[ticker] route error boundary", {
      route: "/ticker/[symbol]",
      digest: error.digest ?? null,
      name: error.name,
      message: error.message,
    });
  }, [error]);

  return (
    <section className="rounded-lg border border-white/10 bg-slate-950/80 p-6 shadow-card">
      <p className="text-xs font-semibold uppercase tracking-[0.3em] text-emerald-300">Walnut ticker intelligence</p>
      <h1 className="mt-3 text-2xl font-semibold text-white">This ticker page could not fully load.</h1>
      <p className="mt-2 max-w-2xl text-sm leading-6 text-slate-400">Reload the page, or return to the feed and reopen the ticker.</p>
      <div className="mt-5 flex flex-wrap gap-3">
        <button
          type="button"
          onClick={reset}
          className="rounded-lg bg-emerald-300 px-4 py-2 text-sm font-semibold text-slate-950 transition hover:bg-emerald-200"
        >
          Reload
        </button>
        <a
          href="/?mode=all"
          className="rounded-lg border border-white/10 bg-white/[0.03] px-4 py-2 text-sm font-semibold text-slate-100 transition hover:border-white/25 hover:bg-white/[0.06]"
        >
          Back to feed
        </a>
      </div>
    </section>
  );
}
