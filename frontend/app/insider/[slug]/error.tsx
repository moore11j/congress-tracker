"use client";

import { useEffect } from "react";

export default function InsiderProfileError({
  error,
  reset,
}: {
  error: Error & { digest?: string };
  reset: () => void;
}) {
  useEffect(() => {
    console.error("[insider-profile] route error boundary", {
      route: "/insider/[slug]",
      digest: error.digest ?? null,
      name: error.name,
      message: error.message,
    });
  }, [error]);

  return (
    <section className="rounded-3xl border border-white/10 bg-slate-950/80 p-6 shadow-card">
      <p className="text-xs font-semibold uppercase tracking-[0.3em] text-emerald-300">Insider profile</p>
      <h1 className="mt-3 text-2xl font-semibold text-white">This insider profile could not fully load.</h1>
      <p className="mt-2 max-w-2xl text-sm leading-6 text-slate-400">Try again, or return to the landing page and reopen the profile.</p>
      <div className="mt-5 flex flex-wrap gap-3">
        <button
          type="button"
          onClick={reset}
          className="rounded-lg bg-emerald-300 px-4 py-2 text-sm font-semibold text-slate-950 transition hover:bg-emerald-200"
        >
          Reload
        </button>
        <a
          href="https://walnutmarkets.com/"
          className="rounded-lg border border-white/10 bg-white/[0.03] px-4 py-2 text-sm font-semibold text-slate-100 transition hover:border-white/25 hover:bg-white/[0.06]"
        >
          Back to landing
        </a>
      </div>
    </section>
  );
}
