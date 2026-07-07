export function FeedShellFallback() {
  return (
    <section className="flex flex-col gap-3">
      <p className="text-xs font-semibold uppercase tracking-[0.3em] text-emerald-300">Live Market Flow</p>
      <h1 className="text-4xl font-semibold text-white sm:text-5xl">Unified disclosure and market intelligence feed.</h1>
      <p className="max-w-2xl text-sm text-slate-400">
        One intelligence workflow: switch between All, Congress, Insider, Government Contracts, and Institutional Activity with mode-aware filters.
      </p>
      <div className="mt-4 h-2 max-w-xl rounded-full bg-white/10" aria-hidden="true">
        <div className="h-full w-1/3 rounded-full bg-emerald-300/60" />
      </div>
    </section>
  );
}
