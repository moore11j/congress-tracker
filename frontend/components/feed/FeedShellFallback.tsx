export function FeedShellFallback() {
  return (
    <section className="flex flex-col gap-3">
      <div className="pt-4 text-left sm:pt-5">
        <p className="text-xs font-semibold uppercase tracking-[0.3em] text-emerald-300">Live Market Flow</p>
        <h1 className="mt-3 max-w-2xl text-4xl font-semibold text-white sm:text-5xl">Recent Congress and insider trades.</h1>
        <p className="mt-3 max-w-2xl text-sm leading-6 text-slate-400">
          One intelligence workflow: switch between All, Congress, Insider, Government Contracts, and Institutional Activity with mode-aware filters.
        </p>
      </div>
      <div className="mt-4 h-2 max-w-xl rounded-full bg-white/10" aria-hidden="true">
        <div className="h-full w-1/3 rounded-full bg-emerald-300/60" />
      </div>
    </section>
  );
}
