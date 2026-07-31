export function FeedShellFallback() {
  return (
    <section className="flex flex-col gap-3">
      <div className="pt-4 sm:pt-5">
        <p className="text-xs font-semibold uppercase tracking-[0.3em] text-emerald-300">Search Walnut Markets</p>
        <h1 className="mt-3 max-w-2xl text-2xl font-semibold leading-tight text-white sm:text-3xl">
          Search stocks. Follow the insiders.
        </h1>
        <p className="mt-3 max-w-2xl text-sm leading-6 text-slate-400">
          Find tickers, Congress members, insiders, institutions and government departments or browse the latest disclosed activity below.
        </p>
      </div>
      <div className="mt-5 grid max-w-2xl gap-3 rounded-lg border border-white/10 bg-slate-950/80 p-2 shadow-2xl shadow-black/25 sm:mt-8 sm:grid-cols-[1fr_auto]">
        <div className="flex min-w-0 items-center gap-3 rounded-md bg-white/[0.035] px-3 py-3">
          <div className="h-5 w-5 rounded-full border border-slate-500" aria-hidden="true" />
          <div className="h-4 min-w-0 flex-1 rounded bg-white/10" aria-hidden="true" />
        </div>
        <div className="h-11 rounded-lg border border-emerald-300/30 bg-emerald-300/10 px-5 py-3" aria-hidden="true" />
      </div>
      <div className="mt-5 flex flex-col gap-2 sm:mt-6 lg:mt-7">
        <p className="text-xs font-semibold uppercase tracking-[0.3em] text-emerald-300">Live Market Flow</p>
        <h2 className="text-4xl font-semibold text-white sm:text-5xl">Recent Congress and insider trades.</h2>
        <p className="max-w-2xl text-sm text-slate-400">
          One intelligence workflow: switch between All, Congress, Insider, Government Contracts, and Institutional Activity with mode-aware filters.
        </p>
      </div>
      <div className="mt-4 h-2 max-w-xl rounded-full bg-white/10" aria-hidden="true">
        <div className="h-full w-1/3 rounded-full bg-emerald-300/60" />
      </div>
    </section>
  );
}
