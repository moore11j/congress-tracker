import { SkeletonBlock } from "@/components/ui/LoadingSkeleton";

const loadingMessages = [
  "Loading market components",
  "Loading index universe",
  "Auditing index names",
  "Checking symbol coverage",
  "Loading market weights",
  "Ranking sector pressure",
  "Building sector layout",
  "Rendering pressure map",
];

export default function MarketPressureLoading() {
  return (
    <div className="space-y-5" aria-busy="true">
      <section className="space-y-3">
        <p className="text-xs font-semibold uppercase tracking-[0.3em] text-emerald-300">Loading</p>
        <div className="flex max-w-lg items-center gap-3">
          <div className="min-w-0 flex-1 overflow-hidden rounded-md border border-emerald-300/20 bg-slate-950/80 p-1 shadow-inner shadow-black/30">
            <div className="market-pressure-progress-fill h-3 rounded-sm bg-gradient-to-r from-emerald-500 via-emerald-300 to-lime-100 shadow-[0_0_18px_rgba(52,211,153,0.5)]" />
          </div>
          <span className="market-pressure-progress-percent w-11 shrink-0 text-right text-xs font-semibold tabular-nums text-emerald-100" aria-hidden="true" />
        </div>
        <p className="sr-only" aria-live="polite">Loading market pressure map.</p>
        <div className="market-pressure-loading-message text-xs font-semibold uppercase tracking-[0.2em] text-emerald-100/85" aria-hidden="true">
          {loadingMessages.map((message) => (
            <span key={message}>{message}</span>
          ))}
        </div>
      </section>

      <section className="rounded-md border border-white/10 bg-slate-900/45 p-3 shadow-card sm:p-4">
        <div className="mb-3 space-y-2">
          <SkeletonBlock className="h-6 w-44" />
          <SkeletonBlock className="h-4 w-56" />
        </div>
        <div className="mb-4 flex flex-wrap gap-1">
          {Array.from({ length: 7 }).map((_, index) => (
            <SkeletonBlock key={index} className="h-10 w-20 rounded-none" />
          ))}
        </div>
        <div className="min-h-[34rem] overflow-hidden rounded-md border border-slate-950 bg-slate-950 p-1 sm:min-h-[42rem]">
          <div className="grid h-[34rem] grid-cols-[1.2fr_0.8fr_1fr] grid-rows-[0.9fr_1.1fr_0.8fr] gap-1 sm:h-[42rem]">
            <SkeletonBlock className="col-span-1 row-span-2 h-full w-full rounded-none" />
            <SkeletonBlock className="h-full w-full rounded-none" />
            <SkeletonBlock className="row-span-2 h-full w-full rounded-none" />
            <SkeletonBlock className="h-full w-full rounded-none" />
            <SkeletonBlock className="col-span-2 h-full w-full rounded-none" />
          </div>
        </div>
      </section>
    </div>
  );
}
