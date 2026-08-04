import { SkeletonBlock } from "@/components/ui/LoadingSkeleton";

const loadingMessages = [
  "Loading peer fundamentals",
  "Checking valuation evidence",
  "Loading price action",
  "Scanning catalysts",
  "Reviewing risk signals",
  "Checking confirmation score",
  "Building compare layout",
  "Rendering peer comparison",
];

export default function PeerCompareLoading() {
  return (
    <main className="min-h-screen bg-[#06111f] py-6 text-slate-100">
      <div className="mx-auto w-full max-w-none space-y-5">
        <section className="space-y-3" aria-busy="true">
          <p className="text-xs font-semibold uppercase tracking-[0.3em] text-emerald-300">Peer Compare</p>
          <div className="flex max-w-lg items-center gap-3">
            <div className="min-w-0 flex-1 overflow-hidden rounded-md border border-emerald-300/20 bg-slate-950/80 p-1 shadow-inner shadow-black/30">
              <div className="terminal-loading-progress-fill h-3 rounded-sm bg-gradient-to-r from-emerald-500 via-emerald-300 to-lime-100 shadow-[0_0_18px_rgba(52,211,153,0.5)]" />
            </div>
            <span className="terminal-loading-progress-percent w-11 shrink-0 text-right text-xs font-semibold tabular-nums text-emerald-100" aria-hidden="true" />
          </div>
          <p className="sr-only" aria-live="polite">Loading peer comparison.</p>
          <div className="terminal-loading-message text-xs font-semibold uppercase tracking-[0.2em] text-emerald-100/85" aria-hidden="true">
            {loadingMessages.map((message) => (
              <span key={message}>{message}</span>
            ))}
          </div>
        </section>

        <section className="rounded-md border border-white/10 bg-slate-900/45 p-3 shadow-card sm:p-4">
          <div className="mb-4 space-y-2">
            <SkeletonBlock className="h-8 w-56" />
            <SkeletonBlock className="h-4 w-80 max-w-full" />
          </div>
          <div className="mb-5 grid gap-3 lg:grid-cols-[1fr_auto_1fr]">
            <SkeletonBlock className="h-14 rounded-md" />
            <SkeletonBlock className="h-14 w-16 rounded-md" />
            <SkeletonBlock className="h-14 rounded-md" />
          </div>
          <div className="grid gap-3 lg:grid-cols-[1fr_1.2fr_1fr]">
            <SkeletonBlock className="h-36 rounded-md" />
            <SkeletonBlock className="h-36 rounded-md" />
            <SkeletonBlock className="h-36 rounded-md" />
          </div>
        </section>

        <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-3">
          {Array.from({ length: 9 }).map((_, index) => (
            <section key={index} className="rounded-md border border-white/10 bg-slate-900/45 p-3 shadow-card sm:p-4">
              <div className="space-y-3">
                <div className="flex items-center justify-between gap-3">
                  <SkeletonBlock className="h-4 w-36" />
                  <SkeletonBlock className="h-7 w-16 rounded-md" />
                </div>
                {Array.from({ length: 5 }).map((__, row) => (
                  <SkeletonBlock key={row} className="h-8 w-full rounded-md" />
                ))}
              </div>
            </section>
          ))}
        </div>
      </div>
    </main>
  );
}
