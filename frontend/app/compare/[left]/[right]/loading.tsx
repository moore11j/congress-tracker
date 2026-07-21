import { SkeletonBlock, SkeletonCard } from "@/components/ui/LoadingSkeleton";

export default function PeerCompareLoading() {
  return (
    <main className="min-h-screen bg-[#06111f] px-4 py-6 text-slate-100 sm:px-6 lg:px-8">
      <div className="mx-auto max-w-7xl space-y-5">
        <div className="rounded-lg border border-white/10 bg-slate-950/70 p-5">
          <p className="text-xs font-semibold uppercase tracking-[0.3em] text-emerald-300">Peer Compare</p>
          <div className="mt-3 flex flex-wrap items-center justify-between gap-4">
            <div className="space-y-3">
              <SkeletonBlock className="h-8 w-56" />
              <SkeletonBlock className="h-3 w-80 max-w-full" />
            </div>
            <div className="h-10 w-48 rounded-full border border-emerald-300/20 bg-emerald-300/10" />
          </div>
          <div className="mt-5 overflow-hidden rounded-full border border-white/10 bg-white/[0.04]">
            <div className="peer-compare-progress-fill h-2 rounded-full bg-gradient-to-r from-cyan-300 via-emerald-300 to-violet-300" />
          </div>
          <p className="mt-3 text-xs font-semibold uppercase tracking-[0.2em] text-slate-400">
            Loading complete peer metrics
          </p>
        </div>

        <SkeletonCard>
          <div className="grid gap-3 lg:grid-cols-[1fr_1.2fr_1fr]">
            <SkeletonBlock className="h-36 rounded-lg" />
            <SkeletonBlock className="h-36 rounded-lg" />
            <SkeletonBlock className="h-36 rounded-lg" />
          </div>
        </SkeletonCard>

        <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-3">
          {Array.from({ length: 9 }).map((_, index) => (
            <SkeletonCard key={index}>
              <div className="space-y-3">
                <div className="flex items-center justify-between gap-3">
                  <SkeletonBlock className="h-4 w-36" />
                  <SkeletonBlock className="h-7 w-16 rounded-md" />
                </div>
                {Array.from({ length: 5 }).map((__, row) => (
                  <SkeletonBlock key={row} className="h-8 w-full rounded-md" />
                ))}
              </div>
            </SkeletonCard>
          ))}
        </div>
      </div>
    </main>
  );
}
