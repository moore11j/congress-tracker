import { LoadingPageShell, SkeletonBlock, SkeletonCard } from "@/components/ui/LoadingSkeleton";

export default function WatchlistDetailLoading() {
  return (
    <LoadingPageShell eyebrow="Watchlist" titleWidth="w-64" descriptionWidth="w-full max-w-2xl" controlsClassName="h-10 w-32 rounded-lg border border-white/10 bg-slate-900/70">
      <SkeletonCard>
        <div className="flex flex-wrap items-start justify-between gap-4">
          <div className="space-y-3">
            <SkeletonBlock className="h-5 w-56" />
            <SkeletonBlock className="h-3 w-72" />
          </div>
          <div className="flex gap-2">
            <SkeletonBlock className="h-9 w-24 rounded-lg" />
            <SkeletonBlock className="h-9 w-24 rounded-lg" />
          </div>
        </div>
        <div className="mt-5 flex flex-wrap gap-2">
          {Array.from({ length: 8 }).map((_, idx) => (
            <SkeletonBlock key={idx} className="h-8 w-16 rounded-full" />
          ))}
        </div>
      </SkeletonCard>
      <div className="grid gap-4 lg:grid-cols-[minmax(0,1fr)_320px]">
        <SkeletonCard>
          <div className="mb-4 flex items-center justify-between gap-3">
            <SkeletonBlock className="h-4 w-36" />
            <SkeletonBlock className="h-8 w-28 rounded-lg" />
          </div>
          <div className="space-y-3">
            {Array.from({ length: 6 }).map((_, idx) => (
              <SkeletonBlock key={idx} className="h-16 w-full rounded-xl" />
            ))}
          </div>
        </SkeletonCard>
        <SkeletonCard>
          <SkeletonBlock className="h-4 w-44" />
          <div className="mt-4 space-y-3">
            {Array.from({ length: 5 }).map((_, idx) => (
              <SkeletonBlock key={idx} className="h-10 w-full rounded-lg" />
            ))}
          </div>
        </SkeletonCard>
      </div>
    </LoadingPageShell>
  );
}
