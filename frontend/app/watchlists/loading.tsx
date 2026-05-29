import { LoadingPageShell, SkeletonBlock, SkeletonCard } from "@/components/ui/LoadingSkeleton";

export default function WatchlistsLoading() {
  return (
    <LoadingPageShell eyebrow="Watchlists" titleWidth="w-80" descriptionWidth="w-full max-w-2xl" controlsClassName="h-10 w-32 rounded-lg border border-white/10 bg-slate-900/70">
      <SkeletonCard>
        <div className="flex flex-wrap items-center justify-between gap-4">
          <div className="space-y-2">
            <SkeletonBlock className="h-4 w-36" />
            <SkeletonBlock className="h-3 w-64" />
          </div>
          <SkeletonBlock className="h-10 w-36 rounded-lg" />
        </div>
      </SkeletonCard>
      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
        {Array.from({ length: 6 }).map((_, idx) => (
          <SkeletonCard key={idx}>
            <div className="space-y-4">
              <div className="flex items-start justify-between gap-3">
                <div className="space-y-2">
                  <SkeletonBlock className="h-5 w-36" />
                  <SkeletonBlock className="h-3 w-24" />
                </div>
                <SkeletonBlock className="h-7 w-16 rounded-lg" />
              </div>
              <div className="flex flex-wrap gap-2">
                {Array.from({ length: 5 }).map((__, symbol) => (
                  <SkeletonBlock key={symbol} className="h-7 w-14 rounded-full" />
                ))}
              </div>
            </div>
          </SkeletonCard>
        ))}
      </div>
    </LoadingPageShell>
  );
}
