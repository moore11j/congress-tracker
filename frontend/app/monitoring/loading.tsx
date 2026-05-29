import { LoadingPageShell, SkeletonBlock, SkeletonCard } from "@/components/ui/LoadingSkeleton";

export default function MonitoringLoading() {
  return (
    <LoadingPageShell eyebrow="Monitoring" titleWidth="w-28" descriptionWidth="w-full max-w-2xl" controlsClassName="h-10 w-32 rounded-lg border border-white/10 bg-slate-900/70">
      <div className="grid gap-3 md:grid-cols-3">
        {Array.from({ length: 3 }).map((_, idx) => (
          <SkeletonCard key={idx}>
            <SkeletonBlock className="h-3 w-24" />
            <SkeletonBlock className="mt-3 h-8 w-16" />
          </SkeletonCard>
        ))}
      </div>
      <SkeletonCard>
        <div className="mb-4 flex flex-wrap items-center justify-between gap-3">
          <SkeletonBlock className="h-4 w-32" />
          <div className="flex gap-2">
            {Array.from({ length: 3 }).map((_, idx) => (
              <SkeletonBlock key={idx} className="h-8 w-20 rounded-lg" />
            ))}
          </div>
        </div>
        <div className="space-y-3">
          {Array.from({ length: 6 }).map((_, idx) => (
            <div key={idx} className="rounded-xl border border-white/10 bg-slate-950/40 p-4">
              <div className="flex items-start justify-between gap-4">
                <div className="w-full space-y-3">
                  <SkeletonBlock className="h-3 w-24" />
                  <SkeletonBlock className="h-4 w-3/4" />
                  <SkeletonBlock className="h-3 w-1/2" />
                </div>
                <SkeletonBlock className="h-7 w-20 rounded-lg" />
              </div>
            </div>
          ))}
        </div>
      </SkeletonCard>
    </LoadingPageShell>
  );
}
