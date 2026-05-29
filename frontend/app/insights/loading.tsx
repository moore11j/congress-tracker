import { LoadingPageShell, SkeletonBlock, SkeletonCard } from "@/components/ui/LoadingSkeleton";

export default function InsightsLoading() {
  return (
    <LoadingPageShell eyebrow="Insights" titleWidth="w-36" descriptionWidth="w-full max-w-3xl">
      <SkeletonCard>
        <div className="mb-5 flex items-center justify-between gap-3">
          <SkeletonBlock className="h-4 w-36" />
          <SkeletonBlock className="h-4 w-24" />
        </div>
        <div className="grid gap-3 md:grid-cols-4">
          {Array.from({ length: 8 }).map((_, idx) => (
            <div key={idx} className="rounded-xl border border-white/10 bg-slate-950/40 p-3">
              <SkeletonBlock className="h-3 w-20" />
              <SkeletonBlock className="mt-3 h-6 w-24" />
              <SkeletonBlock className="mt-2 h-3 w-16" />
            </div>
          ))}
        </div>
      </SkeletonCard>
      <SkeletonCard>
        <div className="mb-5 flex items-center justify-between gap-3">
          <SkeletonBlock className="h-4 w-40" />
          <SkeletonBlock className="h-4 w-14" />
        </div>
        <div className="space-y-4">
          {Array.from({ length: 5 }).map((_, idx) => (
            <div key={idx} className="grid gap-4 border-b border-white/5 pb-4 last:border-0 sm:grid-cols-[120px_1fr]">
              <SkeletonBlock className="h-20 w-full rounded-xl" />
              <div className="space-y-3">
                <SkeletonBlock className="h-4 w-3/4" />
                <SkeletonBlock className="h-3 w-full" />
                <SkeletonBlock className="h-3 w-2/3" />
              </div>
            </div>
          ))}
        </div>
      </SkeletonCard>
    </LoadingPageShell>
  );
}
