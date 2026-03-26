import { LoadingPageShell, SkeletonBlock, SkeletonCard, SkeletonTable } from "@/components/ui/LoadingSkeleton";

export default function TickerLoading() {
  return (
    <LoadingPageShell eyebrow="Ticker intelligence" titleWidth="w-48" descriptionWidth="w-72" controlsClassName="h-16 rounded-3xl border border-white/10 bg-slate-900/70">
      <SkeletonCard>
        <div className="space-y-4">
          <SkeletonBlock className="h-4 w-40" />
          <SkeletonBlock className="h-64 w-full" />
        </div>
      </SkeletonCard>

      <div className="grid gap-6 lg:grid-cols-2">
        <SkeletonCard>
          <div className="space-y-4">
            <SkeletonBlock className="h-4 w-44" />
            <SkeletonTable columns={4} rows={5} />
          </div>
        </SkeletonCard>
        <SkeletonCard>
          <div className="space-y-4">
            <SkeletonBlock className="h-4 w-44" />
            <SkeletonTable columns={4} rows={5} />
          </div>
        </SkeletonCard>
      </div>
    </LoadingPageShell>
  );
}
