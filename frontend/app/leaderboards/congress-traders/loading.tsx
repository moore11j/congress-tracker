import { LoadingPageShell, SkeletonBlock, SkeletonCard, SkeletonTable } from "@/components/ui/LoadingSkeleton";

export default function LeaderboardLoading() {
  return (
    <LoadingPageShell eyebrow="Leaderboards" titleWidth="w-80" descriptionWidth="w-full max-w-3xl">
      <SkeletonCard>
        <div className="grid grid-cols-2 gap-3 md:grid-cols-5">
          {Array.from({ length: 5 }).map((_, idx) => (
            <div key={idx} className="space-y-2">
              <SkeletonBlock className="h-3 w-20" />
              <SkeletonBlock className="h-10 w-full rounded-2xl" />
            </div>
          ))}
          <SkeletonBlock className="col-span-2 h-8 w-40 md:col-span-5" />
        </div>
      </SkeletonCard>

      <SkeletonCard>
        <div className="space-y-4">
          <SkeletonBlock className="h-4 w-52" />
          <SkeletonTable columns={8} rows={8} />
        </div>
      </SkeletonCard>
    </LoadingPageShell>
  );
}
