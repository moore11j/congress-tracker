import { FeedLoadingMountProbe } from "@/components/feed/FeedLoadingMountProbe";
import { LoadingPageShell, SkeletonBlock, SkeletonCard } from "@/components/ui/LoadingSkeleton";

function FeedSkeletonCards() {
  return (
    <div className="space-y-3">
      {Array.from({ length: 6 }).map((_, idx) => (
        <div key={idx} className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
          <div className="flex items-start justify-between gap-3">
            <div className="space-y-2">
              <SkeletonBlock className="h-3 w-24" />
              <SkeletonBlock className="h-5 w-56" />
            </div>
            <SkeletonBlock className="h-6 w-16 rounded-full" />
          </div>
          <div className="mt-4 grid grid-cols-2 gap-2 sm:grid-cols-4">
            {Array.from({ length: 4 }).map((__, stat) => (
              <SkeletonBlock key={stat} className="h-3 w-full" />
            ))}
          </div>
        </div>
      ))}
    </div>
  );
}

export default function MainFeedLoading() {
  return (
    <>
      <FeedLoadingMountProbe />
      <LoadingPageShell eyebrow="Unified tape" titleWidth="w-56" descriptionWidth="w-full max-w-2xl">
      <SkeletonCard>
        <div className="grid grid-cols-2 gap-3 md:grid-cols-5">
          {Array.from({ length: 5 }).map((_, idx) => (
            <div key={idx} className="space-y-2">
              <SkeletonBlock className="h-3 w-16" />
              <SkeletonBlock className="h-10 w-full rounded-2xl" />
            </div>
          ))}
        </div>
      </SkeletonCard>
      <FeedSkeletonCards />
    </LoadingPageShell>
    </>
  );
}
