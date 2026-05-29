import { LoadingPageShell, SkeletonBlock, SkeletonCard } from "@/components/ui/LoadingSkeleton";

export default function AppLoading() {
  return (
    <LoadingPageShell eyebrow="Loading" titleWidth="w-48" descriptionWidth="w-full max-w-xl">
      <SkeletonCard>
        <div className="grid gap-3 sm:grid-cols-3">
          {Array.from({ length: 3 }).map((_, idx) => (
            <div key={idx} className="space-y-2">
              <SkeletonBlock className="h-3 w-20" />
              <SkeletonBlock className="h-12 w-full rounded-2xl" />
            </div>
          ))}
        </div>
      </SkeletonCard>
      <SkeletonCard>
        <div className="space-y-3">
          {Array.from({ length: 4 }).map((_, idx) => (
            <SkeletonBlock key={idx} className="h-12 w-full rounded-xl" />
          ))}
        </div>
      </SkeletonCard>
    </LoadingPageShell>
  );
}
