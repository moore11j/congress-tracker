import { LoadingPageShell, SkeletonBlock, SkeletonCard } from "@/components/ui/LoadingSkeleton";

export default function BillingLoading() {
  return (
    <LoadingPageShell eyebrow="Billing" titleWidth="w-40" descriptionWidth="w-full max-w-2xl">
      <SkeletonCard>
        <div className="flex flex-wrap items-center justify-between gap-4">
          <div className="space-y-3">
            <SkeletonBlock className="h-4 w-36" />
            <SkeletonBlock className="h-8 w-44" />
            <SkeletonBlock className="h-3 w-64" />
          </div>
          <SkeletonBlock className="h-10 w-32 rounded-lg" />
        </div>
      </SkeletonCard>
      <div className="grid gap-4 lg:grid-cols-3">
        {Array.from({ length: 3 }).map((_, idx) => (
          <SkeletonCard key={idx}>
            <SkeletonBlock className="h-4 w-24" />
            <SkeletonBlock className="mt-4 h-8 w-28" />
            <div className="mt-5 space-y-3">
              {Array.from({ length: 4 }).map((__, feature) => (
                <SkeletonBlock key={feature} className="h-3 w-full" />
              ))}
            </div>
            <SkeletonBlock className="mt-5 h-10 w-full rounded-lg" />
          </SkeletonCard>
        ))}
      </div>
    </LoadingPageShell>
  );
}
