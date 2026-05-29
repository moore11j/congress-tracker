import { LoadingPageShell, SkeletonBlock, SkeletonCard } from "@/components/ui/LoadingSkeleton";

export default function PricingLoading() {
  return (
    <div className="mx-auto max-w-6xl">
      <LoadingPageShell eyebrow="Pricing" titleWidth="w-48" descriptionWidth="w-full max-w-2xl">
        <div className="grid gap-4 lg:grid-cols-3">
          {Array.from({ length: 3 }).map((_, idx) => (
            <SkeletonCard key={idx}>
              <div className="space-y-4">
                <SkeletonBlock className="h-4 w-24" />
                <SkeletonBlock className="h-10 w-32" />
                <SkeletonBlock className="h-4 w-full" />
                <SkeletonBlock className="h-10 w-full rounded-xl" />
                <div className="space-y-3 pt-2">
                  {Array.from({ length: 5 }).map((__, feature) => (
                    <SkeletonBlock key={feature} className="h-3 w-full" />
                  ))}
                </div>
              </div>
            </SkeletonCard>
          ))}
        </div>
      </LoadingPageShell>
    </div>
  );
}
