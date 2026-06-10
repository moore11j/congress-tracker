import { LoadingPageShell, SkeletonBlock, SkeletonCard } from "@/components/ui/LoadingSkeleton";

export default function AccountSettingsLoading() {
  return (
    <LoadingPageShell eyebrow="Account Settings" titleWidth="w-64" descriptionWidth="w-full max-w-2xl">
      <SkeletonCard>
        <SkeletonBlock className="h-4 w-48" />
        <div className="mt-5 grid gap-4 md:grid-cols-2">
          {Array.from({ length: 6 }).map((_, idx) => (
            <div key={idx} className="space-y-2">
              <SkeletonBlock className="h-3 w-24" />
              <SkeletonBlock className="h-10 w-full rounded-lg" />
            </div>
          ))}
        </div>
        <SkeletonBlock className="mt-5 h-10 w-32 rounded-lg" />
      </SkeletonCard>
      <SkeletonCard>
        <SkeletonBlock className="h-4 w-40" />
        <div className="mt-4 space-y-3">
          {Array.from({ length: 4 }).map((_, idx) => (
            <div key={idx} className="flex items-center justify-between gap-4">
              <SkeletonBlock className="h-3 w-56" />
              <SkeletonBlock className="h-6 w-12 rounded-full" />
            </div>
          ))}
        </div>
      </SkeletonCard>
    </LoadingPageShell>
  );
}
