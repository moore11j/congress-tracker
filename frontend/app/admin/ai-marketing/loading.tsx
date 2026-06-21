import { LoadingPageShell, SkeletonBlock, SkeletonCard } from "@/components/ui/LoadingSkeleton";

export default function AdminAiMarketingLoading() {
  return (
    <LoadingPageShell eyebrow="Operations" titleWidth="w-40" descriptionWidth="w-full max-w-2xl" controlsClassName="h-10 w-28 rounded-lg border border-white/10 bg-slate-900/70">
      <SkeletonCard>
        <SkeletonBlock className="h-4 w-44" />
        <div className="mt-5 grid gap-4 lg:grid-cols-2">
          <SkeletonBlock className="h-52 w-full rounded-lg" />
          <SkeletonBlock className="h-52 w-full rounded-lg" />
        </div>
      </SkeletonCard>
      <SkeletonCard>
        <SkeletonBlock className="h-4 w-40" />
        <SkeletonBlock className="mt-4 h-32 w-full rounded-lg" />
      </SkeletonCard>
    </LoadingPageShell>
  );
}
