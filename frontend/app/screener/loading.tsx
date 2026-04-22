import { LoadingPageShell, SkeletonBlock, SkeletonCard, SkeletonTable } from "@/components/ui/LoadingSkeleton";

export default function ScreenerLoading() {
  return (
    <LoadingPageShell eyebrow="Idea screener">
      <SkeletonCard>
        <div className="space-y-4">
          <SkeletonBlock className="h-4 w-48" />
          <SkeletonTable columns={11} rows={8} />
        </div>
      </SkeletonCard>
    </LoadingPageShell>
  );
}
