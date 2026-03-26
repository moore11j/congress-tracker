import { LoadingPageShell, SkeletonBlock, SkeletonCard, SkeletonTable } from "@/components/ui/LoadingSkeleton";

export default function SignalsLoading() {
  return (
    <LoadingPageShell eyebrow="Unusual signals">
      <SkeletonCard>
        <div className="space-y-4">
          <SkeletonBlock className="h-4 w-40" />
          <SkeletonTable columns={9} rows={6} />
        </div>
      </SkeletonCard>
    </LoadingPageShell>
  );
}
