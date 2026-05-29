import { LoadingPageShell, SkeletonBlock, SkeletonCard, SkeletonTable } from "@/components/ui/LoadingSkeleton";

export default function AdminSettingsLoading() {
  return (
    <LoadingPageShell eyebrow="Operations" titleWidth="w-44" descriptionWidth="w-full max-w-2xl" controlsClassName="h-10 w-28 rounded-lg border border-white/10 bg-slate-900/70">
      <SkeletonCard>
        <SkeletonBlock className="h-4 w-44" />
        <div className="mt-5 grid gap-4 md:grid-cols-2">
          {Array.from({ length: 4 }).map((_, idx) => (
            <div key={idx} className="space-y-2">
              <SkeletonBlock className="h-3 w-28" />
              <SkeletonBlock className="h-10 w-full rounded-lg" />
            </div>
          ))}
        </div>
      </SkeletonCard>
      <SkeletonCard>
        <SkeletonBlock className="mb-4 h-4 w-40" />
        <SkeletonTable columns={4} rows={6} />
      </SkeletonCard>
    </LoadingPageShell>
  );
}
