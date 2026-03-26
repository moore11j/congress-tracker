import { LoadingPageShell, SkeletonBlock, SkeletonCard, SkeletonTable } from "@/components/ui/LoadingSkeleton";

export default function MemberLoading() {
  return (
    <LoadingPageShell eyebrow="Member profile" titleWidth="w-64" descriptionWidth="w-80" controlsClassName="h-12 w-32 rounded-2xl border border-white/10 bg-slate-900/70">
      <SkeletonCard>
        <div className="space-y-4">
          <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-5">
            {Array.from({ length: 5 }).map((_, idx) => (
              <div key={idx} className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
                <SkeletonBlock className="h-3 w-24" />
                <SkeletonBlock className="mt-3 h-7 w-20" />
              </div>
            ))}
          </div>
          <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
            <SkeletonBlock className="h-4 w-44" />
            <SkeletonBlock className="mt-2 h-3 w-56" />
            <SkeletonBlock className="mt-4 h-56 w-full" />
          </div>
        </div>
      </SkeletonCard>

      <SkeletonCard>
        <div className="space-y-4">
          <SkeletonBlock className="h-4 w-40" />
          <SkeletonTable columns={7} rows={6} />
        </div>
      </SkeletonCard>
    </LoadingPageShell>
  );
}
