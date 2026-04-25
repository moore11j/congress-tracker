import { LoadingPageShell, SkeletonBlock, SkeletonCard, SkeletonTable } from "@/components/ui/LoadingSkeleton";

export default function BacktestingLoading() {
  return (
    <LoadingPageShell eyebrow="Premium Research" controlsClassName="h-20 rounded-3xl border border-white/10 bg-slate-900/70">
      <section className="grid gap-6 xl:grid-cols-[0.92fr_1.08fr]">
        <SkeletonCard>
          <div className="space-y-4">
            <SkeletonBlock className="h-4 w-40" />
            <SkeletonBlock className="h-10 w-72" />
            <SkeletonBlock className="h-4 w-full max-w-xl" />
            <div className="grid gap-3 md:grid-cols-2">
              {Array.from({ length: 6 }).map((_, index) => (
                <SkeletonBlock key={index} className="h-11 w-full" />
              ))}
            </div>
          </div>
        </SkeletonCard>
        <SkeletonCard>
          <div className="space-y-4">
            <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-3">
              {Array.from({ length: 6 }).map((_, index) => (
                <div key={index} className="rounded-2xl border border-white/10 bg-white/[0.03] px-4 py-3">
                  <SkeletonBlock className="h-3 w-24" />
                  <SkeletonBlock className="mt-3 h-7 w-20" />
                </div>
              ))}
            </div>
            <SkeletonBlock className="h-[280px] w-full" />
            <SkeletonTable columns={6} rows={5} />
          </div>
        </SkeletonCard>
      </section>
    </LoadingPageShell>
  );
}
