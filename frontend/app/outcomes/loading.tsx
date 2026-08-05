import { LoadingPageShell, SkeletonBlock, SkeletonTable } from "@/components/ui/LoadingSkeleton";

export default function OutcomesLoading() {
  return (
    <LoadingPageShell eyebrow="Outcomes" titleWidth="w-60" descriptionWidth="w-full max-w-3xl">
      <section className="rounded-lg border border-white/10 bg-slate-900/60 p-5">
        <SkeletonBlock className="h-4 w-32" />
        <SkeletonBlock className="mt-3 h-7 w-52" />
        <div className="mt-5 grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
          {Array.from({ length: 6 }).map((_, index) => (
            <SkeletonBlock key={index} className="h-24 w-full" />
          ))}
        </div>
      </section>
      <section className="rounded-lg border border-white/10 bg-slate-900/60 p-5">
        <SkeletonTable columns={9} rows={6} />
      </section>
    </LoadingPageShell>
  );
}
