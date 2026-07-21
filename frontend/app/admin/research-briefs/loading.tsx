import { LoadingPageShell } from "@/components/ui/LoadingSkeleton";

export default function AdminResearchBriefsLoading() {
  return (
    <LoadingPageShell eyebrow="Admin" titleWidth="w-56" descriptionWidth="w-full max-w-3xl">
      <div className="grid gap-4 xl:grid-cols-[minmax(0,0.92fr)_minmax(28rem,1.08fr)]">
        <div className="h-[38rem] rounded-lg border border-white/10 bg-slate-950/55" />
        <div className="h-[38rem] rounded-lg border border-white/10 bg-slate-950/55" />
      </div>
    </LoadingPageShell>
  );
}
