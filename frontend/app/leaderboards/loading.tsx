import { LoadingPageShell, SkeletonBlock, SkeletonTable } from "@/components/ui/LoadingSkeleton";

export default function Loading() {
  return <LoadingPageShell eyebrow="Market performance" titleWidth="w-64" descriptionWidth="w-full max-w-3xl"><div className="space-y-5"><SkeletonTable rows={8} columns={5} /><div className="grid gap-5 lg:grid-cols-2"><SkeletonBlock className="h-72" /><SkeletonBlock className="h-72" /></div><SkeletonTable rows={6} columns={4} /></div></LoadingPageShell>;
}
