import { WalnutChartSkeleton } from "@/components/charts/WalnutChartContainer";
export default function Loading() { return <div className="mx-auto max-w-[1600px] space-y-6 py-8" role="status"><p className="text-sm text-slate-400">Loading retirement calculator…</p><WalnutChartSkeleton label="Loading retirement projection" heightClassName="h-96" /></div>; }
