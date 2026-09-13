import type { ReactNode } from "react";

/** Secondary controls remain mounted so expanding a panel never resets a form. */
export function GrowthDisclosure({ title, children, open = false, className = "" }: {
  title: string; children: ReactNode; open?: boolean; className?: string;
}) {
  return <details open={open || undefined} className={`group rounded-lg border border-white/10 bg-slate-950/30 p-3 ${className}`}>
    <summary className="cursor-pointer text-sm font-medium text-slate-200 focus-visible:outline focus-visible:outline-2 focus-visible:outline-emerald-300">{title}</summary>
    <div className="mt-4 space-y-4">{children}</div>
  </details>;
}
