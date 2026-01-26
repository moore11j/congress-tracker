import type { ReactNode } from "react";

export type BadgeTone =
  | "neutral"
  | "dem"
  | "rep"
  | "ind"
  | "house"
  | "senate"
  | "pos"
  | "neg";

const toneStyles: Record<BadgeTone, string> = {
  neutral: "border-white/10 bg-white/5 text-slate-200",
  dem: "border-sky-400/30 bg-sky-400/15 text-sky-100",
  rep: "border-rose-400/30 bg-rose-400/15 text-rose-100",
  ind: "border-emerald-400/30 bg-emerald-400/15 text-emerald-100",
  house: "border-indigo-400/30 bg-indigo-400/15 text-indigo-100",
  senate: "border-violet-400/30 bg-violet-400/15 text-violet-100",
  pos: "border-emerald-400/30 bg-emerald-400/15 text-emerald-100",
  neg: "border-rose-400/30 bg-rose-400/15 text-rose-100",
};

export function Badge({ tone = "neutral", children, className }: { tone?: BadgeTone; children: ReactNode; className?: string }) {
  return (
    <span
      className={`inline-flex items-center rounded-full border px-2.5 py-1 text-xs font-semibold uppercase tracking-wide ${
        toneStyles[tone]
      } ${className ?? ""}`}
    >
      {children}
    </span>
  );
}
