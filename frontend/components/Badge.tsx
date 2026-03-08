import type { ReactNode } from "react";

export type BadgeTone =
  | "neutral"
  | "dem"
  | "rep"
  | "ind"
  | "house"
  | "senate"
  | "pos"
  | "neg"
  | "insider_ceo"
  | "insider_cfo"
  | "insider_coo"
  | "insider_cto"
  | "insider_cco"
  | "insider_clo"
  | "insider_cao"
  | "insider_evp"
  | "insider_svp"
  | "insider_pres"
  | "insider_vp"
  | "insider_dir"
  | "insider_officer"
  | "insider_default";

const toneStyles: Record<BadgeTone, string> = {
  neutral: "border-white/10 bg-white/5 text-slate-200",
  dem: "border-sky-400/30 bg-sky-400/15 text-sky-100",
  rep: "border-rose-400/30 bg-rose-400/15 text-rose-100",
  ind: "border-emerald-400/30 bg-emerald-400/15 text-emerald-100",
  house: "border-indigo-400/30 bg-indigo-400/15 text-indigo-100",
  senate: "border-violet-400/30 bg-violet-400/15 text-violet-100",
  pos: "border-emerald-400/30 bg-emerald-400/15 text-emerald-100",
  neg: "border-rose-400/30 bg-rose-400/15 text-rose-100",
  insider_ceo: "border-emerald-400/40 bg-emerald-400/15 text-emerald-100",
  insider_cfo: "border-cyan-400/40 bg-cyan-400/15 text-cyan-100",
  insider_coo: "border-blue-400/40 bg-blue-400/15 text-blue-100",
  insider_cto: "border-indigo-400/40 bg-indigo-400/15 text-indigo-100",
  insider_cco: "border-fuchsia-400/40 bg-fuchsia-400/15 text-fuchsia-100",
  insider_clo: "border-amber-400/40 bg-amber-400/15 text-amber-100",
  insider_cao: "border-orange-400/40 bg-orange-400/15 text-orange-100",
  insider_evp: "border-pink-400/40 bg-pink-400/15 text-pink-100",
  insider_svp: "border-purple-400/40 bg-purple-400/15 text-purple-100",
  insider_pres: "border-violet-400/40 bg-violet-400/15 text-violet-100",
  insider_vp: "border-sky-400/40 bg-sky-400/15 text-sky-100",
  insider_dir: "border-slate-400/40 bg-slate-400/15 text-slate-100",
  insider_officer: "border-teal-400/40 bg-teal-400/15 text-teal-100",
  insider_default: "border-white/15 bg-white/5 text-slate-200",
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
