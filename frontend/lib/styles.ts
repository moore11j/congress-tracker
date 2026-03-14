export const cardClassName =
  "rounded-3xl border border-white/10 bg-slate-900/70 p-6 shadow-card backdrop-blur";

export const inputClassName =
  "w-full rounded-2xl border border-white/10 bg-slate-900/70 px-3 py-2 text-sm text-slate-100 placeholder:text-slate-400 transition-colors duration-150 hover:border-white/80 focus:border-emerald-300/70 focus:outline-none focus:ring-2 focus:ring-emerald-300/25";

export const selectClassName =
  "w-full rounded-2xl border border-white/10 bg-slate-900/70 px-3 py-2 text-sm text-slate-100 transition-colors duration-150 hover:border-white/80 focus:border-emerald-300/70 focus:outline-none focus:ring-2 focus:ring-emerald-300/25";

export const primaryButtonClassName =
  "inline-flex items-center justify-center rounded-2xl bg-emerald-400/90 px-4 py-2 text-sm font-semibold text-slate-950 shadow-lg shadow-emerald-400/20 transition hover:bg-emerald-300";

export const ghostButtonClassName =
  "inline-flex items-center justify-center rounded-2xl border border-white/10 bg-transparent px-4 py-2 text-sm font-semibold text-slate-200 transition hover:border-white/20 hover:text-white";

export const pillClassName =
  "inline-flex items-center gap-2 rounded-full border border-white/10 bg-slate-900/60 px-3 py-1 text-xs font-semibold uppercase tracking-wide text-slate-200";

export const compactInteractiveSurfaceClassName =
  "group rounded-xl border border-white/10 bg-white/[0.04] text-slate-200 transition-all duration-150 hover:-translate-y-px hover:border-emerald-400/40 hover:bg-emerald-300/[0.08] hover:shadow-[0_10px_26px_-18px_rgba(52,211,153,0.75)]";

export const compactInteractiveTitleClassName =
  "transition-colors text-slate-100 group-hover:text-white";

export function filterControlClassName(active: boolean, className = "") {
  return [
    "inline-flex items-center justify-center rounded-full border px-3 py-1.5 text-xs font-semibold uppercase tracking-wide transition-colors duration-150 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-emerald-300/50 focus-visible:ring-offset-2 focus-visible:ring-offset-slate-950",
    active
      ? "border-emerald-300 bg-emerald-500 text-emerald-50 hover:border-emerald-200 hover:bg-emerald-500/85"
      : "border-white/10 bg-slate-900/60 text-slate-300 hover:border-white hover:text-white",
    className,
  ].join(" ");
}

export function segmentedFilterControlClassName(active: boolean, className = "") {
  return [
    "inline-flex items-center justify-center rounded-full border px-3 py-1.5 text-xs font-semibold uppercase tracking-wide transition-colors duration-150 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-emerald-300/50 focus-visible:ring-offset-2 focus-visible:ring-offset-slate-950",
    active
      ? "border-emerald-300/90 bg-emerald-400/35 text-emerald-50 hover:border-emerald-200/80 hover:bg-emerald-400/28"
      : "border-white/10 bg-slate-900/60 text-slate-300 hover:border-white hover:text-white",
    className,
  ].join(" ");
}

export function filterFieldClassName(baseClassName: string, active: boolean) {
  return active ? `${baseClassName} border-emerald-300/60 bg-slate-950/55` : baseClassName;
}

export const tickerLinkClassName =
  "text-sm font-semibold text-emerald-200 underline-offset-4 transition-colors hover:text-emerald-100 hover:underline focus-visible:outline-none focus-visible:underline";

export const tickerMonoLinkClassName =
  "font-mono text-emerald-200 underline-offset-4 transition-colors hover:text-emerald-100 hover:underline focus-visible:outline-none focus-visible:underline";
