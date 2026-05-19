import type { ReactNode } from "react";

export const SIGNALS_COLUMN_DEFINITIONS = {
  baseline: "The symbol's historical median disclosed amount, used as the comparison point for unusual activity.",
  multiple: "How large the disclosed amount is relative to the symbol's historical baseline.",
  conviction:
    "A normalized score that ranks how strong the signal appears based on activity, confirmation, freshness, and available supporting evidence.",
  confirmation:
    "Shows whether multiple independent signal sources are pointing in the same direction, such as Congress activity, insider activity, price/volume, contracts, options flow, or other overlays.",
  freshness: "How recent the underlying activity is. Fresher signals usually deserve more immediate review.",
  source: "The disclosure or market activity stream that contributed to the signal.",
} as const;

export function SignalColumnHeaderTooltip({
  id,
  label,
  description,
  align = "left",
}: {
  id: string;
  label: ReactNode;
  description: string;
  align?: "left" | "right";
}) {
  return (
    <span className="group/header-tip relative inline-flex max-w-full items-center gap-1.5">
      <span className="min-w-0 truncate underline decoration-slate-600/70 decoration-dotted underline-offset-4">{label}</span>
      <button
        type="button"
        aria-label="Column definition"
        aria-describedby={id}
        className="inline-flex h-4 w-4 shrink-0 items-center justify-center rounded-full border border-slate-700/70 bg-slate-900/70 text-[10px] font-semibold leading-none text-slate-500 transition hover:border-emerald-400/40 hover:text-emerald-200 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-emerald-400/30"
      >
        i
      </button>
      <span
        id={id}
        role="tooltip"
        className={`pointer-events-none invisible absolute top-full z-40 mt-2 w-56 rounded-lg border border-white/10 bg-slate-950/95 p-2.5 text-left text-[11px] font-medium normal-case leading-4 tracking-normal text-slate-200 opacity-0 shadow-2xl shadow-black/40 backdrop-blur transition delay-75 group-hover/header-tip:visible group-hover/header-tip:opacity-100 group-focus-within/header-tip:visible group-focus-within/header-tip:opacity-100 ${align === "right" ? "right-0" : "left-0"}`}
      >
        {description}
      </span>
    </span>
  );
}
