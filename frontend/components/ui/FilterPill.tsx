import type { ButtonHTMLAttributes, ReactNode } from "react";

type FilterPillProps = {
  active?: boolean;
  children: ReactNode;
} & ButtonHTMLAttributes<HTMLButtonElement>;

export function FilterPill({ active = false, children, className = "", ...props }: FilterPillProps) {
  return (
    <button
      type="button"
      className={[
        "relative inline-flex items-center justify-center rounded-full border px-3 py-1.5 text-xs uppercase tracking-wide transition-colors duration-150",
        active
          ? "border-white/30 bg-white/[0.06] text-white font-medium"
          : "border-white/10 bg-transparent text-white/60 font-semibold",
        className,
      ].join(" ")}
      {...props}
    >
      <span
        aria-hidden="true"
        className={[
          "pointer-events-none absolute left-2 right-2 top-0 h-0.5 rounded-full transition-colors duration-150",
          active ? "bg-white/80" : "bg-transparent",
        ].join(" ")}
      />
      {children}
    </button>
  );
}
