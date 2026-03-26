import { cardClassName } from "@/lib/styles";
import type { ReactNode } from "react";

type SkeletonBlockProps = {
  className: string;
};

export function SkeletonBlock({ className }: SkeletonBlockProps) {
  return <div className={`animate-pulse rounded bg-white/10 ${className}`} />;
}

type LoadingPageShellProps = {
  eyebrow: string;
  titleWidth?: string;
  descriptionWidth?: string;
  controlsClassName?: string;
  children: ReactNode;
};

export function LoadingPageShell({
  eyebrow,
  titleWidth = "w-72",
  descriptionWidth = "w-full max-w-2xl",
  controlsClassName = "h-16 rounded-3xl border border-white/10 bg-slate-900/70",
  children,
}: LoadingPageShellProps) {
  return (
    <div className="space-y-8">
      <section className="space-y-4">
        <div>
          <p className="text-xs font-semibold uppercase tracking-[0.3em] text-emerald-300">{eyebrow}</p>
          <SkeletonBlock className={`mt-2 h-8 ${titleWidth}`} />
          <SkeletonBlock className={`mt-3 h-4 ${descriptionWidth}`} />
        </div>
        <div className={controlsClassName} />
      </section>
      {children}
    </div>
  );
}

type SkeletonTableProps = {
  columns: number;
  rows: number;
};

export function SkeletonTable({ columns, rows }: SkeletonTableProps) {
  return (
    <div className="overflow-hidden rounded-2xl border border-white/10">
      <div
        className="grid gap-2 bg-white/5 px-4 py-3"
        style={{ gridTemplateColumns: `repeat(${columns}, minmax(0, 1fr))` }}
      >
        {Array.from({ length: columns }).map((_, index) => (
          <SkeletonBlock key={`head-${index}`} className="h-3" />
        ))}
      </div>
      <div className="divide-y divide-white/5">
        {Array.from({ length: rows }).map((_, row) => (
          <div
            key={`row-${row}`}
            className="grid gap-2 px-4 py-3"
            style={{ gridTemplateColumns: `repeat(${columns}, minmax(0, 1fr))` }}
          >
            {Array.from({ length: columns }).map((_, cell) => (
              <SkeletonBlock key={`cell-${row}-${cell}`} className="h-3" />
            ))}
          </div>
        ))}
      </div>
    </div>
  );
}

type SkeletonCardProps = {
  children: ReactNode;
};

export function SkeletonCard({ children }: SkeletonCardProps) {
  return <section className={cardClassName}>{children}</section>;
}
