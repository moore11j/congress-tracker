"use client";

import { useEffect, useRef, useState, type ReactNode } from "react";

type Props = {
  children: ReactNode;
  className?: string;
  heightClassName?: string;
  loading?: boolean;
  error?: string | null;
  onRetry?: () => void;
  lazy?: boolean;
  label: string;
};

export function WalnutChartContainer({
  children,
  className = "",
  heightClassName = "h-72",
  loading = false,
  error = null,
  onRetry,
  lazy = false,
  label,
}: Props) {
  const ref = useRef<HTMLDivElement>(null);
  const [nearViewport, setNearViewport] = useState(!lazy);

  useEffect(() => {
    if (!lazy || nearViewport || !ref.current) return;
    const observer = new IntersectionObserver(
      ([entry]) => {
        if (!entry.isIntersecting) return;
        setNearViewport(true);
        observer.disconnect();
      },
      { rootMargin: "320px 0px" },
    );
    observer.observe(ref.current);
    return () => observer.disconnect();
  }, [lazy, nearViewport]);

  const showSkeleton = loading || !nearViewport;
  return (
    <div ref={ref} className={`relative min-w-0 ${className}`} aria-busy={showSkeleton || undefined} aria-label={label}>
      {showSkeleton ? <WalnutChartSkeleton heightClassName={heightClassName} label={label} /> : null}
      {error && !showSkeleton ? <WalnutChartErrorState message={error} onRetry={onRetry} heightClassName={heightClassName} /> : null}
      {!showSkeleton && !error ? <div className="animate-[walnut-chart-fade_240ms_ease-out]">{children}</div> : null}
    </div>
  );
}

export function WalnutChartSkeleton({ heightClassName = "h-72", label = "Loading chart" }: { heightClassName?: string; label?: string }) {
  return (
    <div aria-label={label} className={`overflow-hidden rounded-xl border border-white/10 bg-white/[0.025] p-4 ${heightClassName}`}>
      <div className="h-3 w-28 animate-pulse rounded bg-slate-700/70" />
      <div className="mt-5 grid h-[calc(100%-2rem)] grid-rows-5 gap-5">
        {[0, 1, 2, 3, 4].map((row) => <div key={row} className="border-t border-slate-700/40" />)}
      </div>
      <div aria-hidden className="absolute inset-x-8 bottom-8 h-16 rounded-t-[55%] bg-gradient-to-t from-emerald-300/10 to-transparent" />
    </div>
  );
}

export function WalnutChartErrorState({ message, onRetry, heightClassName = "h-72" }: { message: string; onRetry?: () => void; heightClassName?: string }) {
  return (
    <div className={`flex flex-col items-center justify-center rounded-xl border border-dashed border-white/10 bg-white/[0.02] px-5 text-center ${heightClassName}`} role="status">
      <p className="text-sm font-semibold text-slate-200">Chart unavailable</p>
      <p className="mt-1 text-sm text-slate-400">{message}</p>
      {onRetry ? <button type="button" onClick={onRetry} className="mt-4 rounded-lg border border-white/15 px-3 py-1.5 text-xs font-semibold text-slate-100 hover:bg-white/5">Retry</button> : null}
    </div>
  );
}
