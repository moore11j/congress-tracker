"use client";

import { Children, useMemo, useState, type ReactNode } from "react";

const DEFAULT_VISIBLE_COUNT = 5;

export function ExpandableTickerSection({
  id,
  title,
  emptyState,
  children,
  className,
}: {
  id: string;
  title: string;
  emptyState: ReactNode;
  children: ReactNode;
  className: string;
}) {
  const items = useMemo(() => Children.toArray(children), [children]);
  const [visibleCount, setVisibleCount] = useState(DEFAULT_VISIBLE_COUNT);
  const visibleItems = items.slice(0, visibleCount);
  const hasMore = visibleCount < items.length;

  return (
    <section id={id} className={`${className} scroll-mt-6`}>
      <h2 className="text-lg font-semibold text-white">{title}</h2>
      <div className="mt-4 space-y-2.5">
        {items.length === 0 ? emptyState : visibleItems}
      </div>
      {hasMore ? (
        <button
          type="button"
          className="mt-3 inline-flex w-full items-center justify-center rounded-lg border border-white/10 bg-white/[0.035] px-3 py-2 text-sm font-semibold text-emerald-100 transition hover:border-emerald-300/35 hover:bg-emerald-300/10 focus:outline-none focus-visible:ring-2 focus-visible:ring-emerald-300/50"
          onClick={() => setVisibleCount((count) => count + DEFAULT_VISIBLE_COUNT)}
        >
          Show more
        </button>
      ) : null}
    </section>
  );
}
