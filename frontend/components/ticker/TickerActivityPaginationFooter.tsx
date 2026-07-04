"use client";

import { type MouseEvent, useEffect } from "react";
import { usePathname, useRouter, useSearchParams } from "next/navigation";

const pendingScrollKey = "ct:ticker-activity-scroll";

type Props = {
  sectionId: string;
  pageParam: string;
  page: number;
  limit: number;
  total: number | null;
  itemCount: number;
  hasNext: boolean;
};

function scrollToActivitySection(sectionId: string) {
  const section = document.getElementById(sectionId);
  if (!section) return;
  const scrollRegion = section.querySelector<HTMLElement>("[data-activity-scroll-region]");
  if (scrollRegion) scrollRegion.scrollTop = 0;
  section.scrollIntoView({ behavior: "smooth", block: "start" });
}

export function TickerActivityPaginationFooter({
  sectionId,
  pageParam,
  page,
  limit,
  total,
  itemCount,
  hasNext,
}: Props) {
  const pathname = usePathname();
  const router = useRouter();
  const searchParams = useSearchParams();
  const safeLimit = Math.max(limit, 1);
  const hasExactTotal = typeof total === "number";
  const showingStart = itemCount > 0 ? page * safeLimit + 1 : 0;
  const showingEnd = itemCount > 0 ? showingStart + Math.max(itemCount - 1, 0) : 0;
  const exactShowingStart = hasExactTotal && total > 0 ? Math.min(showingStart, total) : showingStart;
  const exactShowingEnd = hasExactTotal && total > 0 ? Math.min(showingEnd, total) : showingEnd;
  const hasPrevious = page > 0;
  const rangeLabel = hasExactTotal
    ? `Showing ${exactShowingStart}-${exactShowingEnd} of ${total}`
    : hasNext
      ? `Showing ${showingStart}-${showingEnd} · More available`
      : `Showing ${showingStart}-${showingEnd}`;

  useEffect(() => {
    const pendingTarget = window.sessionStorage.getItem(pendingScrollKey);
    if (pendingTarget !== `${sectionId}:${page}`) return;
    window.sessionStorage.removeItem(pendingScrollKey);

    const frame = window.requestAnimationFrame(() => scrollToActivitySection(sectionId));
    return () => window.cancelAnimationFrame(frame);
  }, [page, sectionId, itemCount, total]);

  function buildHref(nextPage: number, includeHash = false) {
    const params = new URLSearchParams(searchParams.toString());
    if (nextPage <= 0) {
      params.delete(pageParam);
    } else {
      params.set(pageParam, String(nextPage));
    }
    const query = params.toString();
    return `${pathname}${query ? `?${query}` : ""}${includeHash ? `#${sectionId}` : ""}`;
  }

  function goToPage(event: MouseEvent<HTMLAnchorElement>, nextPage: number) {
    event.preventDefault();
    const nextUrl = buildHref(nextPage);
    window.sessionStorage.setItem(pendingScrollKey, `${sectionId}:${nextPage}`);
    router.push(nextUrl, { scroll: false });
  }

  return (
    <div className="flex flex-wrap items-center justify-between gap-3 border-t border-white/10 pt-3">
      <span className="text-xs text-slate-500">
        {rangeLabel}
      </span>
      <div className="flex items-center gap-2">
        {hasPrevious ? (
          <a
            href={buildHref(Math.max(page - 1, 0), true)}
            role="button"
            onClick={(event) => goToPage(event, Math.max(page - 1, 0))}
            className="rounded-lg border border-white/10 bg-slate-900/70 px-3 py-1.5 text-xs font-semibold text-slate-200 transition hover:bg-white/10"
          >
            Previous
          </a>
        ) : null}
        {hasNext ? (
          <a
            href={buildHref(page + 1, true)}
            role="button"
            onClick={(event) => goToPage(event, page + 1)}
            className="rounded-lg border border-emerald-300/40 bg-emerald-300/10 px-3 py-1.5 text-xs font-semibold text-emerald-100 transition hover:bg-emerald-300/15"
          >
            Show more
          </a>
        ) : null}
      </div>
    </div>
  );
}
