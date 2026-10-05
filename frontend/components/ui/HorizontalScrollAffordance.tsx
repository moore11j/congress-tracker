"use client";

import { useCallback, useEffect, useRef, useState } from "react";

type ScrollAffordanceState = {
  canScrollLeft: boolean;
  canScrollRight: boolean;
};

export function useHorizontalScrollAffordance<T extends HTMLElement>() {
  const scrollRef = useRef<T | null>(null);
  const [state, setState] = useState<ScrollAffordanceState>({
    canScrollLeft: false,
    canScrollRight: false,
  });

  const updateScrollState = useCallback(() => {
    const node = scrollRef.current;
    if (!node) return;

    const maxScrollLeft = node.scrollWidth - node.clientWidth;
    const canScroll = maxScrollLeft > 2;
    setState({
      canScrollLeft: canScroll && node.scrollLeft > 2,
      canScrollRight: canScroll && maxScrollLeft - node.scrollLeft > 2,
    });
  }, []);

  const scrollByPage = useCallback((direction: -1 | 1) => {
    const node = scrollRef.current;
    if (!node) return;
    node.scrollBy({
      left: direction * Math.max(120, node.clientWidth * 0.75),
      behavior: window.matchMedia("(prefers-reduced-motion: reduce)").matches ? "auto" : "smooth",
    });
  }, []);

  useEffect(() => {
    const node = scrollRef.current;
    if (!node) return;

    updateScrollState();
    const animationFrame = window.requestAnimationFrame(updateScrollState);

    const handleResize = () => updateScrollState();
    window.addEventListener("resize", handleResize);
    window.addEventListener("orientationchange", handleResize);

    const resizeObserver =
      typeof ResizeObserver === "undefined" ? null : new ResizeObserver(updateScrollState);
    resizeObserver?.observe(node);

    return () => {
      window.cancelAnimationFrame(animationFrame);
      window.removeEventListener("resize", handleResize);
      window.removeEventListener("orientationchange", handleResize);
      resizeObserver?.disconnect();
    };
  }, [updateScrollState]);

  return {
    scrollRef,
    canScrollLeft: state.canScrollLeft,
    canScrollRight: state.canScrollRight,
    updateScrollState,
    scrollByPage,
  };
}

export function HorizontalScrollIndicators({
  canScrollLeft,
  canScrollRight,
  className = "lg:hidden",
  onScrollLeft,
  onScrollRight,
  ariaControls,
}: ScrollAffordanceState & {
  className?: string;
  onScrollLeft?: () => void;
  onScrollRight?: () => void;
  ariaControls?: string;
}) {
  if (onScrollLeft && onScrollRight) {
    return (
      <>
        {(["left", "right"] as const).map((direction) => (
          <button
            key={direction}
            type="button"
            aria-label={`Scroll tabs ${direction}`}
            aria-controls={ariaControls}
            disabled={direction === "left" ? !canScrollLeft : !canScrollRight}
            onClick={direction === "left" ? onScrollLeft : onScrollRight}
            className={`absolute inset-y-0 z-10 flex w-9 items-center justify-center bg-slate-950 text-emerald-300 transition-colors hover:bg-slate-800 focus-visible:outline focus-visible:outline-2 focus-visible:outline-inset focus-visible:outline-amber-300 disabled:cursor-default disabled:text-slate-600 disabled:hover:bg-slate-950 ${direction === "left" ? "left-0" : "right-0"} ${className}`}
          >
            <svg aria-hidden="true" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
              <path d={direction === "left" ? "M15 18l-6-6 6-6" : "M9 6l6 6-6 6"} />
            </svg>
          </button>
        ))}
      </>
    );
  }
  return (
    <>
      {canScrollLeft ? (
        <span
          aria-hidden="true"
          className={`pointer-events-none absolute inset-y-0 left-0 z-10 flex w-9 items-center justify-start bg-gradient-to-r from-slate-950/95 via-slate-950/70 to-transparent pl-1 ${className}`}
        >
          <span className="h-0 w-0 border-y-[5px] border-r-[8px] border-y-transparent border-r-emerald-300 drop-shadow-[0_0_8px_rgba(16,185,129,0.75)]" />
        </span>
      ) : null}
      {canScrollRight ? (
        <span
          aria-hidden="true"
          className={`pointer-events-none absolute inset-y-0 right-0 z-10 flex w-9 items-center justify-end bg-gradient-to-l from-slate-950/95 via-slate-950/70 to-transparent pr-1 ${className}`}
        >
          <span className="h-0 w-0 border-y-[5px] border-l-[8px] border-y-transparent border-l-emerald-300 drop-shadow-[0_0_8px_rgba(16,185,129,0.75)]" />
        </span>
      ) : null}
    </>
  );
}
