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
  };
}

export function HorizontalScrollIndicators({
  canScrollLeft,
  canScrollRight,
  className = "lg:hidden",
}: ScrollAffordanceState & { className?: string }) {
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
