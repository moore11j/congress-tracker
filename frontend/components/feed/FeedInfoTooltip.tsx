"use client";

import { createPortal } from "react-dom";
import { type ReactNode, useCallback, useEffect, useLayoutEffect, useRef, useState } from "react";

export type TooltipDetail = { label: string; value: string };

type TooltipPosition = {
  left: number;
  top: number;
};

function clamp(value: number, min: number, max: number): number {
  return Math.min(Math.max(value, min), max);
}

export function FeedInfoTooltip({
  id,
  title,
  body,
  details = [],
  children,
}: {
  id: string;
  title: string;
  body: string;
  details?: TooltipDetail[];
  children: ReactNode;
}) {
  const triggerRef = useRef<HTMLDivElement | null>(null);
  const tooltipRef = useRef<HTMLSpanElement | null>(null);
  const [mounted, setMounted] = useState(false);
  const [open, setOpen] = useState(false);
  const [position, setPosition] = useState<TooltipPosition | null>(null);

  useEffect(() => {
    setMounted(true);
  }, []);

  useEffect(() => {
    function closeOtherTooltip(event: Event) {
      const openedId = event instanceof CustomEvent ? event.detail : null;
      if (openedId !== id) setOpen(false);
    }

    window.addEventListener("feed-tooltip-open", closeOtherTooltip);
    return () => window.removeEventListener("feed-tooltip-open", closeOtherTooltip);
  }, [id]);

  const showTooltip = useCallback(() => {
    window.dispatchEvent(new CustomEvent("feed-tooltip-open", { detail: id }));
    setOpen(true);
  }, [id]);

  const updatePosition = useCallback(() => {
    const trigger = triggerRef.current;
    const tooltip = tooltipRef.current;
    if (!trigger || !tooltip) return;

    const triggerRect = trigger.getBoundingClientRect();
    const tooltipRect = tooltip.getBoundingClientRect();
    const viewportWidth = window.innerWidth;
    const viewportHeight = window.innerHeight;
    const margin = 16;
    const gap = 8;
    const tooltipWidth = Math.min(tooltipRect.width || 256, viewportWidth - margin * 2);
    const tooltipHeight = tooltipRect.height || 120;

    const left = clamp(triggerRect.right - tooltipWidth, margin, viewportWidth - tooltipWidth - margin);
    let top = triggerRect.bottom + gap;
    if (top + tooltipHeight > viewportHeight - margin && triggerRect.top - tooltipHeight - gap > margin) {
      top = triggerRect.top - tooltipHeight - gap;
    }

    setPosition({ left, top });
  }, []);

  useLayoutEffect(() => {
    if (!open || !mounted) return;
    updatePosition();
  }, [mounted, open, updatePosition, title, body, details.length]);

  useEffect(() => {
    if (!open || !mounted) return;
    window.addEventListener("resize", updatePosition);
    window.addEventListener("scroll", updatePosition, true);
    return () => {
      window.removeEventListener("resize", updatePosition);
      window.removeEventListener("scroll", updatePosition, true);
    };
  }, [mounted, open, updatePosition]);

  return (
    <div className="relative inline-flex max-w-full items-center">
      <div
        ref={triggerRef}
        tabIndex={0}
        aria-describedby={id}
        className="inline-flex max-w-full cursor-help rounded-md focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-emerald-400/30"
        onMouseEnter={showTooltip}
        onMouseLeave={() => setOpen(false)}
        onPointerEnter={showTooltip}
        onPointerLeave={() => setOpen(false)}
        onFocus={showTooltip}
        onBlur={() => setOpen(false)}
        onClick={showTooltip}
        onKeyDown={(event) => {
          if (event.key === "Escape") setOpen(false);
        }}
      >
        {children}
      </div>
      {mounted
        ? createPortal(
            <span
              ref={tooltipRef}
              id={id}
              role="tooltip"
              className={`pointer-events-none fixed z-[80] w-64 max-w-[calc(100vw-2rem)] rounded-lg border border-white/10 bg-slate-950/95 p-2.5 text-left text-[11px] font-medium normal-case leading-4 tracking-normal text-slate-200 shadow-2xl shadow-black/40 backdrop-blur transition delay-75 ${
                open && position ? "visible opacity-100" : "invisible opacity-0"
              }`}
              style={{
                left: position ? `${position.left}px` : 0,
                top: position ? `${position.top}px` : 0,
              }}
            >
              <span className="block text-xs font-semibold text-white">{title}</span>
              <span className="mt-1 block text-slate-300">{body}</span>
              {details.length > 0 ? (
                <span className="mt-2 block space-y-1 border-t border-white/10 pt-2 text-slate-400">
                  {details.map((detail) => (
                    <span key={detail.label} className="flex items-center justify-between gap-4">
                      <span>{detail.label}</span>
                      <span className="text-right font-semibold text-slate-200">{detail.value}</span>
                    </span>
                  ))}
                </span>
              ) : null}
            </span>,
            document.body,
          )
        : null}
    </div>
  );
}
