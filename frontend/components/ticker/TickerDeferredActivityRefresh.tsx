"use client";

import { useEffect, useRef } from "react";
import { useRouter } from "next/navigation";

const refreshedSymbols = new Set<string>();

export function TickerDeferredActivityRefresh({ enabled, symbol }: { enabled: boolean; symbol: string }) {
  const router = useRouter();
  const markerRef = useRef<HTMLDivElement | null>(null);
  const triggeredRef = useRef(false);

  useEffect(() => {
    if (!enabled) return;
    const key = symbol.trim().toUpperCase();
    if (refreshedSymbols.has(key)) return;

    const trigger = () => {
      if (triggeredRef.current || document.hidden) return;
      triggeredRef.current = true;
      refreshedSymbols.add(key);
      const url = new URL(window.location.href);
      if (url.searchParams.get("activity_details") === "1") return;
      url.searchParams.set("activity_details", "1");
      router.replace(`${url.pathname}${url.search}${url.hash}`, { scroll: false });
    };

    const handleVisibility = () => {
      if (!document.hidden) trigger();
    };

    const node = markerRef.current;
    let timer: number | null = null;
    let observer: IntersectionObserver | null = null;

    document.addEventListener("visibilitychange", handleVisibility);
    if (!node || typeof IntersectionObserver === "undefined") {
      timer = window.setTimeout(trigger, 800);
    } else {
      observer = new IntersectionObserver(
        (entries) => {
          if (entries.some((entry) => entry.isIntersecting)) trigger();
        },
        { rootMargin: "700px 0px" },
      );
      observer.observe(node);
    }

    return () => {
      document.removeEventListener("visibilitychange", handleVisibility);
      if (timer !== null) window.clearTimeout(timer);
      observer?.disconnect();
    };
  }, [enabled, router, symbol]);

  if (!enabled) return null;
  return <div ref={markerRef} aria-hidden="true" className="h-px w-full" />;
}
