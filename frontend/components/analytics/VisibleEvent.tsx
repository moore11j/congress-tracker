"use client";

import { usePathname } from "next/navigation";
import { useEffect, useRef, type ReactNode } from "react";
import { trackEvent } from "@/lib/productAnalytics";
import type { FunnelEvent } from "@/lib/funnelEvents";
import type { AnalyticsProperties } from "@/lib/analyticsContext";
import { privacyConsentChangedEvent } from "@/lib/privacyConsent";

export function VisibleEvent({ name, properties = {}, children, className, enabled = true }: { name: FunnelEvent; properties?: AnalyticsProperties; children: ReactNode; className?: string; enabled?: boolean }) {
  const element = useRef<HTMLDivElement>(null);
  const sent = useRef(false);
  const pathname = usePathname();
  const previousKey = useRef("");
  const serialized = JSON.stringify(properties);
  useEffect(() => {
    const visitKey = `${pathname}:${name}:${serialized}`;
    if (visitKey !== previousKey.current) { sent.current = false; previousKey.current = visitKey; }
    const target = element.current;
    if (!enabled || !target || !window.IntersectionObserver) return;
    let visible = false;
    const emit = () => { if (visible && !sent.current && document.visibilityState !== "hidden") sent.current = trackEvent(name, JSON.parse(serialized)); };
    const observer = new IntersectionObserver(([entry]) => { visible = entry.isIntersecting; emit(); }, { threshold: 0.1 });
    observer.observe(target);
    window.addEventListener(privacyConsentChangedEvent, emit);
    document.addEventListener("visibilitychange", emit);
    return () => { observer.disconnect(); window.removeEventListener(privacyConsentChangedEvent, emit); document.removeEventListener("visibilitychange", emit); };
  }, [pathname, name, serialized, enabled]);
  return <div ref={element} className={className}>{children}</div>;
}
