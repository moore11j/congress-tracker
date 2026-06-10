"use client";

import { useEffect, useRef } from "react";
import { usePathname } from "next/navigation";
import { recordPageView } from "@/lib/api";

function safePath(value: string | null | undefined) {
  const raw = (value || "").trim();
  if (!raw) return "/";
  try {
    const parsed = new URL(raw, window.location.origin);
    return parsed.pathname || "/";
  } catch {
    return raw.split("?", 1)[0] || "/";
  }
}

function shouldTrack(path: string) {
  return Boolean(path) && !path.startsWith("/_next/") && !path.startsWith("/api/") && !path.includes(".");
}

export function PageAnalyticsTracker() {
  const pathname = usePathname();
  const previousPath = useRef<string | null>(null);

  useEffect(() => {
    const path = safePath(pathname);
    if (!shouldTrack(path)) return;
    const referrer = previousPath.current || safePath(document.referrer);
    previousPath.current = path;
    const timer = window.setTimeout(() => {
      recordPageView({
        path,
        referrer_path: referrer && referrer !== path ? referrer : null,
        title: document.title || null,
      });
    }, 250);
    return () => window.clearTimeout(timer);
  }, [pathname]);

  return null;
}
