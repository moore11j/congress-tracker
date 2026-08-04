"use client";

import { useEffect, useRef, useState } from "react";
import { usePathname } from "next/navigation";
import { recordPageView } from "@/lib/api";
import { hasPrivacyConsent, privacyConsentChangedEvent } from "@/lib/privacyConsent";

type WindowWithGoogleAnalytics = Window & {
  gtag?: (...args: unknown[]) => void;
};

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

function recordGoogleAnalyticsPageView(path: string, title: string | null): boolean {
  if (!hasPrivacyConsent("analytics")) return true;
  const gtag = (window as WindowWithGoogleAnalytics).gtag;
  if (!gtag) return false;
  const location = new URL(path, window.location.origin).toString();
  gtag("event", "page_view", {
    page_location: location,
    page_path: path,
    page_title: title || undefined,
  });
  return true;
}

export function PageAnalyticsTracker() {
  const pathname = usePathname();
  const previousPath = useRef<string | null>(null);
  const [consentRefresh, setConsentRefresh] = useState(0);

  useEffect(() => {
    const refresh = () => setConsentRefresh((current) => current + 1);
    window.addEventListener(privacyConsentChangedEvent, refresh);
    return () => window.removeEventListener(privacyConsentChangedEvent, refresh);
  }, []);

  useEffect(() => {
    const path = safePath(pathname);
    if (!shouldTrack(path)) return;
    const referrer = previousPath.current || safePath(document.referrer);
    previousPath.current = path;
    let retryTimer: number | null = null;
    const timer = window.setTimeout(() => {
      const title = document.title || null;
      recordPageView({
        path,
        referrer_path: referrer && referrer !== path ? referrer : null,
        title,
      });
      if (!recordGoogleAnalyticsPageView(path, title)) {
        retryTimer = window.setTimeout(() => recordGoogleAnalyticsPageView(path, title), 750);
      }
    }, 250);
    return () => {
      window.clearTimeout(timer);
      if (retryTimer !== null) window.clearTimeout(retryTimer);
    };
  }, [pathname, consentRefresh]);

  return null;
}
