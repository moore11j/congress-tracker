"use client";

import { useEffect, useRef, useState } from "react";
import { usePathname } from "next/navigation";
import { recordPageView } from "@/lib/api";
import { recordGoogleAnalyticsPageView } from "@/lib/googleAnalytics";
import { hasPrivacyConsent, privacyConsentChangedEvent } from "@/lib/privacyConsent";

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
  const initialGoogleAnalyticsPath = useRef<string | null>(null);
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
      if (initialGoogleAnalyticsPath.current === null) {
        initialGoogleAnalyticsPath.current = path;
        return;
      }
      if (hasPrivacyConsent("analytics") && initialGoogleAnalyticsPath.current !== path && !recordGoogleAnalyticsPageView(path, title)) {
        retryTimer = window.setTimeout(() => {
          if (hasPrivacyConsent("analytics") && initialGoogleAnalyticsPath.current !== path) recordGoogleAnalyticsPageView(path, title);
        }, 750);
      }
    }, 250);
    return () => {
      window.clearTimeout(timer);
      if (retryTimer !== null) window.clearTimeout(retryTimer);
    };
  }, [pathname, consentRefresh]);

  return null;
}
