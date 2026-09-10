"use client";

import { useEffect, useRef, useState } from "react";
import { usePathname, useSearchParams } from "next/navigation";
import { getMe, recordPageView } from "@/lib/api";
import { recordGoogleAnalyticsPageView } from "@/lib/googleAnalytics";
import { privacyConsentChangedEvent } from "@/lib/privacyConsent";
import { acquisitionProperties, analyticsConsent, safeAnalyticsPath, setAnalyticsIdentity } from "@/lib/analyticsContext";
import { createVisitTracker, routeFunnelEvent } from "@/lib/funnelEvents";
import { trackDiscoveryClick, trackEvent } from "@/lib/productAnalytics";
import { identifyHeyCatchUser } from "@/lib/heycatch";

export function PageAnalyticsTracker() {
  const pathname = usePathname();
  const query = useSearchParams().toString();
  const visits = useRef(createVisitTracker());
  const funnelVisits = useRef(createVisitTracker());
  const previousPath = useRef<string | null>(null);
  const enteredFrom = useRef("/");
  const [consentRefresh, setConsentRefresh] = useState(0);

  useEffect(() => {
    const refresh = () => setConsentRefresh((current) => current + 1);
    const identify = () => { void getMe({ source: "analytics" }).then(({ user }) => { setAnalyticsIdentity(user); if (user) identifyHeyCatchUser(user); }).catch(() => undefined); };
    const click = (event: MouseEvent) => {
      if (!(event.target instanceof Element)) return;
      const anchor = event.target.closest("a[href]");
      if (anchor) trackDiscoveryClick(anchor.getAttribute("href") || "");
    };
    acquisitionProperties();
    identify();
    window.addEventListener(privacyConsentChangedEvent, refresh);
    window.addEventListener("ct:auth-updated", identify);
    document.addEventListener("click", click);
    return () => { window.removeEventListener(privacyConsentChangedEvent, refresh); window.removeEventListener("ct:auth-updated", identify); document.removeEventListener("click", click); };
  }, []);

  useEffect(() => {
    const path = safeAnalyticsPath(pathname || "/");
    if (path.startsWith("/api/") || path.startsWith("/_next/")) return;
    let cancelled = false;
    // Reuse the cached auth request; failure preserves the unknown identity.
    void Promise.race([getMe({ source: "analytics" }).catch(() => null), new Promise<null>((resolve) => window.setTimeout(() => resolve(null), 800))]).then((session) => {
      if (cancelled) return;
      if (!analyticsConsent()) {
        if (process.env.NEXT_PUBLIC_ANALYTICS_DEBUG === "1") console.info("[Walnut analytics] skipped: analytics consent disabled");
        return;
      }
      if (session) { setAnalyticsIdentity(session.user); if (session.user) identifyHeyCatchUser(session.user); }
      const source = previousPath.current || safeAnalyticsPath(document.referrer || "/");
      if (visits.current.enter(path)) {
        enteredFrom.current = source;
        recordPageView({ path, referrer_path: source !== path ? source : null, title: document.title || null });
        recordGoogleAnalyticsPageView(path, document.title || null);
        previousPath.current = path;
      }
      const event = routeFunnelEvent(path, query, window.location.hostname === "walnutmarkets.com" || Boolean(document.querySelector("[data-walnut-homepage]")));
      const key = `${path}:${event?.name || "none"}`;
      if (funnelVisits.current.enter(key) && event) trackEvent(event.name, { ...event.properties, source_page: enteredFrom.current });
    });
    return () => { cancelled = true; };
  }, [pathname, query, consentRefresh]);

  return null;
}
