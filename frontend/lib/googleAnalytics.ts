"use client";

import { isProductionAnalyticsHost } from "@/lib/analyticsEnvironment";
import { hasPrivacyConsent } from "@/lib/privacyConsent";

export const GOOGLE_ANALYTICS_ID = "G-QQTFFK7FBH";

export type GoogleAnalyticsContext = { client_id: string; session_id: string };

/** Use gtag's supported getters; never parse versioned GA cookies or invent IDs. */
export async function getGoogleAnalyticsContext(): Promise<GoogleAnalyticsContext | undefined> {
  try {
    if (!isProductionAnalyticsHost() || !hasPrivacyConsent("analytics")) return undefined;
    const gtag = (window as WindowWithGoogleAnalytics).gtag;
    if (!gtag) return undefined;
    return await new Promise((resolve) => {
      const values: Partial<GoogleAnalyticsContext> = {};
      let finished = false;
      const finish = () => {
        if (finished) return;
        finished = true;
        clearTimeout(timer);
        try { resolve(hasPrivacyConsent("analytics") && values.client_id && values.session_id ? values as GoogleAnalyticsContext : undefined); }
        catch { resolve(undefined); }
      };
      const timer = setTimeout(finish, 250);
      for (const field of ["client_id", "session_id"] as const) {
        gtag("get", GOOGLE_ANALYTICS_ID, field, (value: unknown) => {
          const text = String(value ?? "");
          if ((field === "client_id" ? /^[1-9]\d{0,19}\.[1-9]\d{0,19}$/ : /^[1-9]\d{0,19}$/).test(text)) values[field] = text;
          if (values.client_id && values.session_id) finish();
        });
      }
    });
  } catch { return undefined; }
}

type WindowWithGoogleAnalytics = Window & {
  dataLayer?: unknown[];
  gtag?: (...args: unknown[]) => void;
  __walnutGoogleAnalyticsLoaded?: boolean;
};

type GoogleAnalyticsOptions = {
  analyticsGranted?: boolean;
  marketingGranted?: boolean;
  sendInitialPageView?: boolean;
};

type GoogleAnalyticsEventParameters = Record<string, string | number | boolean | null | undefined>;

function isGoogleLinkerParam(key: string): boolean {
  return key === "_gl" || key === "_ga" || key.startsWith("_ga_");
}

function loadScript(id: string, src: string): void {
  if (document.getElementById(id)) return;
  const script = document.createElement("script");
  script.id = id;
  script.async = true;
  script.src = src;
  document.head.appendChild(script);
}

export function ensureGoogleAnalytics(options: GoogleAnalyticsOptions = {}): boolean {
  if (typeof window === "undefined" || !isProductionAnalyticsHost()) return false;
  const analyticsGranted = options.analyticsGranted ?? true;
  const marketingGranted = options.marketingGranted ?? false;
  const sendInitialPageView = false; // PageAnalyticsTracker owns every pageview.
  const win = window as WindowWithGoogleAnalytics;
  if (!win.gtag) {
    win.dataLayer = win.dataLayer || [];
    win.gtag = function gtag() {
      win.dataLayer?.push(arguments);
    };
  }
  if (win.__walnutGoogleAnalyticsLoaded) return true;
  win.__walnutGoogleAnalyticsLoaded = true;
  win.gtag("consent", "default", {
    analytics_storage: analyticsGranted ? "granted" : "denied",
    ad_storage: marketingGranted ? "granted" : "denied",
    ad_user_data: marketingGranted ? "granted" : "denied",
    ad_personalization: marketingGranted ? "granted" : "denied",
    wait_for_update: 500,
  });
  win.gtag("set", "linker", { domains: [], accept_incoming: false });
  loadScript("walnut-google-analytics", `https://www.googletagmanager.com/gtag/js?id=${GOOGLE_ANALYTICS_ID}`);
  win.gtag("js", new Date());
  win.gtag("config", GOOGLE_ANALYTICS_ID, { send_page_view: sendInitialPageView });
  return true;
}

export function removeGoogleLinkerParams(): void {
  if (typeof window === "undefined") return;
  const url = new URL(window.location.href);
  const keys = Array.from(url.searchParams.keys()).filter(isGoogleLinkerParam);
  if (!keys.length) return;
  keys.forEach((key) => url.searchParams.delete(key));
  const nextUrl = `${url.pathname}${url.search}${url.hash}`;
  window.history.replaceState(window.history.state, "", nextUrl);
}

export function updateGoogleAnalyticsConsent(analyticsGranted: boolean, marketingGranted: boolean): void {
  if (!ensureGoogleAnalytics()) return;
  const gtag = (window as WindowWithGoogleAnalytics).gtag;
  gtag?.("consent", "update", {
    analytics_storage: analyticsGranted ? "granted" : "denied",
    ad_storage: marketingGranted ? "granted" : "denied",
    ad_user_data: marketingGranted ? "granted" : "denied",
    ad_personalization: marketingGranted ? "granted" : "denied",
  });
}

export function recordGoogleAnalyticsPageView(path: string, title: string | null): boolean {
  if (!ensureGoogleAnalytics()) return false;
  const gtag = (window as WindowWithGoogleAnalytics).gtag;
  if (!gtag) return false;
  gtag("event", "page_view", {
    page_location: new URL(path, window.location.origin).toString(),
    page_path: path,
    page_title: title || undefined,
  });
  return true;
}

export function recordGoogleAnalyticsEvent(eventName: string, parameters: GoogleAnalyticsEventParameters = {}): boolean {
  if (!ensureGoogleAnalytics()) return false;
  const gtag = (window as WindowWithGoogleAnalytics).gtag;
  if (!gtag || !eventName.trim()) return false;
  const cleanParameters = Object.fromEntries(
    Object.entries(parameters).filter(([, value]) => value !== null && value !== undefined),
  );
  gtag("event", eventName, cleanParameters);
  return true;
}
