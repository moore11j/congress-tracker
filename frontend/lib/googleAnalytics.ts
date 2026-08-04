"use client";

export const GOOGLE_ANALYTICS_ID = "G-QQTFFK7FBH";

type WindowWithGoogleAnalytics = Window & {
  dataLayer?: unknown[];
  gtag?: (...args: unknown[]) => void;
  __walnutGoogleAnalyticsLoaded?: boolean;
};

function loadScript(id: string, src: string): void {
  if (document.getElementById(id)) return;
  const script = document.createElement("script");
  script.id = id;
  script.async = true;
  script.src = src;
  document.head.appendChild(script);
}

export function ensureGoogleAnalytics(): boolean {
  if (typeof window === "undefined") return false;
  const win = window as WindowWithGoogleAnalytics;
  if (!win.gtag) {
    win.dataLayer = win.dataLayer || [];
    win.gtag = (...args: unknown[]) => {
      win.dataLayer?.push(args);
    };
  }
  if (win.__walnutGoogleAnalyticsLoaded) return true;
  win.__walnutGoogleAnalyticsLoaded = true;
  win.gtag("consent", "default", {
    analytics_storage: "denied",
    ad_storage: "denied",
    ad_user_data: "denied",
    ad_personalization: "denied",
    wait_for_update: 500,
  });
  loadScript("walnut-google-analytics", `https://www.googletagmanager.com/gtag/js?id=${GOOGLE_ANALYTICS_ID}`);
  win.gtag("js", new Date());
  win.gtag("config", GOOGLE_ANALYTICS_ID, { send_page_view: false });
  return true;
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
