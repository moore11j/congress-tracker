"use client";

export const GOOGLE_ANALYTICS_ID = "G-QQTFFK7FBH";

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
  if (typeof window === "undefined") return false;
  const analyticsGranted = options.analyticsGranted ?? true;
  const marketingGranted = options.marketingGranted ?? false;
  const sendInitialPageView = options.sendInitialPageView ?? analyticsGranted;
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
