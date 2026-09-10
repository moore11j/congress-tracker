import { isProductionAnalyticsHost } from "./analyticsEnvironment";
import { hasPrivacyConsent } from "./privacyConsent";

export type AnalyticsProperties = Record<string, string | number | boolean | null>;
let identity: AnalyticsProperties = { authenticated: null, current_plan: "unknown" };
let memorySession = "";
let lastActivity = 0;
let memoryAcquisition: AnalyticsProperties | null = null;

export function setAnalyticsIdentity(user: { id: number; current_plan?: string | null; entitlement_tier?: string | null; plan?: string | null } | null) {
  identity = { authenticated: Boolean(user), current_plan: user?.entitlement_tier || user?.current_plan || user?.plan || "free" };
}

export function analyticsIdentity() { return { ...identity }; }

export function analyticsConsent(): boolean {
  try { return typeof window !== "undefined" && hasPrivacyConsent("analytics"); } catch { return false; }
}

function cookie(name: string): string | null {
  try {
    const value = document.cookie.split(";").map((part) => part.trim()).find((part) => part.startsWith(`${name}=`));
    return value ? decodeURIComponent(value.slice(name.length + 1)) : null;
  } catch { return null; }
}

function saveCookie(name: string, value: string) {
  if (!isProductionAnalyticsHost() || !analyticsConsent()) return;
  try { document.cookie = `${name}=${encodeURIComponent(value)}; Path=/; Domain=walnutmarkets.com; Max-Age=1800; SameSite=Lax; Secure`; } catch { /* Storage may be disabled. */ }
}

export function analyticsSessionId(): string {
  if (!analyticsConsent()) return "";
  // Shared by the marketing/app hosts, retained through login and OAuth. An
  // inactivity window creates a new session; never use the authentication token.
  const shared = cookie("ct_analytics_sid");
  if (lastActivity && Date.now() - lastActivity > 1800000) { memorySession = ""; memoryAcquisition = null; }
  lastActivity = Date.now();
  if (!memorySession || (shared && shared !== memorySession)) memorySession = shared || window.crypto.randomUUID();
  saveCookie("ct_analytics_sid", memorySession);
  return memorySession;
}

export function safeAnalyticsPath(value: string): string {
  try {
    if (/^[a-z]:[\\/]/i.test(value) || value.startsWith("file:")) return "/";
    const path = new URL(value, "https://walnutmarkets.com").pathname;
    return path.slice(0, 300);
  } catch { return "/"; }
}

// No arbitrary query values, emails, click IDs, form fields or full referrers.
export function sourceLabel(value: string | null): string | null {
  return value && /^[a-zA-Z0-9_. -]{1,100}$/.test(value) ? value : null;
}

export function acquisitionProperties(): AnalyticsProperties {
  if (!analyticsConsent()) return {};
  analyticsSessionId(); // Rotate expired sessions before reading cached acquisition.
  if (memoryAcquisition) { saveCookie("walnut_acquisition", JSON.stringify(memoryAcquisition)); return { ...memoryAcquisition }; }
  try {
    const saved = JSON.parse(cookie("walnut_acquisition") || "null");
    if (saved && typeof saved === "object" && sourceLabel(saved.acquisition_source)) {
      memoryAcquisition = Object.fromEntries(["acquisition_source", "utm_source", "utm_medium", "utm_campaign"].map((key) => [key, typeof saved[key] === "string" ? sourceLabel(saved[key]) : null]));
      return { ...memoryAcquisition };
    }
  } catch { /* Ignore invalid optional storage. */ }
  const params = new URLSearchParams(window.location.search);
  let referrerHost = "";
  try { referrerHost = new URL(document.referrer).hostname.toLowerCase(); } catch { /* Direct entry. */ }
  const utmSource = sourceLabel(params.get("utm_source"));
  const source = utmSource || (/(^|\.)reddit\.com$/.test(referrerHost) ? "reddit" : /(^|\.)google\.[a-z.]+$/.test(referrerHost) ? "google_organic" : /(^|\.)rankpilot\./.test(referrerHost) ? "rankpilot" : referrerHost && !["walnutmarkets.com", "app.walnutmarkets.com", window.location.hostname].includes(referrerHost) ? "referral" : "direct");
  memoryAcquisition = { acquisition_source: source, utm_source: utmSource, utm_medium: sourceLabel(params.get("utm_medium")), utm_campaign: sourceLabel(params.get("utm_campaign")) };
  saveCookie("walnut_acquisition", JSON.stringify(memoryAcquisition));
  return { ...memoryAcquisition };
}
