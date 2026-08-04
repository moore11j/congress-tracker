import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const root = process.cwd();
const read = (relativePath) => fs.readFileSync(path.join(root, relativePath), "utf8");

const layout = read("app/layout.tsx");
const manager = read("components/CookieConsentManager.tsx");
const settingsButton = read("components/CookieSettingsButton.tsx");
const consent = read("lib/privacyConsent.ts");
const api = read("lib/api.ts");
const tracker = read("components/PageAnalyticsTracker.tsx");
const googleAnalytics = read("lib/googleAnalytics.ts");
const privacy = read("app/privacy/page.tsx");

test("layout renders consent manager instead of eager tracking scripts", () => {
  assert.match(layout, /import \{ CookieConsentManager \} from "@\/components\/CookieConsentManager";/);
  assert.equal((layout.match(/<CookieConsentManager \/>/g) ?? []).length, 2);
  assert.equal((layout.match(/<PageAnalyticsTracker \/>/g) ?? []).length, 2);
  assert.doesNotMatch(layout, /googletagmanager\.com/);
  assert.doesNotMatch(layout, /redditstatic\.com\/ads\/pixel/);
  assert.doesNotMatch(layout, /dangerouslySetInnerHTML/);
});

test("cookie consent manager offers bottom-bar choices and gates optional scripts", () => {
  assert.match(manager, /fixed inset-x-0 bottom-0/);
  assert.match(manager, /We use essential cookies to keep Walnut secure and working/);
  assert.match(manager, /optional campaign measurement helps us understand what brings people here/);
  assert.match(manager, /Reject optional/);
  assert.match(manager, /Customize/);
  assert.match(manager, /Accept optional/);
  assert.match(manager, /Analytics/);
  assert.match(manager, /Marketing/);
  assert.match(manager, /ensureGoogleAnalytics/);
  assert.match(manager, /updateGoogleAnalyticsConsent/);
  assert.match(manager, /loadRedditPixel/);
  assert.match(manager, /\(consent\?\.analytics \?\? true\) \? <SpeedInsights \/> : null/);
  assert.match(manager, /hasGlobalPrivacyControl/);
});

test("google analytics defaults analytics on while keeping marketing storage denied", () => {
  assert.match(googleAnalytics, /GOOGLE_ANALYTICS_ID = "G-QQTFFK7FBH"/);
  assert.match(googleAnalytics, /gtag\("consent", "default"/);
  assert.match(googleAnalytics, /analytics_storage: "granted"/);
  assert.match(googleAnalytics, /ad_storage: "denied"/);
  assert.match(googleAnalytics, /ad_user_data: "denied"/);
  assert.match(googleAnalytics, /ad_personalization: "denied"/);
  assert.match(googleAnalytics, /send_page_view: false/);
  assert.match(googleAnalytics, /analytics_storage: analyticsGranted \? "granted" : "denied"/);
  assert.match(googleAnalytics, /gtag\("event", "page_view"/);
});

test("analytics runs by default and stops after an explicit opt out", () => {
  assert.match(api, /import \{ hasPrivacyConsent \} from "@\/lib\/privacyConsent";/);
  assert.match(api, /export function recordPageView[\s\S]*if \(!hasPrivacyConsent\("analytics"\)\) return;[\s\S]*window\.sessionStorage\.getItem\(sessionKey\)/);
  assert.match(api, /export function recordProductEvent[\s\S]*if \(!hasPrivacyConsent\("analytics"\)\) return;[\s\S]*const eventName = payload\.event_name\.trim\(\)/);
  assert.match(consent, /if \(!consent\) return category === "analytics";/);
  assert.match(manager, /updateGoogleAnalyticsConsent\(consent\?\.analytics \?\? true, Boolean\(consent\?\.marketing\)\)/);
  assert.match(manager, /\(consent\?\.analytics \?\? true\) && !stored\.analytics/);
  assert.match(tracker, /privacyConsentChangedEvent/);
  assert.match(tracker, /recordGoogleAnalyticsPageView/);
  assert.match(tracker, /hasPrivacyConsent\("analytics"\) && !recordGoogleAnalyticsPageView/);
  assert.match(tracker, /setConsentRefresh\(\(current\) => current \+ 1\)/);
});

test("privacy choices are persisted and can be reopened from the privacy page", () => {
  assert.match(consent, /privacyConsentStorageKey = "walnut:privacy-consent:v1"/);
  assert.match(consent, /privacyConsentCookieName = "walnut_privacy_consent"/);
  assert.match(consent, /privacyConsentOpenEvent = "walnut:privacy-consent:open"/);
  assert.match(consent, /privacyConsentChangedEvent = "walnut:privacy-consent:changed"/);
  assert.match(consent, /globalPrivacyControl/);
  assert.match(settingsButton, /openPrivacyConsentSettings/);
  assert.match(privacy, /<CookieSettingsButton \/>/);
});
