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
const privacy = read("app/privacy/page.tsx");

test("layout renders consent manager instead of eager tracking scripts", () => {
  assert.match(layout, /import \{ CookieConsentManager \} from "@\/components\/CookieConsentManager";/);
  assert.equal((layout.match(/<CookieConsentManager \/>/g) ?? []).length, 2);
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
  assert.match(manager, /loadGoogleAnalytics/);
  assert.match(manager, /send_page_view: false/);
  assert.match(manager, /loadRedditPixel/);
  assert.match(manager, /consent\?\.analytics \? <SpeedInsights \/> : null/);
  assert.match(manager, /hasGlobalPrivacyControl/);
});

test("first-party analytics does not write session storage before analytics consent", () => {
  assert.match(api, /import \{ hasPrivacyConsent \} from "@\/lib\/privacyConsent";/);
  assert.match(api, /export function recordPageView[\s\S]*if \(!hasPrivacyConsent\("analytics"\)\) return;[\s\S]*window\.sessionStorage\.getItem\(sessionKey\)/);
  assert.match(api, /export function recordProductEvent[\s\S]*if \(!hasPrivacyConsent\("analytics"\)\) return;[\s\S]*const eventName = payload\.event_name\.trim\(\)/);
  assert.match(tracker, /privacyConsentChangedEvent/);
  assert.match(tracker, /recordGoogleAnalyticsPageView/);
  assert.match(tracker, /gtag\("event", "page_view"/);
  assert.match(tracker, /page_location/);
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
