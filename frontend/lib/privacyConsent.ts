export const privacyConsentVersion = 1;
export const privacyConsentStorageKey = "walnut:privacy-consent:v1";
export const privacyConsentCookieName = "walnut_privacy_consent";
export const privacyConsentOpenEvent = "walnut:privacy-consent:open";
export const privacyConsentChangedEvent = "walnut:privacy-consent:changed";

export type PrivacyConsent = {
  version: typeof privacyConsentVersion;
  analytics: boolean;
  marketing: boolean;
  updatedAt: string;
};

type ConsentCategory = "analytics" | "marketing";

const consentMaxAgeSeconds = 60 * 60 * 24 * 180;

function browserGlobalPrivacyControl(): boolean {
  if (typeof navigator === "undefined") return false;
  return Boolean((navigator as Navigator & { globalPrivacyControl?: boolean }).globalPrivacyControl);
}

function consentCookieValue(consent: PrivacyConsent): string {
  return [
    `v${consent.version}`,
    consent.analytics ? "a1" : "a0",
    consent.marketing ? "m1" : "m0",
  ].join(".");
}

function parseConsentCookie(raw: string | undefined): PrivacyConsent | null {
  if (!raw) return null;
  const parts = raw.split(".");
  if (!parts.includes(`v${privacyConsentVersion}`)) return null;
  return {
    version: privacyConsentVersion,
    analytics: parts.includes("a1"),
    marketing: parts.includes("m1"),
    updatedAt: new Date(0).toISOString(),
  };
}

function readConsentCookie(): PrivacyConsent | null {
  if (typeof document === "undefined") return null;
  const match = document.cookie
    .split(";")
    .map((part) => part.trim())
    .find((part) => part.startsWith(`${privacyConsentCookieName}=`));
  if (!match) return null;
  return parseConsentCookie(decodeURIComponent(match.split("=").slice(1).join("=")));
}

function writeConsentCookie(consent: PrivacyConsent): void {
  if (typeof document === "undefined") return;
  document.cookie = [
    `${privacyConsentCookieName}=${encodeURIComponent(consentCookieValue(consent))}`,
    "Path=/",
    `Max-Age=${consentMaxAgeSeconds}`,
    "SameSite=Lax",
  ].join("; ");
}

function parseStoredConsent(raw: string | null): PrivacyConsent | null {
  if (!raw) return null;
  try {
    const parsed = JSON.parse(raw) as Partial<PrivacyConsent>;
    if (parsed.version !== privacyConsentVersion) return null;
    return {
      version: privacyConsentVersion,
      analytics: parsed.analytics === true,
      marketing: parsed.marketing === true,
      updatedAt: typeof parsed.updatedAt === "string" ? parsed.updatedAt : new Date(0).toISOString(),
    };
  } catch {
    return null;
  }
}

export function readPrivacyConsent(): PrivacyConsent | null {
  if (typeof window === "undefined") return null;
  const stored = parseStoredConsent(window.localStorage.getItem(privacyConsentStorageKey));
  return stored ?? readConsentCookie();
}

export function writePrivacyConsent(input: { analytics: boolean; marketing: boolean }): PrivacyConsent {
  const consent: PrivacyConsent = {
    version: privacyConsentVersion,
    analytics: input.analytics,
    marketing: input.marketing && !browserGlobalPrivacyControl(),
    updatedAt: new Date().toISOString(),
  };
  if (typeof window !== "undefined") {
    window.localStorage.setItem(privacyConsentStorageKey, JSON.stringify(consent));
    window.dispatchEvent(new Event(privacyConsentChangedEvent));
  }
  writeConsentCookie(consent);
  return consent;
}

export function hasPrivacyConsent(category: ConsentCategory): boolean {
  const consent = readPrivacyConsent();
  if (!consent) return category === "analytics";
  return consent[category] === true;
}

export function hasGlobalPrivacyControl(): boolean {
  return browserGlobalPrivacyControl();
}

export function openPrivacyConsentSettings(): void {
  if (typeof window === "undefined") return;
  window.dispatchEvent(new Event(privacyConsentOpenEvent));
}
