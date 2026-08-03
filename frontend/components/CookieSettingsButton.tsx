"use client";

import { openPrivacyConsentSettings } from "@/lib/privacyConsent";

export function CookieSettingsButton() {
  return (
    <button
      type="button"
      className="rounded border border-emerald-300/35 px-3 py-2 text-sm font-semibold text-emerald-100 transition hover:border-emerald-200 hover:bg-emerald-300/10 focus:outline-none focus-visible:ring-2 focus-visible:ring-emerald-300"
      onClick={openPrivacyConsentSettings}
    >
      Cookie settings
    </button>
  );
}
